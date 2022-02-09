import { Logger } from '@aws-lambda-powertools/logger';
import { ComprehendClient, DetectSentimentCommand, DetectEntitiesCommand } from '@aws-sdk/client-comprehend';
import { KinesisClient, PutRecordsCommand } from '@aws-sdk/client-kinesis';
import { TranslateClient, TranslateTextCommand } from '@aws-sdk/client-translate';
import { KinesisStreamHandler, KinesisStreamRecord } from 'aws-lambda';
import { Promise } from 'bluebird';
import { TweetStreamParse, StreamResult, Deduplicate, Normalize } from '../utils';

const entityScoreThreshold = 0.8;
const region = process.env.AWS_REGION || 'us-west-2';
const indexingStreamName = process.env.INDEXING_STREAM_NAME!;

const logger = new Logger({ logLevel: 'INFO', serviceName: 'analysis' });
const translate = new TranslateClient({ region });
const comprehend = new ComprehendClient({ region });
const kinesis = new KinesisClient({ region });

const getFullText = (stream: StreamResult): string => {
  const tweet = stream.data;
  let fullText: string = tweet.text;
  if (tweet.referenced_tweets?.[0].type == 'retweeted' && stream.includes?.tweets?.[0].text) {
    fullText = stream.includes?.tweets?.[0].text;
  };
  return fullText;
};

const analyze = async (text: string, lang: string) => {
  if (lang !in ['en', 'es', 'fr', 'de', 'it', 'pt', 'ar', 'hi', 'ja', 'ko', 'zh']) {
    const translateTextCommand = new TranslateTextCommand({ Text: text, SourceLanguageCode: lang, TargetLanguageCode: 'en' });
    const res = await translate.send(translateTextCommand);
    text = res.TranslatedText!;
    lang = 'en';
  };
  // Sentiment
  const detectSentimentCommand = new DetectSentimentCommand({ Text: text, LanguageCode: lang });
  const detectSentimentResponse = await comprehend.send(detectSentimentCommand);
  // Entities
  const detectEntitiesCommand = new DetectEntitiesCommand({ Text: text, LanguageCode: lang });
  const detectEntitiesResponse = await comprehend.send(detectEntitiesCommand);
  const filteredEntities = detectEntitiesResponse.Entities?.filter(entity => {
    let flag = true;
    if (entity.Type == 'QUANTITY' || entity.Type == 'DATE') {
      flag = false;
    } else if (entity.Text!.length < 2 || entity.Text!.startsWith('@') ) {
      flag = false;
    } else if (entity.Score! < entityScoreThreshold ) {
      flag = false;
    }
    return flag;
  });
  const entities = Deduplicate(filteredEntities?.map(entity => { return entity.Text!; }));

  const insights = {
    sentiment: detectSentimentResponse.Sentiment!,
    sentiment_score: {
      positive: detectSentimentResponse.SentimentScore?.Positive!,
      negative: detectSentimentResponse.SentimentScore?.Negative!,
      neutral: detectSentimentResponse.SentimentScore?.Neutral!,
      mixed: detectSentimentResponse.SentimentScore?.Mixed!,
    },
    entities: entities,
  };
  return insights;
};

const processRecord = async (record: KinesisStreamRecord) => {
  const payload = TweetStreamParse(record.kinesis.data);
  const tweet = payload.data;
  const lang = tweet.lang || 'ja';
  const fullText = getFullText(payload);
  const normalizedText = Normalize(fullText);
  const insights = await analyze(normalizedText, lang);
  const newData = {
    ...payload,
    analysis: {
      normalized_text: normalizedText,
      ...insights,
    },
  };
  const processedRecord = {
    PartitionKey: record.kinesis.partitionKey,
    Data: Buffer.from(JSON.stringify(newData)),
  };
  return processedRecord;
};

export const handler: KinesisStreamHandler = async (event) => {
  const processedRecords = await Promise.map(event.Records, processRecord, { concurrency: 10 });
  const putRecordsCommand = new PutRecordsCommand({
    StreamName: indexingStreamName,
    Records: processedRecords,
  });
  const putRecords = await kinesis.send(putRecordsCommand);
  logger.info(`failedRecordCount: ${putRecords.FailedRecordCount}`);
};