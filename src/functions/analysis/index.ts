import { Logger } from '@aws-lambda-powertools/logger';
import { ComprehendClient, DetectSentimentCommand, DetectEntitiesCommand } from '@aws-sdk/client-comprehend';
import { KinesisClient, PutRecordsCommand } from '@aws-sdk/client-kinesis';
import { TranslateClient, TranslateTextCommand } from '@aws-sdk/client-translate';
import { KinesisStreamHandler, KinesisStreamRecord } from 'aws-lambda';
import { Promise } from 'bluebird';

const indexingStreamName = process.env.INDEXING_STREAM_NAME!;

const logger = new Logger({ logLevel: 'INFO', serviceName: 'analysis' });
const translate = new TranslateClient({});
const comprehend = new ComprehendClient({});
const kinesis = new KinesisClient({});

const normalize = (text: string): string => {
  const textWithoutRT = text.replace(/^RT /, '');
  const tectWithoutEmoji = textWithoutRT.replace(/[\u2700-\u27BF]|[\uE000-\uF8FF]|\uD83C[\uDC00-\uDFFF]|\uD83D[\uDC00-\uDFFF]|[\u2011-\u26FF]|\uD83E[\uDD10-\uDDFF]/g, '');
  const textWithoutUrl = tectWithoutEmoji.replace(/https?:\/\/[\w/;:%#\$&\?\(\)~\.=\+\-]+/g, '');
  const textWithoutHtml = textWithoutUrl.replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
  const textWithoutSpace = textWithoutHtml.replace(/\n/g, ' ').replace(/[\s]+/g, ' ').trimEnd();
  return textWithoutSpace;
};

const getFullText = (tweet: any): string => {
  let fullText: string = tweet.text;
  if (tweet.retweeted_status?.extended_tweet?.full_text) {
    fullText = tweet.retweeted_status.extended_tweet.full_text;
  } else if (tweet.retweeted_status?.text) {
    fullText = tweet.retweeted_status?.text;
  } else if (tweet.extended_tweet?.full_text) {
    fullText = tweet.extended_tweet.full_text;
  };
  return fullText;
};

interface Entity {
  score: number;
  type: string;
  text: string;
}

interface Insights {
  sentiment: string;
  sentiment_score: {
    positive: number;
    negative: number;
    neutral: number;
    mixed: number;
  };
  entities: Entity[];
}

interface Payload {
  tweet: any;
  ecs_cluster?: string;
  ecs_task_arn?: string;
  ecs_task_definition?: string;
  tag?: string;
  analysis?: {
    normalized_text: string;
    comprehend: Insights;
  };
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
  const entities: Entity[] = detectEntitiesResponse.Entities?.map(entity => {
    return { score: entity.Score!, type: entity.Type!, text: entity.Text! };
  }) || [];

  const insights: Insights = {
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
  const payload: Payload = JSON.parse(Buffer.from(record.kinesis.data, 'base64').toString('utf8'));
  const tweet = payload.tweet;
  const lang: string = tweet.lang;
  const fullText = getFullText(tweet);
  const normalizedText = normalize(fullText);
  const insights = await analyze(normalizedText, lang);
  const data = {
    ...payload,
    analysis: {
      normalized_text: normalizedText,
      comprehend: insights,
    },
  };
  const processedRecord = {
    PartitionKey: record.kinesis.partitionKey,
    Data: Buffer.from(JSON.stringify(data)),
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