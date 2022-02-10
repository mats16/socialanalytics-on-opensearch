import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { ComprehendClient, DetectSentimentCommand, DetectEntitiesCommand, DetectDominantLanguageCommand } from '@aws-sdk/client-comprehend';
import { KinesisClient, PutRecordsCommand } from '@aws-sdk/client-kinesis';
import { TranslateClient, TranslateTextCommand } from '@aws-sdk/client-translate';
import { KinesisStreamHandler, KinesisStreamRecord } from 'aws-lambda';
import { Promise } from 'bluebird';
import { TweetStreamParse, StreamResult, Deduplicate, Normalize, Analysis } from '../utils';

const entityScoreThreshold = 0.8;
const region = process.env.AWS_REGION || 'us-west-2';
const indexingStreamName = process.env.INDEXING_STREAM_NAME!;

const logger = new Logger({ logLevel: 'INFO', serviceName: 'AnalysisFunction' });
const metrics = new Metrics({ namespace: 'SocialAnalytics', serviceName: 'AnalysisFunction' });
const tracer = new Tracer({ serviceName: 'AnalysisFunction' });

const kinesis = new KinesisClient({ region });
const comprehend = new ComprehendClient({ region, maxAttempts: 20 });

const detectLanguage = async (text: string) => {
  const cmd = new DetectDominantLanguageCommand({ Text: text });
  const { Languages, $metadata } = await comprehend.send(cmd);
  const scores = Languages?.map(x => x.Score||0) || [];
  const maxScore = Math.max(...scores);
  const maxScoreLang = Languages?.find(x => x.Score == maxScore);
  return maxScoreLang?.LanguageCode;
};

const detectSentiment = async (text: string, lang: string) => {
  const cmd = new DetectSentimentCommand({ Text: text, LanguageCode: lang });
  const { Sentiment, SentimentScore, $metadata } = await comprehend.send(cmd);
  metrics.addDimension('API', 'DetectSentiment');
  metrics.addMetric('TotalRetryDelay', MetricUnits.Count, $metadata.totalRetryDelay || 0);
  metrics.addMetric('Attempts', MetricUnits.Count, $metadata.attempts || 0);
  metrics.clearDimensions();
  return { Sentiment, SentimentScore };
};

const detectEntities = async (text: string, lang: string) => {
  const cmd = new DetectEntitiesCommand({ Text: text, LanguageCode: lang });
  const { Entities, $metadata } = await comprehend.send(cmd);
  metrics.addDimension('API', 'DetectEntities');
  metrics.addMetric('TotalRetryDelay', MetricUnits.Count, $metadata.totalRetryDelay || 0);
  metrics.addMetric('Attempts', MetricUnits.Count, $metadata.attempts || 0);
  metrics.clearDimensions();
  return { Entities };
};

const translate = async (text: string, sourceLanguageCode: string) => {
  const client = new TranslateClient({ region });
  const cmd = new TranslateTextCommand({ Text: text, SourceLanguageCode: sourceLanguageCode, TargetLanguageCode: 'en' });
  const { TranslatedText } = await client.send(cmd);
  return TranslatedText!;
};

const getFullText = (stream: StreamResult): string => {
  const tweet = stream.data;
  let fullText: string = tweet.text;
  if (tweet.referenced_tweets?.[0].type == 'retweeted' && stream.includes?.tweets?.[0].text) {
    fullText = stream.includes?.tweets?.[0].text;
  };
  return fullText;
};

const analyze = async (text: string, lang: string|undefined): Promise<Analysis> => {
  const normalizedText = Normalize(text);
  if (normalizedText == '') { return {}; };
  let languageCode = lang;
  if (!lang) {
    languageCode = await detectLanguage(text);
  } else if (lang !in ['en', 'es', 'fr', 'de', 'it', 'pt', 'ar', 'hi', 'ja', 'ko', 'zh']) {
    text = await translate(text, lang);
    languageCode = 'en';
  };
  if (!languageCode) { return {}; };
  // Sentiment
  const { Sentiment, SentimentScore } = await detectSentiment(text, languageCode);
  // Entities
  const { Entities } = await detectEntities(text, languageCode);
  const filteredEntities = Entities?.filter(entity => {
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

  const data: Analysis = {
    normalized_text: normalizedText,
    sentiment: Sentiment,
    sentiment_score: {
      positive: SentimentScore?.Positive,
      negative: SentimentScore?.Negative,
      neutral: SentimentScore?.Neutral,
      mixed: SentimentScore?.Mixed,
    },
    entities: entities,
  };
  return data;
};

const processRecord = async (record: KinesisStreamRecord) => {
  const stream = TweetStreamParse(record.kinesis.data);
  const tweet = stream.data;
  const fullText = getFullText(stream);
  const normalizedText = Normalize(fullText);
  const lang = tweet.lang || await detectLanguage(normalizedText);
  const analyzedData = await analyze(normalizedText, lang);
  const analyzedStreamResult: StreamResult = {
    ...stream,
    analysis: analyzedData,
  };
  const processedRecord = {
    PartitionKey: record.kinesis.partitionKey,
    Data: Buffer.from(JSON.stringify(analyzedStreamResult)),
  };
  return processedRecord;
};

export const handler: KinesisStreamHandler = async (event) => {
  metrics.addMetric('IncomingRecordCount', MetricUnits.Count, event.Records.length);
  const processedRecords = await Promise.map(event.Records, processRecord, { concurrency: 10 });
  if (processedRecords.length > 0) {
    const putRecordsCommand = new PutRecordsCommand({
      StreamName: indexingStreamName,
      Records: processedRecords,
    });
    const putRecords = await kinesis.send(putRecordsCommand);
    metrics.addMetric('FailedRecordCount', MetricUnits.Count, putRecords.FailedRecordCount || 0);
    metrics.addMetric('OutgoingRecordCount', MetricUnits.Count, processedRecords.length);
  }
  metrics.publishStoredMetrics();
  return;
};