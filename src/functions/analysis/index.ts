import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { ComprehendClient, DetectSentimentCommand, DetectEntitiesCommand, DetectDominantLanguageCommand } from '@aws-sdk/client-comprehend';
import { KinesisClient, PutRecordsCommand } from '@aws-sdk/client-kinesis';
import { SSMClient, GetParametersByPathCommand } from '@aws-sdk/client-ssm';
import { TranslateClient, TranslateTextCommand } from '@aws-sdk/client-translate';
import { KinesisStreamHandler, KinesisStreamRecord } from 'aws-lambda';
import { Promise } from 'bluebird';
import { TweetV2 } from 'twitter-api-v2';
import { TweetStreamParse, StreamResult, Deduplicate, Normalize, Analysis } from '../utils';

const entityScoreThreshold = 0.8;
let twitterFilterContextDomains: string[] = [];
let twitterFilterSourceLabels: string[] = [];

const region = process.env.AWS_REGION || 'us-west-2';
const indexingStreamName = process.env.INDEXING_STREAM_NAME!;
const twitterParameterPrefix = process.env.TWITTER_PARAMETER_PREFIX!;

const logger = new Logger();
const metrics = new Metrics();
const tracer = new Tracer();

const kinesis = tracer.captureAWSv3Client(new KinesisClient({ region }));
const comprehend = tracer.captureAWSv3Client(new ComprehendClient({ region, maxAttempts: 20 }));

const fetchParameterStore = async () => {
  const parentSubsegment = tracer.getSegment();
  const subsegment = parentSubsegment.addNewSubsegment('Fetch parameters');
  tracer.setSegment(subsegment);
  try {
    const ssm = tracer.captureAWSv3Client(new SSMClient({ region }));
    const cmd = new GetParametersByPathCommand({ Path: twitterParameterPrefix, Recursive: true });
    const { Parameters } = await ssm.send(cmd);
    // Updates
    twitterFilterContextDomains = Parameters!.find(param => param.Name?.endsWith('Filter/ContextDomains'))!.Value!.split(',');
    twitterFilterSourceLabels = Parameters!.find(param => param.Name?.endsWith('Filter/SourceLabels'))!.Value!.split(',');
    logger.info({ message: 'Get palameters from parameter store successfully' });
  } catch (err) {
    tracer.addErrorAsMetadata(err as Error);
  } finally {
    subsegment.close();
    tracer.setSegment(parentSubsegment);
  };
};

const sourceLabelFilter = (tweet: TweetV2): boolean => {
  const sourceLabel = tweet.source || '';
  const isFiltered = twitterFilterSourceLabels.includes(sourceLabel);
  return (isFiltered) ? false: true;
};

const contextDomainFilter = (tweet: TweetV2): boolean => {
  const contextAnnotationsDomains = tweet.context_annotations?.map(a => a.domain.name) || [];
  const isFiltered = contextAnnotationsDomains.some(domain => twitterFilterContextDomains.includes(domain));
  return (isFiltered) ? false: true;
};

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
  return { Sentiment, SentimentScore };
};

const detectEntities = async (text: string, lang: string) => {
  const cmd = new DetectEntitiesCommand({ Text: text, LanguageCode: lang });
  const { Entities, $metadata } = await comprehend.send(cmd);
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
  const entities = Deduplicate(filteredEntities?.map(entity => { return entity.Text!.toLowerCase(); }));

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

const processRecord = async (stream: StreamResult) => {
  const tweet = stream.data;
  const fullText = getFullText(stream);
  const normalizedText = Normalize(fullText);
  const lang = tweet.lang || await detectLanguage(normalizedText);
  if (!stream.analysis) {
    stream.analysis = await analyze(normalizedText, lang);
  }
  const processedRecord = {
    PartitionKey: tweet.id,
    Data: Buffer.from(JSON.stringify(stream)),
  };
  return processedRecord;
};

export const handler: KinesisStreamHandler = async (event) => {
  const segment = tracer.getSegment();
  metrics.addMetric('IncomingRecordCount', MetricUnits.Count, event.Records.length);
  await fetchParameterStore();
  // Analysis & Transform
  const subsegment = segment.addNewSubsegment('Process records');
  tracer.setSegment(subsegment);

  const streamArray = event.Records.map(record => TweetStreamParse(record.kinesis.data));
  const filteredStreamArray = streamArray.filter(stream => sourceLabelFilter(stream.data)).filter(stream => contextDomainFilter(stream.data));
  metrics.addMetric('FilteredTweetsRate', MetricUnits.Percent, (streamArray.length - filteredStreamArray.length) / streamArray.length * 100);

  const processedRecords = await Promise.map(filteredStreamArray, processRecord, { concurrency: 10 });
  subsegment.close();
  tracer.setSegment(segment);
  if (processedRecords.length > 0) {
    const putRecordsCommand = new PutRecordsCommand({
      StreamName: indexingStreamName,
      Records: processedRecords,
    });
    const putRecords = await kinesis.send(putRecordsCommand);
    //metrics.addMetric('FailedRecordCount', MetricUnits.Count, putRecords.FailedRecordCount || 0);
    metrics.addMetric('OutgoingRecordCount', MetricUnits.Count, processedRecords.length);
  }
  metrics.publishStoredMetrics();
  return;
};