import { LambdaInterface } from '@aws-lambda-powertools/commons';
import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { ComprehendClient, DetectSentimentCommand, DetectEntitiesCommand, DetectDominantLanguageCommand } from '@aws-sdk/client-comprehend';
import { KinesisClient, PutRecordsCommand, PutRecordsRequestEntry } from '@aws-sdk/client-kinesis';
import { SSMClient, GetParametersByPathCommand } from '@aws-sdk/client-ssm';
import { TranslateClient, TranslateTextCommand } from '@aws-sdk/client-translate';
import { KinesisStreamHandler, KinesisStreamRecord, KinesisStreamEvent, Context } from 'aws-lambda';
import { Promise, resolve } from 'bluebird';
import { TweetV2 } from 'twitter-api-v2';
import { TweetStreamParse, TweetStreamRecord, Deduplicate, Normalize, Analysis } from '../utils';

const entityScoreThreshold = 0.8;
let twitterFilterSourceLabels: string[];
let twitterFilterContextDomains: string[];

const region = process.env.AWS_REGION || 'us-west-2';
const destStreamName = process.env.DEST_STREAM_NAME!;
const twitterParameterPrefix = process.env.TWITTER_PARAMETER_PREFIX!;

const logger = new Logger();
const metrics = new Metrics();
const tracer = new Tracer();

const ssm = tracer.captureAWSv3Client(new SSMClient({ region }));
const kinesis = tracer.captureAWSv3Client(new KinesisClient({ region }));
const comprehend = tracer.captureAWSv3Client(new ComprehendClient({ region, maxAttempts: 10 }));

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

const getOriginText = (record: TweetStreamRecord): string => {
  const tweet = record.data;
  const index = tweet.referenced_tweets?.findIndex(x => x.type == 'retweeted');
  if (typeof index != 'undefined') {
    const retweetId = tweet.referenced_tweets?.[index]?.id;
    const retweet = record.includes?.tweets?.find(includedTweet => includedTweet.id == retweetId);
    return retweet?.text || tweet.text;
  } else {
    return tweet.text;
  };
};

const analyzeText = async (text: string, lang: string|undefined): Promise<Analysis> => {
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
  const entities: string[] = Deduplicate(filteredEntities?.map(entity => { return entity.Text!.toLowerCase(); }) || []);

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

const analyzeRecord = async (record: TweetStreamRecord): Promise<TweetStreamRecord> => {
  const tweet = record.data;
  const originText = getOriginText(record);
  const normalizedText = Normalize(originText);
  const lang = tweet.lang || await detectLanguage(normalizedText);
  if (typeof record.analysis == 'undefined') {
    record.analysis = await analyzeText(normalizedText, lang);
  };
  return record;
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

class Lambda implements LambdaInterface {

  private transformRecords(kinesisStreamRecords: KinesisStreamRecord[]): TweetStreamRecord[] {
    const records = kinesisStreamRecords.map(record => TweetStreamParse(record.kinesis.data));
    const filteredRecords = records.filter(record => sourceLabelFilter(record.data)).filter(record => contextDomainFilter(record.data));
    metrics.addMetric('FilteredRate', MetricUnits.Percent, (records.length - filteredRecords.length) / records.length * 100);
    return filteredRecords;
  };

  @tracer.captureMethod()
  private async fetchParameter(): Promise<void> {
    const cmd = new GetParametersByPathCommand({ Path: twitterParameterPrefix, Recursive: true });
    const { Parameters } = await ssm.send(cmd);
    twitterFilterContextDomains = Parameters!.find(param => param.Name?.endsWith('Filter/ContextDomains'))!.Value!.split(',');
    twitterFilterSourceLabels = Parameters!.find(param => param.Name?.endsWith('Filter/SourceLabels'))!.Value!.split(',');
    logger.info({ message: 'Get palameters from parameter store successfully' });
    return;
  };

  @tracer.captureMethod()
  private async analyzeRecords(records: TweetStreamRecord[]): Promise<TweetStreamRecord[]> {
    const analyzedRecords = await Promise.map(records, analyzeRecord, { concurrency: 10 });
    return analyzedRecords;
  };

  @tracer.captureMethod()
  private async putRecordsKinesis(records: TweetStreamRecord[]) {
    const entries = records.map(record => {
      const entry: PutRecordsRequestEntry = {
        PartitionKey: record.data.id,
        Data: Buffer.from(JSON.stringify(record)),
      };
      return entry;
    });
    const result = { requestRecordCount: 0, failedRecordCount: 0 };
    if (entries.length > 0) {
      const cmd = new PutRecordsCommand({
        StreamName: destStreamName,
        Records: entries,
      });
      const { Records, FailedRecordCount, $metadata } = await kinesis.send(cmd);
      result.requestRecordCount = Records?.length || 0;
      result.failedRecordCount = FailedRecordCount || 0;
    };
    return result;
  };

  @metrics.logMetrics()
  @tracer.captureLambdaHandler()
  public async handler(event: KinesisStreamEvent, _context: Context): Promise<void> {
    await this.fetchParameter();
    const kinesisStreamRecords = event.Records;
    metrics.addMetric('IncomingRecordCount', MetricUnits.Count, kinesisStreamRecords.length);
    const streamRecords = this.transformRecords(kinesisStreamRecords);
    const analyzedRecords = await this.analyzeRecords(streamRecords);
    const { requestRecordCount, failedRecordCount } = await this.putRecordsKinesis(analyzedRecords);
    metrics.addMetric('OutgoingRecordCount', MetricUnits.Count, requestRecordCount-failedRecordCount);
    return;
  };

};

export const myFunction = new Lambda();
export const handler: KinesisStreamHandler = myFunction.handler;
