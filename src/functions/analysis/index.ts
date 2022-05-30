import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { Entity } from '@aws-sdk/client-comprehend';
import { KinesisClient, PutRecordsCommand, PutRecordsRequestEntry } from '@aws-sdk/client-kinesis';
import { SFNClient, StartSyncExecutionCommand } from '@aws-sdk/client-sfn';
import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import { KinesisStreamHandler, KinesisStreamRecord } from 'aws-lambda';
import { Promise } from 'bluebird';
import { TweetV2 } from 'twitter-api-v2';
import { TweetStreamParse, TweetStreamRecord, Deduplicate, Analysis, ComprehendJobOutput } from '../utils';

const entityScoreThreshold = 0.8;
let twitterFilterContextDomains: string[];
let twitterFilterSourceLabels: string[];

const region = process.env.AWS_REGION || 'us-west-2';
const twitterFilterContextDomainsParameterName = process.env.TWITTER_FILTER_CONTEXT_DOMAINS_PARAMETER_NAME!;
const twitterFilterSourceLabelsParameterName = process.env.TWITTER_FILTER_SOURCE_LABELS_PARAMETER_NAME!;
const destStreamName = process.env.DEST_STREAM_NAME!;
const comprehendJobArn = process.env.COMPREHEND_JOB_ARN!;

const logger = new Logger();
const metrics = new Metrics();
const tracer = new Tracer();

const ssm = tracer.captureAWSv3Client(new SSMClient({ region }));
const kinesis = tracer.captureAWSv3Client(new KinesisClient({ region }));
const sfn = tracer.captureAWSv3Client(new SFNClient({ region }));

const getParameter = async(name: string): Promise<string[]> => {
  const cmd = new GetParameterCommand({ Name: name });
  const { Parameter } = await ssm.send(cmd);
  return Parameter?.Value?.split(',') || [];
};

const comprehend = async (text: string, lang?: string): Promise<ComprehendJobOutput> => {
  const cmd = new StartSyncExecutionCommand({
    stateMachineArn: comprehendJobArn,
    input: JSON.stringify({
      Text: text,
      LanguageCode: lang,
    }),
  });
  const { output } = await sfn.send(cmd);
  const result: ComprehendJobOutput = (typeof output == 'string')
    ? JSON.parse(output)
    : {};
  return result;
};

const entitiesToString = (entities: Entity[]): string[] => {
  const filteredEntities = entities.filter(entity => {
    if (entity.Type == 'QUANTITY' || entity.Type == 'DATE') {
      return false;
    } else if (entity.Text!.length < 2 || entity.Text!.startsWith('@') ) {
      return false;
    } else if (entity.Score! < entityScoreThreshold ) {
      return false;
    } else {
      return true;
    }
  });
  const result: string[] = Deduplicate(filteredEntities.map(entity => entity.Text!.toLowerCase()));
  return result;
};

//const keyPhrasesToString = (keyPhrases: KeyPhrase[]): string[] => {
//  const filteredKeyPhrases = keyPhrases.filter(phrase => {
//    if (phrase.Text!.length < 2 || phrase.Text!.startsWith('@') ) {
//      return false;
//    } else if (phrase.Score! < keyPhraseScoreThreshold ) {
//      return false;
//    } else {
//      return true;
//    }
//  });
//  const result: string[] = Deduplicate(filteredKeyPhrases.map(phrase => phrase.Text!.toLowerCase()));
//  return result;
//};

const analyzeText = async (text: string, lang?: string): Promise<Analysis> => {
  const { NormalizedText, Sentiment, SentimentScore, Entities } = await comprehend(text, lang);
  const entities = (typeof Entities == 'undefined') ? undefined : entitiesToString(Entities);
  //const keyPhrases = (typeof KeyPhrases == 'undefined') ? undefined : keyPhrasesToString(KeyPhrases);
  const data: Analysis = {
    normalized_text: NormalizedText,
    sentiment: Sentiment,
    sentiment_score: {
      positive: SentimentScore?.Positive,
      negative: SentimentScore?.Negative,
      neutral: SentimentScore?.Neutral,
      mixed: SentimentScore?.Mixed,
    },
    entities: entities,
    //key_phrases: keyPhrases,
  };
  return data;
};

const analyzeRecord = async (record: TweetStreamRecord): Promise<TweetStreamRecord> => {
  const tweet = record.data;
  if (typeof record.analysis == 'undefined') {
    record.analysis = await analyzeText(tweet.text, tweet.lang);
  };
  return record;
};

const analyzeRecords = async(records: TweetStreamRecord[]): Promise<TweetStreamRecord[]> => {
  const analyzedRecords = await Promise.map(records, analyzeRecord, { concurrency: 10 });
  return analyzedRecords;
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

const transformRecords = (kinesisStreamRecords: KinesisStreamRecord[]): TweetStreamRecord[] => {
  const records = kinesisStreamRecords.map(record => TweetStreamParse(record.kinesis.data));
  const filteredRecords = records.filter(record => sourceLabelFilter(record.data)).filter(record => contextDomainFilter(record.data));
  metrics.addMetric('FilteredRate', MetricUnits.Percent, (records.length - filteredRecords.length) / records.length * 100);
  return filteredRecords;
};

const putRecordsKinesis = async(records: TweetStreamRecord[]) => {
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

export const handler: KinesisStreamHandler = async(event, _context) => {
  // Load parameters
  twitterFilterContextDomains = await getParameter(twitterFilterContextDomainsParameterName);
  twitterFilterSourceLabels = await getParameter(twitterFilterSourceLabelsParameterName);

  const kinesisStreamRecords = event.Records;
  metrics.addMetric('IncomingRecordCount', MetricUnits.Count, kinesisStreamRecords.length);

  const streamRecords = transformRecords(kinesisStreamRecords);
  const analyzedRecords = await analyzeRecords(streamRecords);
  const { requestRecordCount, failedRecordCount } = await putRecordsKinesis(analyzedRecords);
  metrics.addMetric('OutgoingRecordCount', MetricUnits.Count, requestRecordCount-failedRecordCount);
  return;
};
