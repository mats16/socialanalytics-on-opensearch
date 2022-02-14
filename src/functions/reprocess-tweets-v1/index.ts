import { Readable } from 'stream';
import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { KinesisClient, PutRecordsCommand, PutRecordsRequestEntry } from '@aws-sdk/client-kinesis';
import { S3Client, GetObjectCommand, GetObjectCommandOutput, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { SSMClient, GetParametersByPathCommand } from '@aws-sdk/client-ssm';
import { SQSHandler, SQSRecord, S3Event } from 'aws-lambda';
import { Promise } from 'bluebird';
import { TweetV1, TweetV2, TwitterApi, TweetV2LookupResult, Tweetv2FieldsParams } from 'twitter-api-v2';
import { StreamResult, Deduplicate } from '../utils';

const twitterApiLookupInterval = 2180; // Quota: 300req/900s
let twitterBearerToken: string = '';
let twitterFieldsParams: Partial<Tweetv2FieldsParams> = {};
let twitterFilterContextDomains: string[] = [];
let twitterFilterSourceLabels: string[] = [];

const region = process.env.AWS_REGION || 'us-west-2';
const streamName = process.env.STREAM_NAME!;
const twitterParameterPrefix = process.env.TWITTER_PARAMETER_PREFIX!;

const logger = new Logger();
const metrics = new Metrics();
const twitterApiMetrics = new Metrics({ serviceName: 'twitter-api-v2' });
const tracer = new Tracer();

const s3 = tracer.captureAWSv3Client(new S3Client({ region }));
const kinesis = tracer.captureAWSv3Client(new KinesisClient({ region, maxAttempts: 10 }));

const fetchParameterStore = async () => {
  const parentSubsegment = tracer.getSegment();
  const subsegment = parentSubsegment.addNewSubsegment('Fetch parameters');
  tracer.setSegment(subsegment);
  try {
    const ssm = tracer.captureAWSv3Client(new SSMClient({ region }));
    const cmd = new GetParametersByPathCommand({ Path: twitterParameterPrefix, Recursive: true });
    const { Parameters } = await ssm.send(cmd);
    // Updates
    twitterBearerToken = Parameters!.find(param => param.Name!.endsWith('BearerToken'))!.Value!;
    twitterFieldsParams = JSON.parse(Parameters!.find(param => param.Name?.endsWith('FieldsParams'))!.Value!);
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

const asBuffer = async (response: GetObjectCommandOutput) => {
  const stream = response.Body as Readable;
  const chunks: Buffer[] = [];
  return new Promise<Buffer>((resolve, reject) => {
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('error', (err) => reject(err));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
  });
};
const asString = async (response: GetObjectCommandOutput) => {
  const buffer = await asBuffer(response);
  return buffer.toString();
};

const tweetV1stringToId = (tweetV1string: string) => {
  const tweetV1: TweetV1 = JSON.parse(tweetV1string);
  return tweetV1.id_str;
};

const getObjectLines = async(bucket: string, key: string): Promise<string[]> => {
  const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
  try {
    const output = await s3.send(cmd);
    const strBody = await asString(output);
    const lines = strBody.trimEnd().split('\n');
    return lines;
  } catch {
    logger.warn({ message: 'NoSuchKey', key });
    return [];
  }
};

const deleteObject = async(bucket: string, key: string)=> {
  const cmd = new DeleteObjectCommand({ Bucket: bucket, Key: key });
  try {
    await s3.send(cmd);
  } catch (err) {
    console.log(err);
  }
  return;
};

const deleteAllObjects = async (sqsRecords: SQSRecord[]) => {
  await Promise.map(sqsRecords, async (sqsRecord) => {
    const { Records: s3Records }: S3Event = JSON.parse(sqsRecord.body);
    await Promise.map(s3Records, async (s3Record) => {
      const bucket = s3Record.s3.bucket.name;
      const key = s3Record.s3.object.key.replace(/%3D/g, '=');
      await deleteObject(bucket, key);
    });
  });
  return;
};

const arraySplit = <T = object>(array: T[], n: number): T[][] => {
  return array.reduce((acc: T[][], _c, i: number) => (i % n ? acc : [...acc, ...[array.slice(i, i + n)]]), []);
};

const lookupTweets = async (tweetIds: string[]): Promise<TweetV2LookupResult> => {
  const parentSubsegment = tracer.getSegment();
  const subsegment = parentSubsegment.addNewSubsegment('Lookup tweets');
  tracer.setSegment(subsegment);
  twitterApiMetrics.addDimension('api', 'lookup');
  let lookupResult: TweetV2LookupResult;
  try {
    // Call twitter api v2
    const twitterApi = new TwitterApi(twitterBearerToken);
    lookupResult = await twitterApi.v2.tweets(tweetIds, twitterFieldsParams);
    twitterApiMetrics.addMetric('RequestCount', MetricUnits.Count, 1);
    twitterApiMetrics.addMetric('ResponseErrorRecordCount', MetricUnits.Count, lookupResult.errors?.length||0);
  } catch (err) {
    tracer.addErrorAsMetadata(err as Error);
    throw err;
  } finally {
    subsegment.close();
    tracer.setSegment(parentSubsegment);
  }
  return lookupResult;
};

const lookupResultToStreamResults = (lookupResult: TweetV2LookupResult): StreamResult[] => {
  const includesUsers = lookupResult.includes?.users || [];
  const includesTweets = lookupResult.includes?.tweets || [];
  const tweets = lookupResult.data || [];
  const streamResults = tweets.map(tweet => {
    const referencedTweetIds = tweet.referenced_tweets?.map(referencedTweet => referencedTweet.id) || [];
    const referencedTweets = includesTweets.filter(includesTweet => referencedTweetIds?.includes(includesTweet.id)); // includes からreferenced_tweetに該当するものを探す
    const author = includesUsers.filter(userV2 => userV2.id == tweet.author_id);
    const stream: StreamResult = {
      data: tweet,
      includes: {
        users: author,
        tweets: referencedTweets,
      },
      backup: true,
    };
    return stream;
  });
  return streamResults;
};

const streamResultsToKinesisRecord = (streamResults: StreamResult[]): PutRecordsRequestEntry[] => {
  const records = streamResults.map(result => {
    const record: PutRecordsRequestEntry = {
      PartitionKey: result.data.id,
      Data: Buffer.from(JSON.stringify(result)),
    };
    return record;
  });
  return records;
};

const sleep = async (ms: number) => {
  const parentSubsegment = tracer.getSegment();
  const subsegment = parentSubsegment.addNewSubsegment('Sleep');
  tracer.setSegment(subsegment);
  await new Promise(resolve => setTimeout(resolve, ms));
  subsegment.close();
  tracer.setSegment(parentSubsegment);
  return;
};

const processTweetIds = async (tweetIds: string[]) => {
  const parentSubsegment = tracer.getSegment();
  const subsegment = parentSubsegment.addNewSubsegment('Process 100 tweets');
  tracer.setSegment(subsegment);
  // Get data from twitter api v2
  await sleep(twitterApiLookupInterval); // Sleep for 300req/900s
  const lookupResult = await lookupTweets(tweetIds);
  // Filtering for non-tech topics
  const filteredTweets = lookupResult.data.filter(sourceLabelFilter).filter(contextDomainFilter);
  metrics.addMetric('FilteredTweetsRate', MetricUnits.Percent, (lookupResult.data.length - filteredTweets.length) / lookupResult.data.length * 100);
  lookupResult.data = filteredTweets;
  // Transform to stream data from lookup data
  const streamResults = lookupResultToStreamResults(lookupResult);
  if (streamResults.length > 0) {
    const records = streamResultsToKinesisRecord(streamResults);
    const putRecordsCommand = new PutRecordsCommand({
      StreamName: streamName,
      Records: records,
    });
    const { FailedRecordCount } = await kinesis.send(putRecordsCommand);
    metrics.addMetric('FailedRecordCount', MetricUnits.Count, FailedRecordCount || 0);
  }
  subsegment.close();
  tracer.setSegment(parentSubsegment);
  return;
};

const getTweetIds = async (sqsRecord: SQSRecord) => {
  const { Records: s3Records }: S3Event = JSON.parse(sqsRecord.body);
  const s3Keys = s3Records.map(s3Record => {
    return { bucket: s3Record.s3.bucket.name, key: s3Record.s3.object.key.replace(/%3D/g, '=') };
  });
  const lines = (await Promise.map(s3Keys, async (item) => {return getObjectLines(item.bucket, item.key);})).flat();
  const tweetIds = await Promise.map(lines, tweetV1stringToId);
  return tweetIds;
};

export const handler: SQSHandler = async (event, _context) => {
  const sqsRecords = event.Records;
  // Tracing
  const segment = tracer.getSegment();
  // Fetch parameters
  await fetchParameterStore();
  // Get TweetIds from S3
  const subsegment = segment.addNewSubsegment('Get tweet ids');
  tracer.setSegment(subsegment);
  const tweetIds = (await Promise.map(sqsRecords, getTweetIds)).flat();
  const deduplicatedTweetIds = Deduplicate(tweetIds) || []; // 重複排除
  subsegment.close();
  tracer.setSegment(segment);
  // Process
  const subsegment2 = segment.addNewSubsegment('Main Process');
  tracer.setSegment(subsegment2);
  const tweetIdsArray = arraySplit<string>(deduplicatedTweetIds, 100); // 100件づつのArray
  await Promise.map(tweetIdsArray, processTweetIds, { concurrency: 1 });
  subsegment2.close();
  tracer.setSegment(segment);
  // Detete
  const subsegment3 = segment.addNewSubsegment('Delete Objects');
  tracer.setSegment(subsegment3);
  await deleteAllObjects(sqsRecords);
  subsegment3.close();
  tracer.setSegment(segment);
  // Metrics
  metrics.publishStoredMetrics();
  twitterApiMetrics.publishStoredMetrics();
  return;
};
