import { Readable } from 'stream';
import zlib from 'zlib';
import { LambdaInterface } from '@aws-lambda-powertools/commons';
import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { KinesisClient, PutRecordsCommand, PutRecordsRequestEntry } from '@aws-sdk/client-kinesis';
import { S3Client, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { SSMClient, GetParametersByPathCommand } from '@aws-sdk/client-ssm';
import { SQSHandler, S3Event, S3EventRecord, SQSEvent, Context } from 'aws-lambda';
import { Promise } from 'bluebird';
import { TweetV1, TweetV2, TwitterApi, TweetV2LookupResult, Tweetv2FieldsParams } from 'twitter-api-v2';
import { TweetStreamRecord, Deduplicate } from '../utils';

const twitterApiLookupInterval = 2180; // Quota: 300req/900s
let twitterBearerToken: string;
let twitterFieldsParams: Partial<Tweetv2FieldsParams>;
let twitterFilterSourceLabels: string[];
let twitterFilterContextDomains: string[];

const region = process.env.AWS_REGION || 'us-west-2';
const destStreamName = process.env.DEST_STREAM_NAME!;
const twitterParameterPrefix = process.env.TWITTER_PARAMETER_PREFIX!;

const logger = new Logger();
const metrics = new Metrics();
const twitterMetrics = new Metrics({ serviceName: 'api.twitter.com' });
twitterMetrics.addDimension('path', '/2/tweets');
const tracer = new Tracer();

const ssm = tracer.captureAWSv3Client(new SSMClient({ region }));
const s3 = tracer.captureAWSv3Client(new S3Client({ region }));
const kinesis = tracer.captureAWSv3Client(new KinesisClient({ region, maxAttempts: 10 }));

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

const asBuffer = async (data: unknown): Promise<Buffer> => {
  const stream = data as Readable;
  const chunks: Buffer[] = [];
  const buffer = await new Promise<Buffer>((resolve, reject) => {
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('error', (err) => reject(err));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
  });
  return buffer;
};

const getObject = async (record: S3EventRecord): Promise<string|undefined> => {
  let data: string|undefined;
  try {
    const bucket = record.s3.bucket.name;
    const key = record.s3.object.key.replace(/%3D/g, '=');
    const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
    const { Body, ContentEncoding } = await s3.send(cmd);
    const buffer = await asBuffer(Body);
    if (ContentEncoding == 'gzip') {
      data = zlib.gunzipSync(buffer).toString();
    } else {
      data = buffer.toString();
    }
  } catch (err) {
    logger.error({ message: JSON.stringify(err) });
    data = undefined;
  };
  return data;
};

const deleteObject = async(record: S3EventRecord)=> {
  const bucket = record.s3.bucket.name;
  const objectKey = record.s3.object.key.replace(/%3D/g, '=');
  const cmd = new DeleteObjectCommand({ Bucket: bucket, Key: objectKey });
  try {
    await s3.send(cmd);
  } catch (err: any) {
    logger.warn({ message: JSON.stringify(err) });
  }
  return;
};

const bodyToLines = (objectBody: string|undefined): TweetV1[] => {
  if (objectBody) {
    const lines = objectBody.trimEnd().split('\n');
    const tweets: TweetV1[] = lines.map(line => JSON.parse(line));
    return tweets;
  } else {
    return [];
  }
};

const toTweetStreamRecords = (lookupResult: TweetV2LookupResult): TweetStreamRecord[] => {
  const includesUsers = lookupResult.includes?.users || [];
  const includesTweets = lookupResult.includes?.tweets || [];
  const tweets = lookupResult.data || [];
  const streamResults = tweets.map(tweet => {
    const referencedTweetIds = tweet.referenced_tweets?.map(referencedTweet => referencedTweet.id) || [];
    const referencedTweets = includesTweets.filter(includesTweet => referencedTweetIds?.includes(includesTweet.id)); // includes からreferenced_tweetに該当するものを探す
    const author = includesUsers.filter(userV2 => userV2.id == tweet.author_id);
    const stream: TweetStreamRecord = {
      data: tweet,
      includes: {
        users: author,
        tweets: referencedTweets,
      },
      backup: false, // ###########################
    };
    return stream;
  });
  return streamResults;
};

const tweetsLoader = async (tweetIds: string[], inprogress: string[] = [], i: number = 0) => {
  inprogress.push(tweetIds[i]);
  if (i+1 == tweetIds.length || inprogress.length == 100) {
    const parentSubsegment = tracer.getSegment();
    const subsegment = parentSubsegment.addNewSubsegment('#### tweetsLoader');
    tracer.setSegment(subsegment);
    // segment start
    await myFunction.sleep(twitterApiLookupInterval);
    const lookupResult = await myFunction.lookupTweets(inprogress);
    twitterMetrics.addMetric('RequestCount', MetricUnits.Count, 1);
    const filteredTweets = lookupResult.data.filter(sourceLabelFilter).filter(contextDomainFilter);
    lookupResult.data = filteredTweets;
    //metrics.addMetric('FilteredTweetsRate', MetricUnits.Percent, (lookupResult.data.length - filteredTweets.length) / lookupResult.data.length * 100);
    const twitterStreamRecords = toTweetStreamRecords(lookupResult);
    const entries = twitterStreamRecords.map(record => {
      const entry: PutRecordsRequestEntry = {
        PartitionKey: record.data.id,
        Data: Buffer.from(JSON.stringify(record)),
      };
      return entry;
    });
    const cmd = new PutRecordsCommand({
      StreamName: destStreamName,
      Records: entries,
    });
    const { FailedRecordCount, $metadata } = await kinesis.send(cmd);
    // segment end
    subsegment.close();
    tracer.setSegment(parentSubsegment);
    if (i+1 == tweetIds.length) {
      return;
    } else {
      inprogress.length = 0;
    }
  };
  await tweetsLoader(tweetIds, inprogress, i+1);
  return;
};

class Lambda implements LambdaInterface {

  @tracer.captureMethod()
  private async fetchParameter(): Promise<void> {
    const cmd = new GetParametersByPathCommand({ Path: twitterParameterPrefix, Recursive: true });
    const { Parameters } = await ssm.send(cmd);
    twitterBearerToken = Parameters!.find(param => param.Name!.endsWith('BearerToken'))!.Value!;
    twitterFieldsParams = JSON.parse(Parameters!.find(param => param.Name?.endsWith('FieldsParams'))!.Value!);
    twitterFilterContextDomains = Parameters!.find(param => param.Name?.endsWith('Filter/ContextDomains'))!.Value!.split(',');
    twitterFilterSourceLabels = Parameters!.find(param => param.Name?.endsWith('Filter/SourceLabels'))!.Value!.split(',');
    twitterFilterSourceLabels.push('Video Game');
    logger.info({ message: 'Get palameters from parameter store successfully' });
    return;
  };

  @tracer.captureMethod()
  private async getTweetIds(records: S3EventRecord[]): Promise<string[]> {
    const objectBodyArray = await Promise.map(records, getObject);
    const tweets = objectBodyArray.flatMap(body => bodyToLines(body));
    const tweetIds = tweets.map(tweet => tweet.id_str);
    const deduplicatedTweetIds: string[] = Deduplicate(tweetIds);
    return deduplicatedTweetIds;
  };

  @tracer.captureMethod()
  public async sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
  };

  @tracer.captureMethod()
  public async lookupTweets(tweetIds: string[]) {
    const twitterApi = new TwitterApi(twitterBearerToken);
    const result = await twitterApi.v2.tweets(tweetIds, twitterFieldsParams);
    return result;
  };

  @tracer.captureMethod()
  private async ingestTweets(tweetIds: string[]) {
    return tweetsLoader(tweetIds);
  };

  @tracer.captureMethod()
  public async deleteAllObjects(s3EventRecords: S3EventRecord[]) {
    await Promise.map(s3EventRecords, deleteObject);
    return;
  };

  @metrics.logMetrics()
  @twitterMetrics.logMetrics()
  @tracer.captureLambdaHandler()
  public async handler(event: SQSEvent, _context: Context): Promise<void> {
    await this.fetchParameter();
    const sqsRecords = event.Records;
    const s3Events: S3Event[] = sqsRecords.map(record => JSON.parse(record.body));
    const s3EventRecords = s3Events.flatMap(s3Event => s3Event.Records);
    const tweetIds = await this.getTweetIds(s3EventRecords);
    await this.ingestTweets(tweetIds);
    await this.deleteAllObjects(s3EventRecords);
    return;
  };

};

export const myFunction = new Lambda();
export const handler: SQSHandler = myFunction.handler;
