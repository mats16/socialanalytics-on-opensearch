import { Readable } from 'stream';
import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { KinesisClient, PutRecordsCommand, PutRecordsRequestEntry } from '@aws-sdk/client-kinesis';
import { S3Client, GetObjectCommand, GetObjectCommandOutput, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { SQSHandler, SQSRecord, S3Event } from 'aws-lambda';
import axios from 'axios';
import { Promise } from 'bluebird';
import { TweetV1, TweetV2, TwitterApi, TweetV2LookupResult, Tweetv2FieldsParams } from 'twitter-api-v2';
import { StreamResult, Deduplicate } from '../utils';

const region = process.env.AWS_REGION || 'us-west-2';
const streamName = process.env.STREAM_NAME!;
const appconfigApp = process.env.APPCONFIG_APPLICATION!;
const appconfigEnv = process.env.APPCONFIG_ENVIRONMENT!;
const appconfigConfigTwitterBearerToken = process.env.APPCONFIG_CONFIG_TWITTER_BEARER_TOKEN!;
const appconfigConfigTwitterFieldsParams = process.env.APPCONFIG_CONFIG_TWITTER_FIELDS_PARAMS!;

const logger = new Logger({ logLevel: 'INFO', serviceName: 'ReprocessTweetsV1Function' });
const metrics = new Metrics({ namespace: 'SocialAnalytics', serviceName: 'ReprocessTweetsV1Function' });
const twitterApiMetrics = new Metrics({ namespace: 'SocialAnalytics', serviceName: 'twitter-api-v2' });

const s3 = new S3Client({ region });
const kinesis = new KinesisClient({ region, maxAttempts: 10 });

const getConfig = async (configName: string) => {
  // from AppConfig
  const { data } = await axios.get(`http://localhost:2772/applications/${appconfigApp}/environments/${appconfigEnv}/configurations/${configName}`);
  return data;
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
  if (tweetIds.length > 100) {
    throw new Error('Up to 100 tweets are allowed to lookup.');
  }
  // Sleep for 300req/900s
  await new Promise(resolve => setTimeout(resolve, 2200));
  // Get parameters from AppConfig
  const bearerToken: string = await getConfig(appconfigConfigTwitterBearerToken);
  const fieldsParams: Partial<Tweetv2FieldsParams> = await getConfig(appconfigConfigTwitterFieldsParams);
  // Call twitter api v2
  const twitterApi = new TwitterApi(bearerToken);
  const lookupResult = await twitterApi.v2.tweets(tweetIds, fieldsParams);
  twitterApiMetrics.addDimension('api', 'lookup');
  if (lookupResult.errors) {
    twitterApiMetrics.addMetric('ErrorRecordCount', MetricUnits.Count, lookupResult.errors.length);
  };
  twitterApiMetrics.addMetric('RequestCount', MetricUnits.Count, 1);
  return lookupResult;
};

const domainFilter = (tweet: TweetV2): boolean => {
  const filterDomains = ['Musician', 'Music Genre', 'Actor', 'TV Shows', 'Multimedia Franchise', 'Fictional Character'];
  const contextAnnotationsDomainName = tweet.context_annotations?.map(a => a.domain.name) || [];
  const isFiltered = filterDomains.some(filterDomain => contextAnnotationsDomainName.includes(filterDomain));
  if (isFiltered) {
    return false;
  } else {
    return true;
  }
};

const lookupResultToStreamResults = (lookupResult: TweetV2LookupResult): StreamResult[] => {
  const includesUsers = lookupResult.includes?.users || [];
  const includesTweets = lookupResult.includes?.tweets || [];
  const tweets = lookupResult.data || [];
  // Filtering for non-tech topics
  const filteredTweets = tweets.filter(domainFilter);
  metrics.addMetric('FilteredTweetsCount', MetricUnits.Count, tweets.length - filteredTweets.length);

  const streamResults = filteredTweets.map(tweet => {
    const referencedTweetIds = tweet.referenced_tweets?.map(referencedTweet => referencedTweet.id) || [];
    const referencedTweets = includesTweets.filter(includesTweet => referencedTweetIds?.includes(includesTweet.id)); // includes からreferenced_tweetに該当するものを探す
    const author = includesUsers.filter(userV2 => userV2.id == tweet.author_id);
    const stream: StreamResult = {
      data: tweet,
      includes: {
        users: author,
        tweets: referencedTweets,
      },
      backup: false,
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

const processTweetIds = async (tweetIds: string[]) => {
  // Get data from twitter api v2
  const lookupResult = await lookupTweets(tweetIds);
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
  const tweetIds = (await Promise.map(sqsRecords, getTweetIds)).flat();
  const deduplicatedTweetIds = Deduplicate(tweetIds) || []; // 重複排除
  const tweetIdsArray = arraySplit<string>(deduplicatedTweetIds, 100); // 100件づつのArray
  await Promise.map(tweetIdsArray, processTweetIds, { concurrency: 1 });
  await deleteAllObjects(sqsRecords);
  metrics.publishStoredMetrics();
  twitterApiMetrics.publishStoredMetrics();
  return;
};
