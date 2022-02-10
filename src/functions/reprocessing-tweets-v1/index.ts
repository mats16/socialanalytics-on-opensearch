import { Readable } from 'stream';
import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { KinesisClient, PutRecordsCommand } from '@aws-sdk/client-kinesis';
import { S3Client, GetObjectCommand, GetObjectCommandOutput, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { S3Handler, S3EventRecord } from 'aws-lambda';
import { Promise } from 'bluebird';
import { parse } from 'node-html-parser';
import * as t from 'twitter-api-v2';
import { StreamResult } from '../utils';

const region = process.env.AWS_REGION || 'us-west-2';
const streamName = process.env.STREAM_NAME!;

const logger = new Logger({ logLevel: 'INFO', serviceName: 'reprocessing-tweets-v1' });
const metrics = new Metrics({ namespace: 'SocialAnalytics', serviceName: 'reprocessing-tweets-v1' });

const s3 = new S3Client({ region });
const kinesis = new KinesisClient({ region, maxAttempts: 10 });

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

const hashtagV1toV2 = (hashtagV1: t.HashtagEntityV1) => {
  const hashtagV2: t.TweetEntityHashtagV2 = {
    start: hashtagV1.indices[0],
    end: hashtagV1.indices[1],
    tag: hashtagV1.text,
  };
  return hashtagV2;
};

const urlV1toV2 = (urlV1: t.UrlEntityV1) => {
  const urlV2: t.TweetEntityUrlV2 = {
    start: urlV1.indices[0],
    end: urlV1.indices[0],
    url: urlV1.url,
    expanded_url: urlV1.expanded_url,
    display_url: urlV1.display_url,
    unwound_url: urlV1.unwound?.url || '',
  };
  return urlV2;
};

const mentionV1toV2 = (mentionV1: t.MentionEntityV1) => {
  const mentionV2: t.TweetEntityMentionV2 = {
    start: mentionV1.indices[0],
    end: mentionV1.indices[1],
    username: mentionV1.name,
  };
  return mentionV2;
};

const userV1toV2 = (userV1: t.UserV1) => {
  const userV2: t.UserV2 = {
    id: userV1.id_str,
    name: userV1.name,
    username: userV1.screen_name,
    public_metrics: {
      followers_count: userV1.followers_count,
      following_count: userV1.friends_count,
      tweet_count: userV1.statuses_count,
      listed_count: userV1.listed_count,
    },
  };
  return userV2;
};

const tweetV1toV2 = (tweetV1: t.TweetV1) => {
  const tweetV2: t.TweetV2 = {
    created_at: new Date(tweetV1.created_at).toISOString(),
    id: tweetV1.id_str,
    lang: tweetV1.lang,
    text: tweetV1.full_text || tweetV1.text,
    source: parse(tweetV1.source).text,
    author_id: tweetV1.user.id_str,
    possibly_sensitive: tweetV1.possibly_sensitive || undefined,
    in_reply_to_user_id: tweetV1.in_reply_to_user_id_str || undefined,
    conversation_id: tweetV1.in_reply_to_status_id_str || undefined,
    //reply_settings: undefined,
    public_metrics: {
      retweet_count: tweetV1.retweet_count || 0,
      reply_count: tweetV1.reply_count || 0,
      like_count: tweetV1.favorite_count || 0,
      quote_count: tweetV1.quote_count || 0,
    },
    entities: {
      urls: tweetV1.entities.urls?.map(x => urlV1toV2(x)) || [],
      hashtags: tweetV1.entities.hashtags?.map(x => hashtagV1toV2(x)) || [],
      cashtags: tweetV1.entities.symbols?.map(x => hashtagV1toV2(x)) || [],
      annotations: [],
      mentions: tweetV1.entities.user_mentions?.map(x => mentionV1toV2(x)) || [],
    },
  };
  return tweetV2;
};

const genStreamResult = (tweetV1string: string) => {
  const tweetV1: t.TweetV1 = JSON.parse(tweetV1string);
  const tweetV2 = tweetV1toV2(tweetV1);
  const includes: t.ApiV2Includes = {
    users: [userV1toV2(tweetV1.user)],
    tweets: [],

  };
  if (tweetV1.retweeted_status) {
    tweetV2.referenced_tweets?.push({ id: tweetV1.id_str, type: 'retweeted' });
    includes.tweets?.push(tweetV1toV2(tweetV1.retweeted_status));
  }
  if (tweetV1.quoted_status) {
    tweetV2.referenced_tweets?.push({ id: tweetV1.id_str, type: 'quoted' });
    includes.tweets?.push(tweetV1toV2(tweetV1.quoted_status));
  }
  const stream: StreamResult = {
    data: tweetV2,
    includes: includes,
    backup: false,
  };
  return stream;
};

const getObjectLines = async(bucket: string, key: string)=> {
  const cmd = new GetObjectCommand({
    Bucket: bucket,
    Key: key,
  });
  try {
    const output = await s3.send(cmd);
    const strBody = await asString(output);
    const lines = strBody.trimEnd().split('\n');
    return lines;
  } catch (err) {
    console.log(err);
    return [];
  }
};

const deleteObject = async(bucket: string, key: string)=> {
  const cmd = new DeleteObjectCommand({
    Bucket: bucket,
    Key: key,
  });
  try {
    await s3.send(cmd);
  } catch (err) {
    console.log(err);
  }
  return;
};

const arraySplit = <T = object>(array: T[], n: number): T[][] => {
  return array.reduce((acc: T[][], c, i: number) => (i % n ? acc : [...acc, ...[array.slice(i, i + n)]]), []);
};

const processRecord = async (record: S3EventRecord) => {
  const bucket = record.s3.bucket.name;
  const key = record.s3.object.key.replace(/%3D/g, '=');
  const lines = await getObjectLines(bucket, key);
  metrics.addMetric('SourceRecordCount', MetricUnits.Count, lines.length);
  const streamResults = lines.map(x => genStreamResult(x));

  const streamResultsBy500 = arraySplit<StreamResult>(streamResults, 500);

  await Promise.map(streamResultsBy500, async (results) => {
    const putRecordsCommand = new PutRecordsCommand({
      StreamName: streamName,
      Records: results.map(stream => {
        return { PartitionKey: stream.data.id, Data: Buffer.from(JSON.stringify(stream)) };
      }),
    });
    const { FailedRecordCount } = await kinesis.send(putRecordsCommand);
    metrics.addMetric('FailedRecordCount', MetricUnits.Count, FailedRecordCount || 0);
  });
  await deleteObject(bucket, key);
  return;
};


export const handler: S3Handler = async (event, _context) => {
  await Promise.map(event.Records, processRecord);
  metrics.publishStoredMetrics();
  return;
};
