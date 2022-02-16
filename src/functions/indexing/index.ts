import { Readable } from 'stream';
import { Sha256 } from '@aws-crypto/sha256-js';
import { LambdaInterface } from '@aws-lambda-powertools/commons';
import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { NodeHttpHandler } from '@aws-sdk/node-http-handler';
import { HttpRequest, HttpResponse } from '@aws-sdk/protocol-http';
import { SignatureV4 } from '@aws-sdk/signature-v4';
import { KinesisStreamHandler, KinesisStreamEvent, KinesisStreamRecord, Context } from 'aws-lambda';
import { TweetV2, UserV2, TweetPublicMetricsV2, TTweetReplySettingsV2 } from 'twitter-api-v2';
import { TweetStreamParse, TweetStreamRecord, Deduplicate, Normalize, Analysis } from '../utils';

const allowedTimeRange = 1000 * 60 * 60 * 24 * 365 * 2;
const opensearchDomainEndpoint = process.env.OPENSEARCH_DOMAIN_ENDPOINT!;
const region = process.env.AWS_REGION || 'us-west-2';

const logger = new Logger();
const metrics = new Metrics();
const searchMetrics = new Metrics({ serviceName: 'OpenSearch' });
const tracer = new Tracer();

interface BulkUpdateHeader {
  update: {
    _index: string;
    _id?: string;
  };
};;

interface BulkUpdateDocument {
  doc: Document;
  doc_as_upsert?: boolean;
}

interface Metadata {
  _index: string;
  _id?: string;
};

// https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
interface Document {
  id: string;
  text: string;
  url: string;
  author?: UserV2;
  context_annotations?: {
    domain?: string[];
    entity?: string[];
  };
  conversation_id?: string;
  created_at?: string;
  entities?: {
    annotation?: string[];
    cashtag?: string[];
    hashtag?: string[];
    mention?: string[];
    url?: string[];
  };
  geo?: {
    coordinates?: {
      type?: string;
      coordinates?: {
        lat?: number;
        lon?: number;
      };
    };
    place_id?: string;
  };
  in_reply_to_user_id?: string;
  lang?: string;
  possibly_sensitive?: boolean;
  public_metrics?: TweetPublicMetricsV2;
  referenced_tweets?: {
    type?: ('retweeted'|'quoted'|'replied_to')[];
    id?: string[];
  };
  reply_setting?: TTweetReplySettingsV2;
  source?: string;
  matching_rules?: {
    id?: string[];
    tag?: string[];
  };
  includes?: {
    tweets?: TweetV2[];
    users?: UserV2[];
  };
  analysis?: Analysis;
};

type BulkResponseItem = {
  [key in 'create'|'delete'|'index'|'update']: {
    [key: string]: any;
    _index: string;
    _id: string;
    status: number;
    error?: {
      type: string;
      reason: string;
      index: string;
      shard: string;
      index_uuid: string;
    };
  };
};;
interface BulkResponse {
  took: number;
  errors: boolean;
  items: BulkResponseItem[];
};

const asBuffer = async (response: HttpResponse) => {
  const stream = response.body as Readable;
  const chunks: Buffer[] = [];
  return new Promise<Buffer>((resolve, reject) => {
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('error', (err) => reject(err));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
  });
};
const responseParse = async (response: HttpResponse) => {
  const buffer = await asBuffer(response);
  const bufferString = buffer.toString();
  return JSON.parse(bufferString);
};

const author = (record: TweetStreamRecord) => {
  const tweet = record.data;
  if (typeof tweet.author_id == 'undefined') {
    return undefined;
  } else {
    return record.includes?.users?.find(user => user.id == tweet.author_id);
  };
};

const context_annotations = (tweet: TweetV2) => {
  if (typeof tweet.context_annotations == 'undefined') {
    return undefined;
  } else {
    return {
      domain: Deduplicate(tweet.context_annotations?.map(x => x.domain.name) || []),
      entity: Deduplicate(tweet.context_annotations?.map(x => x.entity.name) || []),
    };
  };
};

const entities = (tweet: TweetV2) => {
  if (typeof tweet.entities == 'undefined') {
    return undefined;
  } else {
    return {
      annotation: tweet.entities?.annotations?.map(x => x.normalized_text.toLowerCase()),
      cashtag: tweet.entities?.cashtags?.map(x => x.tag?.toLowerCase()),
      hashtag: tweet.entities?.hashtags?.map(x => x.tag?.toLowerCase()),
      mention: tweet.entities?.mentions?.map(x => x.username?.toLowerCase()),
      url: tweet.entities?.urls?.map(x => x.expanded_url),
    };
  };
};

const geo = (tweet: TweetV2) => {
  if (typeof tweet.geo?.coordinates == 'undefined' && typeof tweet.geo?.place_id == 'undefined') {
    return undefined;
  } else {
    let geoPoint = undefined;
    if (tweet.geo?.coordinates?.coordinates) {
      geoPoint = {
        lat: tweet.geo.coordinates.coordinates[1],
        lon: tweet.geo.coordinates.coordinates[0],
      };
    };
    return {
      coordinates: {
        type: tweet.geo?.coordinates?.type,
        coordinates: geoPoint,
      },
      place_id: tweet.geo?.place_id,
    };
  };
};

const referenced_tweets = (tweet: TweetV2) => {
  if (typeof tweet.referenced_tweets == 'undefined') {
    return undefined;
  } else {
    return {
      type: tweet.referenced_tweets?.map(x => x.type),
      id: tweet.referenced_tweets?.map(x => x.id),
    };
  };
};

const matching_rules = (record: TweetStreamRecord) => {
  if (typeof record.matching_rules == 'undefined') {
    return undefined;
  } else {
    return {
      id: record.matching_rules?.map(rule => rule.id.toString()),
      tag: Deduplicate(record.matching_rules?.map(rule => rule.tag) || []),
    };
  };
};

const toDocument = (record: TweetStreamRecord): Document => {
  const tweet = record.data;
  const doc: Document = {
    ...tweet,
    text: record.analysis?.normalized_text || Normalize(tweet.text),
    url: `https://twitter.com/${tweet.author_id}/status/${tweet.id}`,
    author: author(record),
    context_annotations: context_annotations(tweet),
    entities: entities(tweet),
    geo: geo(tweet),
    referenced_tweets: referenced_tweets(tweet),
    matching_rules: matching_rules(record),
    includes: record.includes,
    analysis: record.analysis,
  };
  return doc;
};

const addOriginTweet = (record: TweetStreamRecord): TweetStreamRecord[] => {
  const records = [record];
  const now = new Date();
  record.includes?.tweets?.map(tweet => {
    const date = new Date(tweet.created_at || 0);
    if (now.getTime() - date.getTime() < allowedTimeRange ) {
      // 元ツイートがN年以内だったらメトリクスも更新する
      records.push({ data: tweet });
    };
  });
  return records;
};

const toBulkAction = (record: TweetStreamRecord): [BulkUpdateHeader, BulkUpdateDocument] => {
  const tweet = record.data;
  const date = (tweet.created_at) ? new Date(tweet.created_at) : new Date();
  const index = 'tweets-' + date.toISOString().substring(0, 7);
  const header: BulkUpdateHeader = {
    update: {
      _index: index,
      _id: tweet.id,
    },
  };
  const updateDoc: BulkUpdateDocument = {
    doc: toDocument(record),
    doc_as_upsert: true,
  };
  return [header, updateDoc];
};

class Lambda implements LambdaInterface {

  @tracer.captureMethod()
  public async bulkRequest(host: string, body: string): Promise<BulkResponse> {
    const httpClient = new NodeHttpHandler();
    const request = new HttpRequest({
      headers: { 'Content-Type': 'application/json', host },
      hostname: host,
      method: 'POST',
      path: '_bulk',
      body: body,
    });
    const signer = new SignatureV4({
      credentials: defaultProvider(),
      region: region,
      service: 'es',
      sha256: Sha256,
    });
    const signedRequest = await signer.sign(request) as HttpRequest;
    const { response } = await httpClient.handle(signedRequest);
    const { took, items, errors }: BulkResponse = await responseParse(response);
    const errorItems = items.filter(item => typeof item.update.error != 'undefined');
    metrics.addMetric('BulkApiLatency', MetricUnits.Milliseconds, took);
    metrics.addMetric('BulkErrorRate', MetricUnits.Percent, errorItems.length/items.length*100);
    if (errors) {
      searchMetrics.addMetric('ErrorCount', MetricUnits.Count, errorItems.length);
      errorItems.map(item => {
        logger.error({ message: item.update.error!.reason, item });
      });
    };
    return { took, items, errors };
  };

  @metrics.logMetrics()
  @tracer.captureLambdaHandler()
  public async handler(event: KinesisStreamEvent, _context: Context): Promise<void> {
    const kinesisStreamRecords = event.Records;
    metrics.addMetric('IncomingRecordCount', MetricUnits.Count, kinesisStreamRecords.length);
    const tweetStreamRecords = kinesisStreamRecords.map(record => TweetStreamParse(record.kinesis.data));
    const originTweetStreamRecords = tweetStreamRecords.flatMap(addOriginTweet);
    const bulkActions = originTweetStreamRecords.flatMap(toBulkAction);
    const stringBulkActions = bulkActions.map(item => JSON.stringify(item));
    const body = stringBulkActions.join('\n') + '\n';
    const { took, items, errors } = await this.bulkRequest(opensearchDomainEndpoint, body);
    metrics.addMetric('OutgoingRecordCount', MetricUnits.Count, items.length);
    return;
  };

};

export const myFunction = new Lambda();
export const handler: KinesisStreamHandler = myFunction.handler;
