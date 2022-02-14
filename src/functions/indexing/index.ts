import { Readable } from 'stream';
import { Sha256 } from '@aws-crypto/sha256-js';
import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { NodeHttpHandler } from '@aws-sdk/node-http-handler';
import { HttpRequest, HttpResponse } from '@aws-sdk/protocol-http';
import { SignatureV4 } from '@aws-sdk/signature-v4';
import { KinesisStreamHandler } from 'aws-lambda';
import { TweetV2, UserV2, TweetPublicMetricsV2, TTweetReplySettingsV2 } from 'twitter-api-v2'
import { TweetStreamParse, StreamResult, Deduplicate, Normalize, Analysis } from '../utils';

const allowedTimeRange = 1000 * 60 * 60 * 24 * 365 * 2;
const hostname = process.env.OPENSEARCH_DOMAIN_ENDPOINT!;
const region = process.env.AWS_REGION || 'us-west-2';

const logger = new Logger();
const metrics = new Metrics();
const searchMetrics = new Metrics({ serviceName: 'OpenSearch' });
const tracer = new Tracer();

const client = new NodeHttpHandler();

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
  },
  conversation_id?: string;
  created_at?: string;
  entities?: {
    annotation?: string[];
    cashtag?: string[];
    hashtag?: string[];
    mention?: string[];
    url?: string[]
  },
  geo?: {
    coordinates?: {
      type?: string
      coordinates?: {
        lat?: number
        lon?: number
      }
    }
    place_id?: string
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

const getSearchMetadata = (stream: StreamResult): Metadata => {
  const tweet = stream.data;
  const date = (tweet.created_at)
    ? new Date(tweet.created_at)
    : new Date();
  const index = 'tweets-' + date.toISOString().substring(0, 7);
  const metadata: Metadata = {
    _index: index,
    _id: tweet.id,
  };
  return metadata;
};

const getSearchDocument = (stream: StreamResult): Document => {
  const tweet = stream.data;
  const author = stream.includes?.users?.find(x => x.id == tweet.author_id);
  delete tweet.author_id; // author.id と被るので削除
  const doc: Document = {
    ...tweet,
    text: stream.analysis?.normalized_text || Normalize(tweet.text),
    url: `https://twitter.com/${tweet.author_id}/status/${tweet.id}`,
    author,
    context_annotations: {
      domain: Deduplicate(tweet.context_annotations?.map(x => x.domain.name)),
      entity: Deduplicate(tweet.context_annotations?.map(x => x.entity.name)),
    },
    entities: {
      annotation: tweet.entities?.annotations?.map(x => x.normalized_text.toLowerCase()),
      cashtag: tweet.entities?.cashtags?.map(x => x.tag?.toLowerCase()),
      hashtag: tweet.entities?.hashtags?.map(x => x.tag?.toLowerCase()),
      mention: tweet.entities?.mentions?.map(x => x.username?.toLowerCase()),
      url: tweet.entities?.urls?.map(x => x.expanded_url),
    },
    geo: {
      coordinates: {
        type: tweet.geo?.coordinates?.type,
        coordinates: {
          lat: tweet.geo?.coordinates?.coordinates?.[0],
          lon: tweet.geo?.coordinates?.coordinates?.[1],
        }
      },
      place_id: tweet.geo?.place_id,
    },
    referenced_tweets: {
      type: tweet.referenced_tweets?.map(x => x.type),
      id: tweet.referenced_tweets?.map(x => x.id),
    },
    matching_rules: {
      id: stream.matching_rules?.map(rule => rule.id.toString()),
      tag: Deduplicate(stream.matching_rules?.map(rule => rule.tag)),
    },
    includes: stream.includes,
    analysis: stream.analysis,
  };
  return doc;
};

const addBulkAction = (bulkActions: string[], stream: StreamResult) => {
  const metadata = getSearchMetadata(stream);
  const doc = getSearchDocument(stream);
  const bulkHeader = JSON.stringify({ update: metadata });
  const bulkBody = JSON.stringify({ doc, doc_as_upsert: true });
  bulkActions.push(bulkHeader, bulkBody);
};

const genBulkActions = (stream: StreamResult): string[]=> {
  const bulkActions: string[] = [];
  addBulkAction(bulkActions, stream);
  const now = new Date();
  stream.includes?.tweets?.map(x => {
    // 元ツイートがN年以内だったらメトリクスも更新する
    const date = new Date(x.created_at || 0);
    if (now.getTime() - date.getTime() < allowedTimeRange ) {
      addBulkAction(bulkActions, { data: x });
    };
  });
  return bulkActions;
};

export const handler: KinesisStreamHandler = async (event) => {
  const segment = tracer.getSegment();
  metrics.addMetric('IncomingRecordCount', MetricUnits.Count, event.Records.length);
  // Transforming
  const subsegment = segment.addNewSubsegment('Trasform records');
  tracer.setSegment(subsegment);
  const bulkActions = event.Records.flatMap(record => {
    const tweetStreamData = TweetStreamParse(record.kinesis.data);
    return genBulkActions(tweetStreamData);
  });
  subsegment.close();
  tracer.setSegment(segment);
  // Call _bulk API
  const subsegment2 = segment.addNewSubsegment('Bulk load');
  tracer.setSegment(subsegment2);
  const request = new HttpRequest({
    headers: {
      'Content-Type': 'application/json',
      'host': hostname,
    },
    hostname,
    method: 'POST',
    path: '_bulk',
    body: bulkActions.join('\n') + '\n',
  });
  const signer = new SignatureV4({
    credentials: defaultProvider(),
    region: region,
    service: 'es',
    sha256: Sha256,
  });
  const signedRequest = await signer.sign(request) as HttpRequest;
  const { response } = await client.handle(signedRequest);
  subsegment2.close();
  tracer.setSegment(segment);
  // Perse response
  const responseBody: BulkResponse = await responseParse(response);
  if (responseBody.errors) {
    const errorCount = responseBody.items.filter(item => { item.update.error; }).length;
    searchMetrics.addMetric('ErrorCount', MetricUnits.Count, errorCount);
  };
  metrics.addMetric('OutgoingRecordCount', MetricUnits.Count, bulkActions.length/2);
  metrics.publishStoredMetrics();
  return;
};