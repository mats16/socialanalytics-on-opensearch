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
import { TweetStreamParse, StreamResult, Deduplicate, Normalize } from '../utils';

const allowedTimeRange = 1000 * 60 * 60 * 24 * 365 * 2;
const hostname = process.env.OPENSEARCH_DOMAIN_ENDPOINT!;
const region = process.env.AWS_REGION || 'us-west-2';

const logger = new Logger({ logLevel: 'INFO', serviceName: 'IndexingFunction' });
const metrics = new Metrics({ namespace: 'SocialAnalytics', serviceName: 'IndexingFunction' });
const searchMetrics = new Metrics({ namespace: 'SocialAnalytics', serviceName: 'OpenSearch' });
const tracer = new Tracer({ serviceName: 'IndexingFunction' });

const client = new NodeHttpHandler();

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

const getSearchMetadata = (stream: StreamResult) => {
  const tweet = stream.data;
  const date = (tweet.created_at)
    ? new Date(tweet.created_at)
    : new Date();
  const index = 'tweets-' + date.toISOString().substring(0, 7);
  const metadata = {
    _index: index,
    _id: tweet.id,
  };
  return metadata;
};

const getSearchDocument = (stream: StreamResult) => {
  const tweet = stream.data;
  const author = stream.includes?.users?.find(x => x.id == tweet.author_id);
  delete tweet.author_id; // author.id と被るので削除
  const doc = {
    ...tweet,
    text: stream.analysis?.normalized_text || Normalize(tweet.text),
    url: `https://twitter.com/${tweet.author_id}/status/${tweet.id}`,
    author,
    context_annotations: {
      domain: Deduplicate(tweet.context_annotations?.map(x => x.domain.name)),
      entity: Deduplicate(tweet.context_annotations?.map(x => x.entity.name)),
    },
    entities: {
      annotation: tweet.entities?.annotations?.map(x => x.normalized_text),
      cashtag: tweet.entities?.cashtags?.map(x => x.tag?.toLowerCase()),
      hashtag: tweet.entities?.hashtags?.map(x => x.tag?.toLowerCase()),
      mention: tweet.entities?.mentions?.map(x => x.username?.toLowerCase()),
    },
    referenced_tweets: {
      type: tweet.referenced_tweets?.map(x => x.type),
      id: tweet.referenced_tweets?.map(x => x.id),
    },
    matching_rules: {
      id: stream.matching_rules?.map(rule => rule.id.toString()),
      tag: Deduplicate(stream.matching_rules?.map(rule => rule.tag)),
    },
    analysis: stream.analysis,
    includes: stream.includes,
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
  const bulkActions = event.Records.flatMap(record => {
    const tweetStreamData = TweetStreamParse(record.kinesis.data);
    return genBulkActions(tweetStreamData);
  });
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
  const responseBody: BulkResponse = await responseParse(response);
  searchMetrics.addDimension('API', 'Bulk');
  searchMetrics.addMetric('TookTime', MetricUnits.Milliseconds, responseBody.took);
  if (responseBody.errors) {
    const errorCount = responseBody.items.filter(item => { item.update.error; }).length;
    searchMetrics.addMetric('ErrorCount', MetricUnits.Count, errorCount);
  };
  searchMetrics.clearDimensions();
  metrics.addMetric('OutgoingRecordCount', MetricUnits.Count, bulkActions.length/2);
  metrics.publishStoredMetrics();
  return;
};