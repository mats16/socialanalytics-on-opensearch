import { Sha256 } from '@aws-crypto/sha256-js';
import { Logger } from '@aws-lambda-powertools/logger';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { NodeHttpHandler } from '@aws-sdk/node-http-handler';
import { HttpRequest } from '@aws-sdk/protocol-http';
import { SignatureV4 } from '@aws-sdk/signature-v4';
import { KinesisStreamHandler } from 'aws-lambda';
import { TweetStreamParse, StreamResult, Deduplicate, Normalize } from '../utils';


const allowedTimeRange = 1000 * 60 * 60 * 24 * 365 * 2;

const hostname = process.env.OPENSEARCH_DOMAIN_ENDPOINT!;
const region = process.env.AWS_REGION!;

const headers = {
  'Content-Type': 'application/json',
  'host': hostname,
};

const signer = new SignatureV4({
  credentials: defaultProvider(),
  region: region,
  service: 'es',
  sha256: Sha256,
});

const client = new NodeHttpHandler();

const logger = new Logger({ logLevel: 'INFO', serviceName: 'indexing' });

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
    //'@timestamp': tweet.created_at,
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
      cashtag: tweet.entities?.cashtags?.map(x => x.tag.toLowerCase()),
      hashtag: tweet.entities?.hashtags?.map(x => x.tag.toLowerCase()),
      mention: tweet.entities?.mentions?.map(x => x.username.toLowerCase()),
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
  const bulkActions = event.Records.flatMap(record => {
    const tweetStreamData = TweetStreamParse(record.kinesis.data);
    return genBulkActions(tweetStreamData);
  });

  const request = new HttpRequest({
    headers,
    hostname,
    method: 'POST',
    path: '_bulk',
    body: bulkActions.join('\n') + '\n',
  });
  const signedRequest = await signer.sign(request) as HttpRequest;
  const { response } = await client.handle(signedRequest);
  logger.info({ message: `statusCode: ${response.statusCode}` });

};