import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { AttributeValue } from '@aws-sdk/client-dynamodb';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { Client } from '@opensearch-project/opensearch';
import { AwsSigv4Signer } from '@opensearch-project/opensearch/aws';
import { Handler, DynamoDBStreamEvent, DynamoDBRecord } from 'aws-lambda';
import { TweetItem } from '../types';
import { Document, toDocument } from './opensearch-utils';

const region = process.env.AWS_REGION!;
const opensearchDomainEndpoint = process.env.OPENSEARCH_DOMAIN_ENDPOINT!;

const logger = new Logger();
const metrics = new Metrics();
const tracer = new Tracer();

const unmarshallOptions = {
  wrapNumbers: false, // false, by default.
};

const client = new Client({
  ...AwsSigv4Signer({
    region,
    getCredentials: () => {
      // Any other method to acquire a new Credentials object can be used.
      const credentialsProvider = defaultProvider();
      return credentialsProvider();
    },
  }),
  node: `https://${opensearchDomainEndpoint}`,
});

const bulk = async (docs: Document[]) => {
  const stats = await client.helpers.bulk({
    datasource: docs,
    onDocument (doc: Document) {
      const date = new Date(doc.created_at!);
      const index = 'tweets-' + date.toISOString().substring(0, 7);
      return [
        { update: { _index: index, _id: doc.id } },
        { doc_as_upsert: true },
      ];
    },
    onDrop (doc) {
      logger.warn({ message: 'doc is dropped', doc: JSON.stringify(doc) });
    },
  });
  return stats;
};

const toTweetItem = (record: DynamoDBRecord): TweetItem => {
  const newImage = record.dynamodb?.NewImage;
  const validImage = JSON.parse(JSON.stringify(newImage).replace(/"Bool"/g, '"BOOL"').replace(/"Nul"/g, '"NULL"')); // for Java SDK
  const tweet = unmarshall(validImage, unmarshallOptions) as TweetItem;
  return tweet;
};

export const handler: Handler<DynamoDBStreamEvent> = async(event, _context) => {
  metrics.addMetric('IncomingRecordCount', MetricUnits.Count, event.Records.length);

  const tweetItems = event.Records.map(toTweetItem);
  const docs = tweetItems.map(toDocument);

  const stats = await bulk(docs);
  metrics.addMetric('RequestTotalDocCount', MetricUnits.Count, stats.total);
  metrics.addMetric('RequestFailedDocCount', MetricUnits.Count, stats.failed);
  metrics.addMetric('RequestSuccessfulDocCount', MetricUnits.Count, stats.successful);
  metrics.addMetric('RequestRetryDocCount', MetricUnits.Count, stats.retry);
  metrics.addMetric('RequestTookTime', MetricUnits.Milliseconds, stats.time);
  metrics.addMetric('RequestBytes', MetricUnits.Bytes, stats.bytes);

  metrics.publishStoredMetrics();
};
