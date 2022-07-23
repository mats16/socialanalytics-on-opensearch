import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
//import { Tracer } from '@aws-lambda-powertools/tracer';
import { AttributeValue } from '@aws-sdk/client-dynamodb';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { Client, Connection } from '@opensearch-project/opensearch';
import { Handler, DynamoDBStreamEvent, DynamoDBRecord } from 'aws-lambda';
import * as xray from 'aws-xray-sdk';
import * as aws4 from 'aws4';
import { TweetItem } from '../common-utils';
import { Document, toDocument } from './utils';

// eslint-disable-next-line @typescript-eslint/no-require-imports
xray.captureHTTPsGlobal(require('https'));

const opensearchDomainEndpoint = process.env.OPENSEARCH_DOMAIN_ENDPOINT!;

const logger = new Logger();
const metrics = new Metrics();
//const tracer = new Tracer();

const unmarshallOptions = {
  wrapNumbers: false, // false, by default.
};

const createAwsConnector = (credentials: aws4.Credentials) => {
  class AmazonConnection extends Connection {
    buildRequestObject(params: any) {
      const request = super.buildRequestObject(params) as aws4.Request;
      request.service = 'es';
      request.region = process.env.AWS_REGION || 'us-east-1';
      request.headers = request.headers || {};
      request.headers.host = request.hostname;

      return aws4.sign(request, credentials);
    }
  }
  return {
    Connection: AmazonConnection,
  };
};

const getClient = async (host: string) => {
  const credentials = await defaultProvider()();
  return new Client({
    ...createAwsConnector(credentials),
    node: `https://${host}`,
  });
};

const bulk = async (host: string, docs: Document[]) => {
  const client = await getClient(host);
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

  const records = event.Records.filter(record => record.eventName == 'MODIFY');
  const tweetItems = records.map(toTweetItem).filter(tweet => typeof tweet.created_at == 'string');
  const docs = tweetItems.map(toDocument);

  const stats = await bulk(opensearchDomainEndpoint, docs);
  metrics.addMetric('RequestTotalDocCount', MetricUnits.Count, stats.total);
  metrics.addMetric('RequestFailedDocCount', MetricUnits.Count, stats.failed);
  metrics.addMetric('RequestSuccessfulDocCount', MetricUnits.Count, stats.successful);
  metrics.addMetric('RequestRetryDocCount', MetricUnits.Count, stats.retry);
  metrics.addMetric('RequestTookTime', MetricUnits.Milliseconds, stats.time);
  metrics.addMetric('RequestBytes', MetricUnits.Bytes, stats.bytes);

  metrics.publishStoredMetrics();
};
