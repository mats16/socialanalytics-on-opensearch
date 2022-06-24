import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { AttributeValue } from '@aws-sdk/client-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { Handler, DynamoDBStreamEvent, KinesisStreamEvent, DynamoDBRecord, KinesisStreamRecord } from 'aws-lambda';
import { TweetItem, b64decode } from '../utils';
import { sendBulkOperation, toBulkAction, BulkUpdateHeader, BulkUpdateDocument } from './opensearch_utils';

const region = process.env.AWS_REGION || 'us-west-2';
const opensearchDomainEndpoint = process.env.OPENSEARCH_DOMAIN_ENDPOINT!;

const logger = new Logger();
const metrics = new Metrics();
const tracer = new Tracer();

const unmarshallOptions = {
  wrapNumbers: false, // false, by default.
};

const toTweetItem = (record: DynamoDBRecord|KinesisStreamRecord): TweetItem|undefined => {
  if (record.eventSource == 'aws:dynamodb' && record.eventName == 'MODIFY') {
    const r = record as DynamoDBRecord;
    const newImage = r.dynamodb?.NewImage as Record<string, AttributeValue>;
    const tweet = unmarshall(newImage, unmarshallOptions) as TweetItem;
    return tweet;
  } else if (record.eventSource == 'aws:kinesis') {
    const r = record as KinesisStreamRecord;
    const decodedText = b64decode(r.kinesis.data);
    const tweet: TweetItem = JSON.parse(decodedText);
    return tweet;
  } else {
    return undefined;
  }
};

// eslint-disable-next-line max-len
const bulkLoader = async (host: string, records: (DynamoDBRecord|KinesisStreamRecord)[], inprogress: [BulkUpdateHeader, BulkUpdateDocument][] = [], i: number = 0) => {
  const record = records[i];
  const tweet = toTweetItem(record);
  if (typeof tweet != 'undefined') {
    const bulkAction = toBulkAction(tweet);
    inprogress.push(bulkAction);
  }

  if (i+1 == records.length || inprogress.length == 100) {
    if (inprogress.length > 0) {
      const { took, items, errors } = await sendBulkOperation(host, inprogress);
      metrics.addMetric('OpenSearchBulkApiTookTime', MetricUnits.Milliseconds, took);
      if (errors) {
        const errorItems = items.filter(item => typeof item.update.error != 'undefined');
        metrics.addMetric('OpenSearchBulkApiErrorCount', MetricUnits.Count, errorItems.length);
        errorItems.map(item => {
          logger.error({ message: item.update.error!.reason, item });
        });
      }
    }
    if (i+1 == records.length) {
      return;
    } else {
      inprogress.length = 0;
    }
  }
  await bulkLoader(host, records, inprogress, i+1);
  return;
};

export const handler: Handler<DynamoDBStreamEvent|KinesisStreamEvent> = async(event, _context) => {
  metrics.addMetric('IncomingRecordCount', MetricUnits.Count, event.Records.length);

  await bulkLoader(opensearchDomainEndpoint, event.Records);

  metrics.publishStoredMetrics();
};
