//import { Logger } from '@aws-lambda-powertools/logger';
//import { Metrics } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { EventBridgeClient, PutEventsCommand, PutEventsRequestEntry } from '@aws-sdk/client-eventbridge';
import { SQSHandler, SQSRecord } from 'aws-lambda';
import { TweetV2SingleStreamResult } from 'twitter-api-v2';

const region = process.env.AWS_REGION;
const eventBusArn = process.env.EVENT_BUS_ARN!;

//const logger = new Logger();
//const metrics = new Metrics();
const tracer = new Tracer();
const eventBridge = tracer.captureAWSv3Client(new EventBridgeClient({ region }));

const recordToEntry = (record: SQSRecord): PutEventsRequestEntry => {
  const body = record.body;
  const event: TweetV2SingleStreamResult = JSON.parse(body);
  const eventTime = (typeof event.data.created_at == 'string') ? new Date(event.data.created_at) : undefined;
  const entry: PutEventsRequestEntry = {
    EventBusName: eventBusArn,
    Source: 'twitter.api.v2',
    DetailType: 'Tweet',
    Detail: body,
    Time: eventTime,
  };
  return entry;
};

export const handler: SQSHandler = async (event, _context) => {
  const entries = event.Records.map(recordToEntry);
  const putEventsCommand = new PutEventsCommand({ Entries: entries });
  await eventBridge.send(putEventsCommand);
};
