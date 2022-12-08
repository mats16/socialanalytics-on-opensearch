//import { Logger } from '@aws-lambda-powertools/logger';
//import { Metrics } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { EventBridgeClient, PutEventsCommand, PutEventsRequestEntry } from '@aws-sdk/client-eventbridge';
import { SQSHandler, SQSRecord } from 'aws-lambda';
import * as xray from 'aws-xray-sdk';
import { TweetV2SingleStreamResult } from 'twitter-api-v2';

const region = process.env.AWS_REGION;
const eventBusArn = process.env.EVENT_BUS_ARN;

//const logger = new Logger();
//const metrics = new Metrics();
const tracer = new Tracer();

const eventBridge = new EventBridgeClient({ region });

const startSegment = (startTime: number, traceHeader?: string) => {
  const traceData = xray.utils.processTraceData(traceHeader);
  const segment = new xray.Segment('Queue Processor', traceData.root, traceData.parent);
  segment.origin = 'AWS::Lambda::Function';
  segment.start_time = startTime;
  //segment.addPluginData({
  //  function_arn: context.invokedFunctionArn,
  //  region: record.awsRegion,
  //  request_id: context.awsRequestId,
  //});
  xray.setSegment(segment);
  return segment;
};

const putEvent = async (event: TweetV2SingleStreamResult) => {
  const client = tracer.captureAWSv3Client(eventBridge);
  const eventTime = (typeof event.data.created_at == 'string') ? new Date(event.data.created_at) : undefined;
  const entry: PutEventsRequestEntry = {
    EventBusName: eventBusArn,
    Source: 'twitter.api.v2',
    DetailType: 'Tweet',
    Detail: JSON.stringify(event),
    Time: eventTime,
  };
  const cmd = new PutEventsCommand({ Entries: [entry] });
  await client.send(cmd);
};

const processRecord = async (record: SQSRecord) => {
  const traceHeader = record.attributes.AWSTraceHeader;
  const sqsEndTime = Number(record.attributes.ApproximateFirstReceiveTimestamp) / 1000;
  const segment = startSegment(sqsEndTime, traceHeader);
  try {
    const body = record.body;
    const event: TweetV2SingleStreamResult = JSON.parse(body);
    await putEvent(event);
  } catch (err) {
    throw err;
  } finally {
    segment.close();
  }
};

export const handler: SQSHandler = async (event, _context) => {
  await Promise.all(event.Records.map(processRecord));
};
