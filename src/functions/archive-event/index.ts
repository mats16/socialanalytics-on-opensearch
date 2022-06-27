import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { FirehoseClient, PutRecordCommand } from '@aws-sdk/client-firehose';
import { fromUtf8 } from '@aws-sdk/util-utf8-node';
import { EventBridgeHandler } from 'aws-lambda';
import { TweetV2SingleStreamResult } from 'twitter-api-v2';

const region = process.env.AWS_REGION || 'us-west-2';
const deliveryStreamName = process.env.DELIVERY_STREAM_NAME;

const logger = new Logger();
const metrics = new Metrics();
const tracer = new Tracer();

const firehose = tracer.captureAWSv3Client(new FirehoseClient({ region }));

const putRecord = async(result: TweetV2SingleStreamResult) => {
  const cmd = new PutRecordCommand({
    DeliveryStreamName: deliveryStreamName,
    Record: {
      Data: fromUtf8(JSON.stringify(result)),
    },
  });
  await firehose.send(cmd);
};

export const handler: EventBridgeHandler<'Tweet', TweetV2SingleStreamResult, void> = async(event, _context) => {
  await putRecord(event.detail);
  //event.detail.data.context_annotations?.shift()?.domain
};
