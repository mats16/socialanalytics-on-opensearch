import { EventBridgeClient, PutEventsCommand, PutEventsRequestEntry } from '@aws-sdk/client-eventbridge';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import * as xray from 'aws-xray-sdk';
import axios from 'axios';
import { TwitterApi, ETwitterStreamEvent, Tweetv2FieldsParams, TweetV2SingleStreamResult } from 'twitter-api-v2';
import { getLogger } from './logger';

xray.enableManualMode();

const region = process.env.AWS_REGION;
const ecsMetadataUri = process.env.ECS_CONTAINER_METADATA_URI_V4;
const eventBusArn = process.env.EVENT_BUS_ARN;
const twitterBearerToken: string = process.env.TWITTER_BEARER_TOKEN!;
const twitterFieldsParams: Partial<Tweetv2FieldsParams> = JSON.parse(process.env.TWITTER_FIELDS_PARAMS || '{}');

const logger = getLogger();

const eventBridge = new EventBridgeClient({ region });
const sqs = new SQSClient({ region });

const getPluginData = async () => {
  if (typeof ecsMetadataUri == 'string') {
    const { data } = await axios.get(ecsMetadataUri);
    const pluginData = {
      container: data.DockerName as string,
      container_id: data.DockerId as string,
      container_arn: data.ContainerARN as string,
    };
    return pluginData;
  } else {
    return {};
  }
};

const startSegment = (event: TweetV2SingleStreamResult) => {
  const segment = new xray.Segment('Twitter Stream Producer');
  segment.origin = 'AWS::ECS::Container';
  getPluginData()
    .then(data => segment.addPluginData(data))
    .catch(err => logger.error(err));
  segment.addMetadata('id', event.data.id, 'tweet');
  return segment;
};

const putEvent = async (event: TweetV2SingleStreamResult, segment: xray.Segment) => {
  const client = xray.captureAWSv3Client(eventBridge, segment);
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

const sendDeadLetterQueueMessage = async (message: object, segment: xray.Segment) => {
  logger.warn({ message: 'An error has occurred and will be sent to DLQ.' });
  try {
    const client = xray.captureAWSv3Client(sqs, segment);
    const cmd = new SendMessageCommand({
      QueueUrl: process.env.DEAD_LETTER_QUEUE_URL,
      MessageBody: JSON.stringify(message),
    });
    await client.send(cmd);
  } catch (err) {
    logger.error(err);
  }
};

const publishEvent = async (event: TweetV2SingleStreamResult, segment: xray.Segment) => {
  if (typeof eventBusArn == 'string') {
    try {
      await putEvent(event, segment);
    } catch {
      await sendDeadLetterQueueMessage(event, segment);
    }
  } else {
    console.log(JSON.stringify(event));
  }
};

const tw = new TwitterApi(twitterBearerToken);

export const twitterStreamProducer = async () => {

  logger.info({ message: 'Connect to twitter api v2...' });
  const stream = await tw.v2.searchStream({ ...twitterFieldsParams, autoConnect: true });

  stream.on(
    // Emitted when Node.js {response} emits a 'error' event (contains its payload).
    ETwitterStreamEvent.ConnectionError,
    err => logger.error({ name: err.name, message: err.message, cause: err.cause, stack: err.stack }),
  );

  stream.on(
    // Emitted when Node.js {response} is closed by remote or using .close().
    ETwitterStreamEvent.ConnectionClosed,
    () => logger.info({ message: 'Connection has been closed.' }),
  );

  stream.on(
    // Emitted when a Twitter payload (a tweet or not, given the endpoint).
    ETwitterStreamEvent.Data,
    eventData => {
      const segment = startSegment(eventData);
      publishEvent(eventData, segment).finally(() => {
        segment.close();
      });
      eventData.errors?.map(err => logger.error(err));
    },
  );

  stream.on(
    // Emitted when a Twitter sent a signal to maintain connection active
    ETwitterStreamEvent.DataKeepAlive,
    () => logger.info({ message: 'Twitter has a keep-alive packet.' }),
  );

  process.on('SIGTERM', () => {
    logger.info({ message: 'SIGTERM received. Try to close the connection...' });
    stream.close();
  });
};
