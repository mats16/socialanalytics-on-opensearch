import { EventBridgeClient, PutEventsCommand, PutEventsRequestEntry } from '@aws-sdk/client-eventbridge';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import * as xray from 'aws-xray-sdk';
import { TwitterApi, ETwitterStreamEvent, Tweetv2FieldsParams, TweetV2SingleStreamResult } from 'twitter-api-v2';
import { getLogger } from './logger';

xray.enableManualMode();
const logger = getLogger();

const region = process.env.AWS_REGION || 'us-west-2';
const eventBusArn = process.env.EVENT_BUS_ARN;
const deadLetterQueueUrl = process.env.DEAD_LETTER_QUEUE_URL;
const twitterBearerToken: string = process.env.TWITTER_BEARER_TOKEN!;
const twitterFieldsParams: Partial<Tweetv2FieldsParams> = JSON.parse(process.env.TWITTER_FIELDS_PARAMS || '{}');

const eventBridge = new EventBridgeClient({ region });
const sqs = new SQSClient({ region });

const publishEvent = async (event: TweetV2SingleStreamResult, segment: xray.Segment) => {
  try {
    const client = xray.captureAWSv3Client(eventBridge, segment);
    const entry: PutEventsRequestEntry = {
      Time: (typeof event.data.created_at == 'string') ? new Date(event.data.created_at) : new Date(),
      EventBusName: eventBusArn,
      Source: 'twitter.api.v2',
      DetailType: 'Tweet',
      Detail: JSON.stringify(event),
    };
    const cmd = new PutEventsCommand({ Entries: [entry] });
    await client.send(cmd);
  } catch (e) {
    logger.error({ message: 'An error has occurred and will be sent to DLQ.' });
    const client = xray.captureAWSv3Client(sqs, segment);
    const cmd = new SendMessageCommand({
      QueueUrl: deadLetterQueueUrl,
      MessageBody: JSON.stringify(event),
    });
    await client.send(cmd);
  }
};

const client = new TwitterApi(twitterBearerToken);

export const twitterStreamProducer = async () => {

  logger.info({ message: 'Connect to twitter api v2...' });
  const stream = await client.v2.searchStream({ ...twitterFieldsParams, autoConnect: true });

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
      const seg = new xray.Segment('twitter.com');
      seg.addMetadata('tweet_id', eventData.data.id);
      publishEvent(eventData, seg).finally(() => {
        seg.close();
        seg.flush();
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
