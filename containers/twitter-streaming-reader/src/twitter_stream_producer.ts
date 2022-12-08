import { trace, SpanKind } from '@opentelemetry/api';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { TwitterApi, ETwitterStreamEvent, Tweetv2FieldsParams, TweetV2SingleStreamResult } from 'twitter-api-v2';
import { getLogger } from './logger';

const region = process.env.AWS_REGION;
const twitterBearerToken: string = process.env.TWITTER_BEARER_TOKEN!;
const twitterFieldsParams: Partial<Tweetv2FieldsParams> = JSON.parse(process.env.TWITTER_FIELDS_PARAMS || '{}');

const tracer = trace.getTracer('my-tracer');
const logger = getLogger();

const sqs = new SQSClient({ region });

const queueUrls = Object.keys(process.env).filter(key => key.startsWith('QUEUE_URL_')).sort().map(key => process.env[key]!);

const sendQueueMessage = async (queueUrls: string[], event: TweetV2SingleStreamResult) => {
  for await (let url of queueUrls) {
    const QueueUrl = url;
    const MessageBody = JSON.stringify(event);
    const MessageDeduplicationId = event.data.id;
    const MessageGroupId = event.data.id;
    const isFifo = QueueUrl.endsWith('.fifo');
    const cmd = (isFifo)
      ? new SendMessageCommand({ QueueUrl, MessageBody, MessageDeduplicationId, MessageGroupId })
      : new SendMessageCommand({ QueueUrl, MessageBody });
    try {
      await sqs.send(cmd);
      break;
    } catch (err) {
      logger.error({ message: err });
    }
  }
};

const publishEvent = async (event: TweetV2SingleStreamResult) => {
  if (queueUrls.length == 0) {
    console.log(JSON.stringify(event));
  } else {
    await sendQueueMessage(queueUrls, event);
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
      const now = new Date();
      const eventTime = (typeof eventData.data.created_at == 'string') ? new Date(eventData.data.created_at) : now;

      tracer.startActiveSpan('server', { startTime: eventTime, kind: SpanKind.SERVER }, (span) => {
        tracer.startSpan('Twitter Internal', { startTime: eventTime }).end(now);
        publishEvent(eventData).finally(() => {
          span.end();
        });
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
