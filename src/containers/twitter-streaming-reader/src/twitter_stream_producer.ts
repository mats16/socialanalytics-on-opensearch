import { trace, SpanKind, ROOT_CONTEXT } from '@opentelemetry/api';
import { TwitterApi, ETwitterStreamEvent, Tweetv2FieldsParams, TweetV2SingleStreamResult } from 'twitter-api-v2';
import { getLogger } from './logger';
import { putTweetEvent, sendQueueMessage } from './utils';

const eventBusArn = process.env.EVENT_BUS_ARN;
const deadLetterQueueUrl = process.env.DEAD_LETTER_QUEUE_URL || 'N/A';
const twitterBearerToken: string = process.env.TWITTER_BEARER_TOKEN!;
const twitterFieldsParams: Partial<Tweetv2FieldsParams> = JSON.parse(process.env.TWITTER_FIELDS_PARAMS || '{}');

const tracer = trace.getTracer('my-tracer');
const logger = getLogger();

const publishEvent = async (event: TweetV2SingleStreamResult) => {
  if (typeof eventBusArn == 'undefined') {
    console.log(JSON.stringify(event));
  } else {
    try {
      await putTweetEvent(eventBusArn, event);
    } catch {
      await sendQueueMessage(deadLetterQueueUrl, event);
    }
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
