import * as fluentLogger from 'fluent-logger';
import { TwitterApi, ETwitterStreamEvent, Tweetv2FieldsParams, TweetV2SingleStreamResult } from 'twitter-api-v2';
import { getLogger } from './logger';

const twitterBearerToken: string = process.env.TWITTER_BEARER_TOKEN!;
const twitterFieldsParams: Partial<Tweetv2FieldsParams> = JSON.parse(process.env.TWITTER_FIELDS_PARAMS || '{}');
const loggerType: string = process.env.LOGGER_TYPE || 'stdout';

const logger = getLogger();
fluentLogger.configure('tweets', {
  host: 'localhost',
  port: 24224,
  timeout: 3.0,
  reconnectInterval: 600000, // 10 minutes
});
const tweetLogger = (result: TweetV2SingleStreamResult) => {
  if (loggerType == 'fluent') {
    const eventTime = (typeof result.data.created_at == 'string') ? new Date(result.data.created_at) : new Date();
    fluentLogger.emit(result, eventTime);
  } else {
    console.log(JSON.stringify(result));
  }
};

const client = new TwitterApi(twitterBearerToken);

export const twitterStreamProducer = async () => {

  logger.info({ message: 'Connect to twitter api v2...' });
  const stream = await client.v2.searchStream({ ...twitterFieldsParams, autoConnect: true });

  stream.on(
    // Emitted when Node.js {response} emits a 'error' event (contains its payload).
    ETwitterStreamEvent.ConnectionError,
    err => logger.error(err),
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
      eventData.errors?.map(error => logger.error(error));
      tweetLogger(eventData);
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
