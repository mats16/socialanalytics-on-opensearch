import { TwitterApi, ETwitterStreamEvent, Tweetv2FieldsParams } from 'twitter-api-v2';
import { getLogger } from './logger';

const logger = getLogger();
const tweetLogger = getLogger('tweets/search/stream');

const twitterBearerToken: string = process.env.TWITTER_BEARER_TOKEN!;
const twitterFieldsParamsString: string = process.env.TWITTER_FIELDS_PARAMS!;
const twitterFieldsParams: Partial<Tweetv2FieldsParams> = JSON.parse(twitterFieldsParamsString);

const client = new TwitterApi(twitterBearerToken);

export const twitterStreamProducer = async () => {

  logger.info({ message: 'Connect to twitter...' });
  const stream = await client.v2.searchStream({ ...twitterFieldsParams, autoConnect: true });

  stream.on(
    // Emitted when Node.js {response} emits a 'error' event (contains its payload).
    ETwitterStreamEvent.ConnectionError,
    err => logger.error({ name: err.name, message: err.message }),
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
      const { data, includes, matching_rules, errors } = eventData;
      tweetLogger.info({ data, includes, matching_rules, errors });
      logger.error({ message: 'Errors received from stream api.', errors: errors });
    },
  );

  stream.on(
    // Emitted when a Twitter sent a signal to maintain connection active
    ETwitterStreamEvent.DataKeepAlive,
    () => logger.info({ message: 'Twitter has a keep-alive packet.' }),
  );

  process.on('SIGTERM', () => {
    stream.close();
    logger.info({ message: 'SIGTERM received.' });
  });
};
