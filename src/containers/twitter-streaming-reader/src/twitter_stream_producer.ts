import { TwitterApi, ETwitterStreamEvent, TweetSearchV2StreamParams } from 'twitter-api-v2';
import { getLogger } from './logger';

const logger = getLogger();
const tweetLogger = getLogger('tweets/search/stream');

const twitterBearerToken: string = process.env.TWITTER_BEARER_TOKEN!;

const client = new TwitterApi(twitterBearerToken);

export const twitterStreamProducer = async () => {

  const options: Partial<TweetSearchV2StreamParams> = {
    'tweet.fields': [ // https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
      'id', 'text', // default
      //'attachments',
      'author_id',
      'context_annotations',
      'conversation_id',
      'created_at',
      'entities',
      'geo',
      'in_reply_to_user_id',
      'lang',
      //'non_public_metrics',
      //'organic_metrics',
      'possibly_sensitive',
      //'promoted_metrics',
      'public_metrics',
      'referenced_tweets',
      'reply_settings',
      'source',
      //'withheld'
    ],
    'user.fields': [ // https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/user
      'id', 'name', 'username', // default
      'public_metrics',
    ],
    'place.fields': [
      'contained_within', 'country', 'country_code', 'full_name', 'geo', 'id', 'name', 'place_type',
    ],
    'expansions': ['author_id', 'entities.mentions.username', 'referenced_tweets.id', 'referenced_tweets.id.author_id'],
  };
  logger.info({ message: 'Connect to twitter...' });
  const stream = await client.v2.searchStream({ ...options, autoConnect: true });

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
