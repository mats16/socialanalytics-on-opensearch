import { getLogger } from './logger';
import { twitterStreamProducer } from './twitter_stream_producer';

const logger = getLogger('main');

twitterStreamProducer().catch(err => {
  logger.error({ message: err });
});
