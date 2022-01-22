const util = require('util');
const Twit = require('twit');
const fluentLogger = require('fluent-logger');

import { config } from './config';
import { twitterConfig } from './twitter_reader_config';
import { getLogger } from './logger';

const logger = getLogger();
fluentLogger.configure('twitter', {});

export function twitterStreamProducer() {
  const waitBetweenPutRecordsCallsInMilliseconds = config.waitBetweenPutRecordsCallsInMilliseconds;
  const T = new Twit(twitterConfig.credentials);

  function _sendToFluent() {

    const twitterParams = {
      track: twitterConfig.topics,
      language: twitterConfig.languages,
      filter_level: twitterConfig.filterLevel,
      stall_warnings: true,
    };

    const stream = T.stream('statuses/filter', twitterParams);

    logger.info('start streaming...');
    stream.on('tweet', function (tweet: any) {
      fluentLogger.emit('stream', { tweet });
    },
    );
  }

  return {
    run: function () {
      logger.info(util.format('Configured wait between consecutive PutRecords call in milliseconds: %d', waitBetweenPutRecordsCallsInMilliseconds));
      _sendToFluent();
    },
  };
}
