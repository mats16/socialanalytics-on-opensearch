'use strict';

const config = require('./config');
const twitter_config = require('./twitter_reader_config.js');
const Twit = require('twit');
const util = require('util');
const logger = require('./util/logger');
const fluentLogger = require('fluent-logger');

fluentLogger.configure('twitter', {
  host: '127.0.0.1',
  port: 24224,
  timeout: 3.0,
  reconnectInterval: 600000 // 10 minutes
});

function twitterStreamProducer() {
  const log = logger().getLogger('producer');
  const waitBetweenPutRecordsCallsInMilliseconds = config.waitBetweenPutRecordsCallsInMilliseconds;
  const T = new Twit(twitter_config.credentials)

  function _sendToFluent() {

    const twitterParams = {
      track: twitter_config.topics,
      language: twitter_config.languages,
      filter_level: twitter_config.filter_level,
      stall_warnings: true
    }

    const stream = T.stream('statuses/filter', twitterParams);

    log.info('start streaming...')
    stream.on('tweet', function (tweet) {
      fluentLogger.emit('stream', tweet);
    }
    );
  }

  return {
    run: function () {
      log.info(util.format('Configured wait between consecutive PutRecords call in milliseconds: %d',
        waitBetweenPutRecordsCallsInMilliseconds));
      _sendToFluent();
    }
  }
}

module.exports = twitterStreamProducer;
