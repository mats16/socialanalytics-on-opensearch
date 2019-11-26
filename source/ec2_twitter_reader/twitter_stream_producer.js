/***
Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at

http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
***/

'use strict';

var AWS = require('aws-sdk');
var config = require('./config');
var twitter_config = require('./twitter_reader_config.js');
var Twit = require('twit');
var util = require('util');
var logger = require('./util/logger');

function twitterStreamProducer() {
  var log = logger().getLogger('producer');
  var waitBetweenPutRecordsCallsInMilliseconds = config.waitBetweenPutRecordsCallsInMilliseconds;
  var T = new Twit(twitter_config.twitter)

  function _sendToFirehose() {
    var kinesis = new AWS.Kinesis({apiVersion: '2013-12-02'});
    var firehose = new AWS.Firehose({ apiVersion: '2015-08-04' });
    var stream = T.stream('statuses/filter', { track: twitter_config.topics, language: twitter_config.languages, filter_level: twitter_config.filter_level, stall_warnings: true });

    log.info('start streaming...')
    stream.on('tweet', function (tweet) {
      var tweetString = JSON.stringify(tweet)
      // Analytics
      if (typeof tweet.retweeted_status === "undefined") {
        var kinesisParams = {
          Data: tweetString +'\n',
          PartitionKey: tweet.id_str,
          StreamName: twitter_config.analyze_stream_name,
        };
        kinesis.putRecord(kinesisParams, function (err, data) {
          if (err) {
            log.error(err);
          }
        });
      } else {
        ;
      }
      // Archives
      var firehoseParams = {
        DeliveryStreamName: twitter_config.archive_stream_name,
        Record: {
          Data: tweetString + '\n'
        }
      };
      firehose.putRecord(firehoseParams, function (err, data) {
        if (err) {
          log.error(err);
        }
      });
    }
    );
  }


  return {
    run: function () {
      log.info(util.format('Configured wait between consecutive PutRecords call in milliseconds: %d',
        waitBetweenPutRecordsCallsInMilliseconds));
      _sendToFirehose();
    }
  }
}

module.exports = twitterStreamProducer;
