//import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { EventBridgeClient, PutEventsCommand, PutEventsRequestEntry } from '@aws-sdk/client-eventbridge';
import { EventBridgeHandler } from 'aws-lambda';
import { TweetV2SingleStreamResult } from 'twitter-api-v2';

//const logger = new Logger();
const metrics = new Metrics();
const tracer = new Tracer();

const region = process.env.AWS_REGION;
const eventBusArn = process.env.EVENT_BUS_ARN!;

const eventBridge = tracer.captureAWSv3Client(new EventBridgeClient({ region }));

export const handler: EventBridgeHandler<'Tweet', TweetV2SingleStreamResult, void> = async(event, _context) => {
  const eventTime = new Date(event.time);
  const includes = event.detail.includes;
  const includedTweets = includes?.tweets || [];
  if (includedTweets.length > 0) {
    const entries = includedTweets.map(tweet => {
      const eventDetail: TweetV2SingleStreamResult = {
        data: tweet,
        includes,
        matching_rules: [],
      };
      const entry: PutEventsRequestEntry = {
        EventBusName: eventBusArn,
        Source: 'twitter.api.v2',
        DetailType: 'IncludedTweet',
        Detail: JSON.stringify(eventDetail),
        Time: eventTime,
      };
      return entry;
    });
    const putEventsCommand = new PutEventsCommand({ Entries: entries });
    await eventBridge.send(putEventsCommand);
  }
  //metrics.publishStoredMetrics();
};
