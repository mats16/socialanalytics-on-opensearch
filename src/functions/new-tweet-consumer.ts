//import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { EventBridgeHandler } from 'aws-lambda';
import { TweetV2SingleStreamResult } from 'twitter-api-v2';
import { TweetItem } from './types';
import { toUnixtime, ComprehendStateMachine } from './utils';

//const logger = new Logger();
const metrics = new Metrics();
const tracer = new Tracer();

const region = process.env.AWS_REGION;
const tweetTableName = process.env.TWEET_TABLE_NAME!;
const comprehendStateMachineArn = process.env.COMPREHEND_SFN_ARN!;

const comprehend = new ComprehendStateMachine(comprehendStateMachineArn);

const marshallOptions = {
  convertEmptyValues: true, // false, by default.
  removeUndefinedValues: true, // false, by default.
  convertClassInstanceToMap: true, // false, by default.
};
const unmarshallOptions = {
  wrapNumbers: false, // false, by default.
};
const translateConfig = { marshallOptions, unmarshallOptions };

const ddbClient = tracer.captureAWSv3Client(new DynamoDBClient({ region }));
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient, translateConfig);

const putTweetItem = async (item: TweetItem) => {
  const cmd = new PutCommand({
    TableName: tweetTableName,
    Item: item,
  });
  await ddbDocClient.send(cmd);
};

export const handler: EventBridgeHandler<'Tweet', TweetV2SingleStreamResult, void> = async(event, _context) => {
  const updated_at = toUnixtime(event.time); // seconds
  const tweetEvent = event.detail;
  const tweet = tweetEvent.data;
  await putTweetItem({
    ...tweet,
    updated_at,
    author: tweetEvent.includes?.users?.find(user => user.id == tweet.author_id),
    comprehend: await comprehend.analyzeText(tweet.text, tweet.lang),
    _all: 'stub',
  });

  metrics.publishStoredMetrics();
};
