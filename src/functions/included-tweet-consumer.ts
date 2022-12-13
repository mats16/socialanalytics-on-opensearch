//import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { LambdaClient, InvokeCommand, InvocationType } from '@aws-sdk/client-lambda';
import { DynamoDBDocumentClient, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { fromUtf8 } from '@aws-sdk/util-utf8-node';
import { EventBridgeHandler } from 'aws-lambda';
import { TweetV2SingleStreamResult } from 'twitter-api-v2';
import { TweetItem } from './types';
import { toUnixtime } from './utils';

//const logger = new Logger();
const metrics = new Metrics();
const tracer = new Tracer();

const region = process.env.AWS_REGION;
const tweetTableName = process.env.TWEET_TABLE_NAME!;
const newTweetConsumerArn = process.env.NEW_TWEET_CONSUMER_ARN!;

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
const lambda = tracer.captureAWSv3Client(new LambdaClient({ region }));

const updateTweetItem = async(item: TweetItem) => {
  //metrics.addMetric('UpdateMetricsCount', MetricUnits.Count, 1);
  let isNewItem: boolean = false;
  const cmd = new UpdateCommand({
    TableName: tweetTableName,
    Key: { id: item.id },
    UpdateExpression: 'SET public_metrics = :p, updated_at = :u',
    ExpressionAttributeValues: {
      ':p': item.public_metrics,
      ':u': item.updated_at,
    },
    ConditionExpression: '(updated_at < :u) or (attribute_not_exists(id))',
    ReturnValues: 'ALL_NEW',
  });
  try {
    const output = await ddbDocClient.send(cmd);
    const newItem = output.Attributes as Partial<TweetItem>;
    if (typeof newItem.text == 'undefined') {
      isNewItem = true;
      //metrics.addMetric('UpdateMetricsItemNotExistCount', MetricUnits.Count, 1);
    }
  } catch (error) {
    //metrics.addMetric('UpdateMetricsErrorCount', MetricUnits.Count, 1);
    console.warn(error);
  }
  return { isNewItem };
};

export const handler: EventBridgeHandler<'IncludedTweet', TweetV2SingleStreamResult, void> = async(event, _context) => {
  const updated_at = toUnixtime(event.time); // seconds
  const tweet = event.detail.data;
  const { isNewItem } = await updateTweetItem({
    ...tweet,
    updated_at,
  });
  if (isNewItem) {
    const cmd = new InvokeCommand({
      FunctionName: newTweetConsumerArn,
      InvocationType: InvocationType.Event,
      Payload: fromUtf8(JSON.stringify(event)),
    });
    await lambda.send(cmd);
  }
  //metrics.publishStoredMetrics();
};
