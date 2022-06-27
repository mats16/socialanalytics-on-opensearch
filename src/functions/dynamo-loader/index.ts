import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { EventBridgeHandler } from 'aws-lambda';
//import * as xray from 'aws-xray-sdk';
import { Promise } from 'bluebird';
import { TweetV2SingleStreamResult, TweetV2 } from 'twitter-api-v2';
import { TweetItem } from '../common-utils';

const allowedDateAfter = new Date('2019-12-01T00:00:00.000Z');
const region = process.env.AWS_REGION || 'us-west-2';
const tweetTableName = process.env.TWEET_TABLE_NAME!;
const twitterFilterContextDomainsParameterName = process.env.TWITTER_FILTER_CONTEXT_DOMAINS_PARAMETER_NAME!;
const twitterFilterSourceLabelsParameterName = process.env.TWITTER_FILTER_SOURCE_LABELS_PARAMETER_NAME!;

let twitterFilterContextDomains: string[];
let twitterFilterSourceLabels: string[];

const logger = new Logger();
const metrics = new Metrics();
const tracer = new Tracer();

const marshallOptions = {
  convertEmptyValues: true, // false, by default.
  removeUndefinedValues: true, // false, by default.
  convertClassInstanceToMap: true, // false, by default.
};
const unmarshallOptions = {
  wrapNumbers: false, // false, by default.
};
const translateConfig = { marshallOptions, unmarshallOptions };

const ssm = tracer.captureAWSv3Client(new SSMClient({ region }));
const ddbClient = tracer.captureAWSv3Client(new DynamoDBClient({ region }));
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient, translateConfig);

const getParameter = async(name: string): Promise<string[]> => {
  const cmd = new GetParameterCommand({ Name: name });
  const { Parameter } = await ssm.send(cmd);
  return Parameter?.Value?.split(',') || [];
};

const loadParameters = async () => {
  twitterFilterContextDomains = await getParameter(twitterFilterContextDomainsParameterName);
  twitterFilterSourceLabels = await getParameter(twitterFilterSourceLabelsParameterName);
};

const putTweet = async (item: TweetItem) => {
  const cmd = new PutCommand({
    TableName: tweetTableName,
    Item: item,
  });
  await ddbDocClient.send(cmd);
};

const updateTweetMetrics = async (item: TweetItem) => {
  metrics.addMetric('UpdateMetricsCount', MetricUnits.Count, 1);
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
    console.log(newItem);
    if (typeof newItem.text == 'undefined') {
      metrics.addMetric('UpdateMetricsItemNotExistCount', MetricUnits.Count, 1);
      await putTweet(item);
    }
  } catch (error) {
    metrics.addMetric('UpdateMetricsErrorCount', MetricUnits.Count, 1);
    console.warn(error);
  }
};

const sourceLabelFilter = (tweet: TweetV2): boolean => {
  const sourceLabel = tweet.source || '';
  const result = !twitterFilterSourceLabels.includes(sourceLabel);
  return result;
};

const contextDomainFilter = (tweet: TweetV2): boolean => {
  const contextAnnotationsDomains = tweet.context_annotations?.map(a => a.domain.name) || [];
  const result = !contextAnnotationsDomains.some(domain => twitterFilterContextDomains.includes(domain));
  return result;
};

const createdAtFilter = (tweet: TweetV2): boolean => {
  return typeof tweet.created_at == 'string' && new Date(tweet.created_at) > allowedDateAfter;
};

const toUnixtime = (text?: string): number => {
  const unixtime = (typeof text == 'string') ? new Date(text).valueOf() : Date.now(); // microseconds
  return Math.floor(unixtime / 1000); // seconds
};

const processTweetStreamResult = async (result: TweetV2SingleStreamResult) => {
  const { data, includes = {} } = result;
  const { users: includedUsers = [], tweets: includedTweets = [] } = includes;
  const updated_at = toUnixtime(data.created_at); // seconds
  await putTweet({
    ...data,
    author: includedUsers.find(user => user.id == data.author_id),
    created_at_year: data.created_at?.split('-').shift(),
    updated_at,
  });

  const validTweets = includedTweets.filter(sourceLabelFilter).filter(contextDomainFilter).filter(createdAtFilter);

  await Promise.map(validTweets, async(tweet) => {
    await updateTweetMetrics({
      ...tweet,
      author: includedUsers.find(user => user.id == tweet.author_id),
      created_at_year: tweet.created_at?.split('-').shift(),
      updated_at,
    });
  });

};

export const handler: EventBridgeHandler<'Tweet', TweetV2SingleStreamResult, void> = async(event, _context) => {
  await loadParameters();

  const result = event.detail;
  if (sourceLabelFilter(result.data) && contextDomainFilter(result.data) && createdAtFilter(result.data)) {
    await processTweetStreamResult(result);
  }

  metrics.publishStoredMetrics();
};
