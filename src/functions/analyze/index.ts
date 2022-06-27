import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { DynamoDBClient, AttributeValue } from '@aws-sdk/client-dynamodb';
import { SFNClient, StartSyncExecutionCommand } from '@aws-sdk/client-sfn';
import { DynamoDBDocumentClient, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { DynamoDBStreamHandler, DynamoDBRecord } from 'aws-lambda';
//import * as xray from 'aws-xray-sdk';
import { Promise } from 'bluebird';
import { TweetItem, ComprehendJobOutput } from '../common-utils';

const region = process.env.AWS_REGION || 'us-west-2';
const tweetTableName = process.env.TWEET_TABLE_NAME!;
const comprehendJobArn = process.env.COMPREHEND_JOB_ARN!;

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

const ddbClient = new DynamoDBClient({ region });
const ddbDocClient = tracer.captureAWSv3Client(DynamoDBDocumentClient.from(ddbClient, translateConfig));
const sfn = tracer.captureAWSv3Client(new SFNClient({ region }));

const comprehendJob = async (text: string, lang?: string): Promise<ComprehendJobOutput> => {
  const cmd = new StartSyncExecutionCommand({
    stateMachineArn: comprehendJobArn,
    input: JSON.stringify({
      Text: text,
      LanguageCode: lang,
    }),
  });
  const { output } = await sfn.send(cmd);
  const result: ComprehendJobOutput = (typeof output == 'string')
    ? JSON.parse(output)
    : {};
  return result;
};

const updateTweetItem = async (item: TweetItem) => {
  const cmd = new UpdateCommand({
    TableName: tweetTableName,
    Key: { id: item.id },
    UpdateExpression: 'SET normalized_text = :n, comprehend = :c',
    ExpressionAttributeValues: {
      ':n': item.normalized_text,
      ':c': item.comprehend,
    },
    ConditionExpression: '(attribute_exists(id))',
  });
  await ddbDocClient.send(cmd);
};

const processRecord = async (record: DynamoDBRecord) => {
  const eventName = record.eventName;
  if (eventName == 'INSERT' || eventName == 'MODIFY') {
    const newImage = record.dynamodb?.NewImage as Record<string, AttributeValue>;
    const tweet = unmarshall(newImage, unmarshallOptions) as TweetItem;
    if (typeof tweet.text != 'undefined' && typeof tweet.comprehend == 'undefined') {
      const { NormalizedText, Sentiment, SentimentScore, Entities } = await comprehendJob(tweet.text, tweet.lang);
      tweet.normalized_text = NormalizedText;
      tweet.comprehend = { Entities, Sentiment, SentimentScore };
      await updateTweetItem(tweet);
    }
  }
};

export const handler: DynamoDBStreamHandler = async(event, _context) => {
  metrics.addMetric('IncomingRecordCount', MetricUnits.Count, event.Records.length);

  await Promise.map(event.Records, processRecord, { concurrency: 10 });

  metrics.publishStoredMetrics();
};
