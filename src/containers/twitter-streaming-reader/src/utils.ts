import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { TweetV2SingleStreamResult } from 'twitter-api-v2';

const region = process.env.AWS_REGION;

const eventBridge = new EventBridgeClient({ region });
const sqs = new SQSClient({ region });

export const putTweetEvent = async (eventBusName: string, event: TweetV2SingleStreamResult) => {
  const eventTime = (typeof event.data.created_at == 'string') ? new Date(event.data.created_at) : undefined;
  const cmd = new PutEventsCommand({
    Entries: [{
      EventBusName: eventBusName,
      Source: 'twitter.api.v2',
      DetailType: 'Tweet',
      Detail: JSON.stringify(event),
      Time: eventTime,
    }],
  });
  await eventBridge.send(cmd);
};

export const sendQueueMessage = async (queueUrl: string, message: object) => {
  const cmd = new SendMessageCommand({
    QueueUrl: queueUrl,
    MessageBody: JSON.stringify(message),
  });
  try {
    await sqs.send(cmd);
  } catch (err) {
    console.error(err);
  }
};