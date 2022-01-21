import { Logger } from '@aws-lambda-powertools/logger';
import { KinesisStreamHandler, KinesisStreamRecord } from 'aws-lambda';
import { Promise } from 'bluebird';

const logger = new Logger({ logLevel: 'INFO', serviceName: 'analysis' });

interface Entity {
  score: number;
  type: string;
  text: string;
}

interface Insights {
  sentiment: string;
  sentiment_score: {
    positive: number;
    negative: number;
    neutral: number;
    mixed: number;
  };
  entities: Entity[];
}

interface Payload {
  tweet: any;
  ecs_cluster?: string;
  ecs_task_arn?: string;
  ecs_task_definition?: string;
  tag?: string;
  analysis?: {
    normalized_text: string;
    comprehend: Insights;
  };
};

const processRecord = async (record: KinesisStreamRecord) => {
  const payload: Payload = JSON.parse(Buffer.from(record.kinesis.data, 'base64').toString('utf8'));
  //const tweet = payload.tweet;
  console.log(payload);
};

export const handler: KinesisStreamHandler = async (event) => {
  await Promise.map(event.Records, processRecord);
};