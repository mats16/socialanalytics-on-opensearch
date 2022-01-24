import { Logger } from '@aws-lambda-powertools/logger';
import { KinesisStreamHandler, KinesisStreamRecord } from 'aws-lambda';
import { Promise } from 'bluebird';
import { TweetV2SingleStreamResult } from 'twitter-api-v2';

const logger = new Logger({ logLevel: 'INFO', serviceName: 'indexing' });

interface ComprehendEntity {
  score: number;
  type: string;
  text: string;
}

interface ComprehendInsights {
  sentiment: string;
  sentiment_score: {
    positive: number;
    negative: number;
    neutral: number;
    mixed: number;
  };
  entities: ComprehendEntity[];
}

interface StreamData extends Partial<TweetV2SingleStreamResult> {
  analysis?: {
    normalized_text: string;
    comprehend: ComprehendInsights;
  };
};

const processRecord = async (record: KinesisStreamRecord) => {
  const payload: StreamData = JSON.parse(Buffer.from(record.kinesis.data, 'base64').toString('utf8'));
  //const tweet = payload.tweet;
  console.log(JSON.stringify(payload));
};

export const handler: KinesisStreamHandler = async (event) => {
  await Promise.map(event.Records, processRecord);
};