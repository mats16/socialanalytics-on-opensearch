import { SentimentScore, Entity } from '@aws-sdk/client-comprehend';
import { TweetV2SingleStreamResult, TweetV2, UserV2 } from 'twitter-api-v2';

export interface KinesisEmulatedRecord {
  kinesis: {
    data: string;
  };
}

export interface KinesisEmulatedEvent {
  Records: KinesisEmulatedRecord[];
}

export interface ComprehendJobOutput {
  NormalizedText?: string;
  Entities?: Entity[];
  Sentiment?: string;
  SentimentScore?: SentimentScore;
  //KeyPhrases?: KeyPhrase[];
}

export interface TweetEvent extends TweetV2SingleStreamResult {
  data: TweetV2 & { comprehend?: ComprehendJobOutput };
}

export interface TweetItem extends Partial<TweetV2> {
  id: string;
  updated_at: number;
  created_at_year?: string;
  normalized_text?: string;
  comprehend?: ComprehendJobOutput;
  author?: UserV2;
}
