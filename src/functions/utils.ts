import { SentimentScore, Entity, KeyPhrase } from '@aws-sdk/client-comprehend';
import { TweetV2SingleStreamResult } from 'twitter-api-v2';

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
  Sentiment?: string;
  SentimentScore?: SentimentScore;
  Entities?: Entity[];
  KeyPhrases?: KeyPhrase[];
}

export interface Analysis {
  normalized_text?: string;
  sentiment?: string;
  sentiment_score?: {
    positive?: number;
    negative?: number;
    neutral?: number;
    mixed?: number;
  };
  entities?: string[];
  //key_phrases?: string[];
}

export interface TweetStreamRecord extends Partial<TweetV2SingleStreamResult> {
  data: TweetV2SingleStreamResult['data'];
  analysis?: Analysis;
  backup?: boolean;
}

export const b64encode = (text: string) => Buffer.from(text).toString('base64');
export const b64decode = (b64string: string) => Buffer.from(b64string, 'base64').toString('utf8');

export const TweetStreamParse = (b64string: string): TweetStreamRecord => {
  const decodedText = b64decode(b64string);
  const record: TweetStreamRecord = JSON.parse(decodedText);
  return record;
};

export const Deduplicate = (array: any[]) => {
  const set = new Set(array);
  const deduplicatedArray = Array.from(set);
  return deduplicatedArray;
};

export const Normalize = (text: string): string => {
  const textWithoutRT = text.replace(/^RT /, '');
  //const tectWithoutEmoji = textWithoutRT.replace(/[\u2700-\u27BF]|[\uE000-\uF8FF]|\uD83C[\uDC00-\uDFFF]|\uD83D[\uDC00-\uDFFF]|[\u2011-\u26FF]|\uD83E[\uDD10-\uDDFF]/g, '');
  const textWithoutUrl = textWithoutRT.replace(/https?:\/\/[\w/;:%#\$&\?\(\)~\.=\+\-]+/g, '');
  const textWithoutHtml = textWithoutUrl.replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
  const textWithoutSpace = textWithoutHtml.replace(/\n/g, ' ').replace(/[\s]+/g, ' ').trimEnd();
  return textWithoutSpace;
};
