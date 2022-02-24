import { TweetV2SingleStreamResult } from 'twitter-api-v2';

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
};

export interface TweetStreamRecord extends Partial<TweetV2SingleStreamResult> {
  data: TweetV2SingleStreamResult['data'];
  analysis?: Analysis;
  backup?: boolean;
};

export const TweetStreamParse = (b64string: string) => {
  const record = JSON.parse(Buffer.from(b64string, 'base64').toString('utf8'));
  return record as TweetStreamRecord;
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
