import { TweetV2SingleStreamResult } from 'twitter-api-v2';

export interface TweetStreamData extends Partial<TweetV2SingleStreamResult> {
  data: TweetV2SingleStreamResult['data'];
  analysis?: {
    normalized_text: string;
    sentiment: string | undefined;
    sentiment_score: {
      positive: number | undefined;
      negative: number | undefined;
      neutral: number | undefined;
      mixed: number | undefined;
    };
    entities: string[] | undefined;
  };
  backup?: boolean;
};

export const TweetStreamParse = (data: string): TweetStreamData => {
  return JSON.parse(Buffer.from(data, 'base64').toString('utf8'));
};

export const Deduplicate = (array: string[]|undefined) => {
  if (typeof array === 'undefined') {
    return array;
  } else {
    const set = new Set(array);
    const deduplicatedArray = Array.from(set);
    return deduplicatedArray;
  };
};

export const Normalize = (text: string): string => {
  const textWithoutRT = text.replace(/^RT /, '');
  const tectWithoutEmoji = textWithoutRT.replace(/[\u2700-\u27BF]|[\uE000-\uF8FF]|\uD83C[\uDC00-\uDFFF]|\uD83D[\uDC00-\uDFFF]|[\u2011-\u26FF]|\uD83E[\uDD10-\uDDFF]/g, '');
  const textWithoutUrl = tectWithoutEmoji.replace(/https?:\/\/[\w/;:%#\$&\?\(\)~\.=\+\-]+/g, '');
  const textWithoutHtml = textWithoutUrl.replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
  const textWithoutSpace = textWithoutHtml.replace(/\n/g, ' ').replace(/[\s]+/g, ' ').trimEnd();
  return textWithoutSpace;
};
