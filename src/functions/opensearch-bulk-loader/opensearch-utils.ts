import { Entity } from '@aws-sdk/client-comprehend';
import { UserV2, TweetPublicMetricsV2, TTweetReplySettingsV2, ReferencedTweetV2, TweetGeoV2, TweetEntitiesV2, TweetContextAnnotationV2 } from 'twitter-api-v2';
import { TweetItem, ComprehendJobOutput } from '../types';
import { Deduplicate } from '../utils';

const entityScoreThreshold = 0.8;

// https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
export interface Document {
  id: string;
  text?: string;
  normalized_text?: string;
  url: string;
  author?: UserV2;
  context_annotations?: {
    domain?: string[];
    entity?: string[];
  };
  conversation_id?: string;
  created_at?: string;
  entities?: {
    annotation?: string[];
    cashtag?: string[];
    hashtag?: string[];
    mention?: string[];
    url?: {
      display_domain?: string[];
      display_url?: string[];
      expanded_url?: string[];
      title?: string[];
      description?: string[];
    };
  };
  geo?: {
    coordinates?: {
      type?: string;
      coordinates?: [number, number];
    };
    place_id?: string;
  };
  in_reply_to_user_id?: string;
  lang?: string;
  possibly_sensitive?: boolean;
  public_metrics?: TweetPublicMetricsV2;
  referenced_tweets?: {
    type?: ('retweeted'|'quoted'|'replied_to')[];
    id?: string[];
  };
  reply_setting?: TTweetReplySettingsV2;
  source?: string;
  comprehend?: {
    entities?: string[];
    sentiment?: string;
    sentiment_score?: {
      positive?: number;
      negative?: number;
      neutral?: number;
      mixed?: number;
    };
  };
};

const convertContextAnnotations = (contextAnnotations?: TweetContextAnnotationV2[]) => {
  let result = undefined;
  if (typeof contextAnnotations != 'undefined') {
    result = {
      domain: Deduplicate(contextAnnotations.map(x => x.domain.name) || []),
      entity: Deduplicate(contextAnnotations.map(x => x.entity.name) || []),
    };
  };
  return result;
};

const convertTweetEntities = (entities?: TweetEntitiesV2) => {
  let result = undefined;
  if (typeof entities != 'undefined') {
    result = {
      annotation: entities?.annotations?.map(x => x.normalized_text?.toLowerCase()),
      cashtag: entities?.cashtags?.map(x => x.tag?.toLowerCase()),
      hashtag: entities?.hashtags?.map(x => x.tag?.toLowerCase()),
      mention: entities?.mentions?.map(x => x.username?.toLowerCase()),
      url: {
        display_domain: Deduplicate(entities?.urls?.map(entityUrl => entityUrl.display_url.split('/').shift()).filter((item): item is string => typeof item == 'string')) as string[],
        display_url: entities?.urls?.map(entityUrl => entityUrl.display_url),
        expanded_url: entities?.urls?.map(entityUrl => entityUrl.expanded_url),
        title: entities?.urls?.map(entityUrl => entityUrl.title).filter((item): item is string => typeof item == 'string'),
        description: entities?.urls?.map(entityUrl => entityUrl.description).filter((item): item is string => typeof item == 'string'),
      },
    };
  };
  return result;
};

const convertTweetGeo = (tweetGeo?: TweetGeoV2) => {
  let result = undefined;
  if (typeof tweetGeo?.coordinates != 'undefined' || typeof tweetGeo?.place_id != 'undefined') {
    result = {
      coordinates: {
        type: tweetGeo.coordinates?.type,
        coordinates: tweetGeo.coordinates?.coordinates || undefined,
      },
      place_id: tweetGeo.place_id,
    };
  }
  return result;
};

const convertReferencedTweets = (referencedTweets?: ReferencedTweetV2[]) => {
  let result = undefined;
  if (typeof referencedTweets != 'undefined') {
    result = {
      type: referencedTweets.map(x => x.type),
      id: referencedTweets.map(x => x.id),
    };
  };
  return result;
};

const comprehendEntitiesToString = (entities: Entity[]): string[] => {
  const filteredEntities = entities.filter(entity => {
    if (entity.Type == 'QUANTITY' || entity.Type == 'DATE') {
      return false;
    } else if (entity.Text!.length < 2 || entity.Text!.startsWith('@') ) {
      return false;
    } else if (entity.Score! < entityScoreThreshold ) {
      return false;
    } else {
      return true;
    }
  });
  const result: string[] = Deduplicate(filteredEntities.map(entity => entity.Text!.toLowerCase()));
  return result;
};

const convertComprehendOutput = (output?: ComprehendJobOutput) => {
  let result = undefined;
  if (typeof output != 'undefined') {
    const { Sentiment, SentimentScore, Entities } = output;
    result = {
      entities: (typeof Entities == 'undefined') ? undefined : comprehendEntitiesToString(Entities),
      sentiment: Sentiment,
      sentiment_score: {
        positive: SentimentScore?.Positive,
        negative: SentimentScore?.Negative,
        neutral: SentimentScore?.Neutral,
        mixed: SentimentScore?.Mixed,
      },
    };
  }
  return result;
};

export const toDocument = (tweet: TweetItem): Document => {
  const doc: Document = {
    ...tweet,
    comprehend: convertComprehendOutput(tweet.comprehend),
    context_annotations: convertContextAnnotations(tweet.context_annotations),
    entities: convertTweetEntities(tweet.entities),
    geo: convertTweetGeo(tweet.geo),
    referenced_tweets: convertReferencedTweets(tweet.referenced_tweets),
    url: `https://twitter.com/0/status/${tweet.id}`,
  };
  if (doc.referenced_tweets?.type?.includes('retweeted')) {
    doc.public_metrics = {
      like_count: 0,
      quote_count: 0,
      reply_count: 0,
      retweet_count: 0,
    };
  }
  return doc;
};
