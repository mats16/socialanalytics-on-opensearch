import { Readable } from 'stream';
import { Sha256 } from '@aws-crypto/sha256-js';
import { Entity } from '@aws-sdk/client-comprehend';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { NodeHttpHandler } from '@aws-sdk/node-http-handler';
import { HttpRequest, HttpResponse } from '@aws-sdk/protocol-http';
import { SignatureV4 } from '@aws-sdk/signature-v4';
import { UserV2, TweetPublicMetricsV2, TTweetReplySettingsV2, ReferencedTweetV2, TweetGeoV2, TweetEntitiesV2, TweetContextAnnotationV2 } from 'twitter-api-v2';
import { TweetItem, ComprehendJobOutput, Deduplicate } from '../utils';

const entityScoreThreshold = 0.8;
const region = process.env.AWS_REGION || 'us-west-2';

export interface BulkUpdateHeader {
  update: {
    _index: string;
    _id?: string;
  };
};;

export interface BulkUpdateDocument {
  doc: Document;
  doc_as_upsert?: boolean;
}

interface Metadata {
  _index: string;
  _id?: string;
};

// https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
interface Document {
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

type BulkResponseItem = {
  [key in 'create'|'delete'|'index'|'update']: {
    [key: string]: any;
    _index: string;
    _id: string;
    status: number;
    error?: {
      type: string;
      reason: string;
      index: string;
      shard: string;
      index_uuid: string;
    };
  };
};;
interface BulkResponse {
  took: number;
  errors: boolean;
  items: BulkResponseItem[];
};

const asBuffer = async (response: HttpResponse) => {
  const stream = response.body as Readable;
  const chunks: Buffer[] = [];
  return new Promise<Buffer>((resolve, reject) => {
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('error', (err) => reject(err));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
  });
};
const responseParse = async (response: HttpResponse) => {
  const buffer = await asBuffer(response);
  const bufferString = buffer.toString();
  return JSON.parse(bufferString);
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

const toDocument = (tweet: TweetItem): Document => {
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

export const toBulkAction = (tweet: TweetItem): [BulkUpdateHeader, BulkUpdateDocument] => {
  const date = (tweet.created_at) ? new Date(tweet.created_at) : new Date();
  const index = 'tweets-' + date.toISOString().substring(0, 7);
  const header: BulkUpdateHeader = {
    update: {
      _index: index,
      _id: tweet.id,
    },
  };
  const updateDoc: BulkUpdateDocument = {
    doc: toDocument(tweet),
    doc_as_upsert: true,
  };
  return [header, updateDoc];
};

const httpClient = new NodeHttpHandler();

export const sendBulkOperation = async(host: string, operations: [BulkUpdateHeader, BulkUpdateDocument][]): Promise<BulkResponse> => {
  const body = operations.flatMap(ops => ops.map(x => JSON.stringify(x))).join('\n') + '\n';
  const request = new HttpRequest({
    headers: { 'Content-Type': 'application/json', host },
    hostname: host,
    method: 'POST',
    path: '_bulk',
    body: body,
  });
  const signer = new SignatureV4({
    credentials: defaultProvider(),
    region: region,
    service: 'es',
    sha256: Sha256,
  });
  const signedRequest = await signer.sign(request) as HttpRequest;
  const { response } = await httpClient.handle(signedRequest);
  const res: BulkResponse = await responseParse(response);
  return res;
};