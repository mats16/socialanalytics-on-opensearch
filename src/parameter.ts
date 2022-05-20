import { Tweetv2FieldsParams } from 'twitter-api-v2';

export const tweetFieldsParams: Partial<Tweetv2FieldsParams> = {
  // https://developer.twitter.com/en/docs/twitter-api/data-dictionary/introduction
  'tweet.fields': [
    'id', 'text', // default
    //'attachments',
    'author_id',
    'context_annotations',
    'conversation_id',
    'created_at',
    'entities',
    'geo',
    'in_reply_to_user_id',
    'lang',
    //'non_public_metrics',
    //'organic_metrics',
    'possibly_sensitive',
    //'promoted_metrics',
    'public_metrics',
    'referenced_tweets',
    'reply_settings',
    'source',
    //'withheld'
  ],
  'user.fields': [
    'id', 'name', 'username', // default
    'url',
    'verified',
    'public_metrics',
  ],
  'place.fields': [
    'id', 'full_name', // default
    'contained_within',
    'country',
    'country_code',
    'geo',
    'name',
    'place_type',
  ],
  'expansions': [
    //https://developer.twitter.com/en/docs/twitter-api/expansions
    'author_id',
    'entities.mentions.username',
    'in_reply_to_user_id',
    'referenced_tweets.id',
    'referenced_tweets.id.author_id',
  ],
};