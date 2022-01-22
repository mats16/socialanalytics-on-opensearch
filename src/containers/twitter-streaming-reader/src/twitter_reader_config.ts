'use strict';

interface TwitterCredentials {
  consumer_key: string;
  consumer_secret: string;
  access_token: string;
  access_token_secret: string;
};

const credentials: TwitterCredentials = JSON.parse(process.env.TWITTER_CREDENTIALS!);
const topics: string[] = process.env.TWITTER_TOPICS?.split(',') || ['AWS'];
const languages: string[] = process.env.TWITTER_LANGUAGES?.split(',') || ['en'];
const filterLevel: string = process.env.TWITTER_FILTER_LEVEL || 'none';

export const twitterConfig = { credentials, topics, languages, filterLevel };

//export const twitterConfig: TwitterConfig {
//    credentials: JSON.parse(process.env.TWITTER_CREDENTIALS),
//    topics: process.env.TWITTER_TOPICS?.split(',') || ['AWS'],
//    languages: process.env.TWITTER_LANGUAGES?.split(',') || ['en'],
//    filter_level: process.env.TWITTER_FILTER_LEVEL
//};
