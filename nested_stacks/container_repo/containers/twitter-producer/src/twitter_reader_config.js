'use strict';

const twitter_config = module.exports = {
    credentials: JSON.parse(process.env.TWITTER_CREDENTIALS),
    topics: process.env.TWITTER_TOPICS.split(','),
    languages: process.env.TWITTER_LANGUAGES.split(','),
    filter_level: process.env.TWITTER_FILTER_LEVEL
}
