const twitter_config = module.exports = {
    twitter: {
        consumer_key: process.env.CONSUMER_KEY,
        consumer_secret: process.env.CONSUMER_SECRET,
        access_token: process.env.ACCESS_TOKEN,
        access_token_secret: process.env.ACCESS_TOKEN_SECRET
    },
    topics: process.env.TWITTER_TOPICS.split(','),
    languages: process.env.TWITTER_LANGUAGES.split(','),
    filter_level: process.env.TWITTER_FILTER_LEVEL,
    dest: process.env.DESTINATION.split(',')
}
