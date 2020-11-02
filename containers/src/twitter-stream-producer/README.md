# Twitter Streaming Reader

## How to use this image

```bash
$ docker run \
    -e CONSUMER_KEY=xxxxx \
    -e CONSUMER_SECRET=xxxxx \
    -e ACCESS_TOKEN=xxxxx \
    -e ACCESS_TOKEN_SECRET=xxxxx \
    mats16/twitter-streaming-reader
```

### about Twitter Authentication

- `-e CONSUMER_KEY=...`
- `-e CONSUMER_SECRET=...`
- `-e ACCESS_TOKEN=...`
- `-e ACCESS_TOKEN_SECRET=...`

### Other options

- `-e TWITTER_TOPICS=AWS,EC2,RDS`
- `-e TWITTER_LANGUAGES=en,ja`
- `-e TWITTER_FILTER_LEVEL=none`  # default: none
- `-e DESTINATION=kinesis:<your_kinesis_stream_name>,firehose:<your_firehose_stream_name>,stdout`  # default: stdout
