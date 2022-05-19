import { EventBridgeClient, PutEventsCommand, PutEventsRequestEntry } from '@aws-sdk/client-eventbridge';
import { KinesisStreamHandler } from 'aws-lambda';
import { TweetStreamParse, TweetStreamRecord } from '../utils';

const eventBusName = process.env.EVENT_BUS_NAME!;

const eventBridge = new EventBridgeClient({});

const tweetType = (record: TweetStreamRecord) => {
  const referencedTweet = record.data.referenced_tweets?.pop();
  switch (referencedTweet?.type) {
    case 'retweeted': return 'Retweet';
    case 'quoted': return 'Quote';
    case 'replied_to': return 'Reply';
    default: return 'Tweet';
  }
};

const toEntry = (record: TweetStreamRecord): PutEventsRequestEntry => {
  const entry: PutEventsRequestEntry = {
    EventBusName: eventBusName,
    Source: 'twitter',
    Time: new Date(record.data.created_at!),
    DetailType: tweetType(record),
    Detail: JSON.stringify(record),
  };
  return entry;
};

const putEvents = async(records: TweetStreamRecord[], entries: PutEventsRequestEntry[] = [], i: number = 0) => {
  const record = records[i];
  delete record.backup;
  const entry = toEntry(record);
  entries.push(entry);
  if (i+1 == records.length || entries.length == 10) {
    const cmd = new PutEventsCommand({ Entries: entries });
    await eventBridge.send(cmd);
    if (i+1 == records.length) {
      return;
    } else {
      entries.length = 0;
    }
  }
  await putEvents(records, entries, i+1);
  return;
};

export const handler: KinesisStreamHandler = async (event, _context) => {
  const kinesisStreamRecords = event.Records;
  const tweetStreamRecords = kinesisStreamRecords.map(record => TweetStreamParse(record.kinesis.data));
  const liveTweetStreamRecords = tweetStreamRecords.filter(record => { return record.backup; });
  await putEvents(liveTweetStreamRecords);
};
