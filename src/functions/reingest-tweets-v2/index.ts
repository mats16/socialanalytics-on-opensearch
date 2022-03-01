import { Readable } from 'stream';
import zlib from 'zlib';
import { LambdaInterface } from '@aws-lambda-powertools/commons';
import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { KinesisClient, PutRecordsCommand, PutRecordsRequestEntry } from '@aws-sdk/client-kinesis';
import { S3Client, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { SQSHandler, SQSEvent, S3Event, S3EventRecord, Context } from 'aws-lambda';
import { Promise } from 'bluebird';
import { TweetStreamRecord } from '../utils';

const region = process.env.AWS_REGION || 'us-west-2';
const destStreamName = process.env.DEST_STREAM_NAME!;

const logger = new Logger();
const metrics = new Metrics();
const tracer = new Tracer();

const s3 = tracer.captureAWSv3Client(new S3Client({ region }));
const kinesis = tracer.captureAWSv3Client(new KinesisClient({ region, maxAttempts: 10 }));

const asBuffer = async (data: unknown): Promise<Buffer> => {
  const stream = data as Readable;
  const chunks: Buffer[] = [];
  const buffer = await new Promise<Buffer>((resolve, reject) => {
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('error', (err) => reject(err));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
  });
  return buffer;
};

const getObject = async (record: S3EventRecord): Promise<string|undefined> => {
  let data: string|undefined;
  try {
    const bucket = record.s3.bucket.name;
    const key = record.s3.object.key.replace(/%3D/g, '=');
    const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
    const { Body, ContentEncoding } = await s3.send(cmd);
    const buffer = await asBuffer(Body);
    if (ContentEncoding == 'gzip') {
      data = zlib.gunzipSync(buffer).toString();
    } else {
      data = buffer.toString();
    }
  } catch (err) {
    logger.error({ message: JSON.stringify(err) });
    data = undefined;
  };
  return data;
};

const deleteObject = async(record: S3EventRecord)=> {
  const bucket = record.s3.bucket.name;
  const objectKey = record.s3.object.key.replace(/%3D/g, '=');
  const cmd = new DeleteObjectCommand({ Bucket: bucket, Key: objectKey });
  try {
    await s3.send(cmd);
  } catch (err: any) {
    logger.warn({ message: JSON.stringify(err) });
  }
  return;
};

const bodyToLines = (objectBody: string|undefined): TweetStreamRecord[] => {
  if (objectBody) {
    const lines = objectBody.trimEnd().split('\n');
    const records: TweetStreamRecord[] = lines.map(line => JSON.parse(line));
    const newRecords = records.map(record => {
      const newRecord: TweetStreamRecord = {
        ...record,
        backup: true,
      };
      return newRecord;
    });
    return newRecords;
  } else {
    return [];
  }
};

const kinesisStreamLoader = async (tweetStreamRecords: TweetStreamRecord[], inprogress: PutRecordsRequestEntry[] =[], i: number = 0) => {
  const record = tweetStreamRecords[i];
  const entry: PutRecordsRequestEntry = {
    PartitionKey: record.data.id,
    Data: Buffer.from(JSON.stringify(record)),
  };
  inprogress.push(entry);
  if (i+1 == tweetStreamRecords.length || inprogress.length == 500) {
    const cmd = new PutRecordsCommand({
      StreamName: destStreamName,
      Records: inprogress,
    });
    const { FailedRecordCount, $metadata } = await kinesis.send(cmd);
    if (i+1 == tweetStreamRecords.length) {
      return;
    } else {
      inprogress.length = 0;
    }
  }
  await kinesisStreamLoader(tweetStreamRecords, inprogress, i+1);
  return;
};

class Lambda implements LambdaInterface {

  @tracer.captureMethod()
  private async getAllObjects(records: S3EventRecord[]): Promise<TweetStreamRecord[]> {
    const objectBodyArray = await Promise.map(records, getObject);
    const tweetStreamRecords = objectBodyArray.flatMap(body => bodyToLines(body));
    return tweetStreamRecords;
  };

  @tracer.captureMethod()
  private async putKinesis(tweetStreamRecords: TweetStreamRecord[]) {
    await kinesisStreamLoader(tweetStreamRecords);
    return;
  };

  @tracer.captureMethod()
  public async deleteAllObjects(s3EventRecords: S3EventRecord[]) {
    await Promise.map(s3EventRecords, deleteObject);
    return;
  };

  @metrics.logMetrics()
  @tracer.captureLambdaHandler()
  public async handler(event: SQSEvent, _context: Context): Promise<void> {
    const sqsRecords = event.Records;
    const s3Events: S3Event[] = sqsRecords.map(record => JSON.parse(record.body));
    const s3EventRecords = s3Events.flatMap(s3Event => s3Event.Records);
    const tweetStreamRecords = await this.getAllObjects(s3EventRecords);
    await this.putKinesis(tweetStreamRecords);
    await this.deleteAllObjects(s3EventRecords);
    metrics.publishStoredMetrics();
    return;
  };

};

export const myFunction = new Lambda();
export const handler: SQSHandler = myFunction.handler;
