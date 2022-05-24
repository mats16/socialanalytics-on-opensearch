import { Readable } from 'stream';
import zlib from 'zlib';
import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';
import { S3Client, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { fromUtf8 } from '@aws-sdk/util-utf8-node';
import { SQSHandler, S3Event, S3EventRecord } from 'aws-lambda';
import { Promise } from 'bluebird';
import { TweetStreamRecord, KinesisEmulatedEvent, KinesisEmulatedRecord } from '../utils';

const region = process.env.AWS_REGION || 'us-west-2';
const indexingFunctionArn = process.env.INDEXING_FUNCTION_ARN!;

const logger = new Logger();
const metrics = new Metrics();
const tracer = new Tracer();

const s3 = tracer.captureAWSv3Client(new S3Client({ region }));

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
  if (typeof objectBody == 'string') {
    const lines = objectBody.trimEnd().split('\n');
    const records: TweetStreamRecord[] = lines.map(line => JSON.parse(line));
    return records;
  } else {
    return [];
  }
};

const getAllObjects = async(records: S3EventRecord[]): Promise<TweetStreamRecord[]> => {
  const objectBodyArray = await Promise.map(records, getObject);
  const tweetStreamRecords = objectBodyArray.flatMap(body => bodyToLines(body));
  return tweetStreamRecords;
};

const toKinesisEvent = (records: TweetStreamRecord[]): KinesisEmulatedEvent => {
  const emulatedEvent: KinesisEmulatedEvent = {
    Records: records.map(record => {
      const emulatedRecord: KinesisEmulatedRecord = { kinesis: { data: Buffer.from(JSON.stringify(record)).toString('base64') } };
      return emulatedRecord;
    }),
  };
  return emulatedEvent;
};

const invokeFunction = async(functionName: string, event: KinesisEmulatedEvent) => {
  const client = tracer.captureAWSv3Client(new LambdaClient({ region }));
  const cmd = new InvokeCommand({
    FunctionName: functionName,
    Payload: fromUtf8(JSON.stringify(event)),
  });
  await client.send(cmd);
};

const dataLoader = async (tweetStreamRecords: TweetStreamRecord[], inprogress: TweetStreamRecord[] = [], i: number = 0) => {
  const record = tweetStreamRecords[i];
  inprogress.push(record);
  if (i+1 == tweetStreamRecords.length || inprogress.length == 500) {
    const emulatedEvent = toKinesisEvent(inprogress);
    await invokeFunction(indexingFunctionArn, emulatedEvent);
    if (i+1 == tweetStreamRecords.length) {
      return;
    } else {
      inprogress.length = 0;
    }
  }
  await dataLoader(tweetStreamRecords, inprogress, i+1);
};

const deleteAllObjects = async(s3EventRecords: S3EventRecord[]) => {
  await Promise.map(s3EventRecords, deleteObject);
  return;
};

export const handler: SQSHandler = async(event, _context) => {
  const sqsRecords = event.Records;
  const s3Events: S3Event[] = sqsRecords.map(record => JSON.parse(record.body));
  const s3EventRecords = s3Events.flatMap(s3Event => s3Event.Records);
  const tweetStreamRecords = await getAllObjects(s3EventRecords);
  await dataLoader(tweetStreamRecords);
  await deleteAllObjects(s3EventRecords);
  metrics.publishStoredMetrics();
};
