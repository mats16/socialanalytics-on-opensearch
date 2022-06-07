import { Readable } from 'stream';
import zlib from 'zlib';
import { Logger } from '@aws-lambda-powertools/logger';
import { Metrics } from '@aws-lambda-powertools/metrics';
import { Tracer } from '@aws-lambda-powertools/tracer';
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';
import { S3Client, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { fromUtf8 } from '@aws-sdk/util-utf8-node';
import { SQSHandler, SQSRecord, S3Event, S3EventRecord } from 'aws-lambda';
import { Promise } from 'bluebird';
import { TweetStreamRecord, KinesisEmulatedEvent, KinesisEmulatedRecord } from '../utils';

const region = process.env.AWS_REGION || 'us-west-2';
const indexingFunctionArn = process.env.INDEXING_FUNCTION_ARN!;

const logger = new Logger();
const metrics = new Metrics();
const tracer = new Tracer();

const s3 = tracer.captureAWSv3Client(new S3Client({ region }));
const lambda = tracer.captureAWSv3Client(new LambdaClient({ region }));

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

const deleteObject = async(record: S3EventRecord): Promise<void> => {
  const bucket = record.s3.bucket.name;
  const objectKey = record.s3.object.key.replace(/%3D/g, '=');
  const cmd = new DeleteObjectCommand({ Bucket: bucket, Key: objectKey });
  await s3.send(cmd);
};

const bodyToLines = (objectBody: string): TweetStreamRecord[] => {
  const lines = objectBody.trimEnd().split('\n');
  const records: TweetStreamRecord[] = lines.map(line => JSON.parse(line));
  return records;
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
  const cmd = new InvokeCommand({
    FunctionName: functionName,
    Payload: fromUtf8(JSON.stringify(event)),
  });
  await lambda.send(cmd);
};

const dataLoader = async (functionName: string, tweetStreamRecords: TweetStreamRecord[], inprogress: TweetStreamRecord[] = [], i: number = 0) => {
  const record = tweetStreamRecords[i];
  inprogress.push(record);
  if (i+1 == tweetStreamRecords.length || inprogress.length == 300) {
    const emulatedEvent = toKinesisEvent(inprogress);
    await invokeFunction(functionName, emulatedEvent);
    if (i+1 == tweetStreamRecords.length) {
      return;
    } else {
      inprogress.length = 0;
    }
  }
  await dataLoader(functionName, tweetStreamRecords, inprogress, i+1);
};

const deleteAllObjects = async(s3EventRecords: S3EventRecord[]): Promise<void> => {
  try {
    await Promise.map(s3EventRecords, deleteObject);
  } catch (err: any) {
    logger.warn({ message: JSON.stringify(err) });
  }
};

export const handler: SQSHandler = async(event, _context) => {
  const sqsRecords = event.Records;
  const s3Events: S3Event[] = sqsRecords.map(record => JSON.parse(record.body));
  const s3EventRecords = s3Events.flatMap(s3Event => s3Event.Records);
  for await (let s3EventRecord of s3EventRecords) {
    const body = await getObject(s3EventRecord);
    if (typeof body != 'undefined') {
      const tweetStreamRecords = bodyToLines(body);
      await dataLoader(indexingFunctionArn, tweetStreamRecords);
    }
  }
  await deleteAllObjects(s3EventRecords);
  metrics.publishStoredMetrics();
};
