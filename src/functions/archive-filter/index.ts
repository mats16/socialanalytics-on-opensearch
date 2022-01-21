import { Logger } from '@aws-lambda-powertools/logger';
import { FirehoseTransformationHandler, FirehoseTransformationEventRecord, FirehoseTransformationResultRecord, FirehoseRecordTransformationStatus } from 'aws-lambda';

const tagName = process.env.TAG_NAME!;

const logger = new Logger({ logLevel: 'INFO', serviceName: 'analysis' });

interface Payload {
  tweet: any;
  tag?: string;
  ecs_cluster?: string;
  ecs_task_arn?: string;
  ecs_task_definition?: string;
}

const filterByTag = (record: FirehoseTransformationEventRecord) => {
  const recordId = record.recordId;
  let data = record.data;
  let result: FirehoseRecordTransformationStatus = 'Dropped';
  try {
    const payload: Payload = JSON.parse(Buffer.from(data, 'base64').toString('utf8'));
    const { tweet, tag } = payload;
    if (tag == tagName) {
      data = Buffer.from(JSON.stringify(tweet)).toString('base64');
      result = 'Ok';
    };
  } catch (error) {
    logger.error(`ProcessingFailed: ${error}`);
    result = 'ProcessingFailed';
  };
  const resultRecord: FirehoseTransformationResultRecord = { result, recordId, data };
  return resultRecord;
};

export const handler: FirehoseTransformationHandler = async (event) => {
  const filteredRecords = event.records.map(record => filterByTag(record));
  const count = {
    ok: filteredRecords.filter(record => record.result == 'Ok').length,
    dropped: filteredRecords.filter(record => record.result == 'Dropped').length,
    processingFailed: filteredRecords.filter(record => record.result == 'ProcessingFailed').length,
  };
  logger.info(`ok: ${count.ok}, dropped: ${count.dropped}, processingFailed: ${count.processingFailed}`);
  return { records: filteredRecords };
};