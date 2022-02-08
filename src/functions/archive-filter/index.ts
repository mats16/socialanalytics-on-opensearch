import { Logger } from '@aws-lambda-powertools/logger';
import { FirehoseTransformationHandler, FirehoseTransformationEventRecord, FirehoseTransformationResultRecord, FirehoseRecordTransformationStatus } from 'aws-lambda';
import { TweetStreamParse } from '../utils';

const logger = new Logger({ logLevel: 'INFO', serviceName: 'filter' });

const liveStreamFilter = (record: FirehoseTransformationEventRecord) => {
  const recordId = record.recordId;
  let data = record.data;
  let result: FirehoseRecordTransformationStatus = 'Dropped';
  try {
    const payload = TweetStreamParse(data);
    if (payload.backup) {
      const tweet = payload.data;
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
  const filteredRecords = event.records.map(record => liveStreamFilter(record));
  const count = {
    ok: filteredRecords.filter(record => record.result == 'Ok').length,
    dropped: filteredRecords.filter(record => record.result == 'Dropped').length,
    processingFailed: filteredRecords.filter(record => record.result == 'ProcessingFailed').length,
  };
  logger.info(`ok: ${count.ok}, dropped: ${count.dropped}, processingFailed: ${count.processingFailed}`);
  return { records: filteredRecords };
};