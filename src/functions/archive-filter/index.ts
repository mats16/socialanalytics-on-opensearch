import { Logger } from '@aws-lambda-powertools/logger';
import { FirehoseTransformationHandler, FirehoseTransformationEventRecord, FirehoseTransformationResultRecord, FirehoseRecordTransformationStatus } from 'aws-lambda';
import { TweetV2SingleStreamResult } from 'twitter-api-v2';

const logger = new Logger({ logLevel: 'INFO', serviceName: 'filter' });

interface ComprehendEntity {
  score: number;
  type: string;
  text: string;
}

interface ComprehendInsights {
  sentiment: string;
  sentiment_score: {
    positive: number;
    negative: number;
    neutral: number;
    mixed: number;
  };
  entities: ComprehendEntity[];
}

interface StreamData extends Partial<TweetV2SingleStreamResult> {
  analysis?: {
    normalized_text: string;
    comprehend: ComprehendInsights;
  };
};

const liveStreamFilter = (record: FirehoseTransformationEventRecord) => {
  const recordId = record.recordId;
  let data = record.data;
  let result: FirehoseRecordTransformationStatus = 'Dropped';
  try {
    const payload: StreamData = JSON.parse(Buffer.from(data, 'base64').toString('utf8'));
    if (payload.matching_rules) {
      data = Buffer.from(JSON.stringify(payload.data)).toString('base64');
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