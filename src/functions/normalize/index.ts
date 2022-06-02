import { createHash } from 'crypto';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Handler } from 'aws-lambda';
import { Normalize } from '../utils';

interface Result {
  Value: string;
  SHA256: string;
}

const metrics = new Metrics();

const toHash = (text: string): string => {
  const hash = createHash('sha256');
  hash.update(text);
  const digest = hash.digest('hex');
  hash.destroy();
  return digest;
};

export const handler: Handler<string, Result> = async (text, _context) => {
  const normalizedText = Normalize(text);
  const result: Result = {
    Value: normalizedText,
    SHA256: toHash(normalizedText),
  };
  metrics.addMetric('ExecutedCount', MetricUnits.Count, 1);
  metrics.publishStoredMetrics();
  return result;
};
