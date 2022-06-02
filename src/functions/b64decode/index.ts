import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Handler } from 'aws-lambda';
import { b64decode } from '../utils';

const metrics = new Metrics();

export const handler: Handler<string, any> = async (event, _context) => {
  const text = b64decode(event);
  const result = JSON.parse(text);
  metrics.addMetric('CacheHitCount', MetricUnits.Count, 1);
  metrics.publishStoredMetrics();
  return result;
};
