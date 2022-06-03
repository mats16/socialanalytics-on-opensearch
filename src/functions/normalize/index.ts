import { createHash } from 'crypto';
import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { Handler } from 'aws-lambda';
import { Normalize } from '../utils';

interface Event {
  Text: string;
  LanguageCode?: string;
}

interface Result extends Event {
  CacheKey: string;
}

const metrics = new Metrics();

const toHash = (text: string): string => {
  const hash = createHash('sha256');
  hash.update(text);
  const digest = hash.digest('hex');
  hash.destroy();
  return digest;
};

export const handler: Handler<Event, Result> = async (event, _context) => {
  const normalizedText = Normalize(event.Text);
  const languageCode = (typeof event.LanguageCode == 'undefined') ? 'auto' : event.LanguageCode;
  const result: Result = {
    Text: normalizedText,
    LanguageCode: languageCode,
    CacheKey: toHash(normalizedText),
  };
  metrics.addMetric('ExecutedCount', MetricUnits.Count, 1);
  metrics.publishStoredMetrics();
  return result;
};
