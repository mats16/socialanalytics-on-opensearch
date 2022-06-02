import { Metrics, MetricUnits } from '@aws-lambda-powertools/metrics';
import { DetectDominantLanguageCommandOutput } from '@aws-sdk/client-comprehend';
import { Handler } from 'aws-lambda';

const metrics = new Metrics();

export const handler: Handler<DetectDominantLanguageCommandOutput, string> = async (event, _context) => {
  const languages = event.Languages || [];
  const scores = languages?.map(x => x.Score||0);
  const maxScore = Math.max(...scores);
  const dominantLanguage = languages.find(lang => lang.Score == maxScore);
  const dominantLanguageCode = dominantLanguage?.LanguageCode || 'en';
  metrics.addMetric('NoLanguageCodeCount', MetricUnits.Count, 1);
  metrics.publishStoredMetrics();
  return dominantLanguageCode;
};
