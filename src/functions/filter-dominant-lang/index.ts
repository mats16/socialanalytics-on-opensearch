import { DetectDominantLanguageCommandOutput } from '@aws-sdk/client-comprehend';
import { Handler } from 'aws-lambda';

export const handler: Handler<DetectDominantLanguageCommandOutput, string> = async (event, _context) => {
  const languages = event.Languages || [];
  const scores = languages?.map(x => x.Score||0);
  const maxScore = Math.max(...scores);
  const dominantLanguage = languages.find(lang => lang.Score == maxScore);
  const dominantLanguageCode = dominantLanguage?.LanguageCode || 'en';
  return dominantLanguageCode;
};
