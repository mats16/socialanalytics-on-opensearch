import { createHash } from 'crypto';
import { Handler } from 'aws-lambda';
import { Normalize } from '../utils';

interface Result {
  Value: string;
  SHA256: string;
}

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
  return result;
};
