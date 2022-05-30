import { createHash } from 'crypto';
import { Handler } from 'aws-lambda';

export const handler: Handler<string, string> = async (text, _context) => {
  const hash = createHash('sha256');
  hash.update(text);
  const digest = hash.digest('hex');
  hash.destroy();
  return digest;
};
