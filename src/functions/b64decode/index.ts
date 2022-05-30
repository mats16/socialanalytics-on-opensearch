import { Handler } from 'aws-lambda';
import { b64decode } from '../utils';

export const handler: Handler<string, any> = async (event, _context) => {
  const text = b64decode(event);
  const result = JSON.parse(text);
  return result;
};
