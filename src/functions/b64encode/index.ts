import { Handler } from 'aws-lambda';
import { b64encode } from '../utils';

interface Result {
  Value: string;
  Expire: string;
}

const cacheTtlDays = Number(process.env.CACHE_TTL_DAYS || '30');

export const handler: Handler<any, Result> = async (event, _context) => {
  const now = Math.floor(new Date().valueOf() / 1000);
  const expire = now + 60 * 60 * 24 * cacheTtlDays;
  const result: Result = {
    Value: b64encode(JSON.stringify(event)),
    Expire: expire.toString(),
  };
  return result;
};
