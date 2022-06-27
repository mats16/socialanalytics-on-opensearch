import { Handler } from 'aws-lambda';
import { b64encode } from '../common-utils';

interface Result {
  Value: string;
  Expire: string;
}

const cacheExpireDays = Number(process.env.CACHE_EXPIRE_DAYS || '7');

export const handler: Handler<any, Result> = async (event, _context) => {
  const now = Math.floor(new Date().valueOf() / 1000);
  const expire = now + 60 * 60 * 24 * cacheExpireDays;
  const result: Result = {
    Value: b64encode(JSON.stringify(event)),
    Expire: expire.toString(),
  };
  return result;
};
