export * from './sfn-comprehend';
export * from './ssm-parameters';

export const toUnixtime = (text?: string): number => {
  const unixtime = (typeof text == 'string') ? new Date(text).valueOf() : Date.now(); // microseconds
  return Math.floor(unixtime / 1000); // seconds
};

export const b64encode = (text: string) => Buffer.from(text).toString('base64');

export const b64decode = (b64string: string) => Buffer.from(b64string, 'base64').toString('utf8');

export const Deduplicate = (array: any[]) => {
  const set = new Set(array);
  const deduplicatedArray = Array.from(set);
  return deduplicatedArray;
};

export const Normalize = (text: string): string => {
  const textWithoutRT = text.replace(/^RT /, '');
  //const tectWithoutEmoji = textWithoutRT.replace(/[\u2700-\u27BF]|[\uE000-\uF8FF]|\uD83C[\uDC00-\uDFFF]|\uD83D[\uDC00-\uDFFF]|[\u2011-\u26FF]|\uD83E[\uDD10-\uDDFF]/g, '');
  const textWithoutUrl = textWithoutRT.replace(/https?:\/\/[\w/;:%#\$&\?\(\)~\.=\+\-]+/g, '');
  const textWithoutHtml = textWithoutUrl.replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
  const textWithoutSpace = textWithoutHtml.replace(/\n/g, ' ').replace(/[\s]+/g, ' ').trimEnd();
  return textWithoutSpace;
};