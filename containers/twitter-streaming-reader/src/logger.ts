import * as log4js from 'log4js';

log4js.addLayout('json', function(config) {
  return function(logEvent) {
    const date = new Date(logEvent.startTime);
    const dateString = date.toISOString();
    const logEventData = logEvent.data.shift();
    const log = {
      time: dateString,
      level: logEvent.level.levelStr,
      //category: logEvent.categoryName,
      ...logEventData,
    };
    return JSON.stringify(log);
  };
});

log4js.configure({
  appenders: {
    stgout: {
      type: 'stdout',
      layout: {
        type: 'json',
      },
    },
  },
  categories: {
    default: { appenders: ['stgout'], level: 'info' },
  },
});

export const getLogger = log4js.getLogger;
