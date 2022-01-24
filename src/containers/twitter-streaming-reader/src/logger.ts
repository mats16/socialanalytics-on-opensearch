import * as log4js from 'log4js';

log4js.addLayout('json', function(config) {
  return function(logEvent) {
    const indent: number = config.indent;
    const date = new Date(logEvent.startTime);
    const dateString = date.toISOString();
    const log = {
      time: dateString,
      level: logEvent.level.levelStr,
      category: logEvent.categoryName,
    };
    const message = logEvent.data[0];
    if (typeof(message) == 'string') {
      return JSON.stringify({ data: message, ...log });
    } else {
      return JSON.stringify({ ...logEvent.data[0], ...log });
    };
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
