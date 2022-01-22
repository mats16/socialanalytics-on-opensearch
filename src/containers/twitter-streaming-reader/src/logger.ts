import * as log4js from 'log4js';

log4js.configure({
  appenders: {
    console: {
      type: 'console',
      layout: {
        type: 'pattern',
        pattern: JSON.stringify({
          time: '%d{yyyy-MM-ddThh:mm:ss.SSS}',
          level: '%p',
          category: '%c',
          message: '%m',
        }),
      },
    },
  },
  categories: { default: { appenders: ['console'], level: 'info' } },
});

export const getLogger = log4js.getLogger;
