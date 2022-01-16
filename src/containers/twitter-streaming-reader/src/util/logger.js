'use strict';

const log4js = require('log4js');

function logger() {
  const config = {
    "appenders": [
      {
        "type": "console",
        "layout": {
          "type": "pattern",
          "pattern": JSON.stringify({
            time: '%d{yyyy-MM-ddThh:mm:ss.SSS}',
            level: '%p',
            category: '%c',
            message: '%m'
          }),
        }
      }
    ]
  };

  log4js.configure(config, {});

  return {
    getLogger: function(category) {
      return log4js.getLogger(category);
    }
  };
}

module.exports = logger;
