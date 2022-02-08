import { Logger } from '@aws-lambda-powertools/logger';
import { PreSignUpTriggerHandler } from 'aws-lambda';

const allowedSignupDomains = process.env.ALLOWED_SIGNUP_DOMAINS;

const logger = new Logger({ logLevel: 'INFO', serviceName: 'pre-sign-up' });

export const handler: PreSignUpTriggerHandler = async (event) => {
  const email = event.request.userAttributes.email;
  const domain = email.split('@').pop()!;
  if (typeof(allowedSignupDomains) == undefined) {
    logger.info(`@${domain} is allowed to registe.`);
  } else if (allowedSignupDomains?.split(',').includes(domain)) {
    logger.info(`@${domain} is allowed to registe.`);
  } else {
    throw new Error(`@${domain} is not allowed to registe.`);
  }
  return event;
};