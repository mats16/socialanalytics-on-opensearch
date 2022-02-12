import { App } from 'aws-cdk-lib';
import { SocialAnalyticsStack } from './social-analytics-stack';

const devEnv = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

const defaultTwitterBearerToken = process.env.TWITTER_BEARER_TOKEN;

const app = new App();

new SocialAnalyticsStack(app, 'SocialAnalytics', { env: devEnv, defaultTwitterBearerToken });

app.synth();