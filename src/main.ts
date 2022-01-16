import { App } from 'aws-cdk-lib';
import { SocialMediaDashboardStack } from './social-media-dashboard-stack';

const devEnv = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

const app = new App();

new SocialMediaDashboardStack(app, 'SocialMediaDashboardStack', { env: devEnv });

app.synth();