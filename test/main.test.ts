//import '@aws-cdk/assert/jest';
import { App } from 'aws-cdk-lib/core';
//import { MyStack } from '../src/main';
import { SocialAnalyticsStack } from '../src/social-analytics-stack';

test('Snapshot', () => {
  const app = new App();
  const stack = new SocialAnalyticsStack(app, 'test');

  //expect(stack).not.toHaveResource('AWS::S3::Bucket');
  expect(app.synth().getStackArtifact(stack.artifactId).template).toMatchSnapshot();
});