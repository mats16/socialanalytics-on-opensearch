const { awscdk } = require('projen');
const project = new awscdk.AwsCdkTypeScriptApp({
  cdkVersion: '2.12.0',
  defaultReleaseBranch: 'main',
  name: 'social-analytics',
  // cdkDependencies: undefined,  /* Which AWS CDK modules (those that start with "@aws-cdk/") this app uses. */
  // deps: [],                    /* Runtime dependencies of this module. */
  // description: undefined,      /* The description is just a string that helps people understand the purpose of the package. */
  // devDeps: [],                 /* Build dependencies for this module. */
  // packageName: undefined,      /* The "name" in package.json. */
  // release: undefined,          /* Add release management to this project. */
  deps: [
    '@aws-cdk/aws-kinesisfirehose-alpha',
    '@aws-cdk/aws-kinesisfirehose-destinations-alpha',
    '@aws-sdk/client-cognito-identity',
    '@aws-sdk/client-cognito-identity-provider',
    '@aws-sdk/client-comprehend',
    '@aws-sdk/client-kinesis',
    '@aws-sdk/client-s3',
    '@aws-sdk/client-ssm',
    '@aws-sdk/client-sts',
    '@aws-sdk/client-translate',
    '@aws-sdk/credential-provider-node',
    '@aws-sdk/node-http-handler',
    '@aws-sdk/protocol-http',
    '@aws-sdk/signature-v4',
    '@aws-crypto/sha256-js',
    '@aws-lambda-powertools/logger',
    '@aws-lambda-powertools/metrics',
    '@aws-lambda-powertools/tracer',
    '@types/aws-lambda',
    'axios',
    'bluebird',
    'node-html-parser',
    'twitter-api-v2',
    'yaml',
  ],
  devDeps: [
    '@types/bluebird',
    '@types/cfn-response',
    '@types/log4js',
  ],
  tsconfig: {
    compilerOptions: {
      noUnusedLocals: false,
      //strictPropertyInitialization: false,
    },
  },
});
project.synth();