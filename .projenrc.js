const awsSdkVersion = '^3.224.0';
const { awscdk } = require('projen');
const project = new awscdk.AwsCdkTypeScriptApp({
  typescriptVersion: '4.6.4',
  cdkVersion: '2.53.0',
  defaultReleaseBranch: 'main',
  name: 'social-analytics',
  // cdkDependencies: undefined,  /* Which AWS CDK modules (those that start with "@aws-cdk/") this app uses. */
  // deps: [],                    /* Runtime dependencies of this module. */
  // description: undefined,      /* The description is just a string that helps people understand the purpose of the package. */
  // devDeps: [],                 /* Build dependencies for this module. */
  // packageName: undefined,      /* The "name" in package.json. */
  // release: undefined,          /* Add release management to this project. */
  deps: [
    `@aws-sdk/client-cognito-identity@${awsSdkVersion}`,
    `@aws-sdk/client-cognito-identity-provider@${awsSdkVersion}`,
    `@aws-sdk/client-comprehend@${awsSdkVersion}`,
    `@aws-sdk/client-dynamodb@${awsSdkVersion}`,
    `@aws-sdk/client-eventbridge@${awsSdkVersion}`,
    `@aws-sdk/client-firehose@${awsSdkVersion}`,
    `@aws-sdk/client-kinesis@${awsSdkVersion}`,
    `@aws-sdk/client-lambda@${awsSdkVersion}`,
    `@aws-sdk/client-opensearch@${awsSdkVersion}`,
    `@aws-sdk/client-s3@${awsSdkVersion}`,
    `@aws-sdk/client-sfn@${awsSdkVersion}`,
    `@aws-sdk/client-ssm@${awsSdkVersion}`,
    `@aws-sdk/client-sts@${awsSdkVersion}`,
    `@aws-sdk/credential-provider-node@${awsSdkVersion}`,
    '@aws-sdk/lib-dynamodb',
    '@aws-sdk/node-http-handler',
    '@aws-sdk/protocol-http',
    '@aws-sdk/signature-v4',
    '@aws-sdk/util-dynamodb',
    '@aws-sdk/util-utf8-node',
    '@aws-crypto/sha256-js',
    '@aws-lambda-powertools/commons',
    '@aws-lambda-powertools/logger',
    '@aws-lambda-powertools/metrics',
    '@aws-lambda-powertools/tracer',
    '@types/aws-lambda',
    '@opensearch-project/opensearch@2.0.0',
    '@opentelemetry/api',
    '@opentelemetry/core',
    '@opentelemetry/exporter-trace-otlp-http',
    '@opentelemetry/exporter-trace-otlp-proto',
    '@opentelemetry/id-generator-aws-xray',
    '@opentelemetry/instrumentation',
    '@opentelemetry/instrumentation-aws-lambda',
    '@opentelemetry/instrumentation-aws-sdk',
    '@opentelemetry/instrumentation-http',
    '@opentelemetry/propagator-aws-xray',
    '@opentelemetry/resource-detector-aws',
    '@opentelemetry/resources',
    '@opentelemetry/sdk-trace-base',
    '@opentelemetry/sdk-trace-node',
    '@opentelemetry/semantic-conventions',
    'aws-xray-sdk@^3.3.6',
    'aws4@1.11.0',
    'axios@0.27.2',
    'bluebird@3.7.2',
    'node-html-parser@5.3.3',
    'twitter-api-v2@1.12.3',
    'yaml@2.0.1',
  ],
  devDeps: [
    '@types/aws4',
    '@types/bluebird',
    '@types/log4js',
  ],
  tsconfig: {
    compilerOptions: {
      noUnusedLocals: false,
      //strictPropertyInitialization: false,
    },
  },
  gitignore: [
    'cdk.context.json',
  ],
  depsUpgrade: false,
});
project.synth();