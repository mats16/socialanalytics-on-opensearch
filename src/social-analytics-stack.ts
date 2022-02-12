import { Stack, StackProps, Duration, CfnParameter, RemovalPolicy } from 'aws-cdk-lib';
import { VerificationEmailStyle } from 'aws-cdk-lib/aws-cognito';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { KinesisEventSource, S3EventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import { UserPool } from './resources/cognito-for-opensearch';
import { ContainerInsights } from './resources/container-insights';
import { Dashboard } from './resources/dashboard';
import { DeliveryStream } from './resources/dynamic-partitioning-firehose';
import { TwitterStreamingReader } from './resources/twitter-streaming-reader';

interface SocialAnalyticsStackProps extends StackProps {
  defaultTwitterBearerToken?: string | undefined;
};

export class SocialAnalyticsStack extends Stack {
  constructor(scope: Construct, id: string, props: SocialAnalyticsStackProps = {}) {
    super(scope, id, props);

    const twitterBearerTokenParameter = new CfnParameter(this, 'TwitterBearerTokenParameter', { type: 'String', default: props.defaultTwitterBearerToken, noEcho: true });
    const twitterBearerToken = new StringParameter(this, 'TwitterBearerToken2', {
      description: 'Social Analytics - Twitter Bearer Token',
      parameterName: `/${id}/TwitterBearerToken`,
      stringValue: twitterBearerTokenParameter.valueAsString,
    });

    const bucket = new s3.Bucket(this, 'Bucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [{
        transitions: [{
          storageClass: s3.StorageClass.INTELLIGENT_TIERING,
          transitionAfter: Duration.days(0),
        }],
      }],
    });

    const ingestionStream = new kinesis.Stream(this, 'IngestionStream', {
      encryption: kinesis.StreamEncryption.MANAGED,
    });

    const indexingStream = new kinesis.Stream(this, 'IndexingStream', {
      encryption: kinesis.StreamEncryption.MANAGED,
    });

    const analysisFunction = new NodejsFunction(this, 'AnalysisFunction', {
      description: 'Social Analytics processor - Analysis by Amazon Comprehend',
      entry: './src/functions/analysis/index.ts',
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_14_X,
      architecture: lambda.Architecture.ARM_64,
      insightsVersion: lambda.LambdaInsightsVersion.VERSION_1_0_119_0,
      memorySize: 256,
      timeout: Duration.minutes(5),
      environment: {
        INDEXING_STREAM_NAME: indexingStream.streamName,
      },
      events: [
        new KinesisEventSource(ingestionStream, {
          startingPosition: lambda.StartingPosition.LATEST,
          batchSize: 100,
          maxBatchingWindow: Duration.seconds(15),
          maxRecordAge: Duration.days(1),
        }),
      ],
      initialPolicy: [new iam.PolicyStatement({
        actions: [
          'comprehend:Detect*',
          'translate:TranslateText',
        ],
        resources: ['*'],
      })],
    });
    indexingStream.grantWrite(analysisFunction);

    const archiveFilterFunction = new NodejsFunction(this, 'ArchiveFilterFunction', {
      description: 'Social Analytics filter - Filtering with baclup flag',
      entry: './src/functions/archive-filter/index.ts',
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_14_X,
      architecture: lambda.Architecture.ARM_64,
      insightsVersion: lambda.LambdaInsightsVersion.VERSION_1_0_119_0,
      timeout: Duration.minutes(5),
    });

    const ingestionArchiveStream = new DeliveryStream(this, 'IngestionArchiveStream', {
      sourceStream: ingestionStream,
      destinationBucket: bucket,
      processorFunction: archiveFilterFunction,
    });

    const twitterStreamingReader = new TwitterStreamingReader(this, 'TwitterStreamingReader', {
      twitterBearerToken,
      ingestionStream,
    });

    const containerInsights = new ContainerInsights(this, 'ContainerInsights', {
      targetService: twitterStreamingReader.service,
    });

    const userPool = new UserPool(this, `${id}-UserPool`, {
      removalPolicy: RemovalPolicy.DESTROY,
      signInAliases: {
        username: false,
        email: true,
      },
      autoVerify: {
        email: true,
      },
      userVerification: {
        emailStyle: VerificationEmailStyle.LINK,
      },
      selfSignUpEnabled: true,
      allowedSignupDomains: [
        'amazon.com',
        'amazon.co.jp',
      ],
      cognitoDomainPrefix: [this.stackName.toLowerCase(), this.account].join('-'),
    });

    const dashboard = new Dashboard(this, 'Dashboard', {
      userPool,
    });
    userPool.enableRoleFromToken(`AmazonOpenSearchService-${dashboard.Domain.domainName}-`);

    const indexingFunction = new NodejsFunction(this, 'IndexingFunction', {
      description: 'Social Analytics processor - Bulk load to OpenSearch',
      entry: './src/functions/indexing/index.ts',
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_14_X,
      architecture: lambda.Architecture.ARM_64,
      insightsVersion: lambda.LambdaInsightsVersion.VERSION_1_0_119_0,
      memorySize: 256,
      timeout: Duration.minutes(5),
      environment: {
        OPENSEARCH_DOMAIN_ENDPOINT: dashboard.Domain.domainEndpoint,
      },
      events: [
        new KinesisEventSource(indexingStream, {
          startingPosition: lambda.StartingPosition.LATEST,
          batchSize: 100,
          maxBatchingWindow: Duration.seconds(15),
          maxRecordAge: Duration.days(1),
        }),
      ],
      role: dashboard.BulkOperationRole,
    });

    const reprocessingTweetsV1Function = new NodejsFunction(this, 'ReprocessingTweetsV1Function', {
      description: 'Social Analytics processor - Reprocessing for tweets v1',
      entry: './src/functions/reprocessing-tweets-v1/index.ts',
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_14_X,
      architecture: lambda.Architecture.ARM_64,
      insightsVersion: lambda.LambdaInsightsVersion.VERSION_1_0_119_0,
      memorySize: 256,
      timeout: Duration.minutes(10),
      environment: {
        STREAM_NAME: ingestionStream.streamName,
      },
      events: [
        new S3EventSource(bucket, {
          events: [s3.EventType.OBJECT_CREATED],
          filters: [{ prefix: 'reprocessing/tweets/v1/' }],
        }),
      ],
    });
    bucket.grantRead(reprocessingTweetsV1Function, 'reprocessing/tweets/v1/*');
    bucket.grantDelete(reprocessingTweetsV1Function, 'reprocessing/tweets/v1/*');
    ingestionStream.grantWrite(reprocessingTweetsV1Function);

    const reprocessingTweetsV2Function = new NodejsFunction(this, 'ReprocessingTweetsV2Function', {
      description: 'Social Analytics processor - Reprocessing for tweets v2',
      entry: './src/functions/reprocessing-tweets-v2/index.ts',
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_14_X,
      architecture: lambda.Architecture.ARM_64,
      insightsVersion: lambda.LambdaInsightsVersion.VERSION_1_0_119_0,
      memorySize: 256,
      timeout: Duration.minutes(10),
      environment: {
        STREAM_NAME: ingestionStream.streamName,
      },
      events: [
        new S3EventSource(bucket, {
          events: [s3.EventType.OBJECT_CREATED],
          filters: [{ prefix: 'reprocessing/tweets/v2/' }],
        }),
      ],
    });
    bucket.grantRead(reprocessingTweetsV2Function, 'reprocessing/tweets/v2/*');
    bucket.grantDelete(reprocessingTweetsV2Function, 'reprocessing/tweets/v2/*');
    ingestionStream.grantWrite(reprocessingTweetsV2Function);

  }
}