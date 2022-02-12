import { Stack, StackProps, Duration, CfnParameter, RemovalPolicy } from 'aws-cdk-lib';
import { VerificationEmailStyle } from 'aws-cdk-lib/aws-cognito';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { KinesisEventSource, SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction, NodejsFunctionProps } from 'aws-cdk-lib/aws-lambda-nodejs';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { SqsDestination } from 'aws-cdk-lib/aws-s3-notifications';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import { Tweetv2FieldsParams } from 'twitter-api-v2';
import { Application, ExtensionLayerVersion } from './resources/appconfig';
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
    const twitterBearerToken = new StringParameter(this, 'TwitterBearerToken', {
      description: 'Social Analytics - Twitter Bearer Token',
      parameterName: `/${this.stackName}/Twitter/BearerToken`,
      stringValue: twitterBearerTokenParameter.valueAsString,
    });
    const twitterFieldsParams = new StringParameter(this, 'TwitterFieldsParams', {
      description: 'Social Analytics - Twitter Fields Params',
      parameterName: `/${this.stackName}/Twitter/FieldsParams`,
      stringValue: JSON.stringify({
        'tweet.fields': [
          'id', 'text', // default
          //'attachments',
          'author_id',
          'context_annotations',
          'conversation_id',
          'created_at',
          'entities',
          'geo',
          'in_reply_to_user_id',
          'lang',
          //'non_public_metrics',
          //'organic_metrics',
          'possibly_sensitive',
          //'promoted_metrics',
          'public_metrics',
          'referenced_tweets',
          'reply_settings',
          'source',
          //'withheld'
        ],
        'user.fields': [
          'id', 'name', 'username', // default
          'public_metrics',
        ],
        'place.fields': [
          'contained_within', 'country', 'country_code', 'full_name', 'geo', 'id', 'name', 'place_type',
        ],
        'expansions': ['author_id', 'entities.mentions.username', 'referenced_tweets.id', 'referenced_tweets.id.author_id'],
      } as Partial<Tweetv2FieldsParams>),
    });

    const lambdaCommonSettings: NodejsFunctionProps = {
      runtime: lambda.Runtime.NODEJS_14_X,
      architecture: lambda.Architecture.ARM_64,
      insightsVersion: lambda.LambdaInsightsVersion.VERSION_1_0_119_0,
      logRetention: logs.RetentionDays.TWO_WEEKS,
    };

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
      ...lambdaCommonSettings,
      description: 'Social Analytics processor - Analysis by Amazon Comprehend',
      entry: './src/functions/analysis/index.ts',
      handler: 'handler',
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
      ...lambdaCommonSettings,
      description: 'Social Analytics filter - Filtering with baclup flag',
      entry: './src/functions/archive-filter/index.ts',
      handler: 'handler',
      timeout: Duration.minutes(5),
    });

    const ingestionArchiveStream = new DeliveryStream(this, 'IngestionArchiveStream', {
      sourceStream: ingestionStream,
      processorFunction: archiveFilterFunction,
      destinationBucket: bucket,
      prefix: 'raw/tweets/v2/',
      errorOutputPrefix: 'raw/tweets/v2-error/',
    });

    const indexingArchiveStream = new DeliveryStream(this, 'IndexingArchiveStream', {
      sourceStream: indexingStream,
      processorFunction: archiveFilterFunction,
      destinationBucket: bucket,
      prefix: 'raw-with-analysis/tweets/v2/',
      errorOutputPrefix: 'raw-with-analysis/tweets/v2-error/',
    });

    const twitterStreamingReader = new TwitterStreamingReader(this, 'TwitterStreamingReader', {
      twitterBearerToken,
      twitterFieldsParams,
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
      ...lambdaCommonSettings,
      description: 'Social Analytics processor - Bulk load to OpenSearch',
      entry: './src/functions/indexing/index.ts',
      handler: 'handler',
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

    const appconfigApp = new Application(this, 'AppConfig', { name: this.stackName });
    const appconfigEnv = appconfigApp.addEnvironment('Production');
    const appconfigConfigTwitterBearerToken = appconfigApp.addSSMParameterConfigProfile('TwitterBearerToken', twitterBearerToken);
    appconfigEnv.deploy('TwitterBearerTokenDeploy', appconfigConfigTwitterBearerToken.configurationProfileId);
    const appconfigConfigTwitterFieldsParams = appconfigApp.addSSMParameterConfigProfile('TwitterFieldsParams', twitterFieldsParams);
    appconfigEnv.deploy('TwitterFieldsParamsDeploy', appconfigConfigTwitterFieldsParams.configurationProfileId);

    const reprocessTweetsV1BucketPrefix = 'reprocess/tweets/v1/';
    const reprocessTweetsV1Queue = new sqs.Queue(this, 'ReprocessTweetsV1Queue', { retentionPeriod: Duration.days(14), visibilityTimeout: Duration.seconds(180) });
    bucket.addEventNotification(s3.EventType.OBJECT_CREATED, new SqsDestination(reprocessTweetsV1Queue), { prefix: reprocessTweetsV1BucketPrefix });
    const reprocessTweetsV1Function = new NodejsFunction(this, 'ReprocessTweetsV1Function', {
      ...lambdaCommonSettings,
      description: 'Social Analytics processor - Processing with lookup API for TweetsV1',
      entry: './src/functions/reprocess-tweets-v1/index.ts',
      handler: 'handler',
      architecture: lambda.Architecture.X86_64,
      layers: [ExtensionLayerVersion(this, 'AppConfigLambdaExtension', this.region)],
      memorySize: 512,
      timeout: Duration.seconds(180),
      events: [
        new SqsEventSource(reprocessTweetsV1Queue, {
          batchSize: 12,
          maxBatchingWindow: Duration.seconds(1),
        }),
      ],
      initialPolicy: [
        new iam.PolicyStatement({
          actions: [
            'appconfig:StartConfigurationSession',
            'appconfig:GetLatestConfiguration',
          ],
          resources: ['*'],
        }),
      ],
      environment: {
        AWS_APPCONFIG_EXTENSION_POLL_INTERVAL_SECONDS: '180',
        APPCONFIG_APPLICATION: appconfigApp.applicationName,
        APPCONFIG_ENVIRONMENT: appconfigEnv.environmentName,
        APPCONFIG_CONFIG_TWITTER_BEARER_TOKEN: appconfigConfigTwitterBearerToken.configurationProfileName,
        APPCONFIG_CONFIG_TWITTER_FIELDS_PARAMS: appconfigConfigTwitterFieldsParams.configurationProfileName,
        STREAM_NAME: ingestionStream.streamName,
      },
      reservedConcurrentExecutions: 1,
    });
    bucket.grantRead(reprocessTweetsV1Function, `${reprocessTweetsV1BucketPrefix}*`);
    bucket.grantDelete(reprocessTweetsV1Function, `${reprocessTweetsV1BucketPrefix}*`);
    ingestionStream.grantWrite(reprocessTweetsV1Function);

  }
}