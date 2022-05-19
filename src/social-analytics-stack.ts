import { Stack, StackProps, Duration, CfnParameter, RemovalPolicy, Aws } from 'aws-cdk-lib';
import { VerificationEmailStyle } from 'aws-cdk-lib/aws-cognito';
import * as events from 'aws-cdk-lib/aws-events';
import { KinesisStream } from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { KinesisEventSource, SqsEventSource, SqsEventSourceProps } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction, NodejsFunctionProps } from 'aws-cdk-lib/aws-lambda-nodejs';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { SqsDestination } from 'aws-cdk-lib/aws-s3-notifications';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { StringParameter, StringListParameter } from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import { Tweetv2FieldsParams } from 'twitter-api-v2';
import { UserPool } from './resources/cognito-for-opensearch';
import { ContainerInsights } from './resources/container-insights';
import { Dashboard } from './resources/dashboard';
import { DeliveryStream } from './resources/dynamic-partitioning-firehose';
import { TwitterStreamingReader } from './resources/twitter-streaming-reader';

interface SocialAnalyticsStackProps extends StackProps {
  defaultTwitterBearerToken?: string;
};

// https://developer.twitter.com/en/docs/twitter-api/data-dictionary/introduction
const tweetFieldsParams: Partial<Tweetv2FieldsParams> = {
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
    'url',
    'verified',
    'public_metrics',
  ],
  'place.fields': [
    'id', 'full_name', // default
    'contained_within',
    'country',
    'country_code',
    'geo',
    'name',
    'place_type',
  ],
  'expansions': [
    //https://developer.twitter.com/en/docs/twitter-api/expansions
    'author_id',
    'entities.mentions.username',
    'in_reply_to_user_id',
    'referenced_tweets.id',
    'referenced_tweets.id.author_id',
  ],
};

const lambdaCommonSettings: NodejsFunctionProps = {
  handler: 'handler',
  runtime: lambda.Runtime.NODEJS_16_X,
  architecture: lambda.Architecture.ARM_64,
  insightsVersion: lambda.LambdaInsightsVersion.VERSION_1_0_119_0,
  logRetention: logs.RetentionDays.TWO_WEEKS,
  tracing: lambda.Tracing.ACTIVE,
};

export class SocialAnalyticsStack extends Stack {
  constructor(scope: Construct, id: string, props: SocialAnalyticsStackProps = {}) {
    super(scope, id, props);

    const twitterBearerTokenParameter = new CfnParameter(this, 'TwitterBearerTokenParameter', {
      type: 'String',
      default: props.defaultTwitterBearerToken,
      noEcho: true,
    });

    const twitterBearerToken = new StringParameter(this, 'TwitterBearerToken', {
      description: 'Twitter Bearer Token',
      parameterName: `/${this.stackName}/Twitter/BearerToken`,
      stringValue: twitterBearerTokenParameter.valueAsString,
    });

    const twitterFieldsParams = new StringParameter(this, 'TwitterFieldsParams', {
      description: 'Tweet fields params for API calls',
      parameterName: `/${this.stackName}/Twitter/FieldsParams`,
      stringValue: JSON.stringify(tweetFieldsParams),
    });

    const twitterFilterContextDomains = new StringListParameter(this, 'twitterFilterContextDomains', {
      // https://developer.twitter.com/en/docs/twitter-api/annotations/overview
      description: 'Context domains for filtering',
      parameterName: `/${this.stackName}/Twitter/Filter/ContextDomains`,
      stringListValue: ['Musician', 'Music Genre', 'Actor', 'TV Shows', 'Multimedia Franchise', 'Fictional Character', 'Entertainment Personality'],
    });

    const twitterFilterSourceLabels = new StringListParameter(this, 'twitterFilterSourceLabels', {
      // https://help.twitter.com/en/using-twitter/how-to-tweet#source-labels
      description: 'Tweet source labels for filtering',
      parameterName: `/${this.stackName}/Twitter/Filter/SourceLabels`,
      stringListValue: ['twittbot.net', 'Mk00JapanBot', 'Gakeppu Tweet', 'BelugaCampaignSEA', 'rare_zaiko', 'Wn32ShimaneBot', 'uhiiman_bot', 'atulsbots'],
    });

    const twitterParameterPolicyStatement = new iam.PolicyStatement({
      actions: ['ssm:GetParameter', 'ssm:GetParametersByPath'],
      resources: [`arn:aws:ssm:${this.region}:${this.account}:parameter/${this.stackName}/Twitter/*`],
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

    const eventBus = new events.EventBus(this, 'EventBus');

    const ingestionStream = new kinesis.Stream(this, 'IngestionStream', {
      encryption: kinesis.StreamEncryption.MANAGED,
    });

    const indexingStream = new kinesis.Stream(this, 'IndexingStream', {
      encryption: kinesis.StreamEncryption.MANAGED,
    });

    const analysisFunction = new NodejsFunction(this, 'AnalysisFunction', {
      ...lambdaCommonSettings,
      description: '[SocialAnalytics] Analysis with Amazon Comprehend',
      entry: './src/functions/analysis/index.ts',
      memorySize: 256,
      timeout: Duration.minutes(5),
      environment: {
        POWERTOOLS_SERVICE_NAME: 'AnalysisFunction',
        POWERTOOLS_METRICS_NAMESPACE: this.stackName,
        POWERTOOLS_TRACER_CAPTURE_RESPONSE: 'false',
        TWITTER_PARAMETER_PREFIX: `/${this.stackName}/Twitter/Filter/`,
        DEST_STREAM_NAME: indexingStream.streamName,
      },
      events: [
        new KinesisEventSource(ingestionStream, {
          startingPosition: lambda.StartingPosition.LATEST,
          batchSize: 100,
          maxBatchingWindow: Duration.seconds(15),
          maxRecordAge: Duration.days(1),
        }),
      ],
      initialPolicy: [
        twitterParameterPolicyStatement,
        new iam.PolicyStatement({
          actions: [
            'comprehend:Detect*',
            'translate:TranslateText',
          ],
          resources: ['*'],
        }),
      ],
    });
    indexingStream.grantWrite(analysisFunction);

    const archiveFilterFunction = new NodejsFunction(this, 'ArchiveFilterFunction', {
      ...lambdaCommonSettings,
      description: '[SocialAnalytics] Filtering with backup flag',
      entry: './src/functions/archive-filter/index.ts',
      timeout: Duration.minutes(5),
      environment: {
        POWERTOOLS_SERVICE_NAME: 'ArchiveFilterFunction',
        POWERTOOLS_METRICS_NAMESPACE: this.stackName,
      },
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
      description: '[SocialAnalytics] Bulk operations to load data into OpenSearch',
      entry: './src/functions/indexing/index.ts',
      memorySize: 256,
      timeout: Duration.minutes(5),
      environment: {
        POWERTOOLS_SERVICE_NAME: 'IndexingFunction',
        POWERTOOLS_METRICS_NAMESPACE: this.stackName,
        POWERTOOLS_TRACER_CAPTURE_RESPONSE: 'false',
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

    const putEventsFunction = new NodejsFunction(this, 'PutEventsFunction', {
      ...lambdaCommonSettings,
      description: '[SocialAnalytics] Put events to EventBus',
      entry: './src/functions/put-events/index.ts',
      timeout: Duration.minutes(5),
      environment: {
        EVENT_BUS_NAME: eventBus.eventBusName,
      },
      events: [
        new KinesisEventSource(indexingStream, {
          startingPosition: lambda.StartingPosition.LATEST,
          batchSize: 100,
          maxBatchingWindow: Duration.seconds(15),
          maxRecordAge: Duration.days(1),
        }),
      ],
    });
    eventBus.grantPutEventsTo(putEventsFunction);

    const reingestTweetsV1Function = new S3QueueFunction(this, 'ReingestTweetsV1Function', {
      destStream: ingestionStream,
      event: {
        s3: { bucket, prefix: 'reingest/tweets/v1/' },
      },
      description: 'Re-ingest for TweetsV1',
      entry: './src/functions/reingest-tweets-v1/index.ts',
      memorySize: 512,
      timeout: Duration.seconds(300),
      initialPolicy: [twitterParameterPolicyStatement],
      environment: {
        TWITTER_PARAMETER_PREFIX: `/${this.stackName}/Twitter/`,
      },
      reservedConcurrentExecutions: 1,
    });

    const reingestTweetsV2Function = new S3QueueFunction(this, 'ReingestTweetsV2Function', {
      destStream: ingestionStream,
      event: {
        s3: { bucket, prefix: 'reingest/tweets/v2/' },
      },
      description: 'Re-ingest for TweetsV2',
      entry: './src/functions/reingest-tweets-v2/index.ts',
      memorySize: 512,
      initialPolicy: [twitterParameterPolicyStatement],
      environment: {
        TWITTER_PARAMETER_PREFIX: `/${this.stackName}/Twitter/`,
      },
      reservedConcurrentExecutions: 1,
    });

    const reindexTweetsV2Function = new S3QueueFunction(this, 'ReindexTweetsV2Function', {
      destStream: indexingStream,
      event: {
        s3: { bucket, prefix: 'reindex/tweets/v2/' },
      },
      description: 'Re-index for TweetsV2',
      entry: './src/functions/reingest-tweets-v2/index.ts',
      memorySize: 512,
      initialPolicy: [twitterParameterPolicyStatement],
      environment: {
        TWITTER_PARAMETER_PREFIX: `/${this.stackName}/Twitter/`,
      },
      reservedConcurrentExecutions: 1,
    });
    indexingStream.grantWrite(reindexTweetsV2Function);

  }
};

interface S3QueueFunctionProps extends NodejsFunctionProps {
  event: {
    s3: {
      bucket: s3.IBucket;
      prefix: string;
    };
    sqs?: SqsEventSourceProps;
  };
  destStream?: kinesis.Stream;
};

class S3QueueFunction extends NodejsFunction {

  constructor(scope: Stack, id: string, props: S3QueueFunctionProps) {

    const timeout = Duration.seconds(30); // default
    const maxBatchingWindow = Duration.seconds(10); // default

    super(scope, id, { ...lambdaCommonSettings, timeout, ...props });

    const { bucket, prefix } = props.event.s3;
    const destStream = props.destStream;

    const encryptionMasterKey = new kms.Key(this, 'Key', {
      alias: `${scope.stackName}/sqs/${id}`,
      description: `Key that protects SQS messages for ${id}`,
    });

    const queue = new sqs.Queue(this, 'Queue', {
      retentionPeriod: Duration.days(14),
      visibilityTimeout: this.timeout,
      encryptionMasterKey,
    });
    this.addEventSource(new SqsEventSource(queue, { maxBatchingWindow, ...props.event.sqs }));

    bucket.addObjectCreatedNotification(new SqsDestination(queue), { prefix });
    bucket.grantRead(this, `${prefix}*`);
    bucket.grantDelete(this, `${prefix}*`);

    if (destStream) {
      destStream.grantWrite(this);
      this.addEnvironment('DEST_STREAM_NAME', destStream.streamName);
    };
    this.addEnvironment('POWERTOOLS_SERVICE_NAME', id);
    this.addEnvironment('POWERTOOLS_METRICS_NAMESPACE', Aws.STACK_NAME);
    this.addEnvironment('POWERTOOLS_TRACER_CAPTURE_RESPONSE', 'false');
  }
}