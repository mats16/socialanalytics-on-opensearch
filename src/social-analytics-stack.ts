import { Stack, StackProps, Duration, CfnParameter, RemovalPolicy, CfnOutput } from 'aws-cdk-lib';
import { VerificationEmailStyle } from 'aws-cdk-lib/aws-cognito';
import { Vpc, SubnetType, Port } from 'aws-cdk-lib/aws-ec2';
import * as events from 'aws-cdk-lib/aws-events';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { KinesisEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { StringParameter, StringListParameter } from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import { tweetFieldsParams } from './parameter';
import { UserPool } from './resources/cognito-for-opensearch';
import { ContainerInsights } from './resources/container-insights';
import { Dashboard } from './resources/dashboard';
import { DeliveryStream } from './resources/dynamic-partitioning-firehose';
import { Function, RetryFunction } from './resources/lambda-nodejs';
import { Proxy } from './resources/proxy';
import { TwitterStreamingReader } from './resources/twitter-streaming-reader';

interface SocialAnalyticsStackProps extends StackProps {
  defaultTwitterBearerToken?: string;
};

export class SocialAnalyticsStack extends Stack {
  constructor(scope: Construct, id: string, props: SocialAnalyticsStackProps) {
    super(scope, id, props);

    const twitterBearerTokenParameter = new CfnParameter(this, 'TwitterBearerTokenParameter', {
      type: 'String',
      default: props.defaultTwitterBearerToken,
      noEcho: true,
    });

    const twitterParameterPath = `/${this.stackName}/Twitter`;

    const twitterBearerToken = new StringParameter(this, 'TwitterBearerToken', {
      description: 'Twitter Bearer Token',
      parameterName: `${twitterParameterPath}/BearerToken`,
      stringValue: twitterBearerTokenParameter.valueAsString,
    });

    const twitterFieldsParams = new StringParameter(this, 'TwitterFieldsParams', {
      description: 'Tweet fields params for API calls',
      parameterName: `${twitterParameterPath}/FieldsParams`,
      stringValue: JSON.stringify(tweetFieldsParams),
    });

    const twitterFilterContextDomains = new StringListParameter(this, 'twitterFilterContextDomains', {
      // https://developer.twitter.com/en/docs/twitter-api/annotations/overview
      description: 'Context domains for filtering',
      parameterName: `${twitterParameterPath}/Filter/ContextDomains`,
      stringListValue: ['Musician', 'Music Genre', 'Actor', 'TV Shows', 'Multimedia Franchise', 'Fictional Character', 'Entertainment Personality'],
    });

    const twitterFilterSourceLabels = new StringListParameter(this, 'twitterFilterSourceLabels', {
      // https://help.twitter.com/en/using-twitter/how-to-tweet#source-labels
      description: 'Tweet source labels for filtering',
      parameterName: `${twitterParameterPath}/Filter/SourceLabels`,
      stringListValue: ['twittbot.net', 'Mk00JapanBot', 'Gakeppu Tweet', 'BelugaCampaignSEA', 'rare_zaiko', 'Wn32ShimaneBot', 'uhiiman_bot', 'atulsbots'],
    });

    const twitterParameterPolicyStatement = new iam.PolicyStatement({
      actions: ['ssm:GetParameter', 'ssm:GetParametersByPath'],
      resources: [`arn:aws:ssm:${this.region}:${this.account}:parameter${twitterParameterPath}/*`],
    });

    const comprehendPolicyStatement = new iam.PolicyStatement({
      actions: ['comprehend:Detect*', 'translate:TranslateText'],
      resources: ['*'],
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

    const vpc = new Vpc(this, 'VPC', {
      natGateways: 1,
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: 'Public',
          subnetType: SubnetType.PUBLIC,
        },
        {
          cidrMask: 24,
          name: 'Private',
          subnetType: SubnetType.PRIVATE_WITH_NAT,
        },
      ],
    });

    const eventBus = new events.EventBus(this, 'EventBus');

    const ingestionStream = new kinesis.Stream(this, 'IngestionStream', {
      encryption: kinesis.StreamEncryption.MANAGED,
    });

    const indexingStream = new kinesis.Stream(this, 'IndexingStream', {
      encryption: kinesis.StreamEncryption.MANAGED,
    });

    const analysisFunction = new Function(this, 'AnalysisFunction', {
      description: '[SocialAnalytics] Analysis with Amazon Comprehend',
      entry: './src/functions/analysis/index.ts',
      memorySize: 256,
      environment: {
        POWERTOOLS_SERVICE_NAME: 'AnalysisFunction',
        POWERTOOLS_METRICS_NAMESPACE: this.stackName,
        POWERTOOLS_TRACER_CAPTURE_RESPONSE: 'false',
        TWITTER_FILTER_CONTEXT_DOMAINS_PARAMETER_NAME: twitterFilterContextDomains.parameterName,
        TWITTER_FILTER_SOURCE_LABELS_PARAMETER_NAME: twitterFilterSourceLabels.parameterName,
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
        comprehendPolicyStatement,
      ],
    });
    indexingStream.grantWrite(analysisFunction);

    const archiveFilterFunction = new Function(this, 'ArchiveFilterFunction', {
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
      vpc,
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
      cognitoDomainPrefix: `${this.stackName.toLowerCase()}-${this.account}`,
    });

    const dashboard = new Dashboard(this, 'Dashboard', {
      vpc,
      userPool,
    });
    userPool.enableRoleFromToken(`AmazonOpenSearchService-${dashboard.Domain.domainName}-`);

    const indexingFunction = new Function(this, 'IndexingFunction', {
      description: '[SocialAnalytics] Bulk operations to load data into OpenSearch',
      entry: './src/functions/indexing/index.ts',
      memorySize: 320,
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
      vpc,
    });
    dashboard.Domain.connections.allowFrom(indexingFunction, Port.tcp(443));

    const bulkOperationRole = dashboard.Domain.addRole('BulkOperationRole', {
      name: 'bulk_operation',
      body: {
        description: 'Provide the minimum permissions for a bulk operation user',
        cluster_permissions: ['indices:data/write/bulk'],
        index_permissions: [{
          index_patterns: ['tweets-*'],
          allowed_actions: ['write', 'create_index'],
        }],
      },
    });
    dashboard.Domain.addRoleMapping('BulkOperationRoleMapping', {
      name: bulkOperationRole.getAttString('Name'),
      body: {
        backend_roles: [`${indexingFunction.role?.roleArn}`],
      },
    });

    const putEventsFunction = new Function(this, 'PutEventsFunction', {
      description: '[SocialAnalytics] Put events to EventBus',
      entry: './src/functions/put-events/index.ts',
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

    const reingestTweetsV1Function = new RetryFunction(this, 'ReingestTweetsV1Function', {
      source: { bucket, prefix: 'reingest/tweets/v1/' },
      description: 'Re-ingest for TweetsV1',
      entry: './src/functions/reingest-tweets-v1/index.ts',
      initialPolicy: [twitterParameterPolicyStatement],
      environment: {
        TWITTER_PARAMETER_PREFIX: `/${this.stackName}/Twitter/`,
      },
    });

    const reingestTweetsV2Function = new RetryFunction(this, 'ReingestTweetsV2Function', {
      source: { bucket, prefix: 'reingest/tweets/v2/' },
      description: 'Re-ingest for TweetsV2',
      entry: './src/functions/reingest-tweets-v2/index.ts',
      initialPolicy: [twitterParameterPolicyStatement],
      environment: {
        TWITTER_PARAMETER_PREFIX: `/${this.stackName}/Twitter/`,
      },
    });

    const reindexTweetsV2Function = new RetryFunction(this, 'ReindexTweetsV2Function', {
      source: { bucket, prefix: 'reindex/tweets/v2/' },
      description: 'Re-index for TweetsV2',
      entry: './src/functions/reindex-tweets-v2/index.ts',
      memorySize: 1024,
      initialPolicy: [twitterParameterPolicyStatement],
      environment: {
        TWITTER_PARAMETER_PREFIX: `/${this.stackName}/Twitter/`,
        INDEXING_FUNCTION_ARN: indexingFunction.functionArn,
      },
      reservedConcurrentExecutions: 8,
    });
    indexingFunction.grantInvoke(reindexTweetsV2Function);

    const proxy = new Proxy(this, 'Proxy', {
      vpc,
      openSearchDomain: dashboard.Domain,
      cognitoHost: userPool.domainName,
    });
    new CfnOutput(this, 'url', { value: `https://${proxy.domainName}` });
  }
};
