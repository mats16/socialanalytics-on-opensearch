import { Stack, StackProps, Duration, CfnParameter, Aws } from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { Vpc, SubnetType } from 'aws-cdk-lib/aws-ec2';
import { EventBus, Rule, EventPattern } from 'aws-cdk-lib/aws-events';
import * as eventsTargets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { DynamoEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { StringParameter, StringListParameter } from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import { tweetFieldsParams } from './parameter';
import { Application } from './resources/appconfig';
import { Dashboard } from './resources/dashboard';
import { DeliveryStream } from './resources/dynamic-partitioning-firehose';
import { Function, RetryFunction } from './resources/lambda-nodejs';
import { OpenSearchPackages } from './resources/opensearch-packages';
import { ReIndexBatch } from './resources/sfn-reindex-batch';
import { ComprehendWithCache } from './resources/sfn-state-machines';
import { TweetProducer } from './resources/tweet-producer';

interface SocialAnalyticsStackProps extends StackProps {
  defaultTwitterBearerToken?: string;
};

const insightsVersion = lambda.LambdaInsightsVersion.VERSION_1_0_135_0;

export class SocialAnalyticsStack extends Stack {
  constructor(scope: Construct, id: string, props: SocialAnalyticsStackProps) {
    super(scope, id, props);

    const defaultTwitterBearerToken = props.defaultTwitterBearerToken;

    const twitterParameterPath = `/${Aws.STACK_NAME}/Twitter`;

    const twitterFieldsParams = new StringParameter(this, 'TwitterFieldsParams', {
      description: 'Tweet fields params for API calls',
      parameterName: `${twitterParameterPath}/FieldsParams`,
      stringValue: JSON.stringify(tweetFieldsParams),
      simpleName: false,
    });

    const twitterFilterContextDomains = new StringListParameter(this, 'twitterFilterContextDomains', {
      // https://developer.twitter.com/en/docs/twitter-api/annotations/overview
      description: 'Context domains for filtering',
      parameterName: `${twitterParameterPath}/Filter/ContextDomains`,
      stringListValue: ['Musician', 'Music Genre', 'Actor', 'TV Shows', 'Multimedia Franchise', 'Fictional Character', 'Entertainment Personality'],
      simpleName: false,
    });

    const twitterFilterSourceLabels = new StringListParameter(this, 'twitterFilterSourceLabels', {
      // https://help.twitter.com/en/using-twitter/how-to-tweet#source-labels
      description: 'Tweet source labels for filtering',
      parameterName: `${twitterParameterPath}/Filter/SourceLabels`,
      stringListValue: ['Twitter for Advertisers', 'twittbot.net', 'Mk00JapanBot', 'Gakeppu Tweet', 'BelugaCampaignSEA', 'rare_zaiko', 'Wn32ShimaneBot', 'uhiiman_bot', 'atulsbots'],
      simpleName: false,
    });

    const twitterParameterPolicyStatement = new iam.PolicyStatement({
      actions: ['ssm:GetParameter', 'ssm:GetParametersByPath'],
      resources: [`arn:aws:ssm:${this.region}:${this.account}:parameter${twitterParameterPath}/*`],
    });

    const app = new Application(this, 'AppConfigApplication', { name: this.stackName });
    const appEnv = app.addEnvironment('Production');
    const twitterFilterContextDomainsProfile = app.addSSMParameterConfigProfile('TwitterFilterContextDomains', twitterFilterContextDomains);
    const twitterFilterSourceLabelsProfile = app.addSSMParameterConfigProfile('TwitterFilterSourceLabels', twitterFilterSourceLabels);
    appEnv.deploy('TwitterFilterContextDomainsDeploy', twitterFilterContextDomainsProfile.configurationProfileId);
    appEnv.deploy('TwitterFilterSourceLabelsDeploy', twitterFilterSourceLabelsProfile.configurationProfileId);

    const appConfigPolicyStatement = new iam.PolicyStatement({
      actions: ['appconfig:StartConfigurationSession', 'appconfig:GetLatestConfiguration'],
      resources: [`arn:${Aws.PARTITION}:appconfig:${Aws.REGION}:${Aws.ACCOUNT_ID}:application/${app.applicationId}/environment/${appEnv.environmentId}/configuration/*`],
    });

    const appConfigExtension = lambda.LayerVersion.fromLayerVersionArn(this, 'AppConfig-Extension', 'arn:aws:lambda:us-west-2:359756378197:layer:AWS-AppConfig-Extension-Arm64:3');

    const bucket = new s3.Bucket(this, 'Bucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [{
        prefix: 'raw',
        transitions: [{
          storageClass: s3.StorageClass.INTELLIGENT_TIERING,
          transitionAfter: Duration.days(0),
        }],
      }],
    });

    const tweetTable = new dynamodb.Table(this, 'TweetTable', {
      partitionKey: {
        name: 'id',
        type: dynamodb.AttributeType.STRING,
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      stream: dynamodb.StreamViewType.NEW_IMAGE,
    });
    tweetTable.addGlobalSecondaryIndex({
      indexName: 'created_at-index',
      partitionKey: {
        name: 'created_at_year',
        type: dynamodb.AttributeType.STRING,
      },
      sortKey: {
        name: 'created_at',
        type: dynamodb.AttributeType.STRING,
      },
      projectionType: dynamodb.ProjectionType.KEYS_ONLY,
    });

    const twitterEventBus = new EventBus(this, 'TwitterEventBus');

    const allowedEventPattern: EventPattern = {
      source: ['twitter.api.v2'],
      detail: {
        data: {
          source: [{ 'anything-but': twitterFilterSourceLabels.stringListValue }],
        },
      },
    };

    twitterEventBus.archive('Archive', { eventPattern: allowedEventPattern });

    const archiveStream = new DeliveryStream(this, 'ArchiveStream', {
      destinationBucket: bucket,
      prefix: 'raw/tweets/v2/',
      errorOutputPrefix: 'raw/tweets/v2-error/',
    });

    const comprehendJob = new ComprehendWithCache(this, 'ComprehendJob', { cacheExpireDays: 14 });

    const archiveEventFunction = new Function(this, 'ArchiveEventFunction', {
      description: '[SocialAnalytics] Archive event for Firehose-S3',
      entry: './src/functions/archive-event/index.ts',
      insightsVersion,
      tracing: lambda.Tracing.ACTIVE,
      environment: {
        POWERTOOLS_SERVICE_NAME: 'ArchiveEventFunction',
        POWERTOOLS_METRICS_NAMESPACE: Aws.STACK_NAME,
        DELIVERY_STREAM_NAME: archiveStream.deliveryStreamName,
      },
    });
    archiveEventFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ['firehose:PutRecord'],
      resources: [archiveStream.deliveryStreamArn],
    }));
    new Rule(this, 'ArchiveEventRule', {
      eventBus: twitterEventBus,
      eventPattern: allowedEventPattern,
      targets: [new eventsTargets.LambdaFunction(archiveEventFunction)],
    });

    const dynamoLoaderFunction = new Function(this, 'DynamoLoaderFunction', {
      description: '[SocialAnalytics] Tweet event processor to update DynamoDB',
      entry: './src/functions/dynamo-loader/index.ts',
      layers: [appConfigExtension],
      insightsVersion,
      tracing: lambda.Tracing.ACTIVE,
      environment: {
        POWERTOOLS_SERVICE_NAME: 'DynamoLoaderFunction',
        POWERTOOLS_METRICS_NAMESPACE: Aws.STACK_NAME,
        APPCONFIG_BASE_URL: `http://localhost:2772/applications/${app.applicationName}/environments/${appEnv.environmentName}/configurations`,
        TWEET_TABLE_NAME: tweetTable.tableName,
      },
      initialPolicy: [appConfigPolicyStatement],
    });
    tweetTable.grantWriteData(dynamoLoaderFunction);
    new Rule(this, 'DynamoLoaderRule', {
      eventBus: twitterEventBus,
      eventPattern: allowedEventPattern,
      targets: [new eventsTargets.LambdaFunction(dynamoLoaderFunction)],
    });

    const analyzeFunction = new Function(this, 'AnalyzeFunction', {
      description: '[SocialAnalytics] Amazon Comprehend',
      entry: './src/functions/analyze/index.ts',
      memorySize: 256,
      timeout: Duration.minutes(1),
      insightsVersion,
      tracing: lambda.Tracing.ACTIVE,
      environment: {
        POWERTOOLS_SERVICE_NAME: 'AnalyzeFunction',
        POWERTOOLS_METRICS_NAMESPACE: Aws.STACK_NAME,
        TWEET_TABLE_NAME: tweetTable.tableName,
        COMPREHEND_JOB_ARN: comprehendJob.stateMachine.stateMachineArn,
      },
      events: [
        new DynamoEventSource(tweetTable, {
          startingPosition: lambda.StartingPosition.LATEST,
          batchSize: 100,
          maxBatchingWindow: Duration.seconds(5),
          maxRecordAge: Duration.days(1),
        }),
      ],
    });
    tweetTable.grantWriteData(analyzeFunction);
    comprehendJob.stateMachine.grantStartSyncExecution(analyzeFunction);

    const tweetProducer = new TweetProducer(this, 'TweetProducer', {
      twitterFieldsParams,
      eventBus: twitterEventBus,
      producerCount: 3,
    });
    if (typeof defaultTwitterBearerToken == 'string') {
      tweetProducer.streamReader[0].bearerToken.default = defaultTwitterBearerToken;
    }

    new OpenSearchPackages(this, 'OpenSearchPackages', {
      sourcePath: './src/opensearch-packages',
      stagingBucket: bucket,
      stagingKeyPrefix: 'opensearch/packages/',
    });

    const dashboard = new Dashboard(this, 'Dashboard', {
      snapshotBucketName: bucket.bucketName,
      snapshotBasePath: 'opensearch/snapshot',
    });

    const openSearchLoaderFunction = new Function(this, 'OpenSearchLoaderFunction', {
      description: '[SocialAnalytics] OpenSearch Bulk Loader',
      entry: './src/functions/opensearch-loader/index.ts',
      memorySize: 256,
      timeout: Duration.minutes(1),
      insightsVersion,
      tracing: lambda.Tracing.ACTIVE,
      environment: {
        POWERTOOLS_SERVICE_NAME: 'OpenSearchLoaderFunction',
        POWERTOOLS_METRICS_NAMESPACE: Aws.STACK_NAME,
        OPENSEARCH_DOMAIN_ENDPOINT: dashboard.Domain.domainEndpoint,
      },
      events: [
        new DynamoEventSource(tweetTable, {
          startingPosition: lambda.StartingPosition.LATEST,
          batchSize: 100,
          maxBatchingWindow: Duration.seconds(5),
          maxRecordAge: Duration.days(1),
        }),
      ],
    });

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
        backend_roles: [`${openSearchLoaderFunction.role?.roleArn}`],
      },
    });

    new ReIndexBatch(this, 'ReIndexBatch', {
      tweetTable: tweetTable,
      dataLoadFunction: openSearchLoaderFunction,
    });

    //const reingestTweetsV1Function = new RetryFunction(this, 'ReingestTweetsV1Function', {
    //  source: { bucket, prefix: 'reingest/tweets/v1/' },
    //  description: 'Re-ingest for TweetsV1',
    //  entry: './src/functions/reingest-tweets-v1/index.ts',
    //  timeout: Duration.minutes(5),
    //  insightsVersion,
    //  tracing,
    //  initialPolicy: [twitterParameterPolicyStatement],
    //  environment: {
    //    TWITTER_PARAMETER_PATH: twitterParameterPath,
    //    DEST_STREAM_NAME: ingestionStream.streamName,
    //  },
    //});
    //ingestionStream.grantWrite(reingestTweetsV1Function);

    //const reingestTweetsV2Function = new RetryFunction(this, 'ReingestTweetsV2Function', {
    //  source: { bucket, prefix: 'reingest/tweets/v2/' },
    //  description: 'Re-ingest for TweetsV2',
    //  entry: './src/functions/reingest-tweets-v2/index.ts',
    //  memorySize: 1536,
    //  timeout: Duration.minutes(15),
    //  insightsVersion,
    //  tracing,
    //  initialPolicy: [twitterParameterPolicyStatement],
    //  environment: {
    //    DEST_STREAM_NAME: ingestionStream.streamName,
    //  },
    //});
    //ingestionStream.grantWrite(reingestTweetsV2Function);

    //const reindexTweetsV2Function = new RetryFunction(this, 'ReindexTweetsV2Function', {
    //  source: { bucket, prefix: 'reindex/tweets/v2/' },
    //  description: 'Re-index for TweetsV2',
    //  entry: './src/functions/reindex-tweets-v2/index.ts',
    //  memorySize: 1024,
    //  timeout: Duration.minutes(5),
    //  insightsVersion,
    //  tracing,
    //  initialPolicy: [twitterParameterPolicyStatement],
    //  environment: {
    //    INDEXING_FUNCTION_ARN: indexingFunction.functionArn,
    //  },
    //  reservedConcurrentExecutions: 8,
    //});
    //indexingFunction.grantInvoke(reindexTweetsV2Function);

  }
};
