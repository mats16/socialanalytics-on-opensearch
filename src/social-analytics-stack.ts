import { Stack, StackProps, Duration, Aws } from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { EventBus, Rule } from 'aws-cdk-lib/aws-events';
import * as eventsTargets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { DynamoEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { StringParameter, StringListParameter } from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import { tweetFieldsParams } from './parameter';
import { Dashboard } from './resources/dashboard';
import { DeliveryStream } from './resources/dynamic-partitioning-firehose';
import { OpenSearchPackages } from './resources/opensearch-packages';
import { ReIndexBatch } from './resources/sfn-reindex-batch';
import { ComprehendWithCache } from './resources/sfn-state-machines';
import { TweetProducer } from './resources/tweet-producer';

interface SocialAnalyticsStackProps extends StackProps {
  defaultTwitterBearerToken?: string;
};

//const insightsVersion = lambda.LambdaInsightsVersion.VERSION_1_0_143_0;

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

    const twitterContextDomainDenyList = new StringListParameter(this, 'TwitterContextDomainDenyList', {
      // https://developer.twitter.com/en/docs/twitter-api/annotations/overview
      description: 'Context domains for filtering',
      parameterName: `${twitterParameterPath}/DenyList/ContextDomain`,
      stringListValue: ['Musician', 'Music Genre', 'Actor', 'TV Shows', 'Multimedia Franchise', 'Fictional Character', 'Entertainment Personality'],
      simpleName: false,
    });

    //const twitterSourceLabelDenyList = new StringListParameter(this, 'TwitterSourceLabelDenyList', {
    //  // https://help.twitter.com/en/using-twitter/how-to-tweet#source-labels
    //  description: 'Tweet source labels for filtering',
    //  parameterName: `${twitterParameterPath}/DenyList/SourceLabel`,
    //  stringListValue: ['Twitter for Advertisers', 'twittbot.net', 'Mk00JapanBot', 'Gakeppu Tweet', 'BelugaCampaignSEA', 'rare_zaiko', 'Wn32ShimaneBot', 'uhiiman_bot', 'atulsbots'],
    //  simpleName: false,
    //});

    //const awsParametersAndSecretsLambdaExtension = lambda.LayerVersion.fromLayerVersionArn(this, 'ParametersAndSecretsLambdaExtension', 'arn:aws:lambda:us-west-2:345057560386:layer:AWS-Parameters-and-Secrets-Lambda-Extension-Arm64:2');

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
      indexName: 'created_at',
      partitionKey: {
        name: '_all',
        type: dynamodb.AttributeType.STRING,
      },
      sortKey: {
        name: 'created_at',
        type: dynamodb.AttributeType.STRING,
      },
      projectionType: dynamodb.ProjectionType.KEYS_ONLY,
    });

    const eventBus = new EventBus(this, 'EventBus');

    eventBus.archive('TweetArchive', {
      eventPattern: {
        source: ['twitter.api.v2'],
        detailType: ['Tweet'],
      },
    });

    const comprehendJob = new ComprehendWithCache(this, 'ComprehendJob', { cacheExpireDays: 14 });

    const tweetProducer = new TweetProducer(this, 'TweetProducer', {
      twitterFieldsParams,
      eventBus,
      producerCount: 3,
    });
    if (typeof defaultTwitterBearerToken == 'string') {
      tweetProducer.streamReader[0].bearerToken.default = defaultTwitterBearerToken;
    }

    const archiveStream = new DeliveryStream(this, 'ArchiveStream', {
      destinationBucket: bucket,
      prefix: 'raw/tweets/v2/',
      errorOutputPrefix: 'raw/tweets/v2-error/',
    });

    const archiveEventFunction = new NodejsFunction(this, 'ArchiveEventFunction', {
      description: 'SocialAnalytics - Archive event for Firehose-S3',
      entry: './src/functions/archive-event.ts',
      bundling: {
        externalModules: ['@aws-sdk/*'],
      },
      runtime: lambda.Runtime.NODEJS_18_X,
      architecture: lambda.Architecture.ARM_64,
      timeout: Duration.seconds(10),
      environment: {
        POWERTOOLS_SERVICE_NAME: 'ArchiveEventFunction',
        POWERTOOLS_METRICS_NAMESPACE: Aws.STACK_NAME,
        DELIVERY_STREAM_NAME: archiveStream.deliveryStreamName,
      },
      tracing: lambda.Tracing.ACTIVE,
    });
    archiveEventFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ['firehose:PutRecord'],
      resources: [archiveStream.deliveryStreamArn],
    }));

    new Rule(this, 'ArchiveEventRule', {
      targets: [new eventsTargets.LambdaFunction(archiveEventFunction)],
      eventBus: eventBus,
      eventPattern: {
        'source': ['twitter.api.v2'],
        'detailType': ['Tweet'],
        'replay-name': [{ exists: false }],
      } as any,
    });

    const newTweetConsumer = new NodejsFunction(this, 'NewTweetConsumer', {
      description: 'SocialAnalytics - New tweet event consumer to update DynamoDB',
      entry: './src/functions/new-tweet-consumer.ts',
      bundling: {
        externalModules: ['@aws-sdk/*'],
      },
      runtime: lambda.Runtime.NODEJS_18_X,
      architecture: lambda.Architecture.ARM_64,
      timeout: Duration.seconds(10),
      environment: {
        POWERTOOLS_SERVICE_NAME: 'NewTweetConsumer',
        POWERTOOLS_METRICS_NAMESPACE: Aws.STACK_NAME,
        TWEET_TABLE_NAME: tweetTable.tableName,
        COMPREHEND_SFN_ARN: comprehendJob.stateMachine.stateMachineArn,
      },
      tracing: lambda.Tracing.ACTIVE,
    });
    comprehendJob.stateMachine.grantStartSyncExecution(newTweetConsumer);
    tweetTable.grantWriteData(newTweetConsumer);

    const includedTweetFanoutProducer = new NodejsFunction(this, 'IncludedTweetFanoutProducer', {
      description: 'SocialAnalytics - Included tweet fanout producer',
      entry: './src/functions/included-tweet-fanout-producer.ts',
      bundling: {
        externalModules: ['@aws-sdk/*'],
      },
      runtime: lambda.Runtime.NODEJS_18_X,
      architecture: lambda.Architecture.ARM_64,
      timeout: Duration.seconds(10),
      environment: {
        POWERTOOLS_SERVICE_NAME: 'IncludedTweetFanoutProducer',
        POWERTOOLS_METRICS_NAMESPACE: Aws.STACK_NAME,
        EVENT_BUS_ARN: eventBus.eventBusArn,
      },
      tracing: lambda.Tracing.ACTIVE,
    });
    eventBus.grantPutEventsTo(includedTweetFanoutProducer);

    new Rule(this, 'NewTweetArrival', {
      description: 'SocialAnalytics - New tweet arrival',
      targets: [
        new eventsTargets.LambdaFunction(newTweetConsumer),
        new eventsTargets.LambdaFunction(includedTweetFanoutProducer),
      ],
      eventBus: eventBus,
      eventPattern: {
        source: ['twitter.api.v2'],
        detailType: ['Tweet'],
        detail: {
          data: {
            context_annotations: {
              domain: {
                name: [{ 'anything-but': twitterContextDomainDenyList.stringListValue }],
              },
            },
          },
          matching_rules: {
            id: [{ exists: true }],
          },
        },
      },
    });

    const includedTweetConsumer = new NodejsFunction(this, 'IncludedTweetConsumer', {
      description: 'SocialAnalytics - Included tweets consumer to update DynamoDB',
      entry: './src/functions/included-tweet-consumer.ts',
      bundling: {
        externalModules: ['@aws-sdk/*'],
        nodeModules: ['axios'],
      },
      runtime: lambda.Runtime.NODEJS_18_X,
      architecture: lambda.Architecture.ARM_64,
      timeout: Duration.seconds(10),
      environment: {
        POWERTOOLS_SERVICE_NAME: 'IncludedTweetConsumer',
        POWERTOOLS_METRICS_NAMESPACE: Aws.STACK_NAME,
        TWEET_TABLE_NAME: tweetTable.tableName,
        NEW_TWEET_CONSUMER_ARN: newTweetConsumer.functionArn,
      },
      tracing: lambda.Tracing.ACTIVE,
    });
    tweetTable.grantWriteData(includedTweetConsumer);
    newTweetConsumer.grantInvoke(includedTweetConsumer);

    new Rule(this, 'IncludedTweetsRule', {
      targets: [new eventsTargets.LambdaFunction(includedTweetConsumer)],
      eventBus: eventBus,
      eventPattern: {
        source: ['twitter.api.v2'],
        detailType: ['IncludedTweet'],
        detail: {
          data: {
            created_at: [
              { prefix: '202' },
            ],
            context_annotations: {
              domain: {
                name: [{ 'anything-but': twitterContextDomainDenyList.stringListValue }],
              },
            },
          },
        },
      },
    });

    new OpenSearchPackages(this, 'OpenSearchPackages', {
      sourcePath: './src/opensearch-packages',
      stagingBucket: bucket,
      stagingKeyPrefix: 'opensearch/packages/',
    });

    const dashboard = new Dashboard(this, 'Dashboard', {
      snapshotBucketName: bucket.bucketName,
      snapshotBasePath: 'opensearch/snapshot',
    });

    const openSearchBulkLoader = new NodejsFunction(this, 'OpenSearchBulkLoader', {
      description: 'SocialAnalytics - OpenSearch Bulk Loader',
      entry: './src/functions/opensearch-bulk-loader/index.ts',
      bundling: {
        externalModules: ['@aws-sdk/*'],
      },
      runtime: lambda.Runtime.NODEJS_18_X,
      architecture: lambda.Architecture.ARM_64,
      memorySize: 256,
      timeout: Duration.minutes(1),
      environment: {
        POWERTOOLS_SERVICE_NAME: 'OpenSearchBulkLoader',
        POWERTOOLS_METRICS_NAMESPACE: Aws.STACK_NAME,
        OPENSEARCH_DOMAIN_ENDPOINT: dashboard.Domain.domainEndpoint,
      },
      events: [
        new DynamoEventSource(tweetTable, {
          startingPosition: lambda.StartingPosition.LATEST,
          batchSize: 100,
          maxBatchingWindow: Duration.seconds(5),
          maxRecordAge: Duration.days(1),
          filters: [
            lambda.FilterCriteria.filter({
              eventName: ['INSERT', 'MODIFY'],
              dynamodb: {
                NewImage: {
                  text: {
                    S: [{ exists: true }],
                  },
                  created_at: {
                    S: [{ exists: true }],
                  },
                },
              },
            }),
          ],
        }),
      ],
      tracing: lambda.Tracing.ACTIVE,
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
        backend_roles: [`${openSearchBulkLoader.role?.roleArn}`],
      },
    });

    new ReIndexBatch(this, 'ReIndexBatch', {
      tweetTable: tweetTable,
      dataLoadFunction: openSearchBulkLoader,
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
