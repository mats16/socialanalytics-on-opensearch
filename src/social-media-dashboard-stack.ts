import * as firehose from '@aws-cdk/aws-kinesisfirehose-alpha';
import * as destinations from '@aws-cdk/aws-kinesisfirehose-destinations-alpha';
import { Stack, StackProps, Duration, Size, CfnParameter } from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { KinesisEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Secret, SecretStringValueBeta1 } from 'aws-cdk-lib/aws-secretsmanager';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import { configs } from './configs';
import { ContainerInsights } from './resources/container-insights';
import { DeliveryStream } from './resources/dynamic-partitioning-firehose';
import { TwitterStreamingReader } from './resources/twitter-streaming-reader';

export class SocialMediaDashboardStack extends Stack {
  constructor(scope: Construct, id: string, props: StackProps = {}) {
    super(scope, id, props);

    const twitterBearerTokenParameter = new CfnParameter(this, 'TwitterBearerTokenParameter', { type: 'String', default: 'REPLAC', noEcho: true });
    const twitterBearerToken = new Secret(this, 'TwitterBearerToken', {
      secretStringBeta1: SecretStringValueBeta1.fromUnsafePlaintext(twitterBearerTokenParameter.valueAsString),
    });

    const bucket = new s3.Bucket(this, 'Bucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
    });

    const ingestionStream = new kinesis.Stream(this, 'IngestionStream', {
      streamMode: kinesis.StreamMode.ON_DEMAND,
      encryption: kinesis.StreamEncryption.MANAGED,
    });

    const indexingStream = new kinesis.Stream(this, 'IndexingStream', {
      streamMode: kinesis.StreamMode.ON_DEMAND,
      encryption: kinesis.StreamEncryption.MANAGED,
    });

    const analysisFunction = new NodejsFunction(this, 'AnalysisFunction', {
      description: 'Get insights from Amazon Comprehend',
      entry: './src/functions/analysis/index.ts',
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_14_X,
      architecture: lambda.Architecture.ARM_64,
      timeout: Duration.minutes(5),
      environment: {
        INDEXING_STREAM_NAME: indexingStream.streamName,
      },
      events: [
        new KinesisEventSource(ingestionStream, {
          startingPosition: lambda.StartingPosition.LATEST,
          batchSize: 200,
          maxBatchingWindow: Duration.seconds(15),
          maxRecordAge: Duration.days(1),
        }),
      ],
      initialPolicy: [new iam.PolicyStatement({
        actions: [
          'comprehend:DetectEntities',
          'comprehend:DetectKeyPhrases',
          'comprehend:DetectSentiment',
          'translate:TranslateText',
        ],
        resources: ['*'],
      })],
    });
    indexingStream.grantWrite(analysisFunction);

    const indexingFunction = new NodejsFunction(this, 'IndexingFunction', {
      description: 'Bulk indexing to Amazon OpenSearch Service',
      entry: './src/functions/indexing/index.ts',
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_14_X,
      architecture: lambda.Architecture.ARM_64,
      timeout: Duration.minutes(5),
      environment: {
        OPENSEARCH_HOST: 'hoge',
      },
      events: [
        new KinesisEventSource(indexingStream, {
          startingPosition: lambda.StartingPosition.LATEST,
          batchSize: 500,
          maxBatchingWindow: Duration.seconds(15),
          maxRecordAge: Duration.days(1),
        }),
      ],
    });

    const archiveFilterFunction = new NodejsFunction(this, 'ArchiveFilterFunction', {
      entry: './src/functions/archive-filter/index.ts',
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_14_X,
      architecture: lambda.Architecture.ARM_64,
      timeout: Duration.minutes(5),
      environment: {
        TAG_NAME: 'twitter.stream',
      },
    });

    //const ingestionArchiveStream = new firehose.DeliveryStream(this, 'IngestionArchiveStream', {
    //  sourceStream: ingestionStream,
    //  destinations: [
    //    new destinations.S3Bucket(bucket, {
    //      dataOutputPrefix: 'raw/',
    //      errorOutputPrefix: 'raw-error/!{firehose:error-output-type}/',
    //      bufferingInterval: Duration.seconds(300),
    //      bufferingSize: Size.mebibytes(128),
    //      compression: destinations.Compression.GZIP,
    //      processor: new firehose.LambdaFunctionProcessor(archiveFilterFunction),
    //    }),
    //  ],
    //});

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

  }
}