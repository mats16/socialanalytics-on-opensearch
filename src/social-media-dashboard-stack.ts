import * as firehose from '@aws-cdk/aws-kinesisfirehose-alpha';
import * as destinations from '@aws-cdk/aws-kinesisfirehose-destinations-alpha';
import { Stack, StackProps, Duration, Size, CfnParameter } from 'aws-cdk-lib';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Secret, SecretStringValueBeta1 } from 'aws-cdk-lib/aws-secretsmanager';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import { configs } from './configs';
import { ContainerInsights } from './resources/container-insights';
import { TwitterStreamingReader } from './resources/twitter-streaming-reader';

export class SocialMediaDashboardStack extends Stack {
  constructor(scope: Construct, id: string, props: StackProps = {}) {
    super(scope, id, props);

    const twitterConsumerKey = new CfnParameter(this, 'TwitterConsumerKey', { type: 'String', default: 'REPLACE', noEcho: true });
    const twitterConsumerSecret = new CfnParameter(this, 'TwitterConsumerSecret', { type: 'String', default: 'REPLACE', noEcho: true });
    const twitterAccessToken = new CfnParameter(this, 'TwitterAccessToken', { type: 'String', default: 'REPLACE', noEcho: true });
    const twitterAccessTokenSecret = new CfnParameter(this, 'TwitterAccessTokenSecret', { type: 'String', default: 'REPLACE', noEcho: true });

    this.templateOptions.metadata = {
      'AWS::CloudFormation::Interface': {
        ParameterGroups: [
          {
            Label: { default: 'Twitter API Credentials' },
            Parameters: [
              twitterConsumerKey.logicalId,
              twitterConsumerSecret.logicalId,
              twitterAccessToken.logicalId,
              twitterAccessTokenSecret.logicalId,
            ],
          },
        ],
      },
    };

    const twitterTopics = new ssm.StringListParameter(this, 'TwitterTopics', { stringListValue: configs.twitterTopics });
    const twitterLanguages = new ssm.StringListParameter(this, 'TwitterLanguages', { stringListValue: configs.twitterLanguages });
    const twitterFilterLevel = new ssm.StringParameter(this, 'TwitterFilterLevel', { stringValue: configs.twitterFilterLevel });

    const twitterCredentials = new Secret(this, 'TwitterCredentials', {
      secretStringBeta1: SecretStringValueBeta1.fromUnsafePlaintext(JSON.stringify({
        consumer_key: twitterConsumerKey.valueAsString,
        consumer_secret: twitterConsumerSecret.valueAsString,
        access_token: twitterAccessToken.valueAsString,
        access_token_secret: twitterAccessTokenSecret.valueAsString,
      })),
    });

    const bucket = new s3.Bucket(this, 'Bucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
    });

    const ingestionStream = new kinesis.Stream(this, 'IngestionStream', {
      streamMode: kinesis.StreamMode.ON_DEMAND,
      encryption: kinesis.StreamEncryption.MANAGED,
    });

    const analysisStream = new kinesis.Stream(this, 'AnalysisStream', {
      streamMode: kinesis.StreamMode.ON_DEMAND,
      encryption: kinesis.StreamEncryption.MANAGED,
    });

    const ingestionArchiveStream = new firehose.DeliveryStream(this, 'IngestionArchiveStream', {
      sourceStream: ingestionStream,
      destinations: [
        new destinations.S3Bucket(bucket, {
          dataOutputPrefix: 'raw/',
          errorOutputPrefix: 'raw-error/!{firehose:error-output-type}/',
          bufferingInterval: Duration.seconds(300),
          bufferingSize: Size.mebibytes(128),
          compression: destinations.Compression.GZIP,
        }),
      ],
    });

    const twitterStreamingReader = new TwitterStreamingReader(this, 'TwitterStreamingReader', {
      twitterTopics,
      twitterLanguages,
      twitterFilterLevel,
      twitterCredentials,
      ingestionStream,
    });

    const containerInsights = new ContainerInsights(this, 'ContainerInsights', {
      targetService: twitterStreamingReader.service,
    });

  }
}