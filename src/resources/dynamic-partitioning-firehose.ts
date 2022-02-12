import * as iam from 'aws-cdk-lib/aws-iam';
import { IStream } from 'aws-cdk-lib/aws-kinesis';
import { CfnDeliveryStream } from 'aws-cdk-lib/aws-kinesisfirehose';
import { IFunction } from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import { IBucket } from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';

interface DeliveryStreamProps {
  sourceStream: IStream;
  processorFunction: IFunction;
  destinationBucket: IBucket;
  prefix: string;
  errorOutputPrefix: string;
};

export class DeliveryStream extends Construct {
  deliveryStreamArn: string;

  constructor(scope: Construct, id: string, props: DeliveryStreamProps) {
    super(scope, id);

    const sourceStreamArn = props.sourceStream.streamArn;
    const processorFunctionArn = props.processorFunction.functionArn;
    const bucketArn = props.destinationBucket.bucketArn;
    const { prefix, errorOutputPrefix } = props;

    const logGroup = new logs.LogGroup(this, 'LogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
    });
    const logStreamName = 'ErrorLogs';

    const role = new iam.Role(this, 'Role', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
      inlinePolicies: {
        SourcePolicy: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                'kinesis:DescribeStreamSummary',
                'kinesis:GetRecords',
                'kinesis:GetShardIterator',
                'kinesis:ListShards',
                'kinesis:SubscribeToShard',
                'kinesis:DescribeStream',
                'kinesis:ListStreams',
              ],
              resources: [sourceStreamArn],
            }),
          ],
        }),
        DestinationPolicy: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                's3:GetObject*',
                's3:GetBucket*',
                's3:List*',
                's3:DeleteObject*',
                's3:PutObject',
                's3:Abort*',
              ],
              resources: [
                bucketArn,
                `${bucketArn}/${prefix}*`,
                `${bucketArn}/${errorOutputPrefix}*`,
              ],
            }),
          ],
        }),
        LoggingPolicy: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                'logs:CreateLogStream',
                'logs:PutLogEvents',
              ],
              resources: [`${logGroup.logGroupArn}:${logStreamName}`],
            }),
          ],
        }),
        ProcessorPolicy: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              actions: [
                'lambda:InvokeFunction',
              ],
              resources: [processorFunctionArn],
            }),
          ],
        }),
      },
    });

    const deliveryStream = new CfnDeliveryStream(this, id, {
      deliveryStreamType: 'KinesisStreamAsSource',
      kinesisStreamSourceConfiguration: {
        kinesisStreamArn: sourceStreamArn,
        roleArn: role.roleArn,
      },
      extendedS3DestinationConfiguration: {
        cloudWatchLoggingOptions: {
          enabled: true,
          logGroupName: logGroup.logGroupName,
          logStreamName: logStreamName,
        },
        bucketArn,
        roleArn: role.roleArn,
        prefix: `${prefix}dt=!{partitionKeyFromQuery:dt}/`,
        errorOutputPrefix: `${errorOutputPrefix}!{firehose:error-output-type}/`,
        bufferingHints: {
          intervalInSeconds: 900,
          sizeInMBs: 128,
        },
        compressionFormat: 'GZIP',
        dynamicPartitioningConfiguration: {
          enabled: true,
        },
        processingConfiguration: {
          enabled: true,
          processors: [
            {
              type: 'Lambda',
              parameters: [
                {
                  parameterName: 'LambdaArn',
                  parameterValue: processorFunctionArn,
                },
              ],
            },
            {
              type: 'MetadataExtraction',
              parameters: [
                {
                  parameterName: 'MetadataExtractionQuery',
                  parameterValue: '{dt : .data.created_at | split(".")[0] | strptime("%Y-%m-%dT%H:%M:%S") | strftime("%Y-%m-%d")}', // 2022-01-24T17:41:38.000Z
                },
                {
                  parameterName: 'JsonParsingEngine',
                  parameterValue: 'JQ-1.6',
                },
              ],
            },
            {
              type: 'AppendDelimiterToRecord',
              parameters: [
                {
                  parameterName: 'Delimiter',
                  parameterValue: '\\n',
                },
              ],
            },
          ],
        },
      },
    });
    this.deliveryStreamArn = deliveryStream.attrArn;
  };
};