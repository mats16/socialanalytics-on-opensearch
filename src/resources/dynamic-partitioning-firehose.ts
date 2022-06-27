import * as iam from 'aws-cdk-lib/aws-iam';
import { IStream } from 'aws-cdk-lib/aws-kinesis';
import { CfnDeliveryStream } from 'aws-cdk-lib/aws-kinesisfirehose';
import * as logs from 'aws-cdk-lib/aws-logs';
import { IBucket } from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';

interface DeliveryStreamProps {
  sourceStream?: IStream;
  destinationBucket: IBucket;
  prefix: string;
  errorOutputPrefix: string;
};

export class DeliveryStream extends Construct {
  deliveryStreamArn: string;
  deliveryStreamName: string;

  constructor(scope: Construct, id: string, props: DeliveryStreamProps) {
    super(scope, id);

    const { sourceStream, destinationBucket, prefix, errorOutputPrefix } = props;

    const logGroup = new logs.LogGroup(this, 'LogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
    });
    const logStreamName = 'ErrorLogs';

    const role = new iam.Role(this, 'Role', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });
    destinationBucket.grantPut(role, { prefix });
    destinationBucket.grantPut(role, { prefix: errorOutputPrefix });
    logGroup.grantWrite(role);

    let deliveryStreamType: 'DirectPut'|'KinesisStreamAsSource' = 'DirectPut';
    let kinesisStreamSourceConfiguration: CfnDeliveryStream.KinesisStreamSourceConfigurationProperty|undefined = undefined;

    if (typeof sourceStream != 'undefined') {
      deliveryStreamType = 'KinesisStreamAsSource';
      kinesisStreamSourceConfiguration = {
        kinesisStreamArn: sourceStream.streamArn,
        roleArn: role.roleArn,
      };
      sourceStream.grantRead(role);
    }

    const deliveryStream = new CfnDeliveryStream(this, id, {
      deliveryStreamType,
      kinesisStreamSourceConfiguration,
      extendedS3DestinationConfiguration: {
        cloudWatchLoggingOptions: {
          enabled: true,
          logGroupName: logGroup.logGroupName,
          logStreamName: logStreamName,
        },
        bucketArn: destinationBucket.bucketArn,
        roleArn: role.roleArn,
        prefix: `${prefix}year=!{partitionKeyFromQuery:year}/month=!{partitionKeyFromQuery:month}/day=!{partitionKeyFromQuery:day}/`,
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
              type: 'MetadataExtraction',
              parameters: [
                {
                  parameterName: 'MetadataExtractionQuery',
                  parameterValue: '{\
                    year : .data.created_at | split(".")[0] | strptime("%Y-%m-%dT%H:%M:%S") | strftime("%Y"),\
                    month : .data.created_at | split(".")[0] | strptime("%Y-%m-%dT%H:%M:%S") | strftime("%m"),\
                    day : .data.created_at | split(".")[0] | strptime("%Y-%m-%dT%H:%M:%S") | strftime("%d")\
                  }',
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
    this.deliveryStreamName = deliveryStream.ref;
  };
};