import { Stack } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as logs from 'aws-cdk-lib/aws-logs';
import { IStringParameter } from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';

interface TwitterStreamingReaderProps {
  vpc: ec2.IVpc;
  twitterBearerToken: IStringParameter;
  twitterFieldsParams: IStringParameter;
  ingestionStream: kinesis.IStream;
};

export class TwitterStreamingReader extends Construct {
  service: ecs.FargateService;

  constructor(scope: Stack, id: string, props: TwitterStreamingReaderProps) {
    super(scope, id);

    const { vpc, twitterBearerToken, twitterFieldsParams, ingestionStream } = props;

    const logGroup = new logs.LogGroup(this, 'LogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
    });

    const cluster = new ecs.Cluster(this, 'Cluster', { vpc, containerInsights: true });

    const taskDefinition = new ecs.FargateTaskDefinition(this, 'TaskDefinition', {
      runtimePlatform: {
        operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
        cpuArchitecture: ecs.CpuArchitecture.X86_64,
      },
    });
    logGroup.grantWrite(taskDefinition.taskRole);
    ingestionStream.grantWrite(taskDefinition.taskRole);

    const appContainer = taskDefinition.addContainer('App', {
      containerName: 'app',
      image: ecs.ContainerImage.fromAsset('./src/containers/twitter-streaming-reader'),
      cpu: 128,
      memoryReservationMiB: 256,
      essential: true,
      secrets: {
        TWITTER_BEARER_TOKEN: ecs.Secret.fromSsmParameter(twitterBearerToken),
        TWITTER_FIELDS_PARAMS: ecs.Secret.fromSsmParameter(twitterFieldsParams),
      },
      readonlyRootFilesystem: true,
      logging: new ecs.FireLensLogDriver({}),
    });

    const logRouterContainer = taskDefinition.addFirelensLogRouter('LogRouter', {
      firelensConfig: {
        type: ecs.FirelensLogRouterType.FLUENTBIT,
        options: {
          configFileType: ecs.FirelensConfigFileType.FILE,
          configFileValue: '/fluent-bit/etc/extra.conf',
          enableECSLogMetadata: false,
        },
      },
      containerName: 'log-router',
      image: ecs.ContainerImage.fromAsset('./src/containers/log-router'),
      cpu: 64,
      memoryReservationMiB: 128,
      portMappings: [{
        containerPort: 2020,
        protocol: ecs.Protocol.TCP,
      }],
      healthCheck: {
        command: ['echo', '\'{"health": "check"}\'', '|', 'nc', '127.0.0.1', '8877', '||', 'exit', '1'],
      },
      environment: {
        LOG_GROUP_NAME: logGroup.logGroupName,
        STREAM_NAME: ingestionStream.streamName,
      },
      readonlyRootFilesystem: true,
      logging: new ecs.AwsLogDriver({
        logGroup: logGroup,
        streamPrefix: 'firelens',
      }),
    });

    appContainer.addContainerDependencies({
      container: logRouterContainer,
      condition: ecs.ContainerDependencyCondition.START,
    });

    this.service = new ecs.FargateService(this, 'Service', {
      cluster,
      taskDefinition,
      assignPublicIp: true,
      minHealthyPercent: 0,
      maxHealthyPercent: 100,
    });

  }
}
