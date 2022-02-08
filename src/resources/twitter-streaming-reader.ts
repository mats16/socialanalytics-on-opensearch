import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { DockerImageAsset } from 'aws-cdk-lib/aws-ecr-assets';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';

interface TwitterStreamingReaderProps {
  twitterBearerToken: secretsmanager.ISecret;
  ingestionStream: kinesis.IStream;
};

export class TwitterStreamingReader extends Construct {
  service: ecs.FargateService;

  constructor(scope: Construct, id: string, props: TwitterStreamingReaderProps) {
    super(scope, id);

    const ingestionStream = props.ingestionStream;

    const vpc = new ec2.Vpc(this, 'VPC', {
      subnetConfiguration: [{
        cidrMask: 24,
        name: 'PublicSubnet',
        subnetType: ec2.SubnetType.PUBLIC,
      }],
    });

    const logGroup = new logs.LogGroup(this, 'LogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
    });

    const twitterStreamingReaderImage = new DockerImageAsset(this, 'TwitterStreamingReaderImage', {
      directory: './src/containers/twitter-streaming-reader',
      buildArgs: {
        '--platform': 'linux/arm64',
      },
    });

    const logRouterImage = new DockerImageAsset(this, 'LogRouterImage', {
      directory: './src/containers/log-router',
      buildArgs: {
        '--platform': 'linux/arm64',
      },
    });

    const cluster = new ecs.Cluster(this, 'Cluster', { vpc, containerInsights: true });

    const taskDefinition = new ecs.FargateTaskDefinition(this, 'TaskDefinition', {
      runtimePlatform: {
        operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
        cpuArchitecture: ecs.CpuArchitecture.ARM64,
      },
    });
    logGroup.grantWrite(taskDefinition.taskRole);
    ingestionStream.grantWrite(taskDefinition.taskRole);

    const appContainer = taskDefinition.addContainer('App', {
      containerName: 'app',
      image: ecs.ContainerImage.fromDockerImageAsset(twitterStreamingReaderImage),
      cpu: 128,
      memoryReservationMiB: 256,
      essential: true,
      secrets: {
        TWITTER_BEARER_TOKEN: ecs.Secret.fromSecretsManager(props.twitterBearerToken),
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
      image: ecs.ContainerImage.fromDockerImageAsset(logRouterImage),
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