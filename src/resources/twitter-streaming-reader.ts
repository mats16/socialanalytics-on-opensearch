import * as firehose from '@aws-cdk/aws-kinesisfirehose-alpha';
//import { NestedStack, NestedStackProps } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { DockerImageAsset } from 'aws-cdk-lib/aws-ecr-assets';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';

interface TwitterStreamingReaderProps {
  twitterTopics: ssm.IStringListParameter;
  twitterLanguages: ssm.IStringListParameter;
  twitterFilterLevel: ssm.IStringParameter;
  twitterCredentials: secretsmanager.ISecret;
  ingestionStream: firehose.IDeliveryStream;
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

    const cluster = new ecs.Cluster(this, 'Cluster', { vpc });

    const taskDefinition = new ecs.FargateTaskDefinition(this, 'TaskDefinition', {
      runtimePlatform: {
        operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
        cpuArchitecture: ecs.CpuArchitecture.ARM64,
      },
    });
    logGroup.grantWrite(taskDefinition.taskRole);
    ingestionStream.grantPutRecords(taskDefinition.taskRole);

    const appContainer = taskDefinition.addContainer('App', {
      containerName: 'app',
      image: ecs.ContainerImage.fromDockerImageAsset(twitterStreamingReaderImage),
      essential: true,
      secrets: {
        TWITTER_TOPICS: ecs.Secret.fromSsmParameter(props.twitterTopics),
        TWITTER_LANGUAGES: ecs.Secret.fromSsmParameter(props.twitterLanguages),
        TWITTER_FILTER_LEVEL: ecs.Secret.fromSsmParameter(props.twitterFilterLevel),
        TWITTER_CREDENTIALS: ecs.Secret.fromSecretsManager(props.twitterCredentials),
      },
      logging: new ecs.FireLensLogDriver({}),
      readonlyRootFilesystem: true,
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
      memoryReservationMiB: 50,
      portMappings: [{
        containerPort: 2020,
        protocol: ecs.Protocol.TCP,
      }],
      healthCheck: {
        command: ['echo', '\'{"health": "check"}\'', '|', 'nc', '127.0.0.1', '8877', '||', 'exit', '1'],
      },
      environment: {
        LOG_GROUP_NAME: logGroup.logGroupName,
        DELIVERY_STREAM_NAME: ingestionStream.deliveryStreamName,
      },
      logging: new ecs.AwsLogDriver({
        logGroup: logGroup,
        streamPrefix: 'firelens',
      }),
      
    });

    appContainer.addContainerDependencies({
      container: logRouterContainer,
      condition: ecs.ContainerDependencyCondition.START,
    });

    const service = new ecs.FargateService(this, 'Service', {
      cluster,
      taskDefinition,
      assignPublicIp: true,
    })

  }
}
