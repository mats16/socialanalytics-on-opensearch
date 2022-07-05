import { Stack } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import { IEventBus } from 'aws-cdk-lib/aws-events';
import * as iam from 'aws-cdk-lib/aws-iam';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Queue } from 'aws-cdk-lib/aws-sqs';
import { IStringParameter } from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import { Function } from './lambda-nodejs';

const xrayPolicy = iam.ManagedPolicy.fromAwsManagedPolicyName('AWSXRayDaemonWriteAccess');

interface TwitterStreamingReaderProps {
  vpc: ec2.IVpc;
  twitterBearerToken: IStringParameter;
  twitterFieldsParams: IStringParameter;
  eventBus: IEventBus;
};

export class TwitterStreamingReader extends Construct {
  service: ecs.FargateService;

  constructor(scope: Stack, id: string, props: TwitterStreamingReaderProps) {
    super(scope, id);

    const { vpc, twitterBearerToken, twitterFieldsParams, eventBus } = props;

    const dlq = new Queue(this, 'DLQ');
    const dlqFunction = new Function(this, 'DLQFunction', {
      description: '[SocialAnalytics] Process dead letter queue (DLQ)',
      entry: './src/functions/dlq-processor/index.ts',
      environment: { EVENT_BUS_ARN: eventBus.eventBusArn },
      events: [new SqsEventSource(dlq)],
    });
    dlqFunction.role?.addManagedPolicy(xrayPolicy);
    eventBus.grantPutEventsTo(dlqFunction);

    const logGroup = new logs.LogGroup(this, 'LogGroup', {
      retention: logs.RetentionDays.TWO_WEEKS,
    });

    const awsLogDriver = new ecs.AwsLogDriver({
      logGroup: logGroup,
      streamPrefix: 'ecs',
    });

    const cluster = new ecs.Cluster(this, 'Cluster', { vpc, containerInsights: true });

    const taskDefinition = new ecs.FargateTaskDefinition(this, 'TaskDefinition', {
      runtimePlatform: {
        operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
        cpuArchitecture: ecs.CpuArchitecture.X86_64,
      },
    });
    const taskRole = taskDefinition.taskRole;
    taskRole.addManagedPolicy(xrayPolicy);
    logGroup.grantWrite(taskRole);
    eventBus.grantPutEventsTo(taskRole);
    dlq.grantSendMessages(taskRole);

    const appContainer = taskDefinition.addContainer('App', {
      containerName: 'app',
      image: ecs.ContainerImage.fromAsset('./src/containers/twitter-streaming-reader'),
      cpu: 128,
      memoryReservationMiB: 256,
      essential: true,
      environment: {
        EVENT_BUS_ARN: eventBus.eventBusArn,
        DEAD_LETTER_QUEUE_URL: dlq.queueUrl,
        OTEL_SERVICE_NAME: 'Twitter Stream Producer',
      },
      secrets: {
        TWITTER_BEARER_TOKEN: ecs.Secret.fromSsmParameter(twitterBearerToken),
        TWITTER_FIELDS_PARAMS: ecs.Secret.fromSsmParameter(twitterFieldsParams),
      },
      readonlyRootFilesystem: true,
      logging: awsLogDriver,
    });

    const otelContainer = taskDefinition.addContainer('otel', {
      containerName: 'aws-otel-collector',
      image: ecs.ContainerImage.fromRegistry('public.ecr.aws/aws-observability/aws-otel-collector:v0.19.0'),
      command: ['--config=/etc/ecs/ecs-xray.yaml'],
      cpu: 64,
      memoryReservationMiB: 128,
      readonlyRootFilesystem: true,
      logging: awsLogDriver,
    });

    appContainer.addContainerDependencies({
      container: otelContainer,
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
