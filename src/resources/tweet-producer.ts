import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Queue } from 'aws-cdk-lib/aws-sqs';
import { IStringParameter, StringParameter } from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';

const xrayPolicy = iam.ManagedPolicy.fromAwsManagedPolicyName('AWSXRayDaemonWriteAccess');

interface TweetProducerProps {
  twitterFieldsParams: IStringParameter;
  producerCount: number;
};

export class TweetProducer extends Construct {
  streamReader: StreamReader[] = [];
  queues: Queue[];

  constructor(scope: Construct, id: string, props: TweetProducerProps) {
    super(scope, id);

    const { twitterFieldsParams, producerCount } = props;

    const primaryQueue = new Queue(this, 'PrimaryQueue', { fifo: true });
    const secondaryQueue = new Queue(this, 'SecondaryQueue');
    this.queues = [primaryQueue, secondaryQueue];

    const vpc = new ec2.Vpc(this, 'VPC', {
      subnetConfiguration: [{
        cidrMask: 24,
        name: 'Public',
        subnetType: ec2.SubnetType.PUBLIC,
      }],
    });

    const securityGroup = new ec2.SecurityGroup(this, 'SecurityGroup', { allowAllOutbound: true, vpc });

    const cluster = new ecs.Cluster(this, 'Cluster', { vpc, containerInsights: true });
    const taskRole = new iam.Role(this, 'TaskRole', { assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com') });
    const executionRole = new iam.Role(this, 'TaskExecutionRole', {
      assumedBy: new iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonECSTaskExecutionRolePolicy')],
    });
    const image = ecs.ContainerImage.fromAsset('./containers/twitter-streaming-reader');

    for (let i = 0; i < producerCount; i++) {
      const streamReader = new StreamReader(this, `StreamReader${i+1}`, {
        securityGroup,
        cluster,
        taskRole,
        executionRole,
        image,
        twitterFieldsParams,
        queues: this.queues,
      });
      this.streamReader.push(streamReader);
    }
  }
}

interface StreamReaderProps {
  securityGroup: ec2.SecurityGroup;
  cluster: ecs.Cluster;
  taskRole: iam.Role;
  executionRole: iam.Role;
  image: ecs.ContainerImage;
  twitterFieldsParams: IStringParameter;
  queues: Queue[];
};

class StreamReader extends Construct {
  bearerToken: cdk.CfnParameter;
  service: ecs.FargateService;

  constructor(scope: Construct, id: string, props: StreamReaderProps) {
    super(scope, id);

    const { securityGroup, cluster, taskRole, executionRole, image, twitterFieldsParams, queues } = props;

    this.bearerToken = new cdk.CfnParameter(this, 'BearerToken', {
      description: 'Bearer Token for Twitter API v2',
      type: 'String',
      default: 'N/A',
      noEcho: true,
    });

    const bearerToken = new StringParameter(this, 'BearerTokenParameter', {
      description: `${this.node.path}/BearerTokenParameter`,
      parameterName: `/${cdk.Aws.STACK_NAME}/${scope.node.id}/${id}/BearerToken`,
      stringValue: this.bearerToken.valueAsString,
      simpleName: false,
    });

    const logGroup = new logs.LogGroup(this, 'Logs', {
      retention: logs.RetentionDays.TWO_WEEKS,
    });

    const awsLogDriver = new ecs.AwsLogDriver({
      logGroup: logGroup,
      streamPrefix: 'ecs',
    });

    const taskDefinition = new ecs.FargateTaskDefinition(this, 'TaskDef', {
      taskRole,
      executionRole,
      runtimePlatform: {
        operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
        cpuArchitecture: ecs.CpuArchitecture.X86_64,
      },
    });

    const appContainer = taskDefinition.addContainer('App', {
      containerName: 'app',
      image,
      cpu: 128,
      memoryReservationMiB: 256,
      essential: true,
      environment: {
        OTEL_SERVICE_NAME: `${scope.node.id}/${id}`,
      },
      secrets: {
        TWITTER_BEARER_TOKEN: ecs.Secret.fromSsmParameter(bearerToken),
        TWITTER_FIELDS_PARAMS: ecs.Secret.fromSsmParameter(twitterFieldsParams),
      },
      readonlyRootFilesystem: true,
      logging: awsLogDriver,
    });

    for (let i = 0; i < queues.length; i++) {
      const q = queues[i];
      q.grantSendMessages(taskRole);
      appContainer.addEnvironment(`QUEUE_URL_${i+1}`, q.queueUrl);
    }

    const otelContainer = taskDefinition.addContainer('otel', {
      containerName: 'otel-collector',
      image: ecs.ContainerImage.fromRegistry('public.ecr.aws/aws-observability/aws-otel-collector:latest'),
      command: ['--config=/etc/ecs/ecs-xray.yaml'],
      cpu: 64,
      memoryReservationMiB: 128,
      readonlyRootFilesystem: true,
      logging: awsLogDriver,
    });
    taskDefinition.taskRole.addManagedPolicy(xrayPolicy);

    appContainer.addContainerDependencies({
      container: otelContainer,
      condition: ecs.ContainerDependencyCondition.START,
    });

    this.service = new ecs.FargateService(this, 'Service', {
      cluster,
      taskDefinition,
      securityGroups: [securityGroup],
      assignPublicIp: true,
      minHealthyPercent: 0,
      maxHealthyPercent: 100,
    });

    const disabledCondition = new cdk.CfnCondition(this, 'Disabled', { expression: cdk.Fn.conditionEquals(this.bearerToken, 'N/A') });
    (this.service.node.defaultChild as ecs.CfnService).addPropertyOverride('DesiredCount', cdk.Fn.conditionIf(disabledCondition.logicalId, 0, 1));
  }
}
