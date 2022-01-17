import { Port } from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';
import YAML from 'yaml';

interface ContainerInsightsPrometheusProps {
  targetService: ecs.FargateService;
};

export class ContainerInsights extends Construct {
  constructor(scope: Construct, id: string, props: ContainerInsightsPrometheusProps) {
    super(scope, id);

    const targetService = props.targetService;
    const targetTaskFamily = targetService.taskDefinition.family;
    const cluster = targetService.cluster;

    const ecsAutoDiscoveryPolicy = new iam.Policy(this, 'EcsAutoDiscoveryPolicy', {
      document: new iam.PolicyDocument({
        statements: [new iam.PolicyStatement({
          actions: [
            'ecs:ListTasks',
            'ecs:ListServices',
            'ecs:DescribeContainerInstances',
            'ecs:DescribeServices',
            'ecs:DescribeTasks',
            'ecs:DescribeTaskDefinition',
          ],
          resources: ['*'],
        })],
      }),
    });

    const logGroup = new logs.LogGroup(this, 'LogGroup', {
      retention: logs.RetentionDays.ONE_DAY,
    });

    const prometheusConfig = new ssm.StringParameter(this, 'PrometheusConfig', {
      description: 'Prometheus Scraping SSM Parameter for ECS Cluster',
      stringValue: YAML.stringify({
        global: {
          scrape_interval: '1m',
          scrape_timeout: '10s',
        },
        scrape_configs: [{
          job_name: 'cwagent-ecs-file-sd-config',
          sample_limit: 10000,
          file_sd_configs: [{
            files: ['/tmp/cwagent_ecs_auto_sd.yaml'],
          }],
        }],
      }),
    });

    const cloudWatchConfig = new ssm.StringParameter(this, 'CloudWatchConfig', {
      description: 'CloudWatch-Agent SSM Parameters with EMF Definition for ECS Cluster',
      stringValue: JSON.stringify({
        logs: {
          metrics_collected: {
            prometheus: {
              log_group_name: logGroup.logGroupName,
              prometheus_config_path: 'env:PROMETHEUS_CONFIG_CONTENT',
              ecs_service_discovery: {
                sd_frequency: '1m',
                sd_result_file: '/tmp/cwagent_ecs_auto_sd.yaml',
                docker_label: {
                },
                task_definition_list: [
                  {
                    sd_job_name: 'emf/ecs-firelens-fluentbit',
                    sd_metrics_ports: '2020',
                    sd_task_definition_arn_pattern: `.*:task-definition/${targetTaskFamily}:[0-9]+`,
                    sd_metrics_path: '/api/v1/metrics/prometheus',
                  },
                ],
              },
              emf_processor: {
                metric_declaration: [
                  {
                    source_labels: ['StartedBy'],
                    label_matcher: '^ecs-svc/.*',
                    dimensions: [
                      ['ClusterName', 'TaskGroup', 'name'],
                      ['ClusterName', 'TaskDefinitionFamily', 'name'],
                      ['ClusterName', 'instance', 'name'],
                    ],
                    metric_selectors: [
                      '^fluentbit_input_(records|bytes)_total$',
                      '^fluentbit_output_proc_(records|bytes)_total$',
                      '^fluentbit_output_errors_total$',
                      '^fluentbit_output_retries_(total|failed_total)$',
                    ],
                  },
                ],
              },
            },
          },
          force_flush_interval: 5,
        },
      }),
    });

    const taskDefinition = new ecs.FargateTaskDefinition(this, 'TaskDefinition', {
      runtimePlatform: {
        operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
        cpuArchitecture: ecs.CpuArchitecture.ARM64,
      },
    });

    taskDefinition.addContainer('CloudWatchAgent', {
      containerName: 'cloudwatch-agent',
      image: ecs.ContainerImage.fromRegistry('public.ecr.aws/cloudwatch-agent/cloudwatch-agent:latest'),
      secrets: {
        PROMETHEUS_CONFIG_CONTENT: ecs.Secret.fromSsmParameter(prometheusConfig),
        CW_CONFIG_CONTENT: ecs.Secret.fromSsmParameter(cloudWatchConfig),
      },
      logging: new ecs.AwsLogDriver({
        logGroup: logGroup,
        streamPrefix: 'insights',
      }),
    });

    taskDefinition.taskRole.attachInlinePolicy(ecsAutoDiscoveryPolicy);
    logGroup.grantWrite(taskDefinition.taskRole);

    const service = new ecs.FargateService(this, 'Service', {
      cluster,
      taskDefinition,
      assignPublicIp: true,
    });

    targetService.connections.allowFrom(service, Port.tcp(2020), 'Fluent-Bit Prometheus Metrics');

  };
};