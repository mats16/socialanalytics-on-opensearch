import { Duration, Aws } from 'aws-cdk-lib';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction, NodejsFunctionProps } from 'aws-cdk-lib/aws-lambda-nodejs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { SqsDestination } from 'aws-cdk-lib/aws-s3-notifications';
import { Queue　} from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';

const defaultProps: Partial<NodejsFunctionProps> = {
  timeout: Duration.seconds(5),
  runtime: lambda.Runtime.NODEJS_16_X,
  architecture: lambda.Architecture.ARM_64,
};

export class Function extends NodejsFunction {
  constructor(scope: Construct, id: string, props: NodejsFunctionProps) {
    super(scope, id, { ...defaultProps, ...props });
  }
}

interface RetryFunctionProps extends NodejsFunctionProps {
  source: {
    bucket: s3.IBucket;
    prefix: string;
  };
};

export class RetryFunction extends NodejsFunction {
  constructor(scope: Construct, id: string, props: RetryFunctionProps) {
    super(scope, id, {
      ...defaultProps,
      memorySize: 512,
      reservedConcurrentExecutions: 1,
      ...props,
    });

    const { bucket, prefix } = props.source;

    const queue = new Queue(this, 'Queue', {
      retentionPeriod: Duration.days(14),
      visibilityTimeout: this.timeout,
    });
    this.addEventSource(new SqsEventSource(queue, { maxBatchingWindow: Duration.seconds(10) }));

    bucket.addObjectCreatedNotification(new SqsDestination(queue), { prefix });
    bucket.grantRead(this, `${prefix}*`);
    bucket.grantDelete(this, `${prefix}*`);

    this.addEnvironment('POWERTOOLS_SERVICE_NAME', id);
    this.addEnvironment('POWERTOOLS_METRICS_NAMESPACE', Aws.STACK_NAME);
    this.addEnvironment('POWERTOOLS_TRACER_CAPTURE_RESPONSE', 'false');
  }
}
