import { PutEventsCommandInput } from '@aws-sdk/client-eventbridge';
import { SendMessageCommandInput } from '@aws-sdk/client-sqs';
// https://opentelemetry.io/docs/instrumentation/js/getting-started/nodejs/#setup
import { diag, DiagConsoleLogger, DiagLogLevel, Span } from '@opentelemetry/api';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { AWSXRayIdGenerator } from '@opentelemetry/id-generator-aws-xray';
import { registerInstrumentations } from '@opentelemetry/instrumentation';
import { AwsInstrumentation } from '@opentelemetry/instrumentation-aws-sdk';
import { AWSXRayPropagator } from '@opentelemetry/propagator-aws-xray';
import { Resource } from '@opentelemetry/resources';
import { BatchSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';
import { SemanticResourceAttributes } from '@opentelemetry/semantic-conventions';

const toTraceHeader = (span: Span, sampled: boolean = true) => {
  const { traceId, spanId } = span.spanContext();
  const xrayTraceId = `1-${traceId.substring(0, 8)}-${traceId.substring(8)}`;
  const samplingFlag = (sampled) ? 1 : 0;
  const traceHeader = `Root=${xrayTraceId};Parent=${spanId};Sampled=${samplingFlag}`;
  return traceHeader;
};

// For troubleshooting, set the log level to DiagLogLevel.DEBUG
diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.INFO);

const awsInstrumentation = new AwsInstrumentation({
  suppressInternalInstrumentation: true,
  preRequestHook: (span, info) => {
    const { serviceName, commandName, commandInput } = info.request;
    if (serviceName == 'EventBridge' && commandName == 'PutEvents') {
      const input = commandInput as PutEventsCommandInput;
      input.Entries?.map(entry => {
        entry.TraceHeader = toTraceHeader(span);
      });
    } else if (serviceName == 'SQS' && commandName == 'SendMessage') {
      const input = commandInput as SendMessageCommandInput;
      span.setAttribute('aws.queue.url', input.QueueUrl||'undefined');
      input.MessageSystemAttributes = {
        AWSTraceHeader: {
          DataType: 'String',
          StringValue: toTraceHeader(span),
        },
      };
    }
  },
});

registerInstrumentations({
  instrumentations: [awsInstrumentation],
});

const provider = new NodeTracerProvider({
  idGenerator: new AWSXRayIdGenerator(),
  resource: Resource.default().merge(new Resource({
    [SemanticResourceAttributes.SERVICE_NAME]: process.env.OTEL_SERVICE_NAME,
  })),
});

const exporter = new OTLPTraceExporter();
const processor = new BatchSpanProcessor(exporter);
provider.addSpanProcessor(processor);

const propagator = new AWSXRayPropagator();

provider.register({ propagator: propagator });
