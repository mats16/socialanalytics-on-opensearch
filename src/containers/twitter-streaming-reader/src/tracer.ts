import { PutEventsCommandInput } from '@aws-sdk/client-eventbridge';
import { SendMessageCommandInput } from '@aws-sdk/client-sqs';
import { trace, Span } from '@opentelemetry/api';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { AWSXRayIdGenerator } from '@opentelemetry/id-generator-aws-xray';
import { registerInstrumentations } from '@opentelemetry/instrumentation';
import { AwsInstrumentation } from '@opentelemetry/instrumentation-aws-sdk';
import { AWSXRayPropagator } from '@opentelemetry/propagator-aws-xray';
import { Resource } from '@opentelemetry/resources';
import { BatchSpanProcessor, TracerConfig } from '@opentelemetry/sdk-trace-base';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';
import { SemanticResourceAttributes } from '@opentelemetry/semantic-conventions';

const toTraceHeader = (span: Span, sampled: number =1) => {
  const { traceId, spanId } = span.spanContext();
  const xrayTraceId = `1-${traceId.substring(0, 8)}-${traceId.substring(8)}`;
  const traceHeader = `Root=${xrayTraceId};Parent=${spanId};Sampled=${sampled}`;
  return traceHeader;
};

const tracerConfig: TracerConfig = {
  idGenerator: new AWSXRayIdGenerator(),
  resource: Resource.default().merge(new Resource({
    [SemanticResourceAttributes.SERVICE_NAME]: 'Tweet Producer',
  })),
};
const provider = new NodeTracerProvider(tracerConfig);

const otlpExporter = new OTLPTraceExporter();
provider.addSpanProcessor(new BatchSpanProcessor(otlpExporter));

provider.register({
  propagator: new AWSXRayPropagator(),
});

registerInstrumentations({
  instrumentations: [
    new AwsInstrumentation({
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
    }),
  ],
});

export = trace.getTracer('instrumentation-aws-sdk');