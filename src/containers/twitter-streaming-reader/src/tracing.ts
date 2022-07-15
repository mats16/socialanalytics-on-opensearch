import { PutEventsCommandInput } from '@aws-sdk/client-eventbridge';
import { SendMessageCommandInput } from '@aws-sdk/client-sqs';
import { trace, propagation, Span, ROOT_CONTEXT } from '@opentelemetry/api';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto';
import { AWSXRayIdGenerator } from '@opentelemetry/id-generator-aws-xray';
import { registerInstrumentations } from '@opentelemetry/instrumentation';
import { AwsInstrumentation } from '@opentelemetry/instrumentation-aws-sdk';
import { AWSXRayPropagator, AWSXRAY_TRACE_ID_HEADER } from '@opentelemetry/propagator-aws-xray';
import { awsEcsDetector } from '@opentelemetry/resource-detector-aws';
import { detectResources, envDetector, processDetector } from '@opentelemetry/resources';
import { BatchSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';

const xrayPropagator = new AWSXRayPropagator();
propagation.setGlobalPropagator(xrayPropagator);

const toTraceHeader = (span: Span): string => {
  const ctx = trace.setSpan(ROOT_CONTEXT, span);
  const carrier: { [key: string]: string } = {};
  propagation.inject(ctx, carrier);
  const traceHeader = carrier[AWSXRAY_TRACE_ID_HEADER];
  return traceHeader;
};

const instrumentations = [
  new AwsInstrumentation({
    suppressInternalInstrumentation: true,
    preRequestHook: (span, info) => {
      const { serviceName, commandName, commandInput } = info.request;
      span.setAttribute('aws.service', serviceName);
      if (serviceName == 'EventBridge' && commandName == 'PutEvents') {
        (commandInput as PutEventsCommandInput).Entries?.map(entry => {
          entry.TraceHeader = toTraceHeader(span);
        });
      } else if (serviceName == 'SQS' && commandName == 'SendMessage') {
        span.setAttribute('aws.queue.url', commandInput.QueueUrl);
        (commandInput as SendMessageCommandInput).MessageSystemAttributes = {
          AWSTraceHeader: {
            DataType: 'String',
            StringValue: toTraceHeader(span),
          },
        };
      }
    },
  }),
];

// Register instrumentations synchronously to ensure code is patched even before provider is ready.
registerInstrumentations({ instrumentations });;

const initializeProvider = async () => {
  const resource = await detectResources({
    detectors: [awsEcsDetector, envDetector, processDetector],
  });

  const tracerProvider = new NodeTracerProvider({
    idGenerator: new AWSXRayIdGenerator(),
    resource,
  });

  tracerProvider.addSpanProcessor(new BatchSpanProcessor(new OTLPTraceExporter()));
  tracerProvider.register({ propagator: xrayPropagator });

  // Re-register instrumentation with initialized provider. Patched code will see the update.
  registerInstrumentations({
    instrumentations,
    tracerProvider,
  });
};

initializeProvider().catch(err => console.error(err));