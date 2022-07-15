// https://github.com/open-telemetry/opentelemetry-lambda/blob/main/nodejs/packages/layer/src/wrapper.ts
import { trace, propagation, Context, Span, ROOT_CONTEXT } from '@opentelemetry/api';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto';
import { registerInstrumentations } from '@opentelemetry/instrumentation';
import { AwsLambdaInstrumentation } from '@opentelemetry/instrumentation-aws-lambda';
import { AwsInstrumentation } from '@opentelemetry/instrumentation-aws-sdk';
import { AWSXRayPropagator, AWSXRAY_TRACE_ID_HEADER } from '@opentelemetry/propagator-aws-xray';
import { awsLambdaDetector } from '@opentelemetry/resource-detector-aws';
import { detectResources, envDetector, processDetector } from '@opentelemetry/resources';
import { BatchSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';

const xrayPropagator = new AWSXRayPropagator();
propagation.setGlobalPropagator(xrayPropagator);

export const toContext = (traceHeader?: string): Context => {
  const carrier = { [AWSXRAY_TRACE_ID_HEADER]: `${traceHeader}` };
  const ctx = propagation.extract(ROOT_CONTEXT, carrier);
  return ctx;
};

export const toTraceHeader = (span: Span): string => {
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
      console.log(info);
      const { serviceName, commandName, commandInput } = info.request;
      span.setAttribute('aws.service', serviceName);
      if (serviceName == 'EventBridge' && commandName == 'PutEvents') {
        commandInput.Entries?.map((entry: any) => {
          entry.TraceHeader = toTraceHeader(span);
        });
      } else if (serviceName == 'DynamoDB' && commandName == 'PutItem') {
        commandInput.Item.x_amzn_trace_id = { S: toTraceHeader(span) };
      } else if (serviceName == 'DynamoDB' && commandName == 'UpdateItem') {
        commandInput.UpdateExpression = `${commandInput.UpdateExpression}, x_amzn_trace_id = :x`;
        commandInput.ExpressionAttributeValues[':x'] = { S: toTraceHeader(span) };
      }
    },
    responseHook: (span, info) => {
      const { data, request } = info.response;
      const { serviceName, commandInput } = request;
      if (serviceName == 'DynamoDB') {
        span.setAttributes({
          'aws.table.name': commandInput.TableName,
          'aws.dynamodb.consumed_capacity': data.ConsumedCapacity,
          'aws.dynamodb.item_collection_metrics': data.ItemCollectionMetrics,
        });
      }
    },
  }),
  new AwsLambdaInstrumentation({
    disableAwsContextPropagation: true,
  }),
];

// Register instrumentations synchronously to ensure code is patched even before provider is ready.
registerInstrumentations({ instrumentations });

async function initializeProvider() {
  const resource = await detectResources({
    detectors: [awsLambdaDetector, envDetector, processDetector],
  });

  const tracerProvider = new NodeTracerProvider({ resource });

  tracerProvider.addSpanProcessor(new BatchSpanProcessor(new OTLPTraceExporter()));
  tracerProvider.register({ propagator: xrayPropagator });

  // Re-register instrumentation with initialized provider. Patched code will see the update.
  registerInstrumentations({
    instrumentations,
    tracerProvider,
  });
}

initializeProvider().catch(err => console.error(err));
