import { Sha256 } from '@aws-crypto/sha256-js';
import { Logger } from '@aws-lambda-powertools/logger';
import { STSClient, GetCallerIdentityCommand } from '@aws-sdk/client-sts';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { NodeHttpHandler } from '@aws-sdk/node-http-handler';
import { HttpRequest } from '@aws-sdk/protocol-http';
import { SignatureV4 } from '@aws-sdk/signature-v4';
import { CdkCustomResourceHandler, CdkCustomResourceResponse } from 'aws-lambda';


interface Props {
  host: string;
  path: string;
  name: string;
  body?: any;
  ServiceToken?: string;
};

const region = process.env.AWS_REGION || 'us-west-2';
const logger = new Logger({ logLevel: 'INFO', serviceName: 'opensearch-resources' });

const getRoleArn = async() => {
  const sts = new STSClient({ region });
  const cmd = new GetCallerIdentityCommand({});
  const { Account, Arn } = await sts.send(cmd);
  const roleName = Arn?.split('/')[1];
  const roleArn = `arn:aws:iam::${Account}:role/${roleName}`;
  logger.info({ message: `Lambda Execution Role: ${roleArn}` });
  return roleArn;
};

// https://docs.aws.amazon.com/opensearch-service/latest/developerguide/request-signing.html#request-signing-node
const opensearchRequest = async (method: 'GET'|'PUT'|'DELETE', host: string, path: string, resourceName: string, body?: any) => {
  // Create the HTTP request
  const request = new HttpRequest({
    headers: {
      'Content-Type': 'application/json',
      host,
    },
    hostname: host,
    path: path + resourceName,
    method,
    body: JSON.stringify(body),
  });
  // Sign the request
  const signer = new SignatureV4({
    credentials: defaultProvider(),
    region: region,
    service: 'es',
    sha256: Sha256,
  });
  const signedRequest = await signer.sign(request) as HttpRequest;
  // Send the request
  const client = new NodeHttpHandler();
  const { response } = await client.handle(signedRequest);
  const statusCode = response.statusCode;
  return { statusCode };
};

const waitPermissionReady = async (host: string) => {
  //const roleArn = await getRoleArn();
  const retryRequest = async (count = 0): Promise<number> => {
    if (count == 30) {
      const message = 'Exceeded max retry count.';
      logger.error({ message, host });
      throw new Error(message);
    };
    const { statusCode } = await opensearchRequest('GET', host, '_plugins/_security/api/rolesmapping/', 'all_access');
    if (statusCode == 200) {
      const message = 'This role has all_access permission.';
      logger.info({ message, statusCode, host });
      return statusCode;
    } else if (statusCode == 403) {
      const message = 'Access policy is not ready. Please wait...';
      logger.warn({ message, statusCode, host });
      await new Promise(resolve => setTimeout(resolve, 10*1000)); // 10秒待つ
      return retryRequest(count + 1);
    } else if (statusCode == 401) {
      const message = 'Fine-grained access control is not ready. Please wait...';
      logger.warn({ message, statusCode, host });
      await new Promise(resolve => setTimeout(resolve, 10*1000)); // 10秒待つ
      return retryRequest(count + 1);
    } else {
      const message = 'Request failed.';
      logger.error({ message, statusCode, host });
      throw new Error(message);
    }
  };
  return retryRequest();
};

const onCreate = async (props: Props): Promise<CdkCustomResourceResponse> => {
  const { host, path, name, body } = props;
  const physicalResourceId = `${host}/${path}${name}`;
  const { statusCode } = await opensearchRequest('PUT', host, path, name, body);
  if (statusCode == 200 || statusCode == 201) {
    const message = 'The resource created successfully.';
    logger.info({ message, statusCode, physicalResourceId });
  } else {
    const message = 'Request failed.';
    logger.error({ message, statusCode, physicalResourceId });
    throw new Error(`Request failed. statusCode:${statusCode}`);
  };
  const response: CdkCustomResourceResponse = {
    PhysicalResourceId: physicalResourceId,
    Data: { Host: host, Path: path, Name: name },
  };
  return response;
};

const onDelete = async (props: Props): Promise<CdkCustomResourceResponse> => {
  const { host, path, name } = props;
  const physicalResourceId = `${host}/${path}${name}`;
  const { statusCode } = await opensearchRequest('DELETE', host, path, name);
  if (statusCode == 200 || statusCode == 201) {
    const message = 'The resource deleted successfully.';
    logger.info({ message, statusCode, physicalResourceId });
  } else {
    const message = 'The resource has not been deleted successfully.';
    logger.error({ message, statusCode, physicalResourceId });
  };
  const response: CdkCustomResourceResponse = {};
  return response;
};

const onUpdate = async (props: Props, oldProps: Props): Promise<CdkCustomResourceResponse> => {
  const { PhysicalResourceId, Data } = await onCreate(props);
  const oldPhysicalResourceId = `${oldProps.host}/${oldProps.path}${oldProps.name}`;
  if (PhysicalResourceId != oldPhysicalResourceId) {
    await onDelete(oldProps);
  };
  const response: CdkCustomResourceResponse = { PhysicalResourceId, Data };
  return response;
};

export const handler: CdkCustomResourceHandler = async (event, _context): Promise<CdkCustomResourceResponse> => {
  const props = event.ResourceProperties as Props;

  switch (event.RequestType) {
    case 'Create': {
      await waitPermissionReady(props.host);
      return onCreate(props);
    }
    case 'Update': {
      const oldProps = event.OldResourceProperties as Props;
      return onUpdate(props, oldProps);
    }
    case 'Delete': {
      return onDelete(props);
    }
  };
};