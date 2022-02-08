import { Logger } from '@aws-lambda-powertools/logger';
import { CognitoIdentityClient, GetIdentityPoolRolesCommand, SetIdentityPoolRolesCommand, SetIdentityPoolRolesCommandInput } from '@aws-sdk/client-cognito-identity';
import { CognitoIdentityProviderClient, ListUserPoolClientsCommand } from '@aws-sdk/client-cognito-identity-provider';
import { CdkCustomResourceHandler, CdkCustomResourceResponse } from 'aws-lambda';

interface Props {
  identityPoolId: string;
  userPoolId: string;
  appClinetPrefix: string;
  ServiceToken: string;
};

const region = process.env.AWS_REGION || 'us-west-2';
const logger = new Logger({ logLevel: 'INFO', serviceName: 'enable-role-from-token' });

const getAppClient = async (userPoolId: string, appClinetPrefix: string) => {
  const cognito = new CognitoIdentityProviderClient({ region });
  const cmd = new ListUserPoolClientsCommand({ UserPoolId: userPoolId });
  const { UserPoolClients } = await cognito.send(cmd);
  const { ClientName: clientName, ClientId: clientId } = UserPoolClients?.find(x => x.ClientName?.startsWith(appClinetPrefix))!;
  const cognitoRegion = userPoolId.split('_').shift();
  const identityProvider = `cognito-idp.${cognitoRegion}.amazonaws.com/${userPoolId}:${clientId}`;
  logger.info({ message: 'Fetch app client successfully', clientName, clientId, identityProvider });
  return { clientName, clientId, identityProvider };
};

const getIdentityPoolRoles = async (identityPoolId: string) => {
  const client = new CognitoIdentityClient({ region });
  const cmd = new GetIdentityPoolRolesCommand({ IdentityPoolId: identityPoolId });
  const res = await client.send(cmd);
  logger.info({
    message: 'Get IdentityPoolRoles successfully',
    roles: JSON.stringify(res),
  });
  return res;
};

const setIdentityPoolRoles = async (input: SetIdentityPoolRolesCommandInput) => {
  const client = new CognitoIdentityClient({ region });
  const cmd = new SetIdentityPoolRolesCommand(input);
  const res = await client.send(cmd);
  logger.info({
    message: 'SetIdentityPoolRoles successfully',
    input: JSON.stringify(input),
  });
  return res;
};

const enableRoleFromToken = async (props: Props): Promise<CdkCustomResourceResponse> => {
  const { identityPoolId, userPoolId, appClinetPrefix } = props;
  const { identityProvider } = await getAppClient(userPoolId, appClinetPrefix);
  // update identity pool
  const { RoleMappings, Roles } = await getIdentityPoolRoles(identityPoolId);
  await setIdentityPoolRoles({
    IdentityPoolId: identityPoolId,
    Roles,
    RoleMappings: {
      ...RoleMappings,
      [identityProvider]: {
        Type: 'Token',
        AmbiguousRoleResolution: 'AuthenticatedRole',
      },
    },
  });
  return { PhysicalResourceId: identityProvider };
};

const disableRoleFromToken = async (props: Props): Promise<CdkCustomResourceResponse> => {
  const { identityPoolId, userPoolId, appClinetPrefix } = props;
  const { identityProvider } = await getAppClient(userPoolId, appClinetPrefix);
  // update identity pool
  const { RoleMappings, Roles } = await getIdentityPoolRoles(identityPoolId);
  delete RoleMappings?.[identityProvider];
  await setIdentityPoolRoles({
    IdentityPoolId: identityPoolId,
    Roles,
    RoleMappings,
  });
  return {};
};

export const handler: CdkCustomResourceHandler = async (event, _context) => {
  const props = event.ResourceProperties as Props;

  switch (event.RequestType) {
    case 'Create': {
      return enableRoleFromToken(props);
    }
    case 'Update': {
      return enableRoleFromToken(props);
    }
    case 'Delete': {
      return disableRoleFromToken(props);
    }
  };
};