import { CustomResource } from 'aws-cdk-lib';
import * as cognito from 'aws-cdk-lib/aws-cognito';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import * as cr from 'aws-cdk-lib/custom-resources';
import { Construct } from 'constructs';

interface UserPoolProps extends cognito.UserPoolProps {
  allowedSignupDomains: string[];
  cognitoDomainPrefix: string;
}

export class UserPool extends cognito.UserPool {
  identityPoolId: string;
  authenticatedRole: iam.Role;
  crServiceToken: string;

  constructor(scope: Construct, id: string, props: UserPoolProps) {
    super(scope, id, props);

    const preSignUpFunction = new NodejsFunction(this, 'PreSignUpFunction', {
      description: 'Social Analytics - PreSignUp trigger',
      entry: './src/functions/pre-sign-up/index.ts',
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_14_X,
      architecture: lambda.Architecture.ARM_64,
      environment: {
        ALLOWED_SIGNUP_DOMAINS: props.allowedSignupDomains.join(','),
      },
    });
    this.addTrigger(cognito.UserPoolOperation.PRE_SIGN_UP, preSignUpFunction);

    const domainPrefix = props.cognitoDomainPrefix;

    this.addDomain('UserPoolDomain', {
      cognitoDomain: { domainPrefix },
    });

    this.identityPoolId = new cognito.CfnIdentityPool(this, 'IdentityPool', {
      allowUnauthenticatedIdentities: false,
    }).ref;

    this.authenticatedRole = new iam.Role(this, 'AuthRole', {
      description: 'IdentityPool Auth Role for OpenSearch Dashbord',
      assumedBy: new iam.FederatedPrincipal(
        'cognito-identity.amazonaws.com',
        {
          'StringEquals': { 'cognito-identity.amazonaws.com:aud': this.identityPoolId },
          'ForAnyValue:StringLike': { 'cognito-identity.amazonaws.com:amr': 'authenticated' },
        },
        'sts:AssumeRoleWithWebIdentity',
      ),
    });

    new cognito.CfnIdentityPoolRoleAttachment(this, 'IdentityPoolRoleAttachment', {
      identityPoolId: this.identityPoolId,
      roles: {
        authenticated: this.authenticatedRole.roleArn,
      },
    });

    const onEventHandler = new NodejsFunction(this, 'EnableRoleFromTokenFunction', {
      description: 'Lambda-backed custom resources - Enable role from token',
      entry: './src/custom-resources-functions/enable-role-from-token/index.ts',
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_14_X,
      architecture: lambda.Architecture.ARM_64,
      initialPolicy: [
        new iam.PolicyStatement({
          actions: [
            'cognito-identity:GetIdentityPoolRoles',
            'cognito-identity:SetIdentityPoolRoles',
          ],
          resources: [`arn:aws:cognito-identity:${this.stack.region}:${this.stack.account}:identitypool/${this.identityPoolId}`],
        }),
        new iam.PolicyStatement({
          actions: ['cognito-idp:ListUserPoolClients'],
          resources: [this.userPoolArn],
        }),
        new iam.PolicyStatement({
          actions: ['iam:PassRole'],
          resources: [this.authenticatedRole.roleArn],
        }),
      ],
    });
    const provider = new cr.Provider(this, 'IdentityPoolTokenEnableProvider', { onEventHandler });
    this.crServiceToken = provider.serviceToken;

  }
  enableRoleFromToken(appClinetPrefix: string) {
    const resource = new CustomResource(this, 'EnableRoleFromToken', {
      serviceToken: this.crServiceToken,
      resourceType: 'Custom::EnableRoleFromToken',
      properties: {
        identityPoolId: this.identityPoolId,
        userPoolId: this.userPoolId,
        appClinetPrefix: appClinetPrefix,
      },
    });
    return resource;
  };
}
