import * as apprunner from 'aws-cdk-lib/aws-apprunner';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { DockerImageAsset } from 'aws-cdk-lib/aws-ecr-assets';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Domain } from 'aws-cdk-lib/aws-opensearchservice';
import { Construct } from 'constructs';

interface ProxyProps {
  vpc?: ec2.IVpc;
  openSearchDomain: Domain;
  cognitoHost: string;
};

export class Proxy extends Construct {
  domainName: string;
  securityGroup?: ec2.SecurityGroup;

  constructor(scope: Construct, id: string, props: ProxyProps) {
    super(scope, id);

    const { vpc, openSearchDomain, cognitoHost } = props;
    const dashboardsHost = openSearchDomain.domainEndpoint;

    const asset = new DockerImageAsset(this, 'ImageAsset', {
      directory: './src/containers/proxy',
    });

    let networkConfiguration: apprunner.CfnService.NetworkConfigurationProperty = { egressConfiguration: { egressType: 'DEFAULT' } };
    if (typeof vpc != 'undefined') {
      this.securityGroup = new ec2.SecurityGroup(this, 'SecurityGroup', { vpc });
      openSearchDomain.connections.allowFrom(this.securityGroup, ec2.Port.tcp(443));
      const vpcConnector = new apprunner.CfnVpcConnector(this, 'VpcConnector', {
        subnets: vpc.privateSubnets.map(subnet => subnet.subnetId),
        securityGroups: [this.securityGroup.securityGroupId],
      });
      networkConfiguration = {
        egressConfiguration: {
          egressType: 'VPC',
          vpcConnectorArn: vpcConnector.attrVpcConnectorArn,
        },
      };
    }

    const getAuthorizationTokenStatement = new iam.PolicyStatement({
      actions: ['ecr:GetAuthorizationToken'],
      resources: ['*'],
    });
    const pullImageStatement = new iam.PolicyStatement({
      actions: [
        'ecr:BatchCheckLayerAvailability',
        'ecr:GetDownloadUrlForLayer',
        'ecr:BatchGetImage',
      ],
      resources: [asset.repository.repositoryArn],
    });
    const pullImagePolicy = new iam.PolicyDocument({
      statements: [
        getAuthorizationTokenStatement,
        pullImageStatement,
      ],
    });

    const accessRole = new iam.Role(this, 'AccessRole', {
      assumedBy: new iam.ServicePrincipal('build.apprunner.amazonaws.com'),
      inlinePolicies: { PullImagePolicy: pullImagePolicy },
    });

    const service = new apprunner.CfnService(this, 'Service', {
      sourceConfiguration: {
        authenticationConfiguration: {
          accessRoleArn: accessRole.roleArn,
        },
        imageRepository: {
          imageRepositoryType: 'ECR',
          imageIdentifier: asset.imageUri,
          imageConfiguration: {
            runtimeEnvironmentVariables: [
              { name: 'DASHBOARDS_HOST', value: dashboardsHost },
              { name: 'COGNITO_HOST', value: cognitoHost },
            ],
          },
        },
      },
      networkConfiguration,
    });

    this.domainName = service.attrServiceUrl;
  }
}
