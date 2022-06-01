import * as apprunner from '@aws-cdk/aws-apprunner-alpha';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { DockerImageAsset } from 'aws-cdk-lib/aws-ecr-assets';
import { Domain } from 'aws-cdk-lib/aws-opensearchservice';
import { Construct } from 'constructs';

interface ProxyProps {
  vpc?: ec2.IVpc;
  openSearchDomain: Domain;
  cognitoHost: string;
};

export class Proxy extends Construct {
  service: apprunner.Service;
  securityGroup?: ec2.SecurityGroup;

  constructor(scope: Construct, id: string, props: ProxyProps) {
    super(scope, id);

    const { vpc, openSearchDomain, cognitoHost } = props;
    const dashboardsHost = openSearchDomain.domainEndpoint;

    const asset = new DockerImageAsset(this, 'ImageAsset', {
      directory: './src/containers/proxy',
    });

    let vpcConnector: apprunner.VpcConnector|undefined = undefined;
    if (typeof vpc != 'undefined') {
      this.securityGroup = new ec2.SecurityGroup(this, 'SecurityGroup', { vpc });
      openSearchDomain.connections.allowFrom(this.securityGroup, ec2.Port.tcp(443));
      vpcConnector = new apprunner.VpcConnector(this, 'VpcConnector', { vpc, securityGroups: [this.securityGroup] });
    }

    this.service = new apprunner.Service(this, 'Service', {
      source: apprunner.AssetSource.fromAsset({
        asset,
        imageConfiguration: {
          environment: {
            DASHBOARDS_HOST: dashboardsHost,
            COGNITO_HOST: cognitoHost,
          },
        },
      }),
      vpcConnector,
    });
  }
}
