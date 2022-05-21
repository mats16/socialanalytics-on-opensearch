import * as apprunner from '@aws-cdk/aws-apprunner-alpha';
import { DockerImageAsset } from 'aws-cdk-lib/aws-ecr-assets';
import { Construct } from 'constructs';

interface ProxyProps {
  dashboardsHost: string;
  cognitoHost: string;
};

export class Proxy extends Construct {
  domainName: string;

  constructor(scope: Construct, id: string, props: ProxyProps) {
    super(scope, id);

    const asset = new DockerImageAsset(this, 'ImageAsset', {
      directory: './src/containers/proxy',
    });

    const service = new apprunner.Service(this, 'Service', {
      source: new apprunner.AssetSource({
        asset,
        imageConfiguration: {
          environment: {
            DASHBOARDS_HOST: props.dashboardsHost,
            COGNITO_HOST: props.cognitoHost,
          },
        },
      }),
    });

    this.domainName = service.serviceUrl;
  }
}
