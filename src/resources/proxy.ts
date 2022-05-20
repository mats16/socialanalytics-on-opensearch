import * as apprunner from '@aws-cdk/aws-apprunner-alpha';
import * as ecr from 'aws-cdk-lib/aws-ecr';
import { DockerImageAsset } from 'aws-cdk-lib/aws-ecr-assets';
import * as ecrDeploy from 'cdk-ecr-deployment';
import { Construct } from 'constructs';

interface ProxyProps {
  domainEndpoint: string;
  cognitoHost: string;
};

export class Proxy extends Construct {
  url: string;

  constructor(scope: Construct, id: string, props: ProxyProps) {
    super(scope, id);

    const imageAsset = new DockerImageAsset(this, 'ImageAsset', {
      directory: './src/containers/proxy',
    });

    const repository = new ecr.Repository(this, 'Repo');

    new ecrDeploy.ECRDeployment(this, 'twitterStreamingReaderImageAssetDeploy', {
      src: new ecrDeploy.DockerImageName(imageAsset.imageUri),
      dest: new ecrDeploy.DockerImageName(repository.repositoryUri),
    });

    const service = new apprunner.Service(this, 'Service', {
      source: new apprunner.EcrSource({
        repository,
        imageConfiguration: {
          environment: {
            DOMAIN_ENDPOINT: props.domainEndpoint,
            COGNITO_HOST: props.cognitoHost,
          },
        },
      }),
    });

    this.url = service.serviceUrl;
  }
}
