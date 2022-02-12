import { Stack } from 'aws-cdk-lib';
import * as appconfig from 'aws-cdk-lib/aws-appconfig';
import * as iam from 'aws-cdk-lib/aws-iam';
import { LayerVersion } from 'aws-cdk-lib/aws-lambda';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import { Construct } from 'constructs';

export interface ApplicationProps {
  name?: string;
  description?: string;
};

export interface EnvironmentProps {
  application: Application;
  name?: string;
  description?: string;
};

export interface SSMParameterConfigProfileProps {
  application: Application;
  name?: string;
  description?: string;
  ssmParameter: ssm.StringParameter|ssm.StringListParameter;
}

export interface DeploymentProps {
  applicationId?: string;
  environmentId?: string;
  configurationProfileId: string;
  configurationVersion: string;
  deploymentStrategyId?: string;
  description?: string;
};

export class Application extends Construct {
  applicationId: string;
  applicationName: string;
  defaultDeploymentStrategyId: string;
  retrievalRole: iam.Role;

  constructor(scope: Stack, id: string, props: ApplicationProps) {
    super(scope, id);

    this.applicationName = props.name || id;
    const description = props.description;

    const application = new appconfig.CfnApplication(this, id, {
      name: this.applicationName,
      description,
    });
    this.applicationId = application.ref;

    const defaultDeploymentStrategy = new appconfig.CfnDeploymentStrategy(this, 'FastDeploy', {
      name: scope.stackName + '.FastDeploy',
      growthFactor: 100,
      deploymentDurationInMinutes: 0,
      finalBakeTimeInMinutes: 0,
      replicateTo: 'NONE',
    });
    this.defaultDeploymentStrategyId = defaultDeploymentStrategy.ref;

    this.retrievalRole = new iam.Role(this, 'RetrievalRole', {
      description: 'AppConfigServiceRole',
      assumedBy: new iam.ServicePrincipal('appconfig.amazonaws.com'),
    });

  };

  addEnvironment(name: string, description?: string) {
    const props: EnvironmentProps = { application: this, name, description };
    const environment = new Environment(this, name, props);
    return environment;
  };

  addSSMParameterConfigProfile(name: string, ssmParameter: ssm.StringParameter|ssm.StringListParameter, description?: string) {
    const props: SSMParameterConfigProfileProps = {
      application: this,
      name,
      description,
      ssmParameter,
    };
    const profile = new SSMParameterConfigProfile(this, name, props);
    return profile;
  };
};

export class SSMParameterConfigProfile extends Construct {
  application: Application;
  configurationProfileId: string;
  configurationProfileName: string;

  constructor(scope: Construct, id: string, props: SSMParameterConfigProfileProps) {
    super(scope, id);

    this.application = props.application;
    this.configurationProfileName = props.name || id;
    const { description, ssmParameter } = props;

    const configProfile = new appconfig.CfnConfigurationProfile(this, id, {
      applicationId: this.application.applicationId,
      name: this.configurationProfileName,
      description,
      locationUri: 'ssm-parameter://' + ssmParameter.parameterName,
      retrievalRoleArn: this.application.retrievalRole.roleArn,
    });
    this.configurationProfileId = configProfile.ref;

    ssmParameter.grantRead(this.application.retrievalRole).applyBefore(configProfile);
  };
};

export class Environment extends Construct {
  application: Application;
  defaultDeploymentStrategyId: string;
  environmentId: string;
  environmentName: string;;

  constructor(scope: Construct, id: string, props: EnvironmentProps) {
    super(scope, id);

    this.application = props.application;
    this.defaultDeploymentStrategyId = props.application.defaultDeploymentStrategyId;
    this.environmentName = props.name || id;
    const { description } = props;

    const env = new appconfig.CfnEnvironment(this, id, {
      applicationId: this.application.applicationId,
      name: this.environmentName,
      description,
    });
    this.environmentId = env.ref;
  };

  deploy(deploymentId: string, configurationProfileId: string, configurationVersion: string = '1', deploymentStrategyId: string = this.application.defaultDeploymentStrategyId ) {
    const props: appconfig.CfnDeploymentProps = {
      applicationId: this.application.applicationId,
      environmentId: this.environmentId,
      configurationProfileId,
      configurationVersion: configurationVersion,
      deploymentStrategyId,
    };
    new appconfig.CfnDeployment(this, deploymentId + props.configurationVersion, props);
  };
};

const AppConfigExtensionLayerMap: {[key: string]: string} = {
  // https://docs.aws.amazon.com/appconfig/latest/userguide/appconfig-integration-lambda-extensions.html#appconfig-integration-lambda-extensions-add
  'us-east-1': 'arn:aws:lambda:us-east-1:027255383542:layer:AWS-AppConfig-Extension:61',
  'us-east-2': 'arn:aws:lambda:us-east-2:728743619870:layer:AWS-AppConfig-Extension:47',
  'us-west-1': 'arn:aws:lambda:us-west-1:958113053741:layer:AWS-AppConfig-Extension:61',
  'us-west-2': 'arn:aws:lambda:us-west-2:359756378197:layer:AWS-AppConfig-Extension:89',
  'ca-central-1': 'arn:aws:lambda:ca-central-1:039592058896:layer:AWS-AppConfig-Extension:47',
  'eu-central-1': 'arn:aws:lambda:eu-central-1:066940009817:layer:AWS-AppConfig-Extension:54',
  'eu-west-1': 'arn:aws:lambda:eu-west-1:434848589818:layer:AWS-AppConfig-Extension:59',
  'eu-west-2': 'arn:aws:lambda:eu-west-2:282860088358:layer:AWS-AppConfig-Extension:47',
  'eu-west-3': 'arn:aws:lambda:eu-west-3:493207061005:layer:AWS-AppConfig-Extension:48',
  'eu-north-1': 'arn:aws:lambda:eu-north-1:646970417810:layer:AWS-AppConfig-Extension:86',
  'eu-south-1': 'arn:aws:lambda:eu-south-1:203683718741:layer:AWS-AppConfig-Extension:44',
  'ap-east-1': 'arn:aws:lambda:ap-east-1:630222743974:layer:AWS-AppConfig-Extension:44',
  'ap-northeast-1': 'arn:aws:lambda:ap-northeast-1:980059726660:layer:AWS-AppConfig-Extension:45',
  'ap-northeast-3': 'arn:aws:lambda:ap-northeast-3:706869817123:layer:AWS-AppConfig-Extension:42',
  'ap-northeast-2': 'arn:aws:lambda:ap-northeast-2:826293736237:layer:AWS-AppConfig-Extension:54',
  'ap-southeast-1': 'arn:aws:lambda:ap-southeast-1:421114256042:layer:AWS-AppConfig-Extension:45',
  'ap-southeast-2': 'arn:aws:lambda:ap-southeast-2:080788657173:layer:AWS-AppConfig-Extension:54',
  'ap-southeast-3': 'arn:aws:lambda:ap-southeast-3:418787028745:layer:AWS-AppConfig-Extension:13',
  'ap-south-1': 'arn:aws:lambda:ap-south-1:554480029851:layer:AWS-AppConfig-Extension:55',
  'sa-east-1': 'arn:aws:lambda:sa-east-1:000010852771:layer:AWS-AppConfig-Extension:61',
  'af-south-1': 'arn:aws:lambda:af-south-1:574348263942:layer:AWS-AppConfig-Extension:44',
  'me-south-1': 'arn:aws:lambda:me-south-1:559955524753:layer:AWS-AppConfig-Extension:44',
};
export const ExtensionLayerVersion = (scope: Construct, id: string, region: string) => {
  const layerArn = AppConfigExtensionLayerMap[region];
  return LayerVersion.fromLayerVersionArn(scope, id, layerArn);
};
