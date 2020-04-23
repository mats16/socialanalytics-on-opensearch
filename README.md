# Serverless Social Media Dashboard

## Preparation before deployment

### Create service-linked IAM Role for Amazon ECS

If you use AWS ECS first time, you need to create service-linked roles.

```bash
aws iam create-service-linked-role --aws-service-name ecs.amazonaws.com
```

### Build & Host your own docker image

This template uses [mats16/twitter-streaming-reader](https://hub.docker.com/repository/docker/mats16/twitter-streaming-reader) docker image.
If you need to host own docker images, pleas build image.

### Build & Deploy MeCab Lambda Layer

If you want to analyze japanese with MeCab, please deploy [lambda-layer-mecab-neologd](https://github.com/mats16/lambda-layer-mecab-neologd) in your own AWS account.

## Installing the AWS SAM CLI

This project need to use AWS SAM.
About installing **AWS SAM CLI**, please read [the document](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html).

## How to Buid & Deploy

※ You need to use CAPABILITY_IAM and **CAPABILITY_AUTO_EXPAND**.

```bash
sam build --use-container --skip-pull-image
sam deploy --guided

Configuring SAM deploy
======================

	Looking for samconfig.toml :  Found
	Reading default arguments  :  Success

	Setting default arguments for 'sam deploy'
	=========================================
	Stack Name [social-media-dashboard]:
	AWS Region [us-east-1]:
	Parameter TwitterTermList [AWS,EC2,RDS,S3]:
	Parameter TwitterLanguages [en,es,fr,de,it,pt,ar,hi,ja,ko,zh]: ja
	Parameter TwitterFilterLevel [none]:
	Parameter TwitterReaderDockerImage [mats16/twitter-streaming-reader:0.1.0]:
	Parameter AuthAccessToken []: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
	Parameter AuthAccessTokenSecret:
	Parameter AuthConsumerKey []: XXXXXXXXXXXXXXXXXXXXXXXXX
	Parameter AuthConsumerSecret:
	Parameter ApplicationName [social-media-dashboard]: stg-social-media-dashboard
	Parameter VpcCIDR [10.193.0.0/16]:
	Parameter ComprehendEntityScoreThreshold [0.8]:
	Parameter CognitoAllowedEmailDomains [amazon.com,amazon.co.jp]:
	Parameter MecabLambdaLayerArn [arn:aws:lambda:us-east-1:123456789012:layer:dummy:1]:
	#Shows you resources changes to be deployed and require a 'Y' to initiate deploy
	Confirm changes before deploy [y/N]:
	#SAM needs permission to be able to create roles to connect to the resources in your template
	Allow SAM CLI IAM role creation [Y/n]: n
	Capabilities [CAPABILITY_IAM]: CAPABILITY_IAM CAPABILITY_AUTO_EXPAND
	Save arguments to samconfig.toml [Y/n]:

	Looking for resources needed for deployment: Found!

		Managed S3 bucket: aws-sam-cli-managed-default-samclisourcebucket-xxxxxxxxxxxxx
		A different default S3 bucket can be set in samconfig.toml

	Saved arguments to config file
	Running 'sam deploy' for future deployments will use the parameters saved above.
	The above parameters can be changed by modifying samconfig.toml
	Learn more about samconfig.toml syntax at
	https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-config.html
```
