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

```bash
sam build --use-container --skip-pull-image
sam deploy --guided
```
