# Serverless Social Media Dashboard

## Service-Linked Roles for Amazon ECS
If you use AWS ECS first time, you need to create service-linked roles.

```bash
aws iam create-service-linked-role --aws-service-name ecs.amazonaws.com
```

## Installing the AWS SAM CLI

This project need to use AWS SAM.
About installing **AWS SAM CLI**, please read [the document](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html).

## How to Buid & Deploy

```bash
sam build --use-container --skip-pull-image
sam deploy --guided
```
