# AI-Driven Social Media Dashboard
Voice of customer analytics through social media: Build a social media dashboard using artificial intelligence and business intelligence services.

Organizations want to understand how customers perceive them and who those customers are. For example, what factors are driving the most positive and negative experiences for their offerings? Social media interactions between organizations and customers are a great way to evaluate this and deepen brand awareness. Understanding these conversations are a low-cost way to acquire leads, improve website traffic, develop customer relationships, and improve customer service. Since these conversations are all in unstructured text format, it is difficult to scale the analysis and get the full picture.

## Installing the AWS SAM CLI
This project need to use AWS SAM.
About installing **AWS SAM CLI**, please read [the document](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html).

## Service-Linked Roles for Amazon ECS
If you use AWS ECS first time, you need to create service-linked roles.
```bash
aws iam create-service-linked-role --aws-service-name ecs.amazonaws.com
```

## Building Lambda Package
```bash
sam build --use-container --skip-pull-image

sam package --template-file .aws-sam/build/template.yaml --output-template-file .aws-sam/build/package.yaml --s3-bucket <your_bucket>

aws cloudformation create-stack --stack-name <your_stack> --template-body file://.aws-sam/build/package.yaml \
    --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND \
    --parameters \
        ParameterKey=ApplicationName,ParameterValue=<your_application_name> \
        ParameterKey=AuthConsumerKey,ParameterValue=xxxxxxxxxxxxxxxxxxxxxxxxx \
        ParameterKey=AuthConsumerSecret,ParameterValue=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx \
        ParameterKey=AuthAccessToken,ParameterValue=123456789-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx \
        ParameterKey=AuthAccessTokenSecret,ParameterValue=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

***

Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
