import { PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { S3EventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import { Construct } from 'constructs';
import { Function } from './lambda-nodejs';

interface OpenSearchPackagesProps {
  sourcePath: string;
  stagingBucket: s3.Bucket;
  stagingKeyPrefix?: string;
};

export class OpenSearchPackages extends Construct {
  constructor(scope: Construct, id: string, props: OpenSearchPackagesProps) {
    super(scope, id);

    const { sourcePath, stagingBucket } = props;
    const stagingKeyPrefix = props.stagingKeyPrefix || 'packages/';

    new Function(this, 'RegisterFunction', {
      description: '[SocialAnalytics] OpenSearch package register',
      entry: './src/functions/opensearch-package-register/index.ts',
      events: [
        new S3EventSource(stagingBucket, {
          events: [s3.EventType.OBJECT_CREATED],
          filters: [{ prefix: stagingKeyPrefix, suffix: '.txt' }],
        }),
      ],
      initialPolicy: [
        new PolicyStatement({
          actions: [
            'es:CreatePackage',
            'es:DescribePackages',
            'es:UpdatePackage',
          ],
          resources: ['*'],
        }),
        new PolicyStatement({
          actions: ['s3:GetObject'],
          resources: [`${stagingBucket.bucketArn}/${stagingKeyPrefix}*`],
        }),
      ],
    });

    new s3deploy.BucketDeployment(this, 'Deployment', {
      sources: [s3deploy.Source.asset(sourcePath)],
      destinationBucket: stagingBucket,
      destinationKeyPrefix: stagingKeyPrefix,
    });

  }
}
