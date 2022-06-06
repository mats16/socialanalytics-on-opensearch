import { OpenSearchClient, CreatePackageCommand, UpdatePackageCommand, DescribePackagesCommand, AssociatePackageCommand } from '@aws-sdk/client-opensearch';
import { S3Handler } from 'aws-lambda';

const region = process.env.AWS_REGION!;

const client = new OpenSearchClient({ region });

const describePackage = async (packageName: string) => {
  const cmd = new DescribePackagesCommand({
    Filters: [{ Name: 'PackageName', Value: [packageName] }],
  });
  const { PackageDetailsList } = await client.send(cmd);
  return PackageDetailsList?.shift();
};

const createPackage = async (packageName: string, bucketName: string, key: string) => {
  const cmd = new CreatePackageCommand({
    PackageType: 'TXT-DICTIONARY',
    PackageName: packageName,
    PackageSource: {
      S3BucketName: bucketName,
      S3Key: key,
    },
  });
  const { PackageDetails } = await client.send(cmd);
  return PackageDetails;
};

const updatePackage = async (packageId: string, bucketName: string, key: string) => {
  const cmd = new UpdatePackageCommand({
    PackageID: packageId,
    PackageSource: {
      S3BucketName: bucketName,
      S3Key: key,
    },
  });
  const { PackageDetails } = await client.send(cmd);
  return PackageDetails;
};

const associatePackage = async (packageId: string, domainName: string) => {
  const cmd = new AssociatePackageCommand({
    PackageID: packageId,
    DomainName: domainName,
  });
  await client.send(cmd);
};

export const handler: S3Handler = async (event, _context) => {
  for await (let record of event.Records) {
    const { bucket, object } = record.s3;
    const packageName = object.key.replace('.txt', '').split('/').pop() || 'package';
    const packageDetails = await describePackage(packageName);
    if (typeof packageDetails?.PackageID == 'undefined') {
      await createPackage(packageName, bucket.name, object.key);
    } else {
      await updatePackage(packageDetails.PackageID, bucket.name, object.key);
    }
  }
};