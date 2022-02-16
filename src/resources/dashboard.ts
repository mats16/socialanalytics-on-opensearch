import { RemovalPolicy } from 'aws-cdk-lib';
import * as cognito from 'aws-cdk-lib/aws-cognito';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as opensearch from 'aws-cdk-lib/aws-opensearchservice';
import { Construct } from 'constructs';
import { UserPool } from './cognito-for-opensearch';
import { Domain } from './opensearch-fgac';

interface DashboardProps {
  userPool?: UserPool;
};

export class Dashboard extends Construct {
  Domain: Domain;
  BulkOperationRole: iam.Role;

  constructor(scope: Construct, id: string, props: DashboardProps) {
    super(scope, id);

    const userPool = props.userPool;

    const cognitoOptions: opensearch.CognitoOptions|undefined = (userPool)
      ? {
        userPoolId: userPool?.userPoolId,
        identityPoolId: userPool.identityPoolId,
        role: new iam.Role(this, 'CognitoAccessForAmazonOpenSearch', {
          // https://docs.aws.amazon.com/da_pv/opensearch-service/latest/developerguide/cognito-auth.html
          assumedBy: new iam.ServicePrincipal('es.amazonaws.com'),
          managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonOpenSearchServiceCognitoAccess')],
        }),
      }
      : undefined;

    const removalPolicy = RemovalPolicy.DESTROY;
    const retention = logs.RetentionDays.ONE_MONTH;

    this.Domain = new Domain(this, 'Domain', {
      removalPolicy,
      version: { version: 'OpenSearch_1.1' },
      enableVersionUpgrade: true,
      zoneAwareness: { availabilityZoneCount: 3 },
      capacity: {
        dataNodeInstanceType: 'r6gd.large.search',
        dataNodes: 3,
        masterNodeInstanceType: 'm6g.large.search',
        masterNodes: 3,
      },
      ebs: { enabled: false },
      cognitoDashboardsAuth: cognitoOptions,
      logging: {
        slowSearchLogEnabled: true,
        slowSearchLogGroup: new logs.LogGroup(this, 'SlowSearchLogGroup', { removalPolicy, retention }),
        slowIndexLogEnabled: true,
        slowIndexLogGroup: new logs.LogGroup(this, 'SlowIndexLogEnabled', { removalPolicy, retention }),
        appLogEnabled: true,
        appLogGroup: new logs.LogGroup(this, 'AppLogEnabled', { removalPolicy, retention }),
        auditLogEnabled: true,
        auditLogGroup: new logs.LogGroup(this, 'AuditLogGroup', { removalPolicy, retention }),
      },
    });

    if (userPool) {
      new cognito.CfnUserPoolGroup(this, 'MasterUserGroup', {
        groupName: 'MasterUser',
        description: 'Master user for OpenSearch / fine-grained access control',
        userPoolId: userPool.userPoolId,
        roleArn: this.Domain.masterUserRole.roleArn,
      });

      this.Domain.masterUserRole.assumeRolePolicy?.addStatements(new iam.PolicyStatement({
        principals: [new iam.FederatedPrincipal(
          'cognito-identity.amazonaws.com',
          {
            'StringEquals': { 'cognito-identity.amazonaws.com:aud': userPool.identityPoolId },
            'ForAnyValue:StringLike': { 'cognito-identity.amazonaws.com:amr': 'authenticated' },
          },
        )],
        actions: ['sts:AssumeRoleWithWebIdentity'],
      }));

      const dashboardsUserRole = this.Domain.addRole('DashboardsUserRole', {
        name: 'dashboards_user',
        body: {
          description: 'Provide the minimum permissions for a dashboards user',
          cluster_permissions: ['cluster_composite_ops_ro'],
          index_permissions: [
            {
              index_patterns: ['.kibana_*', '.opensearch_dashboards_*'],
              allowed_actions: ['read', 'delete', 'manage', 'index'],
            },
            {
              index_patterns: ['.tasks', '.management-beats'],
              allowed_actions: ['indices_all'],
            },
            {
              index_patterns: ['tweets-*'],
              allowed_actions: ['read'],
            },
          ],
          tenant_permissions: [{
            tenant_patterns: ['global_tenant'],
            allowed_actions: ['kibana_all_write'],
          }],
        },
      });
      this.Domain.addRoleMapping('DashboardsUserRoleMapping', {
        name: dashboardsUserRole.getAttString('Name'),
        body: {
          backend_roles: [userPool.authenticatedRole.roleArn],
        },
      });
    };

    this.BulkOperationRole = new iam.Role(this, 'BulkOperationRole', {
      description: 'Bulk operator for OpenSearch / fine-grained access control',
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')],
    });
    const bulkOperationRole = this.Domain.addRole('BulkOperationRole', {
      name: 'bulk_operation',
      body: {
        description: 'Provide the minimum permissions for a bulk operation user',
        cluster_permissions: ['indices:data/write/bulk'],
        index_permissions: [{
          index_patterns: ['tweets-*'],
          allowed_actions: ['write', 'create_index'],
        }],
      },
    });
    this.Domain.addRoleMapping('BulkOperationRoleMapping', {
      name: bulkOperationRole.getAttString('Name'),
      body: {
        backend_roles: [this.BulkOperationRole.roleArn],
      },
    });

    this.Domain.addTemplate('TweetsTemplate', {
      name: 'tweets',
      body: {
        index_patterns: ['tweets-*'],
        template: {
          settings: {
            number_of_shards: 3,
            number_of_replicas: 1,
          },
          mappings: {
            _source: {
              enabled: true,
            },
            properties: {
              context_annotations: {
                properties: {
                  domain: {
                    type: 'keyword',
                  },
                  entity: {
                    type: 'keyword',
                  },
                },
              },
              conversation_id: {
                type: 'keyword',
              },
              created_at: {
                type: 'date',
              },
              entities: {
                properties: {
                  annotation: {
                    type: 'keyword',
                  },
                  hashtag: {
                    type: 'keyword',
                  },
                },
              },
              geo: {
                properties: {
                  coordinates: {
                    properties: {
                      type: {
                        type: 'keyword',
                      },
                      coordinates: {
                        type: 'geo_point',
                      },
                    },
                  },
                  place_id: {
                    type: 'keyword',
                  },
                },
              },
              id: {
                type: 'keyword',
              },
              lang: {
                type: 'keyword',
              },
              possibly_sensitive: {
                type: 'boolean',
              },
              public_metrics: {
                properties: {
                  retweet_count: {
                    type: 'long',
                  },
                  reply_count: {
                    type: 'long',
                  },
                  like_count: {
                    type: 'long',
                  },
                  quote_count: {
                    type: 'long',
                  },
                },
              },
              reply_settings: {
                type: 'keyword',
              },
              source: {
                type: 'keyword',
              },
              text: {
                type: 'text',
              },
              url: {
                type: 'keyword',
              },
              author: {
                properties: {
                  id: {
                    type: 'keyword',
                  },
                  name: {
                    type: 'keyword',
                  },
                  username: {
                    type: 'keyword',
                  },
                  url: {
                    type: 'keyword',
                  },
                  verified: {
                    type: 'keyword',
                  },
                  public_metrics: {
                    properties: {
                      followers_count: {
                        type: 'long',
                      },
                      following_count: {
                        type: 'long',
                      },
                      tweet_count: {
                        type: 'long',
                      },
                      listed_count: {
                        type: 'long',
                      },
                    },
                  },
                },
              },
              matching_rules: {
                properties: {
                  id: {
                    type: 'keyword',
                  },
                  tag: {
                    type: 'keyword',
                  },
                },
              },
              analysis: {
                properties: {
                  normalized_text: {
                    type: 'keyword',
                    index: false,
                  },
                  sentiment: {
                    type: 'keyword',
                  },
                  sentiment_score: {
                    properties: {
                      positive: {
                        type: 'double',
                      },
                      negative: {
                        type: 'double',
                      },
                      neutral: {
                        type: 'double',
                      },
                      mixed: {
                        type: 'double',
                      },
                    },
                  },
                  entities: {
                    type: 'keyword',
                  },
                },
              },
              includes: {
                properties: {
                  tweets: {
                    type: 'object',
                    enabled: false,
                  },
                  users: {
                    type: 'object',
                    enabled: false,
                  },
                },
              },
            },
          },
        },
      },
    });

  };
}
