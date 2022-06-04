import { RemovalPolicy } from 'aws-cdk-lib';
import * as cognito from 'aws-cdk-lib/aws-cognito';
import { IVpc } from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as opensearch from 'aws-cdk-lib/aws-opensearchservice';
import { Construct } from 'constructs';
import { UserPool } from './cognito-for-opensearch';
import { Domain } from './opensearch-fgac';

interface DashboardProps {
  vpc?: IVpc;
  userPool?: UserPool;
};

export class Dashboard extends Construct {
  Domain: Domain;

  constructor(scope: Construct, id: string, props: DashboardProps) {
    super(scope, id);

    const { vpc, userPool } = props;

    let cognitoOptions: opensearch.CognitoOptions|undefined = undefined;
    if (typeof userPool != 'undefined') {
      cognitoOptions = {
        userPoolId: userPool?.userPoolId,
        identityPoolId: userPool.identityPoolId,
        role: new iam.Role(this, 'CognitoAccessForAmazonOpenSearch', {
          // https://docs.aws.amazon.com/da_pv/opensearch-service/latest/developerguide/cognito-auth.html
          assumedBy: new iam.ServicePrincipal('es.amazonaws.com'),
          managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonOpenSearchServiceCognitoAccess')],
        }),
      };
    }

    const removalPolicy = RemovalPolicy.DESTROY;
    const retention = logs.RetentionDays.ONE_MONTH;

    this.Domain = new Domain(this, 'Domain', {
      removalPolicy,
      version: opensearch.EngineVersion.OPENSEARCH_1_2,
      enableVersionUpgrade: true,
      zoneAwareness: { availabilityZoneCount: 3 },
      vpc,
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
        slowSearchLogGroup: new logs.LogGroup(this, 'SlowSearchLogs', { removalPolicy, retention }),
        slowIndexLogEnabled: true,
        slowIndexLogGroup: new logs.LogGroup(this, 'SlowIndexLogs', { removalPolicy, retention }),
        appLogEnabled: true,
        appLogGroup: new logs.LogGroup(this, 'AppLogs', { removalPolicy, retention }),
        auditLogEnabled: true,
        auditLogGroup: new logs.LogGroup(this, 'AuditLogs', { removalPolicy, retention }),
      },
    });

    if (typeof userPool != 'undefined') {
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
                  cashtag: {
                    type: 'keyword',
                  },
                  hashtag: {
                    type: 'keyword',
                  },
                  mention: {
                    type: 'keyword',
                  },
                  url: {
                    properties: {
                      domain: {
                        type: 'keyword',
                      },
                      expanded_url: {
                        type: 'text',
                        fielddata: true,
                        fields: {
                          raw: {
                            type: 'keyword',
                          },
                        },
                      },
                      title: {
                        type: 'text',
                        analyzer: 'kuromoji',
                      },
                      description: {
                        type: 'text',
                        analyzer: 'kuromoji',
                      },
                    },
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
              in_reply_to_user_id: {
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
              referenced_tweets: {
                properties: {
                  type: {
                    type: 'keyword',
                  },
                  id: {
                    type: 'keyword',
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
                analyzer: 'kuromoji',
              },
              url: {
                type: 'keyword',
                index: false,
              },
              author_id: {
                type: 'keyword',
                index: false,
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
                    index: false,
                  },
                  verified: {
                    type: 'boolean',
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
                    type: 'text',
                    analyzer: 'kuromoji',
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
                  //key_phrases: {
                  //  type: 'keyword',
                  //},
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
