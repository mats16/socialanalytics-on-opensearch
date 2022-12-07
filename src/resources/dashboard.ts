import { RemovalPolicy } from 'aws-cdk-lib';
import { IVpc } from 'aws-cdk-lib/aws-ec2';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as opensearch from 'aws-cdk-lib/aws-opensearchservice';
import { Construct } from 'constructs';
import { Domain } from './opensearch-fgac';

interface DashboardProps {
  domainName?: string;
  vpc?: IVpc;
  snapshotBucketName?: string;
  snapshotBasePath?: string;
};

export class Dashboard extends Construct {
  Domain: Domain;

  constructor(scope: Construct, id: string, props: DashboardProps) {
    super(scope, id);

    const { vpc, domainName, snapshotBucketName, snapshotBasePath } = props;

    const removalPolicy = RemovalPolicy.DESTROY;
    const retention = logs.RetentionDays.ONE_MONTH;

    this.Domain = new Domain(this, 'Domain', {
      domainName,
      version: opensearch.EngineVersion.openSearch('2.3'),
      enableVersionUpgrade: true,
      zoneAwareness: { availabilityZoneCount: 3 },
      vpc,
      capacity: {
        dataNodeInstanceType: 'r6gd.xlarge.search',
        dataNodes: 3,
        masterNodeInstanceType: 'm6g.large.search',
        masterNodes: 3,
      },
      ebs: { enabled: false },
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

    if (typeof snapshotBucketName == 'string') {
      this.Domain.addSnapshotRepo('DefaultSnapshotRepo', {
        name: 'default',
        body: {
          settings: {
            bucket: snapshotBucketName,
            base_path: snapshotBasePath || 'snapshot',
          },
        },
      });
    }

    const socialAnalyticsReadOnlyRole = this.Domain.addRole('SocialAnalyticsReadOnlyRole', {
      name: 'social_analytics_read_only',
      body: {
        description: 'Provide the minimum permissions for all users',
        cluster_permissions: [
          'cluster_composite_ops_ro',
          'cluster:admin/opendistro/reports/definition/create',
          'cluster:admin/opendistro/reports/definition/update',
          'cluster:admin/opendistro/reports/definition/on_demand',
          'cluster:admin/opendistro/reports/definition/delete',
          'cluster:admin/opendistro/reports/definition/get',
          'cluster:admin/opendistro/reports/definition/list',
          'cluster:admin/opendistro/reports/instance/list',
          'cluster:admin/opendistro/reports/instance/get',
          'cluster:admin/opendistro/reports/menu/download',

        ],
        index_permissions: [
          {
            index_patterns: ['.kibana', '.kibana_*', '.opensearch_dashboards', '.opensearch_dashboards_*'],
            allowed_actions: ['read', 'delete', 'manage', 'index'],
          },
          {
            index_patterns: ['.tasks', '.management-beats'],
            allowed_actions: ['indices_all'],
          },
          {
            index_patterns: ['tweets-*'],
            allowed_actions: ['read', 'indices_monitor'],
          },
        ],
        tenant_permissions: [{
          tenant_patterns: ['global_tenant'],
          allowed_actions: ['kibana_all_read'],
        }],
      },
    });
    this.Domain.addRoleMapping('SocialAnalyticsReadOnlyRoleMapping', {
      name: socialAnalyticsReadOnlyRole.getAttString('Name'),
      body: {
        backend_roles: ['*'],
      },
    });

    const kuromojiComponentTemplate = this.Domain.addComponentTemplate('KuromojiComponentTemplate', {
      name: 'kuromoji_user_dic',
      body: {
        template: {
          settings: {
            index: {
              analysis: {
                analyzer: {
                  kuromoji_user_dic: {
                    type: 'kuromoji',
                  },
                },
              },
            },
          },
        },
      },
    });

    this.Domain.addIndexTemplate('TweetsTemplate', {
      name: 'tweets',
      body: {
        index_patterns: ['tweets-*'],
        composed_of: [kuromojiComponentTemplate.getAttString('Name')],
        template: {
          settings: {
            index: {
              number_of_shards: 3,
              number_of_replicas: 1,
            },
          },
          mappings: {
            _source: {
              enabled: true,
            },
            properties: {
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
              author_id: {
                type: 'keyword',
              },
              comprehend: {
                properties: {
                  entities: {
                    type: 'keyword',
                  },
                  //key_phrases: {
                  //  type: 'keyword',
                  //},
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
                },
              },
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
              created_at_year: {
                type: 'keyword',
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
                      display_domain: {
                        type: 'keyword',
                      },
                      display_url: {
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
                        analyzer: 'kuromoji_user_dic',
                        fielddata: true,
                        fields: {
                          raw: {
                            type: 'keyword',
                          },
                        },
                      },
                      description: {
                        type: 'text',
                        analyzer: 'kuromoji_user_dic',
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
              normalized_text: {
                type: 'text',
                analyzer: 'kuromoji_user_dic',
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
                analyzer: 'kuromoji_user_dic',
              },
              updated_at: {
                type: 'date',
                format: 'epoch_second',
              },
              url: {
                type: 'keyword',
                index: false,
              },
              //matching_rules: {
              //  properties: {
              //    id: {
              //      type: 'keyword',
              //    },
              //    tag: {
              //      type: 'keyword',
              //    },
              //  },
              //},
            },
          },
        },
      },
    });

  };
}
