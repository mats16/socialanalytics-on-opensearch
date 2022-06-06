import { CustomResource, Duration } from 'aws-cdk-lib';
import { Port } from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as opensearch from 'aws-cdk-lib/aws-opensearchservice';
import * as cr from 'aws-cdk-lib/custom-resources';
import { Construct } from 'constructs';
import { Function } from './lambda-nodejs';

interface IndexPermission {
  index_patterns?: string[];
  dls?: string;
  fls?: string[];
  masked_fields?: string[];
  allowed_actions?: string[];
}

interface TenantPermission {
  tenant_patterns?: string[];
  allowed_actions?: ('kibana_all_read'|'kibana_all_write')[];
}

interface RoleProps {
  name?: string;
  body: {
    description?: string;
    cluster_permissions?: string[];
    index_permissions?: IndexPermission[];
    tenant_permissions?: TenantPermission[];
  };
};

interface RoleMappingProps {
  name?: string;
  body: {
    backend_roles?: string[];
    hosts?: string[];
    users?: string[];
  };
};

interface FieldProperty {
  type?: 'boolean'|'byte'|'short'|'integer'|'long'|'float'|'half_float'|'scaled_float'|'double'|'keyword'|'text'|'date'|'ip'|'date'|'binary'|'object'|'nested'|'geo_point';
  analyzer?: string;
  format?: string;
  index?: boolean;
  enabled?: boolean;
  properties?: {
    [key: string]: FieldProperty;
  };
  fielddata?: boolean;
  fields?: {
    [key: string]: FieldProperty;
  };
}

interface TemplateBody {
  aliased?: object;
  settings?: {
    index?: {
      number_of_shards?: number;
      number_of_replicas?: number;
      analysis?: {
        analyzer?: {
          [key: string]: {
            type: string;
            tokenizer?: string;
            user_dictionary?: string;
          };
        };
        filter?: {
          [key: string]: {
            type: string;
            synonyms_path: string[];
          };
        };
      };
    };
  };
  mappings?: {
    _source?: {
      enabled: boolean;
    };
    properties?: {
      [key: string]: FieldProperty;
    };
  };
}

interface IndexTemplateProps {
  name?: string;
  body: {
    index_patterns: string[];
    template: TemplateBody;
    composed_of?: string[];
    priority?: number;
    version?: number;
    _meta?: object;
  };
};

interface ComponentTemplateProps {
  name?: string;
  body: {
    template: TemplateBody;
    priority?: number;
    version?: number;
    _meta?: object;
  };
};

export class Domain extends opensearch.Domain {
  masterUserRole: iam.Role;
  crServiceToken: string;

  constructor(scope: Construct, id: string, props: opensearch.DomainProps) {

    const masterUserRole = new iam.Role(scope, `${id}-MasterUserRole`, {
      description: 'Master user for OpenSearch / fine-grained access control',
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole')],
    });

    const accessPolicy = new iam.PolicyStatement({
      principals: [new iam.AnyPrincipal()],
      actions: ['es:ESHttp*'],
      resources: ['*'],
    });

    const defaultProps: Partial<opensearch.DomainProps> = {
      fineGrainedAccessControl: { masterUserArn: masterUserRole.roleArn },
      enforceHttps: true,
      nodeToNodeEncryption: true,
      encryptionAtRest: { enabled: true },
      accessPolicies: [accessPolicy],
    };

    super(scope, id, { ...defaultProps, ...props });

    this.masterUserRole = masterUserRole;

    const onEventHandler = new Function(this, 'OpenSearchResourceFunction', {
      description: 'Lambda-backed custom resources - OpenSearch resources',
      entry: './src/custom-resources-functions/opensearch-resource/index.ts',
      timeout: Duration.seconds(300),
      role: masterUserRole,
      vpc: props.vpc,
    });
    const provider = new cr.Provider(this, 'Provider', { onEventHandler });
    this.crServiceToken = provider.serviceToken;

    if (typeof props.vpc != 'undefined') {
      this.connections.allowFrom(onEventHandler, Port.tcp(443));
    } else {
      const consoleRole = this.addRole('ConsoleRole', {
        name: 'aws_console',
        body: {
          description: 'Provide the minimum permissions for aws console user',
          cluster_permissions: [
            'cluster:monitor/health',
          ],
          index_permissions: [{
            index_patterns: ['*'],
            allowed_actions: [
              'indices:monitor/stats',
              'indices:admin/mappings/get',
            ],
          }],
        },
      });
      this.addRoleMapping('ConsoleRoleMapping', {
        name: consoleRole.getAttString('Name'),
        body: {
          backend_roles: [`arn:aws:iam::${this.stack.account}:role/*`],
        },
      });
    }

  };
  addRole(id: string, props: RoleProps) {
    const name = props.name || id;
    const body = props.body;
    const resource = new CustomResource(this, id, {
      serviceToken: this.crServiceToken,
      resourceType: 'Custom::OpenSearchRole',
      properties: {
        host: this.domainEndpoint,
        path: '_plugins/_security/api/roles/',
        name,
        body,
      },
    });
    return resource;
  };
  addRoleMapping(id: string, props: RoleMappingProps) {
    const name = props.name || id;
    const body = props.body;
    const resource = new CustomResource(this, id, {
      serviceToken: this.crServiceToken,
      resourceType: 'Custom::OpenSearchRoleMapping',
      properties: {
        host: this.domainEndpoint,
        path: '_plugins/_security/api/rolesmapping/',
        name,
        body,
      },
    });
    return resource;
  };
  addIndexTemplate(id: string, props: IndexTemplateProps) {
    const name = props.name || id;
    const body = props.body;
    const resource = new CustomResource(this, id, {
      serviceToken: this.crServiceToken,
      resourceType: 'Custom::OpenSearchTemplate',
      properties: {
        host: this.domainEndpoint,
        path: '_index_template/',
        name,
        body,
      },
    });
    return resource;
  };
  addComponentTemplate(id: string, props: ComponentTemplateProps) {
    const name = props.name || id;
    const body = props.body;
    const resource = new CustomResource(this, id, {
      serviceToken: this.crServiceToken,
      resourceType: 'Custom::OpenSearchComponentTemplate',
      properties: {
        host: this.domainEndpoint,
        path: '_component_template/',
        name,
        body,
      },
    });
    return resource;
  };
}
