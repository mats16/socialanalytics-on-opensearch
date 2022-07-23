import { Aws } from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import { LambdaInvoke, CallAwsService } from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Construct } from 'constructs';

interface ReIndexBatchProps {
  tweetTable: dynamodb.ITable;
  dataLoadFunction: lambda.IFunction;
}

export class ReIndexBatch extends Construct {
  stateMachine: sfn.StateMachine;

  constructor(scope: Construct, id: string, props: ReIndexBatchProps) {
    super(scope, id);

    const { tweetTable, dataLoadFunction } = props;

    const queryLimit = 50;

    const invokeState = new sfn.Pass(this, 'Invoke', {
      parameters: {
        Input: {
          'created_at_year.$': '$.year',
        },
      },
    });

    const query = new CallAwsService(this, 'Query', {
      service: 'DynamoDB',
      action: 'query',
      iamAction: 'dynamodb:Query',
      iamResources: [tweetTable.tableArn, `${tweetTable.tableArn}/index/*`],
      parameters: {
        TableName: tweetTable.tableName,
        IndexName: 'created_at-index',
        Limit: queryLimit,
        ExpressionAttributeValues: {
          ':y': {
            'S.$': '$.Input.created_at_year',
          },
        },
        KeyConditionExpression: 'created_at_year = :y',
        //'ExclusiveStartKey.$': '$.Result.LastEvaluatedKey',
      },
      resultPath: '$.Result',
    });

    const map = new sfn.Map(this, 'Map', {
      itemsPath: '$.Result.Items',
      //resultSelector: {
      //  'Records.$': '$',
      //},
      resultPath: '$.Result.Items',
    });

    const transform = new sfn.Pass(this, 'Transform', {
      parameters: {
        'awsRegion': Aws.REGION,
        'eventID.$': '$$.Execution.Name',
        'eventName': 'MODIFY',
        'eventVersion': '1.1',
        'eventSource': 'aws:states',
        'eventSourceARN.$': '$$.StateMachine.Id',
        'dynamodb': {
          'NewImage.$': '$',
        },
      },
    });
    map.iterator(transform);

    const openSearchLoad = new LambdaInvoke(this, 'OpenSearchLoad', {
      lambdaFunction: dataLoadFunction,
      payload: sfn.TaskInput.fromObject({
        'Records.$': '$.Result.Items',
      }),
      resultPath: sfn.JsonPath.DISCARD,
    });

    const query2 = new CallAwsService(this, 'Query2', {
      service: 'DynamoDB',
      action: 'query',
      iamAction: 'dynamodb:Query',
      iamResources: [tweetTable.tableArn, `${tweetTable.tableArn}/index/*`],
      parameters: {
        'TableName': tweetTable.tableName,
        'IndexName': 'created_at-index',
        'Limit': queryLimit,
        'ExpressionAttributeValues': {
          ':y': {
            'S.$': '$.Input.created_at_year',
          },
        },
        'KeyConditionExpression': 'created_at_year = :y',
        'ExclusiveStartKey.$': '$.Result.LastEvaluatedKey',
      },
      resultPath: '$.Result',
    });

    query2.next(map);

    const succeed = new sfn.Succeed(this, 'Succeed');

    const checkLastEvaluatedKey = new sfn.Choice(this, 'LastEvaluatedKey?');
    checkLastEvaluatedKey.when(sfn.Condition.isPresent('$.Result.LastEvaluatedKey'), query2);
    checkLastEvaluatedKey.otherwise(succeed);

    invokeState.next(query).next(map).next(openSearchLoad).next(checkLastEvaluatedKey);

    this.stateMachine = new sfn.StateMachine(this, 'StateMachine', {
      definition: invokeState,
      //tracingEnabled: true,
      logs: {
        level: sfn.LogLevel.ERROR,
        destination: new logs.LogGroup(this, 'Logs', { retention: logs.RetentionDays.TWO_WEEKS }),
      },
    });

  }
}
