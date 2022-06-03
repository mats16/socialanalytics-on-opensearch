import { Duration, Aws } from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import { LambdaInvoke, DynamoGetItem, DynamoPutItem, DynamoAttributeValue, CallAwsService } from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Construct } from 'constructs';
import { Function } from './lambda-nodejs';

export class ComprehendWithCache extends Construct {
  stateMachine: sfn.StateMachine;

  constructor(scope: Construct, id: string) {
    super(scope, id);

    const normalizeFunction = new Function(this, 'NormalizeFunction', {
      entry: './src/functions/normalize/index.ts',
      environment: {
        POWERTOOLS_METRICS_NAMESPACE: Aws.STACK_NAME,
        POWERTOOLS_SERVICE_NAME: 'ComprehendWithCache',
      },
    });

    const b64DecodeFunction = new Function(this, 'B64DecodeFunction', {
      entry: './src/functions/b64decode/index.ts',
      environment: {
        POWERTOOLS_METRICS_NAMESPACE: Aws.STACK_NAME,
        POWERTOOLS_SERVICE_NAME: 'ComprehendWithCache',
      },
    });

    const b64EncodeFunction = new Function(this, 'B64EncodeFunction', {
      entry: './src/functions/b64encode/index.ts',
      environment: {
        POWERTOOLS_METRICS_NAMESPACE: Aws.STACK_NAME,
        POWERTOOLS_SERVICE_NAME: 'ComprehendWithCache',
        CACHE_TTL_DAYS: '30',
      },
    });

    const cacheTable = new dynamodb.Table(this, 'CacheTable', {
      partitionKey: {
        name: 'id',
        type: dynamodb.AttributeType.STRING,
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'expire',
    });

    const invokeState = new sfn.Pass(this, 'Invoke', {
      parameters: {
        'input.$': '$',
      },
    });

    const normalizeTextTask = new LambdaInvoke(this, 'Normalize Text', {
      lambdaFunction: normalizeFunction,
      payloadResponseOnly: true,
      inputPath: '$.input',
      resultSelector: {
        'input.$': '$',
        'CacheKey.$': '$.CacheKey',
      },
    });

    const getCacheTask = new DynamoGetItem(this, 'Get Cache', {
      table: cacheTable,
      key: {
        id: DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$')),
      },
      inputPath: '$.CacheKey',
      resultPath: '$.Cache',
    }).addRetry({ maxAttempts: 2 });

    const decodeCacheTask = new LambdaInvoke(this, 'Decode Cache', {
      lambdaFunction: b64DecodeFunction,
      payloadResponseOnly: true,
      inputPath: '$.Cache.Item.value.B',
    });

    const translateTask = new CallAwsService(this, 'Translate', {
      service: 'Translate',
      action: 'translateText',
      iamAction: 'translate:TranslateText',
      iamResources: ['*'],
      inputPath: '$.input',
      parameters: {
        'Text.$': '$.Text',
        'SourceLanguageCode': 'auto',
        'TargetLanguageCode': 'en',
      },
      resultSelector: {
        'Text.$': '$.TranslatedText',
        'LanguageCode': 'en',
      },
      resultPath: '$.input',
    }).addRetry({ maxAttempts: 2 });

    const detectEntitiesTask = new CallAwsService(this, 'Detect Entities', {
      service: 'Comprehend',
      action: 'detectEntities',
      iamAction: 'comprehend:DetectEntities',
      iamResources: ['*'],
      parameters: {
        'Text.$': '$.Text',
        'LanguageCode.$': '$.LanguageCode',
      },
    }).addRetry({ maxAttempts: 20 });

    const detectSentimentTask = new CallAwsService(this, 'Detect Sentiment', {
      service: 'Comprehend',
      action: 'detectSentiment',
      iamAction: 'comprehend:DetectSentiment',
      iamResources: ['*'],
      parameters: {
        'Text.$': '$.Text',
        'LanguageCode.$': '$.LanguageCode',
      },
    }).addRetry({ maxAttempts: 20 });

    //const detectKeyPhrasesTask = new CallAwsService(this, 'Detect KeyPhrases', {
    //  service: 'Comprehend',
    //  action: 'detectKeyPhrases',
    //  iamAction: 'comprehend:DetectKeyPhrases',
    //  iamResources: ['*'],
    //  parameters: {
    //    'Text.$': '$.NormalizedText.Value',
    //    'LanguageCode.$': '$.LanguageCode',
    //  },
    //});

    const detectTask = new sfn.Parallel(this, 'Detection', {
      inputPath: '$.input',
      resultSelector: {
        'Entities.$': '$[0].Entities',
        'Sentiment.$': '$[1].Sentiment',
        'SentimentScore.$': '$[1].SentimentScore',
        //'KeyPhrases.$': '$[2].KeyPhrases',
      },
      resultPath: '$.Comprehend',
    });

    const genResponseTask = new sfn.Pass(this, 'Generate Response', {
      parameters: {
        'NormalizedText.$': '$.input.Text',
        'Entities.$': '$.Comprehend.Entities',
        'Sentiment.$': '$.Comprehend.Sentiment',
        'SentimentScore.$': '$.Comprehend.SentimentScore',
        //'KeyPhrases.$': '$.Comprehend.KeyPhrases',
      },
      resultPath: '$.Response',
    });

    const genCacheValueTask = new LambdaInvoke(this, 'Generate CacheValue', {
      lambdaFunction: b64EncodeFunction,
      payloadResponseOnly: true,
      inputPath: '$.Response',
      resultPath: '$.Cache',
    });

    const storeCacheTask = new DynamoPutItem(this, 'Store Cache', {
      table: cacheTable,
      item: {
        id: DynamoAttributeValue.fromString(sfn.JsonPath.stringAt( '$.CacheKey')),
        value: DynamoAttributeValue.fromBinary(sfn.JsonPath.stringAt('$.Cache.Value')),
        expire: DynamoAttributeValue.numberFromString(sfn.JsonPath.stringAt('$.Cache.Expire')),
      },
      resultPath: '$.tmp',
      outputPath: '$.Response',
    }).addRetry({ maxAttempts: 2 });;

    detectTask.branch(detectEntitiesTask).branch(detectSentimentTask);
    detectTask.next(genResponseTask).next(genCacheValueTask).next(storeCacheTask);

    const checkLanguageCodeSupported = new sfn.Choice(this, 'LanguageCode supported?');
    checkLanguageCodeSupported.when(sfn.Condition.or(
      sfn.Condition.stringEquals('$.input.LanguageCode', 'ar'),
      sfn.Condition.stringEquals('$.input.LanguageCode', 'hi'),
      sfn.Condition.stringEquals('$.input.LanguageCode', 'ko'),
      sfn.Condition.stringEquals('$.input.LanguageCode', 'zh-TW'),
      sfn.Condition.stringEquals('$.input.LanguageCode', 'ja'),
      sfn.Condition.stringEquals('$.input.LanguageCode', 'zh'),
      sfn.Condition.stringEquals('$.input.LanguageCode', 'de'),
      sfn.Condition.stringEquals('$.input.LanguageCode', 'pt'),
      sfn.Condition.stringEquals('$.input.LanguageCode', 'en'),
      sfn.Condition.stringEquals('$.input.LanguageCode', 'it'),
      sfn.Condition.stringEquals('$.input.LanguageCode', 'fr'),
      sfn.Condition.stringEquals('$.input.LanguageCode', 'es'),
    ), detectTask);
    checkLanguageCodeSupported.otherwise(translateTask.next(detectTask));

    //const autoLanguageCode = new sfn.Pass(this, 'Auto LanguageCode', {
    //  parameters: {
    //    'Text.$': '$.input.Text',
    //    'LanguageCode': 'auto',
    //  },
    //  resultPath: '$.input',
    //});

    //const checkLanguageCodeExist = new sfn.Choice(this, 'LanguageCode exist?');
    //checkLanguageCodeExist.when(sfn.Condition.isNotPresent('$.input.LanguageCode'), autoLanguageCode.next(checkLanguageCodeSupported));
    //checkLanguageCodeExist.otherwise(checkLanguageCodeSupported);

    const checkCacheHit = new sfn.Choice(this, 'CacheHit?');
    checkCacheHit.when(sfn.Condition.isPresent('$.Cache.Item.value.B'), decodeCacheTask);
    checkCacheHit.otherwise(checkLanguageCodeSupported);

    const empryResponse = new sfn.Pass(this, 'EmpryResponse', { parameters: {} });

    const checkTextEmpty = new sfn.Choice(this, 'Text empty?');
    checkTextEmpty.when(sfn.Condition.stringEquals('$.input.Text', ''), empryResponse);
    checkTextEmpty.otherwise(getCacheTask.next(checkCacheHit));

    invokeState.next(normalizeTextTask).next(checkTextEmpty);

    this.stateMachine = new sfn.StateMachine(this, 'StateMachine', {
      stateMachineType: sfn.StateMachineType.EXPRESS,
      definition: invokeState,
      tracingEnabled: true,
      logs: {
        level: sfn.LogLevel.ERROR,
        destination: new logs.LogGroup(this, 'Logs', { retention: logs.RetentionDays.TWO_WEEKS }),
      },
    });

    // for Translate auto mode
    this.stateMachine.addToRolePolicy(new iam.PolicyStatement({
      actions: ['comprehend:DetectDominantLanguage'],
      resources: ['*'],
    }));

  }
}
