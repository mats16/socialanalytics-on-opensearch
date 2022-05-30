import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
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
    });

    const filterDominantLangFunction = new Function(this, 'FilterDominantLangFunction', {
      entry: './src/functions/filter-dominant-lang/index.ts',
    });

    const b64DecodeFunction = new Function(this, 'B64DecodeFunction', {
      entry: './src/functions/b64decode/index.ts',
    });

    const b64EncodeFunction = new Function(this, 'B64EncodeFunction', {
      entry: './src/functions/b64encode/index.ts',
      environment: {
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

    const normalizeTextTask = new LambdaInvoke(this, 'Normalize Text', {
      lambdaFunction: normalizeFunction,
      payloadResponseOnly: true,
      inputPath: '$.Text',
      resultPath: '$.NormalizedText',
    });

    const getCacheTask = new DynamoGetItem(this, 'Get Cache', {
      table: cacheTable,
      key: {
        id: DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$')),
      },
      inputPath: '$.NormalizedText.SHA256',
      resultPath: '$.Cache',
    });

    const decodeCacheTask = new LambdaInvoke(this, 'Decode Cache', {
      lambdaFunction: b64DecodeFunction,
      payloadResponseOnly: true,
      inputPath: '$.Cache.Item.value.B',
    });

    const detectDominantLanguageTask = new CallAwsService(this, 'Detect DominantLanguage', {
      service: 'Comprehend',
      action: 'detectDominantLanguage',
      iamAction: 'comprehend:DetectDominantLanguage',
      iamResources: ['*'],
      parameters: {
        'Text.$': '$.NormalizedText.Value',
      },
      resultPath: '$.DetectDominantLanguage',
    });

    const filterDominantLangTask = new LambdaInvoke(this, 'Choice DominantLanguage', {
      lambdaFunction: filterDominantLangFunction,
      payloadResponseOnly: true,
      inputPath: '$.DetectDominantLanguage',
      resultPath: '$.LanguageCode',
    });

    const detectEntitiesTask = new CallAwsService(this, 'Detect Entities', {
      service: 'Comprehend',
      action: 'detectEntities',
      iamAction: 'comprehend:DetectEntities',
      iamResources: ['*'],
      parameters: {
        'Text.$': '$.NormalizedText.Value',
        'LanguageCode.$': '$.LanguageCode',
      },
    });

    const detectSentimentTask = new CallAwsService(this, 'Detect Sentiment', {
      service: 'Comprehend',
      action: 'detectSentiment',
      iamAction: 'comprehend:DetectSentiment',
      iamResources: ['*'],
      parameters: {
        'Text.$': '$.NormalizedText.Value',
        'LanguageCode.$': '$.LanguageCode',
      },
    });

    const detectKeyPhrasesTask = new CallAwsService(this, 'Detect KeyPhrases', {
      service: 'Comprehend',
      action: 'detectKeyPhrases',
      iamAction: 'comprehend:DetectKeyPhrases',
      iamResources: ['*'],
      parameters: {
        'Text.$': '$.NormalizedText.Value',
        'LanguageCode.$': '$.LanguageCode',
      },
    });

    const detectTask = new sfn.Parallel(this, 'Detection', {
      resultSelector: {
        'Entities.$': '$[0].Entities',
        'Sentiment.$': '$[1].Sentiment',
        'SentimentScore.$': '$[1].SentimentScore',
        'KeyPhrases.$': '$[2].KeyPhrases',
      },
      resultPath: '$.Comprehend',
    });

    const genResponseTask = new sfn.Pass(this, 'Generate Response', {
      parameters: {
        'NormalizedText.$': '$.NormalizedText.Value',
        'Entities.$': '$.Comprehend.Entities',
        'Sentiment.$': '$.Comprehend.Sentiment',
        'SentimentScore.$': '$.Comprehend.SentimentScore',
        'KeyPhrases.$': '$.Comprehend.KeyPhrases',
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
        id: DynamoAttributeValue.fromString(sfn.JsonPath.stringAt( '$.NormalizedText.SHA256')),
        value: DynamoAttributeValue.fromBinary(sfn.JsonPath.stringAt('$.Cache.Value')),
        expire: DynamoAttributeValue.numberFromString(sfn.JsonPath.stringAt('$.Cache.Expire')),
      },
      resultPath: '$.tmp',
      outputPath: '$.Response',
    });

    detectTask.branch(detectEntitiesTask).branch(detectSentimentTask).branch(detectKeyPhrasesTask);
    detectTask.next(genResponseTask).next(genCacheValueTask).next(storeCacheTask);

    const checkLanguageCode = new sfn.Choice(this, 'LanguageCode?');
    checkLanguageCode.when(sfn.Condition.isNotPresent('$.LanguageCode'), detectDominantLanguageTask.next(filterDominantLangTask).next(detectTask));
    checkLanguageCode.otherwise(detectTask);

    const checkCache = new sfn.Choice(this, 'CacheHit?');
    checkCache.when(sfn.Condition.isPresent('$.Cache.Item.value.B'), decodeCacheTask);
    checkCache.otherwise(checkLanguageCode);

    normalizeTextTask.next(getCacheTask).next(checkCache);

    this.stateMachine = new sfn.StateMachine(this, 'StateMachine', {
      stateMachineType: sfn.StateMachineType.EXPRESS,
      definition: normalizeTextTask,
      tracingEnabled: true,
    });

  }
}
