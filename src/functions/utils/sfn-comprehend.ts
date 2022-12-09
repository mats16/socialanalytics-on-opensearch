import { Tracer } from '@aws-lambda-powertools/tracer';
import { SFNClient, StartSyncExecutionCommand } from '@aws-sdk/client-sfn';
import { ComprehendJobOutput } from '../types';

const tracer = new Tracer();

export class ComprehendStateMachine extends SFNClient {
  client: SFNClient;
  stateMachineArn: string;
  constructor(stateMachineArn: string) {
    super({ region: process.env.AWS_REGION });
    this.client = tracer.captureAWSv3Client(this);
    this.stateMachineArn = stateMachineArn;
  }

  async analyzeText(text: string, lang?: string) {
    const cmd = new StartSyncExecutionCommand({
      stateMachineArn: this.stateMachineArn,
      input: JSON.stringify({
        Text: text,
        LanguageCode: lang,
      }),
    });
    const { output } = await this.send(cmd);
    const result: ComprehendJobOutput = (typeof output == 'string')
      ? JSON.parse(output)
      : {};
    return result;
  }
}
