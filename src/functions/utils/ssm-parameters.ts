import { GetParameterCommandOutput } from '@aws-sdk/client-ssm';
import axios from 'axios';

export const getParameter = async (parameterPath: string): Promise<string> => {
  const sessionToken = process.env.AWS_SESSION_TOKEN!;
  const port = process.env.PARAMETERS_SECRETS_EXTENSION_HTTP_PORT || '2773';
  const url = encodeURI(`http://localhost:${port}/systemsmanager/parameters/get/?name=${parameterPath}`);
  const headers = { 'X-Aws-Parameters-Secrets-Token': sessionToken };
  const res = await axios.get(url, { headers });
  const output: GetParameterCommandOutput = res.data;
  return output.Parameter?.Value!;
};

export const getListParameter = async (parameterPath: string): Promise<string[]> => {
  const value = await getParameter(parameterPath);
  return value.split(',')!;
};