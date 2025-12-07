import { getContract } from './gateway';
import { OrgMSP } from '../config/orgConfig';

export async function evaluateTx<T = unknown>(
  orgMSP: OrgMSP,
  chaincodeName: string,
  fn: string,
  ...args: string[]
): Promise<T> {
  const { gateway, contract } = await getContract(orgMSP, chaincodeName);
  try {
    const resultBytes = await contract.evaluateTransaction(fn, ...args);
    if (!resultBytes || resultBytes.length === 0) {
      return null as unknown as T;
    }
    const asString = resultBytes.toString();
    try {
      return JSON.parse(asString) as T;
    } catch {
      return asString as unknown as T;
    }
  } finally {
    gateway.close();
  }
}

export async function submitTx<T = unknown>(
  orgMSP: OrgMSP,
  chaincodeName: string,
  fn: string,
  ...args: string[]
): Promise<T> {
  const { gateway, contract } = await getContract(orgMSP, chaincodeName);
  try {
    const resultBytes = await contract.submitTransaction(fn, ...args);
    if (!resultBytes || resultBytes.length === 0) {
      return null as unknown as T;
    }
    const asString = resultBytes.toString();
    try {
      return JSON.parse(asString) as T;
    } catch {
      return asString as unknown as T;
    }
  } finally {
    gateway.close();
  }
}
