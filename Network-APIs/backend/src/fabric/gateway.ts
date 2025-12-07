import { connect, Contract, Gateway, Identity, Signer } from '@hyperledger/fabric-gateway';
import * as grpc from '@grpc/grpc-js';
import * as fs from 'fs';
import * as crypto from 'crypto';
import { OrgMSP, getOrgConnectionConfig } from '../config/orgConfig';

const defaultChannelName = process.env.FABRIC_CHANNEL || 'rawmaterialsupply';

function newGrpcConnection(peerEndpoint: string, tlsCertPath: string): grpc.Client {
  const tlsRootCert = fs.readFileSync(tlsCertPath);
  const credentials = grpc.credentials.createSsl(tlsRootCert);
  return new grpc.Client(peerEndpoint, credentials);
}

function newIdentity(mspId: string, certPath: string): Identity {
  const credentials = fs.readFileSync(certPath);
  return { mspId, credentials };
}

function newSigner(keyPath: string): Signer {
  const privateKeyPem = fs.readFileSync(keyPath);
  const privateKey = crypto.createPrivateKey(privateKeyPem);
  return async (digest: Uint8Array) => {
    const sign = crypto.createSign('sha256');
    sign.update(digest);
    sign.end();
    return sign.sign(privateKey);
  };
}

export async function getContract(
  orgMSP: OrgMSP,
  chaincodeName: string,
  channelName: string = defaultChannelName,
): Promise<{ gateway: Gateway; contract: Contract }> {
  const cfg = getOrgConnectionConfig(orgMSP);

  const client = newGrpcConnection(cfg.peerEndpoint, cfg.tlsCertPath);
  const identity = newIdentity(cfg.mspId, cfg.certPath);
  const signer = newSigner(cfg.keyPath);

  const gateway = connect({
    client,
    identity,
    signer,
    evaluateOptions: () => ({
      deadline: new Date(Date.now() + 5000),
    }),
    submitOptions: () => ({
      deadline: new Date(Date.now() + 15000),
    }),
  });

  const network = gateway.getNetwork(channelName);
  const contract = network.getContract(chaincodeName);

  return { gateway, contract };
}
