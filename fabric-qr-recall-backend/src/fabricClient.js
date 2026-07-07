const { Gateway, Wallets } = require('fabric-network');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

async function getNetworkAndContracts() {
  const ccpPath = process.env.FABRIC_CONNECTION_PROFILE;
  if (!ccpPath) {
    throw new Error('FABRIC_CONNECTION_PROFILE is not set');
  }

  const ccp = JSON.parse(fs.readFileSync(ccpPath, 'utf8'));

  const walletPath = process.env.FABRIC_WALLET_PATH || './wallet';
  const wallet = await Wallets.newFileSystemWallet(walletPath);

  const identityLabel = process.env.FABRIC_IDENTITY_LABEL;
  if (!identityLabel) {
    throw new Error('FABRIC_IDENTITY_LABEL is not set');
  }

  const identity = await wallet.get(identityLabel);
  if (!identity) {
    throw new Error(`Identity "${identityLabel}" not found in wallet at ${walletPath}`);
  }

  const gateway = new Gateway();
  await gateway.connect(ccp, {
    wallet,
    identity: identityLabel,
    discovery: {
      enabled: true,
      asLocalhost: process.env.FABRIC_AS_LOCALHOST === 'true'
    }
  });

  const channelName = process.env.FABRIC_CHANNEL;
  if (!channelName) {
    throw new Error('FABRIC_CHANNEL is not set');
  }

  const network = await gateway.getNetwork(channelName);

  const domainCCName = process.env.FABRIC_DOMAIN_CHAINCODE;
  const recallCCName = process.env.FABRIC_RECALL_CHAINCODE;

  if (!domainCCName) throw new Error('FABRIC_DOMAIN_CHAINCODE is not set');
  if (!recallCCName) throw new Error('FABRIC_RECALL_CHAINCODE is not set');

  const domainContract = network.getContract(domainCCName);
  const recallContract = network.getContract(recallCCName);

  return { gateway, network, domainContract, recallContract };
}

async function evaluateDomain(fnName, ...args) {
  const { gateway, domainContract } = await getNetworkAndContracts();
  try {
    const result = await domainContract.evaluateTransaction(fnName, ...args);
    return result.toString();
  } finally {
    gateway.close();
  }
}

async function evaluateRecall(fnName, ...args) {
  const { gateway, recallContract } = await getNetworkAndContracts();
  try {
    const result = await recallContract.evaluateTransaction(fnName, ...args);
    return result.toString();
  } finally {
    gateway.close();
  }
}

module.exports = {
  getNetworkAndContracts,
  evaluateDomain,
  evaluateRecall
};
