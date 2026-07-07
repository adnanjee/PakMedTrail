const fs = require("fs");

// Loads runtime config from env plus two JSON files you keep out of git:
//   rsk-wallets.json   MSP -> { address, privateKey }   (made by scripts/generate-wallets.js)
//   fabric-orgs.json   MSP -> peer connection details    (copy fabric-orgs.example.json)
function loadConfig() {
  const walletFile = process.env.RSK_WALLETS_FILE || "./rsk-wallets.json";
  let walletMap = {};
  if (fs.existsSync(walletFile)) {
    walletMap = JSON.parse(fs.readFileSync(walletFile, "utf8"));
  }

  const orgsFile = process.env.FABRIC_ORGS_FILE || "./fabric-orgs.json";
  let orgs = {};
  if (fs.existsSync(orgsFile)) {
    orgs = JSON.parse(fs.readFileSync(orgsFile, "utf8"));
  }

  return {
    rsk: {
      rpcUrl: process.env.RSK_RPC_URL || "https://public-node.testnet.rsk.co",
      chainId: parseInt(process.env.RSK_CHAIN_ID || "31", 10),
      confirmations: parseInt(process.env.RSK_CONFIRMATIONS || "2", 10),
      minGasPriceGwei: process.env.RSK_MIN_GAS_PRICE_GWEI || "0.06",
    },
    fabric: {
      channel: process.env.FABRIC_CHANNEL || "rawmaterialsupply",
      chaincode: process.env.FABRIC_PAYMENT_CC || "payment-intent",
      listenerMsp: process.env.FABRIC_LISTENER_MSP || Object.keys(orgs)[0],
      orgs,
    },
    walletMap,
    server: { port: parseInt(process.env.PORT || "3100", 10) },
  };
}

module.exports = { loadConfig };
