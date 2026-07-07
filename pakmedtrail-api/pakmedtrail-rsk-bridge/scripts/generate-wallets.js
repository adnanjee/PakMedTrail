require("dotenv").config();
const { ethers } = require("ethers");
const fs = require("fs");

// Makes one RSK wallet per org MSP and writes them to rsk-wallets.json.
// Fund each address with testnet RBTC for gas, then run deploy:token to hand
// out PST settlement credits.
const msps = (process.env.MSPS || "SupplierMSP,ManufacturerMSP,DistributorMSP,RetailerMSP,DrapMSP")
  .split(",").map((s) => s.trim()).filter(Boolean);

const out = {};
for (const m of msps) {
  const w = ethers.Wallet.createRandom();
  out[m] = { address: w.address, privateKey: w.privateKey };
}

const file = process.env.RSK_WALLETS_FILE || "./rsk-wallets.json";
fs.writeFileSync(file, JSON.stringify(out, null, 2));
console.log("wrote", file);
for (const [m, w] of Object.entries(out)) console.log(`  ${m}: ${w.address}`);
console.log("\nFund each address with testnet RBTC: https://faucet.rootstock.io/");
console.log("KEEP rsk-wallets.json SECRET. It holds private keys. Do not commit it.");
