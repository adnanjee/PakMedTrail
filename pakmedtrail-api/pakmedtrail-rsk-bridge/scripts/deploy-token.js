require("dotenv").config();
const { ethers } = require("ethers");
const fs = require("fs");
const { compileToken } = require("../src/compile");

// Deploys the PST token to RSK and distributes an initial amount to each org
// wallet, mirroring the PST distribution table in the report.
async function main() {
  const rpc = process.env.RSK_RPC_URL || "https://public-node.testnet.rsk.co";
  const chainId = parseInt(process.env.RSK_CHAIN_ID || "31", 10);
  const provider = new ethers.JsonRpcProvider(rpc, { chainId, name: "rsk" });

  const deployerPk = process.env.RSK_DEPLOYER_PK;
  if (!deployerPk) throw new Error("set RSK_DEPLOYER_PK to a funded testnet key");
  const deployer = new ethers.Wallet(deployerPk, provider);

  const { abi, bytecode } = compileToken();
  const factory = new ethers.ContractFactory(abi, bytecode, deployer);
  const initial = ethers.parseUnits(process.env.PST_INITIAL_SUPPLY || "1000000", 18);

  console.log("deploying PST from", deployer.address, "...");
  const token = await factory.deploy("Pharma Settlement Token", "PST", initial);
  await token.waitForDeployment();
  const addr = await token.getAddress();
  console.log("PST deployed at", addr);

  const wallets = JSON.parse(fs.readFileSync(process.env.RSK_WALLETS_FILE || "./rsk-wallets.json", "utf8"));
  const perOrg = process.env.PST_PER_ORG || "100000";
  const per = ethers.parseUnits(perOrg, 18);
  for (const [msp, w] of Object.entries(wallets)) {
    const tx = await token.transfer(w.address, per);
    await tx.wait();
    console.log(`  funded ${msp} (${w.address}) with ${perOrg} PST`);
  }

  console.log(`\nset PST_TOKEN_ADDRESS=${addr} in .env`);
  console.log("use that address as tokenContract when you create payment intents");
}

main().catch((e) => { console.error(e.message); process.exit(1); });
