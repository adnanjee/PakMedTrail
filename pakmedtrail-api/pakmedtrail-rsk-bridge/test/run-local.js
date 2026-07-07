// Local proof: real PST token + real ethers client + real local EVM (ganache),
// with the Fabric side stubbed by FakeLedger (which copies the chaincode state
// machine and access control). This proves the bridge and RSK logic end to end
// without touching RSK testnet or a Fabric network.
const ganache = require("ganache");
const { ethers } = require("ethers");
const { compileToken } = require("../src/compile");
const { RskClient } = require("../src/rskClient");
const { FakeLedger } = require("../src/ledger/fakeLedger");
const { Bridge } = require("../src/bridge");

const logger = {
  info: (m) => console.log("    bridge:", m),
  warn: (m) => console.log("    warn:", m),
  error: (m) => console.log("    err:", m),
};

let failures = 0;
function check(name, cond) {
  console.log((cond ? "PASS  " : "FAIL  ") + name);
  if (!cond) failures++;
}

async function main() {
  const PORT = 8545;
  const server = ganache.server({
    chain: { chainId: 31 },
    wallet: { deterministic: true, totalAccounts: 5 },
    logging: { quiet: true },
  });
  await server.listen(PORT);

  try {
    const url = `http://127.0.0.1:${PORT}`;
    const provider = new ethers.JsonRpcProvider(url, { chainId: 31, name: "rsk-local" });

    const initial = server.provider.getInitialAccounts();
    const accts = Object.entries(initial).map(([address, info]) => ({
      address: ethers.getAddress(address),
      pk: String(info.secretKey).startsWith("0x") ? info.secretKey : "0x" + info.secretKey,
    }));

    const deployer = new ethers.Wallet(accts[0].pk, provider);
    const supplier = { privateKey: accts[1].pk, address: accts[1].address };
    const manufacturer = { privateKey: accts[2].pk, address: accts[2].address };

    // deploy the real PST token, then move some to the payer (supplier)
    const { abi, bytecode } = compileToken();
    const factory = new ethers.ContractFactory(abi, bytecode, deployer);
    const token = await factory.deploy("Pharma Settlement Token", "PST", ethers.parseUnits("1000000", 18));
    await token.waitForDeployment();
    const tokenAddr = await token.getAddress();
    await (await token.transfer(supplier.address, ethers.parseUnits("1000", 18))).wait();
    console.log("    setup: PST at", tokenAddr, "| supplier funded with 1000 PST\n");

    const rsk = new RskClient({ rpcUrl: url, chainId: 31, confirmations: 2, minGasPriceGwei: "0.06", logger });
    const ledger = new FakeLedger({ logger });
    const walletMap = { SupplierMSP: supplier, ManufacturerMSP: manufacturer };
    const bridge = new Bridge({ rsk, ledger, walletMap, logger });

    const before = await rsk.balanceOfToken(tokenAddr, manufacturer.address);

    // Supplier pays Manufacturer 100 PST against a raw lot
    const created = await ledger.createPaymentIntent("SupplierMSP", {
      paymentId: "PAY_TEST_001", refType: "RAW_LOT", refId: "LOT_TEST_001", toMSP: "ManufacturerMSP",
      amount: "100", currency: "PST", tokenSymbol: "PST", tokenContract: tokenAddr, tokenDecimals: "18",
      rskNetwork: "local", rskAddressTo: manufacturer.address,
      metadataJSON: JSON.stringify({ note: "raw lot settlement" }),
    });
    check("intent starts PENDING", created.status === "PENDING");

    // run the bridge; ganache instamines, so mine one extra block to satisfy 2 confirmations
    const settledP = bridge.processPayment(created);
    await new Promise((r) => setTimeout(r, 300));
    await provider.send("evm_mine", []);
    const settled = await settledP;

    check("intent ends CONFIRMED", settled.status === "CONFIRMED");
    check("rskTxHash recorded", typeof settled.rskTxHash === "string" && settled.rskTxHash.startsWith("0x"));
    check("payer RSK address recorded", settled.rskAddressFrom === supplier.address);

    const after = await rsk.balanceOfToken(tokenAddr, manufacturer.address);
    check("payee received exactly 100 PST", after - before === ethers.parseUnits("100", 18));

    // guard: a fresh PENDING cannot jump straight to CONFIRMED (mirrors chaincode)
    let guard = false;
    try {
      await ledger.createPaymentIntent("SupplierMSP", {
        paymentId: "PAY_BAD", refType: "RAW_LOT", refId: "L2", toMSP: "ManufacturerMSP",
        amount: "1", rskAddressTo: manufacturer.address,
      });
      await ledger.markPaymentConfirmed("SupplierMSP", "PAY_BAD");
    } catch (e) { guard = true; }
    check("illegal PENDING -> CONFIRMED rejected", guard);

    // access control: a non party cannot mark sent
    let denied = false;
    try { await ledger.markPaymentSent("DistributorMSP", "PAY_BAD", "0xabc", "0xfrom"); }
    catch (e) { denied = true; }
    check("non party mark rejected", denied);

    console.log("\n    lifecycle the bridge drove: CreatePaymentIntent -> MarkPaymentSent -> MarkPaymentConfirmed");
  } finally {
    await server.close();
  }

  console.log(failures === 0 ? "\nALL CHECKS PASSED" : `\n${failures} CHECK(S) FAILED`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
