const fs = require("fs");
const path = require("path");
const solc = require("solc");

// Compiles the PST token with solcjs so deploy and tests need no external
// compiler download. Returns { abi, bytecode }.
function compileToken() {
  const file = path.join(__dirname, "..", "contracts", "PharmaSettlementToken.sol");
  const source = fs.readFileSync(file, "utf8");

  const input = {
    language: "Solidity",
    sources: { "PharmaSettlementToken.sol": { content: source } },
    settings: {
      // RSK does not support the PUSH0 opcode that newer solc targets emit, so
      // pin the London EVM. This also keeps the bytecode runnable on the local
      // EVM used by the test.
      evmVersion: "london",
      optimizer: { enabled: true, runs: 200 },
      outputSelection: { "*": { "*": ["abi", "evm.bytecode.object"] } },
    },
  };

  const out = JSON.parse(solc.compile(JSON.stringify(input)));
  if (out.errors) {
    const fatal = out.errors.filter((e) => e.severity === "error");
    if (fatal.length) throw new Error(fatal.map((e) => e.formattedMessage).join("\n"));
  }

  const c = out.contracts["PharmaSettlementToken.sol"]["PharmaSettlementToken"];
  return { abi: c.abi, bytecode: "0x" + c.evm.bytecode.object };
}

module.exports = { compileToken };
