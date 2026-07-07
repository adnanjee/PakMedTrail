require("dotenv").config();
const { loadConfig } = require("./config");
const { logger } = require("./logger");
const { RskClient } = require("./rskClient");
const { FabricLedger } = require("./ledger/fabricLedger");
const { Bridge } = require("./bridge");
const { createServer } = require("./server");

async function main() {
  const cfg = loadConfig();

  const rsk = new RskClient({
    rpcUrl: cfg.rsk.rpcUrl,
    chainId: cfg.rsk.chainId,
    confirmations: cfg.rsk.confirmations,
    minGasPriceGwei: cfg.rsk.minGasPriceGwei,
    logger,
  });

  const ledger = new FabricLedger({ ...cfg.fabric, logger });
  await ledger.init();

  const bridge = new Bridge({ rsk, ledger, walletMap: cfg.walletMap, logger });
  await bridge.start();

  const app = createServer({ ledger, bridge, logger });
  app.listen(cfg.server.port, () => logger.info(`rsk-bridge api on :${cfg.server.port}`));

  const shutdown = async () => { await ledger.close(); process.exit(0); };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((e) => { logger.error(e.stack || e.message); process.exit(1); });
