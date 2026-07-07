const express = require("express");

// Companion HTTP API so payments are reachable from Postman, which the rest of
// your gateway did not expose. In production, replace `callerMsp` with your
// real JWT middleware so the payer org comes from the verified token, not a header.
function createServer({ ledger, bridge, logger }) {
  const app = express();
  app.use(express.json());
  const log = logger || console;

  function callerMsp(req) {
    return req.header("x-org-msp") || (req.body && req.body.fromMSP);
  }

  app.get("/health", (req, res) => res.json({ status: "ok", service: "rsk-bridge" }));

  // Create a PENDING intent on chaincode. The event listener settles it on RSK.
  app.post("/api/payments", async (req, res) => {
    try {
      const b = req.body || {};
      const p = await ledger.createPaymentIntent(callerMsp(req), {
        paymentId: b.paymentId, refType: b.refType, refId: b.refId, toMSP: b.toMSP,
        amount: b.amount, currency: b.currency || "", tokenSymbol: b.tokenSymbol || "",
        tokenContract: b.tokenContract || "", tokenDecimals: b.tokenDecimals || "",
        rskNetwork: b.rskNetwork || "testnet", rskAddressTo: b.rskAddressTo,
        metadataJSON: b.metadataJSON || "",
      });
      res.status(201).json(p);
    } catch (e) { res.status(400).json({ error: e.message }); }
  });

  app.get("/api/payments/:id", async (req, res) => {
    try { res.json(await ledger.readPayment(req.params.id)); }
    catch (e) { res.status(404).json({ error: e.message }); }
  });

  app.get("/api/payments", async (req, res) => {
    try { res.json(await ledger.getPaymentsByStatus(req.query.status || "PENDING")); }
    catch (e) { res.status(400).json({ error: e.message }); }
  });

  // Force settle now instead of waiting on the event (handy for testing).
  app.post("/api/payments/:id/settle", async (req, res) => {
    try {
      const p = await ledger.readPayment(req.params.id);
      res.json(await bridge.processPayment(p));
    } catch (e) { res.status(400).json({ error: e.message }); }
  });

  app.post("/api/payments/:id/cancel", async (req, res) => {
    try {
      const reason = (req.body && req.body.reason) || "cancelled";
      res.json(await ledger.cancelPaymentIntent(callerMsp(req), req.params.id, reason));
    } catch (e) { res.status(400).json({ error: e.message }); }
  });

  return app;
}

module.exports = { createServer };
