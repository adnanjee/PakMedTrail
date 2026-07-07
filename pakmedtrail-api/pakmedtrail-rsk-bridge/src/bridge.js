// The bridge turns a PENDING payment intent into a settled RSK transfer, then
// writes the outcome back to Fabric. It depends only on a `ledger` interface,
// so the same code runs against the real Fabric network or the in-memory fake
// used by the local test.
class Bridge {
  constructor({ rsk, ledger, walletMap, logger }) {
    this.rsk = rsk;
    this.ledger = ledger;
    this.walletMap = walletMap;
    this.log = logger || console;
  }

  payerWallet(mspId) {
    const w = this.walletMap[mspId];
    if (!w) throw new Error(`no RSK wallet mapped for MSP ${mspId}`);
    return w;
  }

  async processPayment(p) {
    if (p.status !== "PENDING") {
      this.log.info(`skip ${p.paymentId}, status ${p.status}`);
      return p;
    }

    const payer = this.payerWallet(p.fromMSP);

    let txHash;
    try {
      if (p.tokenContract && String(p.tokenContract).trim() !== "") {
        txHash = await this.rsk.sendToken({
          privateKey: payer.privateKey,
          tokenContract: p.tokenContract,
          to: p.rskAddressTo,
          amount: p.amount,
          decimals: p.tokenDecimals,
        });
      } else {
        txHash = await this.rsk.sendNative({
          privateKey: payer.privateKey,
          to: p.rskAddressTo,
          amount: p.amount,
        });
      }
    } catch (err) {
      // submit failed before broadcast: record it and stop.
      await this.ledger.markPaymentFailed(p.fromMSP, p.paymentId, `send failed: ${err.message}`);
      throw err;
    }

    await this.ledger.markPaymentSent(p.fromMSP, p.paymentId, txHash, payer.address);
    this.log.info(`sent ${p.paymentId} as ${p.fromMSP} tx ${txHash}`);

    try {
      const { confirmations } = await this.rsk.waitForConfirmations(txHash);
      await this.ledger.markPaymentConfirmed(p.fromMSP, p.paymentId);
      this.log.info(`confirmed ${p.paymentId} after ${confirmations} block(s)`);
    } catch (err) {
      await this.ledger.markPaymentFailed(p.fromMSP, p.paymentId, `confirm failed: ${err.message}`);
      throw err;
    }

    return this.ledger.readPayment(p.paymentId);
  }

  // On restart, finish anything left mid flight. PENDING gets sent, SENT gets a
  // confirmation re-check. This makes the bridge safe to crash and restart.
  async sweepPending() {
    const pending = await this.ledger.getPaymentsByStatus("PENDING");
    for (const p of pending) {
      try { await this.processPayment(p); }
      catch (e) { this.log.error(`sweep PENDING ${p.paymentId}: ${e.message}`); }
    }

    const sent = await this.ledger.getPaymentsByStatus("SENT");
    for (const p of sent) {
      if (!p.rskTxHash) continue;
      try {
        const { confirmations } = await this.rsk.waitForConfirmations(p.rskTxHash, { timeoutMs: 8000 });
        await this.ledger.markPaymentConfirmed(p.fromMSP, p.paymentId);
        this.log.info(`recovered and confirmed ${p.paymentId} (${confirmations})`);
      } catch (e) {
        this.log.warn(`still awaiting confirm ${p.paymentId}: ${e.message}`);
      }
    }
  }

  async start() {
    await this.sweepPending();
    await this.ledger.onPaymentCreated(async (p) => {
      try { await this.processPayment(p); }
      catch (e) { this.log.error(`event ${p.paymentId}: ${e.message}`); }
    });
    this.log.info("bridge is listening for PaymentCreated events");
  }
}

module.exports = { Bridge };
