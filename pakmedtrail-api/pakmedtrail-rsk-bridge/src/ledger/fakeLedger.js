// In-memory stand-in for payment-intent-go. Mirrors its state machine and
// access control so local tests prove the bridge logic without a Fabric network.
const TRANSITIONS = {
  PENDING: ["SENT", "CANCELLED"],
  SENT: ["CONFIRMED", "FAILED"],
  FAILED: ["SENT", "CANCELLED"],
  CONFIRMED: [],
  CANCELLED: [],
};
function canMove(from, to) {
  return (TRANSITIONS[from] || []).includes(to);
}

class FakeLedger {
  constructor({ logger } = {}) {
    this.store = new Map();
    this.handlers = [];
    this.log = logger || console;
  }

  _assertParty(caller, p) {
    if (caller !== p.fromMSP && caller !== p.toMSP) {
      throw new Error(`access denied: ${caller} is not a party to ${p.paymentId}`);
    }
  }

  _get(id) {
    const p = this.store.get("PAY_" + id);
    if (!p) throw new Error(`payment ${id} not found`);
    return p;
  }

  async createPaymentIntent(caller, f) {
    const id = String(f.paymentId || "").trim();
    if (!id) throw new Error("paymentId required");
    if (this.store.has("PAY_" + id)) throw new Error(`payment ${id} already exists`);
    if (!f.refType || !f.refId) throw new Error("refType and refId required");
    if (!f.toMSP) throw new Error("toMSP required");
    if (!(parseFloat(f.amount) > 0)) throw new Error(`invalid amount ${f.amount}`);
    if (!f.rskAddressTo) throw new Error("rskAddressTo required");

    const now = new Date().toISOString();
    const p = {
      docType: "pay.intent", paymentId: id,
      refType: String(f.refType).toUpperCase(), refId: f.refId,
      fromMSP: caller, toMSP: f.toMSP, amount: String(f.amount),
      currency: (f.currency || "").toUpperCase(), tokenSymbol: (f.tokenSymbol || "").toUpperCase(),
      tokenContract: f.tokenContract || "", tokenDecimals: f.tokenDecimals || "",
      rskNetwork: f.rskNetwork || "", rskAddressFrom: "", rskAddressTo: f.rskAddressTo,
      rskTxHash: "", status: "PENDING", lastError: "",
      metadata: f.metadataJSON ? JSON.parse(f.metadataJSON) : {},
      createdAt: now, updatedAt: now,
    };
    this.store.set("PAY_" + id, p);
    setImmediate(() => this.handlers.forEach((h) => h({ ...p })));
    return { ...p };
  }

  async markPaymentSent(caller, id, txHash, addrFrom) {
    const p = this._get(id);
    this._assertParty(caller, p);
    if (!canMove(p.status, "SENT")) throw new Error(`invalid transition ${p.status} -> SENT`);
    if (!txHash) throw new Error("rskTxHash required");
    p.rskTxHash = txHash;
    if (addrFrom) p.rskAddressFrom = addrFrom;
    p.status = "SENT";
    p.lastError = "";
    p.updatedAt = new Date().toISOString();
    return { ...p };
  }

  async markPaymentConfirmed(caller, id) {
    const p = this._get(id);
    this._assertParty(caller, p);
    if (!canMove(p.status, "CONFIRMED")) throw new Error(`invalid transition ${p.status} -> CONFIRMED`);
    p.status = "CONFIRMED";
    p.lastError = "";
    p.updatedAt = new Date().toISOString();
    return { ...p };
  }

  async markPaymentFailed(caller, id, reason) {
    const p = this._get(id);
    this._assertParty(caller, p);
    if (!canMove(p.status, "FAILED")) throw new Error(`invalid transition ${p.status} -> FAILED`);
    p.status = "FAILED";
    p.lastError = reason || "";
    p.updatedAt = new Date().toISOString();
    return { ...p };
  }

  async cancelPaymentIntent(caller, id, reason) {
    const p = this._get(id);
    if (caller !== p.fromMSP) throw new Error("only the payer can cancel");
    if (p.status !== "PENDING" && p.status !== "FAILED") throw new Error(`cannot cancel in status ${p.status}`);
    p.status = "CANCELLED";
    p.lastError = reason || "";
    p.updatedAt = new Date().toISOString();
    return { ...p };
  }

  async readPayment(id) { return { ...this._get(id) }; }
  async getPaymentsByStatus(status) {
    return [...this.store.values()].filter((p) => p.status === status).map((p) => ({ ...p }));
  }
  async onPaymentCreated(handler) { this.handlers.push(handler); }
}

module.exports = { FakeLedger };
