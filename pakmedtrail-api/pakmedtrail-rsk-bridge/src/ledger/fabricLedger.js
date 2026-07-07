const fs = require("fs");
const crypto = require("crypto");
const grpc = require("@grpc/grpc-js");
const { connect, hash, signers } = require("@hyperledger/fabric-gateway");

// Real Fabric side of the bridge. Path A: MarkPayment* is submitted as the
// payer org, which satisfies the chaincode access check (caller must be
// FromMSP or ToMSP). So this keeps one connection per org MSP and reads through
// a single listener org. If you later add a dedicated BridgeMSP to the
// chaincode, you can collapse this to one connection.
class FabricLedger {
  constructor({ channel, chaincode, listenerMsp, orgs, logger }) {
    this.channel = channel;
    this.chaincode = chaincode;
    this.listenerMsp = listenerMsp;
    this.orgs = orgs || {};
    this.log = logger || console;
    this.conns = new Map();
    this.handlers = [];
    this._listening = false;
  }

  async init() {
    if (!this.orgs[this.listenerMsp]) {
      throw new Error(`listener MSP ${this.listenerMsp} not found in fabric orgs config`);
    }
    await this._conn(this.listenerMsp);
    this.log.info(`fabric ledger ready (channel ${this.channel}, cc ${this.chaincode})`);
  }

  async _conn(mspId) {
    if (this.conns.has(mspId)) return this.conns.get(mspId);
    const o = this.orgs[mspId];
    if (!o) throw new Error(`no fabric connection config for MSP ${mspId}`);

    const tlsRootCert = fs.readFileSync(o.tlsRootCertPath);
    const credentials = grpc.credentials.createSsl(tlsRootCert);
    const client = new grpc.Client(o.peerEndpoint, credentials, {
      "grpc.ssl_target_name_override": o.peerHostAlias,
    });

    const identity = { mspId, credentials: fs.readFileSync(o.certPath) };
    const privateKey = crypto.createPrivateKey(fs.readFileSync(o.keyPath));
    const signer = signers.newPrivateKeySigner(privateKey);

    const gateway = connect({
      client,
      identity,
      signer,
      hash: hash.sha256,
      evaluateOptions: () => ({ deadline: Date.now() + 10000 }),
      endorseOptions: () => ({ deadline: Date.now() + 20000 }),
      submitOptions: () => ({ deadline: Date.now() + 10000 }),
      commitStatusOptions: () => ({ deadline: Date.now() + 60000 }),
    });

    const network = gateway.getNetwork(this.channel);
    const contract = network.getContract(this.chaincode);
    const conn = { gateway, contract, client, network };
    this.conns.set(mspId, conn);
    return conn;
  }

  _decode(bytes) {
    const s = Buffer.from(bytes).toString("utf8");
    return s ? JSON.parse(s) : null;
  }

  async createPaymentIntent(caller, f) {
    const { contract } = await this._conn(caller);
    const res = await contract.submitTransaction(
      "CreatePaymentIntent",
      f.paymentId, f.refType, f.refId, f.toMSP, f.amount,
      f.currency || "", f.tokenSymbol || "", f.tokenContract || "",
      f.tokenDecimals || "", f.rskNetwork || "", f.rskAddressTo, f.metadataJSON || ""
    );
    return this._decode(res);
  }

  async markPaymentSent(caller, id, txHash, addrFrom) {
    const { contract } = await this._conn(caller);
    return this._decode(await contract.submitTransaction("MarkPaymentSent", id, txHash, addrFrom || ""));
  }
  async markPaymentConfirmed(caller, id) {
    const { contract } = await this._conn(caller);
    return this._decode(await contract.submitTransaction("MarkPaymentConfirmed", id));
  }
  async markPaymentFailed(caller, id, reason) {
    const { contract } = await this._conn(caller);
    return this._decode(await contract.submitTransaction("MarkPaymentFailed", id, reason || ""));
  }
  async cancelPaymentIntent(caller, id, reason) {
    const { contract } = await this._conn(caller);
    return this._decode(await contract.submitTransaction("CancelPaymentIntent", id, reason || ""));
  }
  async readPayment(id) {
    const { contract } = await this._conn(this.listenerMsp);
    return this._decode(await contract.evaluateTransaction("ReadPaymentIntent", id));
  }
  async getPaymentsByStatus(status) {
    const { contract } = await this._conn(this.listenerMsp);
    return this._decode(await contract.evaluateTransaction("GetPaymentsByStatus", status)) || [];
  }

  // Listen for PaymentCreated and call handler(intent). Reconnects on drop.
  async onPaymentCreated(handler) {
    this.handlers.push(handler);
    if (this._listening) return;
    this._listening = true;
    const { network } = await this._conn(this.listenerMsp);

    (async () => {
      while (this._listening) {
        let events;
        try {
          events = await network.getChaincodeEvents(this.chaincode);
          for await (const ev of events) {
            if (ev.eventName !== "PaymentCreated") continue;
            let intent;
            try { intent = JSON.parse(Buffer.from(ev.payload).toString("utf8")); }
            catch (e) { this.log.warn(`bad event payload: ${e.message}`); continue; }
            for (const h of this.handlers) {
              try { await h(intent); }
              catch (e) { this.log.error(`handler error ${intent.paymentId}: ${e.message}`); }
            }
          }
        } catch (e) {
          this.log.warn(`event stream dropped, retry in 5s: ${e.message}`);
          await new Promise((r) => setTimeout(r, 5000));
        } finally {
          if (events) try { events.close(); } catch (e) {}
        }
      }
    })();
  }

  async close() {
    this._listening = false;
    for (const { gateway, client } of this.conns.values()) {
      try { gateway.close(); } catch (e) {}
      try { client.close(); } catch (e) {}
    }
  }
}

module.exports = { FabricLedger };
