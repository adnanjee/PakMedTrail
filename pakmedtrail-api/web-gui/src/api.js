// API client for the PakMedTrail server. One function per endpoint. The token
// is read from the session store and sent as a Bearer header. Errors from the
// server come back as { error, message, details }; we surface the most useful
// text in a thrown Error so the UI can show it in a toast.

import { getApiBase } from "./config.js";
import { getToken } from "./store.js";

class ApiError extends Error {
  constructor(message, status, payload) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.payload = payload;
  }
}

async function request(method, path, body) {
  const headers = {};
  const token = getToken();
  if (token) headers["Authorization"] = `Bearer ${token}`;
  if (body !== undefined) headers["Content-Type"] = "application/json";

  let res;
  try {
    res = await fetch(getApiBase() + path, {
      method,
      headers,
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
  } catch (networkErr) {
    throw new ApiError(
      "Cannot reach the API. Check that the server is running and the API URL is correct.",
      0,
      null
    );
  }

  let data = null;
  const text = await res.text();
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }

  if (!res.ok) {
    const msg =
      (data && (data.message || data.error)) ||
      (typeof data === "string" && data) ||
      `Request failed (${res.status})`;
    // The chaincode sometimes nests the real cause in details.
    const detail =
      data && data.details
        ? typeof data.details === "string"
          ? data.details
          : JSON.stringify(data.details)
        : "";
    const peers = data && Array.isArray(data.unavailablePeers) && data.unavailablePeers.length
      ? ` Unavailable peer(s): ${data.unavailablePeers.join(", ")}.`
      : "";
    const remedies = data && Array.isArray(data.remedies) && data.remedies.length
      ? ` Remedy: ${data.remedies[0]}`
      : "";
    const full = detail && detail !== msg ? `${msg}: ${detail}` : msg;
    throw new ApiError(`${full}${peers}${remedies}`, res.status, data);
  }

  return data;
}

const get = (p) => request("GET", p);
const post = (p, b) => request("POST", p, b ?? {});

export const api = {
  ApiError,

  // ----- health + auth -----
  health: () => get("/health"),
  login: (username, password) => post("/api/auth/login", { username, password }),
  register: (payload) => post("/api/auth/register", payload),
  me: () => get("/api/auth/me"),
  listUsers: () => get("/api/auth/users"),
  ping: () => get("/api/fabric/ping"),
  diagnostics: () => get("/api/fabric/diagnostics"),
  peerStatus: () => get("/api/fabric/peer-status"),

  // ----- lots (supplier raw material) -----
  getLots: () => get("/api/lots"),
  getLot: (lotId) => get(`/api/lots/${encodeURIComponent(lotId)}`),
  createLot: (payload) => post("/api/lots", payload),
  approveLot: (lotId, note) => post(`/api/lots/${encodeURIComponent(lotId)}/drap-approve`, { note }),
  proposeLotTransfer: (lotId, proposedOwnerMSP) =>
    post(`/api/lots/${encodeURIComponent(lotId)}/propose-transfer`, { proposedOwnerMSP }),
  acceptLotTransfer: (lotId) =>
    post(`/api/lots/${encodeURIComponent(lotId)}/accept-transfer`, {}),

  // ----- formulations + batches (manufacturing) -----
  getFormulations: () => get("/api/batches/formulations"),
  getFormulation: (drugCode) => get(`/api/batches/formulations/${encodeURIComponent(drugCode)}`),
  createFormulation: (payload) => post("/api/batches/formulations", payload),

  getBatches: (ownerMSP) =>
    get("/api/batches" + (ownerMSP ? `?owner=${encodeURIComponent(ownerMSP)}` : "")),
  getBatch: (batchId) => get(`/api/batches/${encodeURIComponent(batchId)}`),
  produceBatch: (payload) => post("/api/batches", payload),
  approveBatch: (batchId, note) =>
    post(`/api/batches/${encodeURIComponent(batchId)}/drap-approve`, { note }),
  proposeBatchTransfer: (batchId, proposedOwnerMSP) =>
    post(`/api/batches/${encodeURIComponent(batchId)}/propose-transfer`, { proposedOwnerMSP }),
  acceptBatchTransfer: (batchId) =>
    post(`/api/batches/${encodeURIComponent(batchId)}/accept-transfer`, {}),

  // ----- shipments -----
  getDistributionShipments: (partyMSP) =>
    get("/api/shipments/distribution" + (partyMSP ? `?party=${encodeURIComponent(partyMSP)}` : "")),
  getDistributionShipment: (id) => get(`/api/shipments/distribution/${encodeURIComponent(id)}`),
  createDistributionShipment: (payload) => post("/api/shipments/distribution", payload),
  acceptDistributionShipment: (id) =>
    post(`/api/shipments/distribution/${encodeURIComponent(id)}/accept`, {}),
  deliverDistributionShipment: (id) =>
    post(`/api/shipments/distribution/${encodeURIComponent(id)}/deliver`, {}),

  getRetailShipments: (partyMSP) =>
    get("/api/shipments/retail" + (partyMSP ? `?party=${encodeURIComponent(partyMSP)}` : "")),
  getRetailShipment: (id) => get(`/api/shipments/retail/${encodeURIComponent(id)}`),
  createRetailShipment: (payload) => post("/api/shipments/retail", payload),
  acceptRetailShipment: (id) => post(`/api/shipments/retail/${encodeURIComponent(id)}/accept`, {}),
  deliverRetailShipment: (id) => post(`/api/shipments/retail/${encodeURIComponent(id)}/deliver`, {}),

  createDispense: (payload) => post("/api/shipments/dispense", payload),
  getDispense: (id) => get(`/api/shipments/dispense/${encodeURIComponent(id)}`),

  // ----- recalls -----
  getActiveRecalls: () => get("/api/recalls/active"),
  getRecall: (recallId) => get(`/api/recalls/${encodeURIComponent(recallId)}`),
  initiateRecall: (payload) => post("/api/recalls", payload),
  addAffectedAssets: (recallId, assetType, assetIds) =>
    post(`/api/recalls/${encodeURIComponent(recallId)}/affected-assets`, { assetType, assetIds }),
  acknowledgeRecall: (recallId, note) =>
    post(`/api/recalls/${encodeURIComponent(recallId)}/acknowledge`, { note }),
  quarantineAsset: (recallId, assetType, assetId, reason) =>
    post(`/api/recalls/${encodeURIComponent(recallId)}/quarantine`, { assetType, assetId, reason }),
  closeRecall: (recallId, note) =>
    post(`/api/recalls/${encodeURIComponent(recallId)}/close`, { note }),
  getQuarantine: (assetType, assetId) =>
    get(`/api/recalls/quarantine/${encodeURIComponent(assetType)}/${encodeURIComponent(assetId)}`),
  checkActiveRecall: (assetType, assetId) =>
    get(`/api/recalls/active-check/${encodeURIComponent(assetType)}/${encodeURIComponent(assetId)}`),
  clearQuarantine: (assetType, assetId) =>
    post(
      `/api/recalls/quarantine/${encodeURIComponent(assetType)}/${encodeURIComponent(assetId)}/clear`,
      {}
    ),
};
