// Shipments has three flows that share most of their shape, so the two transfer
// flows (distribution and retail) run through one config driven renderer.
//   distribution: manufacturer ships to distributor
//   retail:       distributor ships to retailer
//   dispense:     retailer gives medicine to a patient (no list endpoint)
// Detail and the accept/deliver actions open in a modal to keep this page on a
// single level.

import { icons } from "../icons.js";
import { api } from "../api.js";
import { isRole, orgMSP } from "../store.js";
import { escape as esc, pick, emptyBlock, readForm, modal, onClick, MSP_OPTIONS } from "../ui.js";
import {
  arr,
  one,
  parseJsonObject,
  runAction,
  detailHeader,
  ledgerBlock,
  badge,
  idOf,
  statusOf,
  mspLabel,
  toast,
  positiveNumber,
} from "./_shared.js";

const TYPES = {
  distribution: {
    label: "Distribution",
    creatorRole: "manufacturer",
    handlerRole: "distributor",
    toMsp: "distributorMSP",
    list: (p) => api.getDistributionShipments(p),
    get: (id) => api.getDistributionShipment(id),
    create: (payload) => api.createDistributionShipment(payload),
    accept: (id) => api.acceptDistributionShipment(id),
    deliver: (id) => api.deliverDistributionShipment(id),
  },
  retail: {
    label: "Retail",
    creatorRole: "distributor",
    handlerRole: "retailer",
    toMsp: "retailerMSP",
    list: (p) => api.getRetailShipments(p),
    get: (id) => api.getRetailShipment(id),
    create: (payload) => api.createRetailShipment(payload),
    accept: (id) => api.acceptRetailShipment(id),
    deliver: (id) => api.deliverRetailShipment(id),
  },
};

let tab = "distribution";
let party = null;

export default async function shipmentsPage(root) {
  if (!party) party = orgMSP() || "distributorMSP";
  if (!TYPES[tab] && tab !== "dispense") tab = "distribution";

  root.innerHTML = `
    <div class="content">
      <div class="tabs">
        <button class="tab ${tab === "distribution" ? "active" : ""}" data-tab="distribution">Distribution</button>
        <button class="tab ${tab === "retail" ? "active" : ""}" data-tab="retail">Retail</button>
        <button class="tab ${tab === "dispense" ? "active" : ""}" data-tab="dispense">Dispense</button>
      </div>
      <div id="ship-body" style="margin-top:18px"></div>
    </div>`;

  root.querySelectorAll("[data-tab]").forEach((b) =>
    b.addEventListener("click", () => {
      tab = b.getAttribute("data-tab");
      shipmentsPage(root);
    })
  );

  const body = root.querySelector("#ship-body");
  if (tab === "dispense") renderDispense(body);
  else renderFlow(body, tab);
}

// ---------- distribution + retail ----------

async function renderFlow(body, type) {
  const cfg = TYPES[type];
  const canCreate = isRole(cfg.creatorRole);
  const partyOpts = MSP_OPTIONS.map(
    (m) => `<option value="${m.value}" ${m.value === party ? "selected" : ""}>${esc(m.label)}</option>`
  ).join("");

  body.innerHTML = `
    <div class="row between">
      <div class="row" style="gap:10px">
        <span class="muted" style="font-size:.82rem">Shipments involving</span>
        <select class="select" id="party-filter" style="width:auto">${partyOpts}</select>
      </div>
      ${canCreate ? `<button class="btn btn-primary btn-sm" data-action="new-ship">${icons.truck(16)} New ${esc(cfg.label.toLowerCase())} shipment</button>` : ""}
    </div>
    <div class="card" style="margin-top:14px">
      <div class="card-head"><div class="card-title">${icons.truck(16)} ${esc(cfg.label)} shipments</div>
        <button class="btn btn-ghost btn-sm" data-action="reload">${icons.refresh(15)} Refresh</button>
      </div>
      <div class="card-body" id="flow-body">
        <div class="loading"><span class="spin"></span><span>Loading…</span></div>
      </div>
    </div>`;

  body.querySelector("#party-filter").addEventListener("change", (e) => {
    party = e.target.value;
    renderFlow(body, type);
  });
  body.querySelector('[data-action="reload"]').addEventListener("click", () => renderFlow(body, type));
  if (canCreate) {
    body.querySelector('[data-action="new-ship"]').addEventListener("click", () => openCreate(body, type));
  }

  const inner = body.querySelector("#flow-body");
  try {
    const list = arr(await cfg.list(party), "shipments");
    if (!list.length) {
      inner.innerHTML = emptyBlock(icons.truck(22), "No shipments", `Nothing for ${mspLabel(party)} in this flow yet.`);
      return;
    }
    inner.innerHTML = flowTable(list);
    inner.querySelectorAll("[data-ship]").forEach((tr) =>
      tr.addEventListener("click", () => openDetail(body, type, tr.getAttribute("data-ship")))
    );
  } catch (err) {
    inner.innerHTML = `<p class="muted" style="margin:0">Could not load shipments. ${esc(err.message)}</p>`;
  }
}

function flowTable(list) {
  const rows = list
    .map((s) => {
      const id = idOf(s, "");
      const batch = pick(s, ["batchId", "BatchID", "batch"], "");
      const from = pick(s, ["fromMSP", "FromMSP", "from", "ownerMSP"], "");
      const to = pick(s, ["toMSP", "ToMSP", "to"], "");
      const qty = pick(s, ["quantity", "Quantity"], "");
      return `
        <tr data-ship="${esc(id)}" style="cursor:pointer">
          <td><b>${esc(id || "—")}</b></td>
          <td>${esc(batch || "—")}</td>
          <td>${esc(mspLabel(from) || "—")} → ${esc(mspLabel(to) || "—")}</td>
          <td>${qty !== "" ? esc(qty) : "—"}</td>
          <td>${badge(statusOf(s) || "n/a")}</td>
        </tr>`;
    })
    .join("");
  return `
    <div class="table-wrap">
      <table class="data">
        <thead><tr><th>Shipment ID</th><th>Batch</th><th>Route</th><th>Qty</th><th>Status</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

function openCreate(body, type) {
  const cfg = TYPES[type];
  modal({
    title: `New ${cfg.label.toLowerCase()} shipment`,
    body: `
      <div class="form-grid">
        <div class="field"><label>Shipment ID *</label><input class="input" name="shipmentId" placeholder="SHIP-001" /></div>
        <div class="field"><label>Batch ID *</label><input class="input" name="batchId" placeholder="BATCH-001" /></div>
        <div class="field"><label>To *</label><select class="select" name="toMSP"><option value="${esc(cfg.toMsp)}">${esc(mspLabel(cfg.toMsp))} (${esc(cfg.toMsp)})</option></select></div>
        <div class="field"><label>Quantity *</label><input class="input" name="quantity" type="number" placeholder="1000" /></div>
        <div class="field span-2"><label>Metadata (JSON)</label><textarea class="textarea" name="metadata" placeholder='{"truck":"ABC-123"}'></textarea><span class="hint">Optional. Must be a JSON object.</span></div>
      </div>`,
    footer: `<button class="btn btn-primary" data-action="save-ship">Create shipment</button>`,
    onMount: (el, close) => {
      onClick(el, "save-ship", (btn) => {
        const form = readForm(el);
        if (!form.shipmentId || !form.batchId || !form.toMSP || !form.quantity) {
          toast("Shipment ID, batch ID, destination, and quantity are required", "err");
          return;
        }
        let payload;
        try {
          payload = {
            shipmentId: form.shipmentId,
            batchId: form.batchId,
            toMSP: form.toMSP,
            quantity: positiveNumber(form.quantity, "Quantity"),
            metadata: parseJsonObject(form.metadata, "Metadata"),
          };
        } catch (e) {
          toast(e.message, "err");
          return;
        }
        runAction(btn, "Creating", () => cfg.create(payload), () => {
          close();
          renderFlow(body, type);
        });
      });
    },
  });
}

async function openDetail(body, type, id) {
  const cfg = TYPES[type];
  const m = modal({
    title: `${cfg.label} shipment`,
    body: `<div class="loading"><span class="spin"></span><span>Loading…</span></div>`,
  });
  const target = m.el.querySelector(".modal-body");

  let ship;
  try {
    ship = one(await cfg.get(id), "shipment");
  } catch (err) {
    target.innerHTML = `<p class="muted" style="margin:0">Could not load shipment. ${esc(err.message)}</p>`;
    return;
  }

  const canHandle = isRole(cfg.handlerRole);
  target.innerHTML = `
    ${detailHeader("truck", idOf(ship, id), ship)}
    ${ledgerBlock(ship)}
    ${canHandle ? `<div class="row" style="gap:10px;margin-top:16px">
      <button class="btn btn-primary btn-sm" data-action="accept">${icons.check(15)} Accept</button>
      <button class="btn btn-outline btn-sm" data-action="deliver">${icons.box(15)} Mark delivered</button>
    </div>` : ""}`;

  if (canHandle) {
    const refresh = () => {
      m.close();
      renderFlow(body, type);
    };
    onClick(target, "accept", (btn) => runAction(btn, "Accepting", () => cfg.accept(id), refresh));
    onClick(target, "deliver", (btn) => runAction(btn, "Delivering", () => cfg.deliver(id), refresh));
  }
}

// ---------- dispense ----------

function renderDispense(body) {
  const canDispense = isRole("retailer");
  body.innerHTML = `
    <div class="grid-2">
      <div class="card">
        <div class="card-head"><div class="card-title">${icons.search(16)} Look up a dispense</div></div>
        <div class="card-body">
          <div class="field"><label>Dispense ID</label>
            <div class="row" style="gap:8px">
              <input class="input" id="dispense-id" placeholder="DISP-001" style="flex:1" />
              <button class="btn btn-primary btn-sm" data-action="find">${icons.search(15)} Find</button>
            </div>
          </div>
          <div id="dispense-result" style="margin-top:12px"></div>
        </div>
      </div>
      <div class="card">
        <div class="card-head"><div class="card-title">${icons.store(16)} Record a dispense</div></div>
        <div class="card-body">
          ${canDispense ? dispenseForm() : `<p class="muted" style="margin:0">Only a retailer can record a dispense to a patient.</p>`}
        </div>
      </div>
    </div>`;

  const idInput = body.querySelector("#dispense-id");
  const result = body.querySelector("#dispense-result");
  const find = () => lookupDispense(idInput.value.trim(), result);
  body.querySelector('[data-action="find"]').addEventListener("click", find);
  idInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") find();
  });

  if (canDispense) {
    body.querySelector('[data-action="save-dispense"]').addEventListener("click", (e) => {
      const btn = e.currentTarget;
      const form = readForm(body.querySelector("#dispense-form"));
      if (!form.dispenseId || !form.batchId || !form.quantity) {
        toast("Dispense ID, batch ID, and quantity are required", "err");
        return;
      }
      let payload;
      try {
        payload = {
          dispenseId: form.dispenseId,
          batchId: form.batchId,
          quantity: positiveNumber(form.quantity, "Quantity"),
          metadata: parseJsonObject(form.metadata, "Metadata"),
        };
      } catch (e) {
        toast(e.message, "err");
        return;
      }
      runAction(btn, "Recording", () => api.createDispense(payload), () => {
        idInput.value = form.dispenseId;
        lookupDispense(form.dispenseId, result);
        body.querySelector("#dispense-form").reset();
      });
    });
  }
}

function dispenseForm() {
  return `
    <form id="dispense-form">
      <div class="form-grid">
        <div class="field"><label>Dispense ID *</label><input class="input" name="dispenseId" placeholder="DISP-001" /></div>
        <div class="field"><label>Batch ID *</label><input class="input" name="batchId" placeholder="BATCH-001" /></div>
        <div class="field"><label>Quantity *</label><input class="input" name="quantity" type="number" placeholder="2" /></div>
        <div class="field span-2"><label>Metadata (JSON)</label><textarea class="textarea" name="metadata" placeholder='{"patientRef":"anon"}'></textarea><span class="hint">Optional. Must be a JSON object.</span></div>
      </div>
    </form>
    <div class="row" style="margin-top:12px">
      <button class="btn btn-primary btn-sm" data-action="save-dispense">${icons.check(15)} Record dispense</button>
    </div>`;
}

async function lookupDispense(id, result) {
  if (!id) {
    toast("Enter a dispense ID", "err");
    return;
  }
  result.innerHTML = `<div class="loading"><span class="spin"></span><span>Looking up…</span></div>`;
  try {
    const d = one(await api.getDispense(id), "dispense");
    if (!d || typeof d !== "object") {
      result.innerHTML = emptyBlock(icons.search(20), "Not found", `No dispense for ${id}.`);
      return;
    }
    result.innerHTML = `${detailHeader("store", idOf(d, id), d)}${ledgerBlock(d)}`;
  } catch (err) {
    result.innerHTML = `<p class="muted" style="margin:0">${esc(err.message)}</p>`;
  }
}
