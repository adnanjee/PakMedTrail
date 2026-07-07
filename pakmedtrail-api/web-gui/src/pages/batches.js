// Batches are finished drug lots a manufacturer produces from raw material.
// The list is filtered by owner (defaulting to your own org, which is what the
// server does too). Manufacturers can produce a batch and hand it down the
// chain; distributors accept; DRAP approves.

import { icons } from "../icons.js";
import { api } from "../api.js";
import { isRole, orgMSP } from "../store.js";
import { escape as esc, pick, emptyBlock, readForm, modal, onClick, MSP_OPTIONS } from "../ui.js";
import {
  arr,
  one,
  wireGo,
  backLink,
  parseJsonObject,
  parseJsonArray,
  runAction,
  detailHeader,
  factGrid,
  ledgerBlock,
  transferModal,
  badge,
  idOf,
  ownerOf,
  statusOf,
  mspLabel,
  toast,
  positiveNumber,
} from "./_shared.js";

let ownerFilter = null;

export default async function batchesPage(root, params) {
  const id = params && params[0];
  if (id) return renderDetail(root, id);
  if (!ownerFilter) ownerFilter = orgMSP() || "manufacturerMSP";
  return renderList(root);
}

async function renderList(root) {
  const canProduce = isRole("manufacturer");
  const ownerOpts = MSP_OPTIONS.map(
    (m) => `<option value="${m.value}" ${m.value === ownerFilter ? "selected" : ""}>${esc(m.label)}</option>`
  ).join("");

  root.innerHTML = `
    <div class="content">
      <div class="row between">
        <div class="row" style="gap:10px">
          <span class="muted" style="font-size:.82rem">Showing batches owned by</span>
          <select class="select" id="owner-filter" style="width:auto">${ownerOpts}</select>
        </div>
        ${canProduce ? `<button class="btn btn-primary btn-sm" data-action="new-batch">${icons.factory(16)} Produce batch</button>` : ""}
      </div>
      <div class="card">
        <div class="card-head"><div class="card-title">${icons.factory(16)} Batches</div>
          <button class="btn btn-ghost btn-sm" data-action="reload">${icons.refresh(15)} Refresh</button>
        </div>
        <div class="card-body" id="batches-body">
          <div class="loading"><span class="spin"></span><span>Loading batches…</span></div>
        </div>
      </div>
    </div>`;

  root.querySelector("#owner-filter").addEventListener("change", (e) => {
    ownerFilter = e.target.value;
    renderList(root);
  });
  root.querySelector('[data-action="reload"]').addEventListener("click", () => renderList(root));
  if (canProduce) {
    root.querySelector('[data-action="new-batch"]').addEventListener("click", () => openProduce(root));
  }

  const body = root.querySelector("#batches-body");
  try {
    const batches = arr(await api.getBatches(ownerFilter), "batches");
    if (!batches.length) {
      body.innerHTML = emptyBlock(icons.factory(22), "No batches here", `${mspLabel(ownerFilter)} does not own any batches yet.`);
      return;
    }
    body.innerHTML = batchesTable(batches);
    wireGo(body);
  } catch (err) {
    body.innerHTML = `<p class="muted" style="margin:0">Could not load batches. ${esc(err.message)}</p>`;
  }
}

function batchesTable(batches) {
  const rows = batches
    .map((b) => {
      const id = idOf(b, "");
      const drug = pick(b, ["drugCode", "DrugCode", "drug"], "");
      const qty = pick(b, ["outputQuantity", "OutputQuantity", "quantity"], "");
      const unit = pick(b, ["unit", "Unit"], "");
      return `
        <tr data-go="/batches/${encodeURIComponent(id)}" style="cursor:pointer">
          <td><b>${esc(id || "—")}</b></td>
          <td>${esc(drug || "—")}</td>
          <td>${qty !== "" ? esc(qty) + " " + esc(unit) : "—"}</td>
          <td>${esc(mspLabel(ownerOf(b)) || "—")}</td>
          <td>${badge(statusOf(b) || "n/a")}</td>
        </tr>`;
    })
    .join("");
  return `
    <div class="table-wrap">
      <table class="data">
        <thead><tr><th>Batch ID</th><th>Drug code</th><th>Output</th><th>Owner</th><th>Status</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

function openProduce(root) {
  modal({
    title: "Produce batch",
    body: `
      <div class="form-grid">
        <div class="field"><label>Batch ID *</label><input class="input" name="batchId" placeholder="BATCH-001" /></div>
        <div class="field"><label>Drug code *</label><input class="input" name="drugCode" placeholder="PARA-500" /></div>
        <div class="field"><label>Output quantity *</label><input class="input" name="outputQuantity" type="number" placeholder="5000" /></div>
        <div class="field"><label>Unit *</label><input class="input" name="unit" placeholder="tablets" /></div>
        <div class="field span-2"><label>Inputs (JSON array)</label><textarea class="textarea" name="inputs" placeholder='[{"lotId":"LOT-001","amount":100}]'></textarea><span class="hint">Optional. The raw material lots used. Must be a JSON array.</span></div>
      </div>`,
    footer: `<button class="btn btn-primary" data-action="save-batch">Produce</button>`,
    onMount: (el, close) => {
      onClick(el, "save-batch", (btn) => {
        const form = readForm(el);
        if (!form.batchId || !form.drugCode || !form.outputQuantity || !form.unit) {
          toast("Batch ID, drug code, output quantity, and unit are required", "err");
          return;
        }
        let payload;
        try {
          payload = {
            batchId: form.batchId,
            drugCode: form.drugCode,
            outputQuantity: positiveNumber(form.outputQuantity, "Output quantity"),
            unit: form.unit,
            inputs: validateInputs(parseJsonArray(form.inputs, "Inputs")),
          };
        } catch (e) {
          toast(e.message, "err");
          return;
        }
        runAction(btn, "Producing", () => api.produceBatch(payload), () => {
          close();
          renderList(root);
        });
      });
    },
  });
}

function validateInputs(inputs) {
  if (!inputs) return undefined;
  inputs.forEach((input, index) => {
    if (!input || typeof input !== "object") throw new Error(`Inputs[${index}] must be an object`);
    const lotId = input.lotId || input.lotID || input.LotID;
    if (!lotId) throw new Error(`Inputs[${index}].lotId is required`);
    input.amount = positiveNumber(input.amount, `Inputs[${index}].amount`);
  });
  return inputs;
}

async function renderDetail(root, id) {
  root.innerHTML = `
    <div class="content">
      <div class="row">${backLink("/batches", "All batches")}</div>
      <div class="card"><div class="card-body" id="batch-detail">
        <div class="loading"><span class="spin"></span><span>Loading batch…</span></div>
      </div></div>
    </div>`;
  wireGo(root);

  const box = root.querySelector("#batch-detail");
  let batch;
  try {
    batch = one(await api.getBatch(id), "batch");
  } catch (err) {
    box.innerHTML = `<p class="muted" style="margin:0">Could not load batch ${esc(id)}. ${esc(err.message)}</p>`;
    return;
  }
  if (!batch || typeof batch !== "object") {
    box.innerHTML = emptyBlock(icons.factory(22), "Batch not found", `No record for ${id}.`);
    return;
  }

  const facts = factGrid([
    ["Drug code", pick(batch, ["drugCode", "DrugCode"], "")],
    ["Output", joinQty(batch)],
    ["Proposed owner", mspLabel(pick(batch, ["proposedOwner", "proposedOwnerMSP", "ProposedOwner"], ""))],
  ]);

  box.innerHTML = `
    ${detailHeader("factory", idOf(batch, id), batch)}
    ${facts}
    ${ledgerBlock(batch)}
    <div class="row" style="gap:10px;margin-top:16px" id="batch-actions"></div>`;

  renderActions(root, id);
}

function joinQty(b) {
  const q = pick(b, ["outputQuantity", "OutputQuantity", "quantity"], "");
  const u = pick(b, ["unit", "Unit"], "");
  return q === "" ? "" : `${q} ${u}`.trim();
}

function renderActions(root, id) {
  const wrap = root.querySelector("#batch-actions");
  const buttons = [];
  if (isRole("manufacturer")) {
    buttons.push(`<button class="btn btn-primary btn-sm" data-action="propose">${icons.send(15)} Propose transfer</button>`);
  }
  if (isRole("distributor")) {
    buttons.push(`<button class="btn btn-primary btn-sm" data-action="accept">${icons.check(15)} Accept transfer</button>`);
  }
  if (isRole("drap")) {
    buttons.push(`<button class="btn btn-outline btn-sm" data-action="approve">${icons.shield(15)} DRAP approve</button>`);
  }
  if (!buttons.length) {
    wrap.innerHTML = `<span class="muted" style="font-size:.8rem">No actions for your role on this batch.</span>`;
    return;
  }
  wrap.innerHTML = buttons.join("");

  const reload = () => renderDetail(root, id);
  const propose = wrap.querySelector('[data-action="propose"]');
  const accept = wrap.querySelector('[data-action="accept"]');
  const approve = wrap.querySelector('[data-action="approve"]');

  if (propose) {
    propose.addEventListener("click", () =>
      transferModal("Transfer batch to distributor", ["distributorMSP"], (msp) =>
        api.proposeBatchTransfer(id, msp).then(reload)
      )
    );
  }
  if (accept) {
    accept.addEventListener("click", (e) => runAction(e.currentTarget, "Accepting", () => api.acceptBatchTransfer(id), reload));
  }
  if (approve) {
    approve.addEventListener("click", () => openApprove(id, reload));
  }
}

function openApprove(id, reload) {
  modal({
    title: "DRAP approve batch",
    body: `<div class="field"><label>Note</label><input class="input" id="approve-note" placeholder="optional note" /></div>`,
    footer: `<button class="btn btn-primary" data-action="do-approve">Approve</button>`,
    onMount: (el, close) => {
      onClick(el, "do-approve", (btn) => {
        const note = el.querySelector("#approve-note").value.trim();
        runAction(btn, "Approving", () => api.approveBatch(id, note), () => {
          close();
          reload();
        });
      });
    },
  });
}
