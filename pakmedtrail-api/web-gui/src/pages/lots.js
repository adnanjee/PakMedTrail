// Lots are the raw material records suppliers put on the ledger. This page
// lists them, lets a supplier create one, and shows a detail view with the
// actions each role is allowed to take (propose transfer, accept transfer,
// DRAP approve). The exact state machine lives in the chaincode, so we offer
// the role's actions and let the server reject anything out of order.

import { icons } from "../icons.js";
import { api } from "../api.js";
import { isRole, orgMSP } from "../store.js";
import { escape as esc, pick, fmtDate, emptyBlock, readForm, modal, onClick } from "../ui.js";
import {
  arr,
  one,
  wireGo,
  backLink,
  parseJsonObject,
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

export default async function lotsPage(root, params) {
  const id = params && params[0];
  if (id) return renderDetail(root, id);
  return renderList(root);
}

async function renderList(root) {
  const canCreate = isRole("supplier");
  root.innerHTML = `
    <div class="content">
      <div class="row between">
        <p class="muted" style="margin:0">Raw material lots on the ledger. Suppliers create them, then hand them to a manufacturer.</p>
        ${canCreate ? `<button class="btn btn-primary btn-sm" data-action="new-lot">${icons.plus(16)} Create lot</button>` : ""}
      </div>
      <div class="card">
        <div class="card-head"><div class="card-title">${icons.lots(16)} All lots</div>
          <button class="btn btn-ghost btn-sm" data-action="reload">${icons.refresh(15)} Refresh</button>
        </div>
        <div class="card-body" id="lots-body">
          <div class="loading"><span class="spin"></span><span>Loading lots…</span></div>
        </div>
      </div>
    </div>`;

  root.querySelector('[data-action="reload"]').addEventListener("click", () => renderList(root));
  if (canCreate) {
    root.querySelector('[data-action="new-lot"]').addEventListener("click", () => openCreate(root));
  }

  const body = root.querySelector("#lots-body");
  try {
    const lots = arr(await api.getLots(), "lots");
    if (!lots.length) {
      body.innerHTML = emptyBlock(icons.lots(22), "No lots yet", canCreate ? "Use Create lot to add the first one." : "A supplier needs to create one first.");
      return;
    }
    body.innerHTML = lotsTable(lots);
    wireGo(body);
  } catch (err) {
    body.innerHTML = `<p class="muted" style="margin:0">Could not load lots. ${esc(err.message)}</p>`;
  }
}

function lotsTable(lots) {
  const rows = lots
    .map((lot) => {
      const id = idOf(lot, "");
      const name = pick(lot, ["name", "Name", "productName", "drugName"], "");
      const qty = pick(lot, ["quantity", "Quantity", "qty"], "");
      const unit = pick(lot, ["unit", "Unit"], "");
      return `
        <tr data-go="/lots/${encodeURIComponent(id)}" style="cursor:pointer">
          <td><b>${esc(id || "—")}</b></td>
          <td>${esc(name || "—")}</td>
          <td>${esc(mspLabel(ownerOf(lot)) || "—")}</td>
          <td>${qty !== "" ? esc(qty) + " " + esc(unit) : "—"}</td>
          <td>${badge(statusOf(lot) || "n/a")}</td>
        </tr>`;
    })
    .join("");
  return `
    <div class="table-wrap">
      <table class="data">
        <thead><tr><th>Lot ID</th><th>Name</th><th>Owner</th><th>Quantity</th><th>Status</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

function openCreate(root) {
  modal({
    title: "Create lot",
    body: `
      <div class="form-grid">
        <div class="field"><label>Lot ID *</label><input class="input" name="lotId" placeholder="LOT-001" /></div>
        <div class="field"><label>Name *</label><input class="input" name="name" placeholder="Paracetamol API" /></div>
        <div class="field"><label>Quantity *</label><input class="input" name="quantity" type="number" placeholder="1000" /></div>
        <div class="field"><label>Unit *</label><input class="input" name="unit" placeholder="kg" /></div>
        <div class="field"><label>Batch number</label><input class="input" name="batchNumber" placeholder="optional" /></div>
        <div class="field"><label>Manufacture date</label><input class="input" name="manufactureDate" type="date" /></div>
        <div class="field"><label>Expiry date</label><input class="input" name="expiryDate" type="date" /></div>
        <div class="field span-2"><label>Metadata (JSON)</label><textarea class="textarea" name="metadata" placeholder='{"origin":"Karachi"}'></textarea><span class="hint">Optional. Must be a JSON object.</span></div>
      </div>`,
    footer: `<button class="btn btn-primary" data-action="save-lot">Create lot</button>`,
    onMount: (el, close) => {
      onClick(el, "save-lot", (btn) => {
        const form = readForm(el);
        if (!form.lotId || !form.name || !form.quantity || !form.unit) {
          toast("Lot ID, name, quantity, and unit are required", "err");
          return;
        }
        let payload;
        try {
          payload = {
            lotId: form.lotId,
            name: form.name,
            quantity: positiveNumber(form.quantity, "Quantity"),
            unit: form.unit,
            batchNumber: form.batchNumber || undefined,
            manufactureDate: form.manufactureDate || undefined,
            expiryDate: form.expiryDate || undefined,
            metadata: parseJsonObject(form.metadata, "Metadata"),
          };
        } catch (e) {
          toast(e.message, "err");
          return;
        }
        runAction(btn, "Creating", () => api.createLot(payload), () => {
          close();
          renderList(root);
        });
      });
    },
  });
}

async function renderDetail(root, id) {
  root.innerHTML = `
    <div class="content">
      <div class="row">${backLink("/lots", "All lots")}</div>
      <div class="card"><div class="card-body" id="lot-detail">
        <div class="loading"><span class="spin"></span><span>Loading lot…</span></div>
      </div></div>
    </div>`;
  wireGo(root);

  const box = root.querySelector("#lot-detail");
  let lot;
  try {
    lot = one(await api.getLot(id), "lot");
  } catch (err) {
    box.innerHTML = `<p class="muted" style="margin:0">Could not load lot ${esc(id)}. ${esc(err.message)}</p>`;
    return;
  }
  if (!lot || typeof lot !== "object") {
    box.innerHTML = emptyBlock(icons.lots(22), "Lot not found", `No record for ${id}.`);
    return;
  }

  const facts = factGrid([
    ["Name", pick(lot, ["name", "Name", "productName"], "")],
    ["Quantity", joinQty(lot)],
    ["Batch number", pick(lot, ["batchNumber", "BatchNumber"], "")],
    ["Manufacture date", fmtDate(pick(lot, ["manufactureDate", "ManufactureDate"], ""))],
    ["Expiry date", fmtDate(pick(lot, ["expiryDate", "ExpiryDate"], ""))],
    ["Proposed owner", mspLabel(pick(lot, ["proposedOwner", "proposedOwnerMSP", "ProposedOwner"], ""))],
  ]);

  box.innerHTML = `
    ${detailHeader("lots", idOf(lot, id), lot)}
    ${facts}
    ${ledgerBlock(lot)}
    <div class="row" style="gap:10px;margin-top:16px" id="lot-actions"></div>`;

  renderActions(root, id, lot);
}

function joinQty(lot) {
  const q = pick(lot, ["quantity", "Quantity", "qty"], "");
  const u = pick(lot, ["unit", "Unit"], "");
  return q === "" ? "" : `${q} ${u}`.trim();
}

function renderActions(root, id, lot) {
  const wrap = root.querySelector("#lot-actions");
  const buttons = [];
  if (isRole("supplier")) {
    buttons.push(`<button class="btn btn-primary btn-sm" data-action="propose">${icons.send(15)} Propose transfer</button>`);
  }
  if (isRole("manufacturer")) {
    buttons.push(`<button class="btn btn-primary btn-sm" data-action="accept">${icons.check(15)} Accept transfer</button>`);
  }
  if (isRole("drap")) {
    buttons.push(`<button class="btn btn-outline btn-sm" data-action="approve">${icons.shield(15)} DRAP approve</button>`);
  }
  if (!buttons.length) {
    wrap.innerHTML = `<span class="muted" style="font-size:.8rem">No actions for your role on this lot.</span>`;
    return;
  }
  wrap.innerHTML = buttons.join("");

  const reload = () => renderDetail(root, id);
  const propose = wrap.querySelector('[data-action="propose"]');
  const accept = wrap.querySelector('[data-action="accept"]');
  const approve = wrap.querySelector('[data-action="approve"]');

  if (propose) {
    propose.addEventListener("click", () =>
      transferModal("Transfer lot to manufacturer", ["manufacturerMSP"], (msp) =>
        api.proposeLotTransfer(id, msp).then(reload)
      )
    );
  }
  if (accept) {
    accept.addEventListener("click", (e) => runAction(e.currentTarget, "Accepting", () => api.acceptLotTransfer(id), reload));
  }
  if (approve) {
    approve.addEventListener("click", () => openApprove(id, reload));
  }
}

function openApprove(id, reload) {
  modal({
    title: "DRAP approve lot",
    body: `<div class="field"><label>Note</label><input class="input" id="approve-note" placeholder="optional note" /></div>`,
    footer: `<button class="btn btn-primary" data-action="do-approve">Approve</button>`,
    onMount: (el, close) => {
      onClick(el, "do-approve", (btn) => {
        const note = el.querySelector("#approve-note").value.trim();
        runAction(btn, "Approving", () => api.approveLot(id, note), () => {
          close();
          reload();
        });
      });
    },
  });
}
