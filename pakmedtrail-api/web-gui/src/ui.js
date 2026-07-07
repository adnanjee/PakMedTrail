// Small DOM and formatting helpers. No framework. The app renders with template
// strings into innerHTML, so escape() is used on every value that comes from the
// API or the user to keep things safe.

import { icons } from "./icons.js";

export function escape(value) {
  if (value === null || value === undefined) return "";
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

export function qs(sel, root = document) {
  return root.querySelector(sel);
}

export function qsa(sel, root = document) {
  return Array.from(root.querySelectorAll(sel));
}

// Event delegation: bind one handler on a root for a [data-action] click.
export function onClick(root, action, handler) {
  root.addEventListener("click", (e) => {
    const target = e.target.closest(`[data-action="${action}"]`);
    if (target && root.contains(target)) handler(target, e);
  });
}

// ---------- toasts ----------

let toastStack;

function ensureToastStack() {
  if (!toastStack) {
    toastStack = document.createElement("div");
    toastStack.className = "toast-stack";
    document.body.appendChild(toastStack);
  }
  return toastStack;
}

export function toast(message, kind = "info", title) {
  const stack = ensureToastStack();
  const node = document.createElement("div");
  node.className = `toast ${kind}`;
  const heads = { ok: "Done", err: "Something went wrong", info: "Notice" };
  node.innerHTML = `
    <div style="flex:1">
      <div class="t-title">${escape(title || heads[kind] || "Notice")}</div>
      <div class="t-msg">${escape(message)}</div>
    </div>`;
  stack.appendChild(node);
  setTimeout(() => {
    node.style.opacity = "0";
    node.style.transition = "opacity .2s";
    setTimeout(() => node.remove(), 220);
  }, kind === "err" ? 6000 : 3600);
}

// ---------- modal ----------

export function modal({ title, body, footer, onMount }) {
  const back = document.createElement("div");
  back.className = "modal-back";
  back.innerHTML = `
    <div class="modal" role="dialog" aria-modal="true">
      <div class="modal-head">
        <h3>${escape(title)}</h3>
        <button class="icon-btn" data-action="close-modal" aria-label="Close">${icons.x(18)}</button>
      </div>
      <div class="modal-body">${body || ""}</div>
      ${footer ? `<div class="modal-foot">${footer}</div>` : ""}
    </div>`;

  function close() {
    back.remove();
    document.removeEventListener("keydown", onKey);
  }
  function onKey(e) {
    if (e.key === "Escape") close();
  }

  back.addEventListener("click", (e) => {
    if (e.target === back) close();
    if (e.target.closest('[data-action="close-modal"]')) close();
  });
  document.addEventListener("keydown", onKey);
  document.body.appendChild(back);

  if (onMount) onMount(back.querySelector(".modal"), close);
  return { el: back, close };
}

// ---------- value helpers ----------

// The exact field names returned by the PakMedTrail chaincode are not known
// ahead of time (Go struct JSON tags can vary in casing). pick() tries a list
// of likely names and returns the first one that is present.
export function pick(obj, candidates, fallback = undefined) {
  if (!obj || typeof obj !== "object") return fallback;
  for (const key of candidates) {
    if (obj[key] !== undefined && obj[key] !== null && obj[key] !== "") {
      return obj[key];
    }
  }
  return fallback;
}

export function idOf(obj, fallback = "") {
  return pick(
    obj,
    ["lotId", "lotID", "LotId", "LotID", "batchId", "batchID", "BatchId", "BatchID",
     "shipmentId", "shipmentID", "ShipmentId", "ShipmentID", "recallId", "recallID", "RecallId", "RecallID",
     "dispenseId", "dispenseID", "DispenseId", "DispenseID", "drugCode", "DrugCode",
     "id", "ID", "Id", "key", "Key"],
    fallback
  );
}

export function ownerOf(obj) {
  return pick(
    obj,
    ["owner", "Owner", "ownerMSP", "ownerMsp", "OwnerMsp", "OwnerMSP", "currentOwner", "CurrentOwner",
     "holder", "Holder", "ownerOrg", "OwnerOrg"],
    ""
  );
}

export function statusOf(obj) {
  return pick(
    obj,
    ["status", "Status", "state", "State", "lifecycleState", "phase"],
    ""
  );
}

// Map a status string to a badge color class.
export function statusBadge(status) {
  const s = String(status || "").toLowerCase();
  if (!s) return "badge-neutral";
  if (/(approv|accept|deliver|active|verif|complete|clos|stock|owned|dispens)/.test(s))
    return "badge-success";
  if (/(pending|propos|offer|transit|await|review|created|hold)/.test(s))
    return "badge-warning";
  if (/(reject|recall|quarantin|expire|cancel|fail|block|destroy)/.test(s))
    return "badge-danger";
  return "badge-primary";
}

export function badge(text, cls) {
  const t = text === undefined || text === null || text === "" ? "n/a" : text;
  return `<span class="badge ${cls || statusBadge(t)}">${escape(t)}</span>`;
}

// MSP label helpers for the role pickers.
export const MSP_OPTIONS = [
  { value: "supplierMSP", label: "Supplier" },
  { value: "manufacturerMSP", label: "Manufacturer" },
  { value: "distributorMSP", label: "Distributor" },
  { value: "retailerMSP", label: "Retailer" },
  { value: "drapMSP", label: "DRAP" },
];

export function mspLabel(msp) {
  const hit = MSP_OPTIONS.find((m) => m.value === msp);
  return hit ? hit.label : msp || "";
}

export function fmtDate(value) {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function shortId(value, head = 10) {
  const s = String(value || "");
  return s.length > head + 6 ? `${s.slice(0, head)}…${s.slice(-4)}` : s;
}

// Render a full key/value table for an object, so nothing the chaincode returns
// is ever hidden from the user.
export function rawFields(obj) {
  if (!obj || typeof obj !== "object") {
    return `<div class="code">${escape(JSON.stringify(obj, null, 2))}</div>`;
  }
  const rows = Object.entries(obj)
    .map(([k, v]) => {
      const val =
        v !== null && typeof v === "object"
          ? `<span class="mono">${escape(JSON.stringify(v))}</span>`
          : escape(v);
      return `<dt>${escape(k)}</dt><dd>${val}</dd>`;
    })
    .join("");
  return `<dl class="kv">${rows}</dl>`;
}

export function spinnerBlock(text = "Loading…") {
  return `<div class="loading"><span class="spin"></span><span>${escape(text)}</span></div>`;
}

export function emptyBlock(icon, title, sub) {
  return `
    <div class="empty">
      <div class="empty-ic">${icon}</div>
      <div><b>${escape(title)}</b></div>
      ${sub ? `<div>${escape(sub)}</div>` : ""}
    </div>`;
}

// Read all named inputs inside a container into a plain object.
export function readForm(root) {
  const out = {};
  qsa("[name]", root).forEach((el) => {
    if (el.type === "checkbox") out[el.name] = el.checked;
    else out[el.name] = el.value.trim();
  });
  return out;
}


export const ASSET_TYPE_OPTIONS = [
  { value: "BATCH", label: "Batch" },
  { value: "LOT", label: "Lot" },
  { value: "DISTRIBUTION_SHIPMENT", label: "Distribution shipment" },
  { value: "RETAIL_SHIPMENT", label: "Retail shipment" },
  { value: "DISPENSE", label: "Dispense" },
];

export function assetTypeOptionsHtml(selected = "BATCH") {
  return ASSET_TYPE_OPTIONS.map((item) =>
    `<option value="${escape(item.value)}" ${item.value === selected ? "selected" : ""}>${escape(item.label)} (${escape(item.value)})</option>`
  ).join("");
}
