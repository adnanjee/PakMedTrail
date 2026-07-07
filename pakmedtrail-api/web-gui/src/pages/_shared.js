// Helpers shared by the action heavy pages (lots, batches, formulations,
// shipments, recalls). Keeps each page focused on its own layout instead of
// repeating the same plumbing for nav links, form reads, and button states.

import { icons } from "../icons.js";
import {
  escape,
  toast,
  modal,
  badge,
  statusOf,
  ownerOf,
  idOf,
  mspLabel,
  rawFields,
  MSP_OPTIONS,
  onClick,
} from "../ui.js";
import { go } from "../router.js";

// Many list endpoints wrap their array, e.g. { lots: [...] }. Accept either the
// wrapper or a bare array so callers do not have to care.
export function arr(value, key) {
  if (Array.isArray(value)) return value;
  if (value && Array.isArray(value[key])) return value[key];
  return [];
}

// Pull the single record out of a { lot } / { batch } style wrapper.
export function one(value, key) {
  if (value && typeof value === "object" && value[key] !== undefined) return value[key];
  return value;
}

// Bind every [data-go] element in a container to the router. Safe to call more
// than once; already wired nodes are skipped.
export function wireGo(root) {
  root.querySelectorAll("[data-go]").forEach((el) => {
    if (el.dataset.wired) return;
    el.dataset.wired = "1";
    el.addEventListener("click", () => go(el.getAttribute("data-go")));
  });
}

// A "back to list" link used at the top of every detail view.
export function backLink(to, label) {
  return `<button class="btn btn-ghost btn-sm" data-go="${escape(to)}">${icons.x(15)} ${escape(label)}</button>`;
}

// Read a textarea that should hold a JSON object. Blank means "not provided".
// Anything else must parse to a plain object or we throw a clear message.
export function parseJsonObject(text, fieldName = "metadata") {
  const t = (text || "").trim();
  if (!t) return undefined;
  let parsed;
  try {
    parsed = JSON.parse(t);
  } catch {
    throw new Error(`${fieldName} must be valid JSON`);
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`${fieldName} must be a JSON object, like {"key":"value"}`);
  }
  return parsed;
}

// Read a textarea that should hold a JSON array (used for batch inputs and
// recall asset id lists).
export function parseJsonArray(text, fieldName = "inputs") {
  const t = (text || "").trim();
  if (!t) return undefined;
  let parsed;
  try {
    parsed = JSON.parse(t);
  } catch {
    throw new Error(`${fieldName} must be valid JSON`);
  }
  if (!Array.isArray(parsed)) {
    throw new Error(`${fieldName} must be a JSON array, like [ ... ]`);
  }
  return parsed;
}

// Some fields (recall directives) have an unknown shape. Try JSON first so a
// list or object passes through cleanly, otherwise send the raw text.
export function parseLoose(text) {
  const t = (text || "").trim();
  if (!t) return undefined;
  try {
    return JSON.parse(t);
  } catch {
    return t;
  }
}

// Run an async action behind a button. Locks the button while it runs, shows a
// toast on success or failure, then calls onDone so the page can refresh.
export async function runAction(btn, label, fn, onDone) {
  if (!btn) return;
  const original = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = `<span class="spin"></span> ${escape(label)}`;
  try {
    await fn();
    toast(`${label} done`, "ok");
    if (onDone) onDone();
  } catch (err) {
    btn.disabled = false;
    btn.innerHTML = original;
    toast(err && err.message ? err.message : "Action failed", "err");
  }
}

// A small rounded icon chip, styled inline so it does not depend on any parent
// container (unlike the dashboard's .stat-ic which only works inside a stat).
export function iconChip(name, size = 20) {
  return `<span style="width:38px;height:38px;border-radius:10px;display:grid;place-items:center;flex:none;color:hsl(var(--primary));background:hsl(var(--primary)/.12)">${icons[name](size)}</span>`;
}

// Standard header strip for a detail view: icon, id, owner, status badge.
export function detailHeader(icon, title, obj) {
  const owner = ownerOf(obj);
  return `
    <div class="row between">
      <div class="row" style="gap:12px">
        ${iconChip(icon)}
        <div class="col" style="gap:2px">
          <b style="font-size:1.05rem">${escape(title)}</b>
          <span class="muted" style="font-size:.78rem">${owner ? "Held by " + escape(mspLabel(owner)) : "Owner not set"}</span>
        </div>
      </div>
      ${badge(statusOf(obj) || "n/a")}
    </div>`;
}

// A compact two column list of friendly fields. Skips any that are missing so
// the layout stays clean across the different record shapes.
export function factGrid(pairs) {
  const rows = pairs
    .filter(([, v]) => v !== undefined && v !== null && v !== "")
    .map(
      ([k, v]) =>
        `<div class="field"><label>${escape(k)}</label><div>${escape(v)}</div></div>`
    )
    .join("");
  if (!rows) return "";
  return `<div class="form-grid" style="margin-top:14px">${rows}</div>`;
}

// The full key/value dump, wrapped in a titled block so users can always see
// exactly what the ledger returned.
export function ledgerBlock(obj) {
  return `
    <div style="margin-top:16px">
      <div class="row" style="gap:8px;margin-bottom:8px">
        ${icons.box(15)}<b style="font-size:.82rem">Ledger record</b>
      </div>
      ${rawFields(obj)}
    </div>`;
}

// Open a transfer dialog: pick the next owner MSP, then confirm. Used by lots
// (supplier hands to manufacturer) and batches (manufacturer hands to
// distributor). allowed limits the visible options to valid next owners.
export function transferModal(title, allowedMsps, onConfirm) {
  const opts = MSP_OPTIONS.filter((m) => allowedMsps.includes(m.value))
    .map((m) => `<option value="${m.value}">${escape(m.label)} (${m.value})</option>`)
    .join("");
  modal({
    title,
    body: `
      <div class="field">
        <label>New owner</label>
        <select class="select" id="transfer-owner">${opts}</select>
        <span class="hint">The other org must accept the transfer before they own it.</span>
      </div>`,
    footer: `<button class="btn btn-primary" data-action="confirm-transfer">Propose transfer</button>`,
    onMount: (el, close) => {
      onClick(el, "confirm-transfer", (btn) => {
        const msp = el.querySelector("#transfer-owner").value;
        runAction(btn, "Proposing", () => onConfirm(msp), close);
      });
    },
  });
}


export function positiveNumber(value, label = "quantity") {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error(`${label} must be a positive number`);
  }
  return n;
}

export function normalizeAssetType(value) {
  return String(value || "BATCH").trim().toUpperCase();
}

export { escape, toast, modal, badge, idOf, ownerOf, statusOf, mspLabel };
