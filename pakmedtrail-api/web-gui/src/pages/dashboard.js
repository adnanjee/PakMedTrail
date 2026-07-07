// Dashboard. Mirrors the PharmaChain "active network flow" idea: a horizontal
// pipeline from supplier to patient with a live count at each stage, plus role
// aware stat tiles, quick actions, and a short recent activity list.
// Stage counts come from several queries run together; if one fails we show a
// dash for that stage instead of breaking the whole page.

import { icons } from "../icons.js";
import { api } from "../api.js";
import { getUser, role, orgMSP } from "../store.js";
import {
  escape,
  idOf,
  ownerOf,
  statusOf,
  badge,
  mspLabel,
  fmtDate,
  emptyBlock,
} from "../ui.js";
import { go } from "../router.js";

const STAGES = [
  { key: "supplier", label: "Supplier", icon: "lots", msp: "supplierMSP" },
  { key: "manufacturer", label: "Manufacturer", icon: "factory", msp: "manufacturerMSP" },
  { key: "drap", label: "DRAP", icon: "shield", msp: "drapMSP" },
  { key: "distributor", label: "Distributor", icon: "truck", msp: "distributorMSP" },
  { key: "pharmacy", label: "Pharmacy", icon: "store", msp: "retailerMSP" },
  { key: "patient", label: "Patient", icon: "patient", msp: null },
];

function arr(value, key) {
  if (Array.isArray(value)) return value;
  if (value && Array.isArray(value[key])) return value[key];
  return [];
}

function quickActions(r) {
  const map = {
    supplier: [{ label: "Create lot", to: "/lots", icon: "plus" }],
    manufacturer: [
      { label: "Produce batch", to: "/batches", icon: "factory" },
      { label: "New formulation", to: "/formulations", icon: "flask" },
      { label: "Ship to distributor", to: "/shipments", icon: "truck" },
    ],
    distributor: [{ label: "Ship to pharmacy", to: "/shipments", icon: "truck" }],
    retailer: [{ label: "Dispense to patient", to: "/shipments", icon: "store" }],
    drap: [
      { label: "Start recall", to: "/recalls", icon: "alert" },
      { label: "Review approvals", to: "/batches", icon: "shield" },
    ],
  };
  const actions = map[r] || [];
  if (!actions.length) return "";
  return `
    <div class="row" style="gap:10px">
      ${actions
        .map(
          (a) =>
            `<button class="btn btn-primary btn-sm" data-go="${a.to}">${icons[a.icon](16)} ${a.label}</button>`
        )
        .join("")}
      <button class="btn btn-outline btn-sm" data-go="/verify">${icons.qr(16)} Verify an item</button>
    </div>`;
}

export default async function dashboardPage(root) {
  const user = getUser();
  const myMSP = orgMSP();

  root.innerHTML = `
    <div class="content">
      <div class="row between">
        <div>
          <h2 style="margin:0;font-size:1.3rem;font-weight:800">Hello, ${escape(user.username)}</h2>
          <p class="muted" style="margin:2px 0 0">Signed in as ${escape(mspLabel(myMSP))}. Here is the current state of the chain.</p>
        </div>
        <button class="btn btn-outline btn-sm" data-action="reload">${icons.refresh(16)} Refresh</button>
      </div>

      <div class="card">
        <div class="card-head">
          <div class="card-title">${icons.box(16)} Active network flow</div>
          <span class="muted" style="font-size:.76rem">Lots at supplier · batches downstream · active recalls at DRAP</span>
        </div>
        <div class="card-body">
          <div class="pipeline" id="pipeline">${pipelineSkeleton()}</div>
        </div>
      </div>

      <div class="stat-grid" id="stats">${statSkeleton()}</div>

      <div class="grid-2">
        <div class="card">
          <div class="card-head">
            <div class="card-title">${icons.clock(16)} Recent lots</div>
            <button class="btn btn-ghost btn-sm" data-go="/lots">View all</button>
          </div>
          <div class="card-body" id="recent-lots">${loading()}</div>
        </div>
        <div class="card">
          <div class="card-head"><div class="card-title">${icons.send(16)} Quick actions</div></div>
          <div class="card-body">${quickActions(role()) || `<p class="muted" style="margin:0">No actions for this role.</p>`}</div>
        </div>
      </div>
    </div>`;

  wireGo(root);
  root.querySelector('[data-action="reload"]').addEventListener("click", () => dashboardPage(root));

  // Fire all queries together.
  const [lotsR, mfgR, distR, retR, recallsR] = await Promise.allSettled([
    api.getLots(),
    api.getBatches("manufacturerMSP"),
    api.getBatches("distributorMSP"),
    api.getBatches("retailerMSP"),
    api.getActiveRecalls(),
  ]);

  const lots = lotsR.status === "fulfilled" ? arr(lotsR.value, "lots") : null;
  const mfg = mfgR.status === "fulfilled" ? arr(mfgR.value, "batches") : null;
  const dist = distR.status === "fulfilled" ? arr(distR.value, "batches") : null;
  const ret = retR.status === "fulfilled" ? arr(retR.value, "batches") : null;
  const recalls = recallsR.status === "fulfilled" ? arr(recallsR.value, "recalls") : null;

  const counts = {
    supplier: lots ? lots.length : null,
    manufacturer: mfg ? mfg.length : null,
    drap: recalls ? recalls.length : null,
    distributor: dist ? dist.length : null,
    pharmacy: ret ? ret.length : null,
    patient: null,
  };

  renderPipeline(root, counts, myMSP);
  renderStats(root, { lots, recalls, myCount: pickMyCount(myMSP, { mfg, dist, ret, lots }) });
  renderRecentLots(root, lots);
}

function pickMyCount(myMSP, sets) {
  if (myMSP === "manufacturerMSP") return sets.mfg ? sets.mfg.length : null;
  if (myMSP === "distributorMSP") return sets.dist ? sets.dist.length : null;
  if (myMSP === "retailerMSP") return sets.ret ? sets.ret.length : null;
  if (myMSP === "supplierMSP") return sets.lots ? sets.lots.length : null;
  return null;
}

function renderPipeline(root, counts, myMSP) {
  const node = root.querySelector("#pipeline");
  const parts = [];
  STAGES.forEach((stage, i) => {
    const count = counts[stage.key];
    const on = stage.msp === myMSP;
    const countLabel =
      count === null ? "—" : `${count} ${stage.key === "drap" ? "active" : "item" + (count === 1 ? "" : "s")}`;
    parts.push(`
      <div class="pl-node ${on ? "on" : ""}" title="${escape(stage.label)}">
        <div class="pl-ic">${icons[stage.icon](26)}</div>
        <div class="pl-name">${stage.label}</div>
        <div class="pl-count">${countLabel}</div>
      </div>`);
    if (i < STAGES.length - 1) parts.push(`<div class="pl-link"></div>`);
  });
  node.innerHTML = parts.join("");
}

function renderStats(root, { lots, recalls, myCount }) {
  const node = root.querySelector("#stats");
  const n = (v) => (v === null || v === undefined ? "—" : v);
  node.innerHTML = `
    ${statTile("Total lots", n(lots ? lots.length : null), "Raw material on the ledger", "lots")}
    ${statTile("My holdings", n(myCount), "Items your org owns now", "box")}
    ${statTile(
      "Active recalls",
      n(recalls ? recalls.length : null),
      recalls && recalls.length ? "Action may be needed" : "None open",
      "alert"
    )}
    ${statTile("Your role", "", mspLabelTile(), "shield", true)}`;
}

function mspLabelTile() {
  return `<span style="font-size:1.15rem;font-weight:800;text-transform:capitalize">${escape(role())}</span>`;
}

function statTile(label, value, foot, icon, htmlValue) {
  return `
    <div class="stat">
      <div class="stat-ic">${icons[icon](18)}</div>
      <div class="stat-label">${escape(label)}</div>
      <div class="stat-value">${htmlValue ? foot && value === "" ? "" : value : escape(value)}</div>
      <div class="stat-foot">${htmlValue ? foot : escape(foot)}</div>
    </div>`;
}

function renderRecentLots(root, lots) {
  const node = root.querySelector("#recent-lots");
  if (lots === null) {
    node.innerHTML = `<p class="muted" style="margin:0">Could not load lots. The ledger may be unreachable.</p>`;
    return;
  }
  if (!lots.length) {
    node.innerHTML = emptyBlock(icons.lots(22), "No lots yet", "Suppliers create lots to start the chain.");
    return;
  }
  const recent = [...lots]
    .sort((a, b) => String(idOf(b)).localeCompare(String(idOf(a))))
    .slice(0, 6);
  node.innerHTML = `
    <div class="col" style="gap:8px">
      ${recent
        .map((lot) => {
          const id = idOf(lot);
          return `
          <button class="row between" data-go="/lots/${encodeURIComponent(id)}"
            style="width:100%;text-align:left;padding:10px 12px;border:1px solid hsl(var(--border));border-radius:10px;background:hsl(var(--card));cursor:pointer">
            <div class="row" style="gap:10px">
              <span style="width:30px;height:30px;border-radius:8px;display:grid;place-items:center;background:hsl(var(--primary)/.12);color:hsl(var(--primary))">${icons.lots(15)}</span>
              <div class="col" style="gap:1px">
                <b style="font-size:.86rem">${escape(idOf(lot, "lot"))}</b>
                <span class="muted" style="font-size:.74rem">${escape(mspLabel(ownerOf(lot)) || "—")}</span>
              </div>
            </div>
            ${badge(statusOf(lot) || "n/a")}
          </button>`;
        })
        .join("")}
    </div>`;
  wireGo(root);
}

function wireGo(root) {
  root.querySelectorAll("[data-go]").forEach((el) => {
    if (el.dataset.wired) return;
    el.dataset.wired = "1";
    el.addEventListener("click", () => go(el.getAttribute("data-go")));
  });
}

function pipelineSkeleton() {
  return STAGES.map(
    (s, i) =>
      `<div class="pl-node"><div class="pl-ic">${icons[s.icon](26)}</div><div class="pl-name">${s.label}</div><div class="pl-count muted">…</div></div>${
        i < STAGES.length - 1 ? '<div class="pl-link"></div>' : ""
      }`
  ).join("");
}

function statSkeleton() {
  return Array(4)
    .fill(0)
    .map(
      () =>
        `<div class="stat"><div class="stat-label">…</div><div class="stat-value">—</div><div class="stat-foot muted">loading</div></div>`
    )
    .join("");
}

function loading() {
  return `<div class="loading"><span class="spin"></span><span>Loading…</span></div>`;
}
