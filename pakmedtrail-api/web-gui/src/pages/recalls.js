// Recalls are DRAP driven. DRAP opens and closes them and clears quarantines;
// any org can acknowledge a recall or quarantine an asset it holds. The page
// shows the active recalls, a detail modal with the right actions per role, and
// a small tool to check whether a given asset is under recall or quarantine.

import { icons } from "../icons.js";
import { api } from "../api.js";
import { isRole } from "../store.js";
import { escape as esc, pick, emptyBlock, readForm, modal, onClick, assetTypeOptionsHtml } from "../ui.js";
import {
  arr,
  one,
  parseLoose,
  runAction,
  detailHeader,
  ledgerBlock,
  badge,
  idOf,
  statusOf,
  toast,
  normalizeAssetType,
} from "./_shared.js";

export default async function recallsPage(root) {
  const canManage = isRole("drap");
  root.innerHTML = `
    <div class="content">
      <div class="row between">
        <p class="muted" style="margin:0">Active drug recalls and quarantine status. DRAP opens recalls; any org can acknowledge or quarantine assets it holds.</p>
        ${canManage ? `<button class="btn btn-danger btn-sm" data-action="new-recall">${icons.alert(16)} Start recall</button>` : ""}
      </div>

      <div class="card">
        <div class="card-head"><div class="card-title">${icons.alert(16)} Active recalls</div>
          <button class="btn btn-ghost btn-sm" data-action="reload">${icons.refresh(15)} Refresh</button>
        </div>
        <div class="card-body" id="recall-body">
          <div class="loading"><span class="spin"></span><span>Loading recalls…</span></div>
        </div>
      </div>

      <div class="card">
        <div class="card-head"><div class="card-title">${icons.shield(16)} Check an asset</div></div>
        <div class="card-body">
          <div class="form-grid">
            <div class="field"><label>Asset type</label><select class="select" id="check-type">${assetTypeOptionsHtml()}</select><span class="hint">Use the same type used by the recall chaincode.</span></div>
            <div class="field"><label>Asset ID</label><input class="input" id="check-id" placeholder="BATCH-001" /></div>
          </div>
          <div class="row" style="margin-top:12px">
            <button class="btn btn-primary btn-sm" data-action="check">${icons.search(15)} Check status</button>
          </div>
          <div id="check-result" style="margin-top:12px"></div>
        </div>
      </div>
    </div>`;

  root.querySelector('[data-action="reload"]').addEventListener("click", () => recallsPage(root));
  if (canManage) {
    root.querySelector('[data-action="new-recall"]').addEventListener("click", () => openInitiate(root));
  }
  root.querySelector('[data-action="check"]').addEventListener("click", () => runCheck(root));

  const body = root.querySelector("#recall-body");
  try {
    const recalls = arr(await api.getActiveRecalls(), "recalls");
    if (!recalls.length) {
      body.innerHTML = emptyBlock(icons.check(22), "No active recalls", "The chain is clear right now.");
      return;
    }
    body.innerHTML = recallTable(recalls);
    body.querySelectorAll("[data-recall]").forEach((tr) =>
      tr.addEventListener("click", () => openDetail(root, tr.getAttribute("data-recall")))
    );
  } catch (err) {
    body.innerHTML = `<p class="muted" style="margin:0">Could not load recalls. ${esc(err.message)}</p>`;
  }
}

function recallTable(recalls) {
  const rows = recalls
    .map((r) => {
      const id = idOf(r, "");
      const title = pick(r, ["title", "Title"], "");
      const sev = pick(r, ["severity", "Severity"], "");
      return `
        <tr data-recall="${esc(id)}" style="cursor:pointer">
          <td><b>${esc(id || "—")}</b></td>
          <td>${esc(title || "—")}</td>
          <td>${sev ? badge(sev) : "—"}</td>
          <td>${badge(statusOf(r) || "active")}</td>
        </tr>`;
    })
    .join("");
  return `
    <div class="table-wrap">
      <table class="data">
        <thead><tr><th>Recall ID</th><th>Title</th><th>Severity</th><th>Status</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

function openInitiate(root) {
  modal({
    title: "Start a recall",
    body: `
      <div class="form-grid">
        <div class="field"><label>Recall ID *</label><input class="input" name="recallId" placeholder="REC-001" /></div>
        <div class="field"><label>Severity</label>
          <select class="select" name="severity">
            <option value="">Not set</option>
            <option value="LOW">Low</option>
            <option value="MEDIUM">Medium</option>
            <option value="HIGH">High</option>
            <option value="CRITICAL">Critical</option>
          </select>
        </div>
        <div class="field span-2"><label>Title *</label><input class="input" name="title" placeholder="Contaminated paracetamol batch" /></div>
        <div class="field span-2"><label>Reason *</label><textarea class="textarea" name="reason" placeholder="Why the recall is being issued"></textarea></div>
        <div class="field span-2"><label>Directives</label><textarea class="textarea" name="directives" placeholder="Steps orgs must follow"></textarea><span class="hint">Optional. JSON or plain text.</span></div>
      </div>`,
    footer: `<button class="btn btn-danger" data-action="save-recall">Open recall</button>`,
    onMount: (el, close) => {
      onClick(el, "save-recall", (btn) => {
        const form = readForm(el);
        if (!form.recallId || !form.title || !form.reason) {
          toast("Recall ID, title, and reason are required", "err");
          return;
        }
        const payload = {
          recallId: form.recallId,
          title: form.title,
          reason: form.reason,
          severity: form.severity || undefined,
          directives: parseLoose(form.directives),
        };
        runAction(btn, "Opening", () => api.initiateRecall(payload), () => {
          close();
          recallsPage(root);
        });
      });
    },
  });
}

async function openDetail(root, id) {
  const m = modal({
    title: "Recall",
    body: `<div class="loading"><span class="spin"></span><span>Loading…</span></div>`,
  });
  const target = m.el.querySelector(".modal-body");

  let recall;
  try {
    recall = one(await api.getRecall(id), "recall");
  } catch (err) {
    target.innerHTML = `<p class="muted" style="margin:0">Could not load recall. ${esc(err.message)}</p>`;
    return;
  }

  const isDrap = isRole("drap");
  const actions = [
    `<button class="btn btn-outline btn-sm" data-action="ack">${icons.check(15)} Acknowledge</button>`,
    `<button class="btn btn-outline btn-sm" data-action="quarantine">${icons.ban(15)} Quarantine asset</button>`,
  ];
  if (isDrap) {
    actions.push(`<button class="btn btn-outline btn-sm" data-action="affected">${icons.box(15)} Add affected assets</button>`);
    actions.push(`<button class="btn btn-outline btn-sm" data-action="clear">${icons.check(15)} Clear quarantine</button>`);
    actions.push(`<button class="btn btn-danger btn-sm" data-action="close-recall">${icons.x(15)} Close recall</button>`);
  }

  target.innerHTML = `
    ${detailHeader("alert", idOf(recall, id), recall)}
    ${ledgerBlock(recall)}
    <div class="row" style="gap:8px;margin-top:16px;flex-wrap:wrap">${actions.join("")}</div>`;

  const reload = () => {
    m.close();
    recallsPage(root);
  };

  onClick(target, "ack", () => promptNote("Acknowledge recall", "Acknowledging", (note) => api.acknowledgeRecall(id, note), reload));
  onClick(target, "quarantine", () => promptQuarantine(id, reload));
  if (isDrap) {
    onClick(target, "affected", () => promptAffected(id, reload));
    onClick(target, "clear", () => promptClear(reload));
    onClick(target, "close-recall", () => promptNote("Close recall", "Closing", (note) => api.closeRecall(id, note), reload));
  }
}

// ---------- small action prompts ----------

function promptNote(title, busy, fn, onDone) {
  modal({
    title,
    body: `<div class="field"><label>Note</label><input class="input" id="note" placeholder="optional note" /></div>`,
    footer: `<button class="btn btn-primary" data-action="go">${esc(title)}</button>`,
    onMount: (el, close) => {
      onClick(el, "go", (btn) => {
        const note = el.querySelector("#note").value.trim();
        runAction(btn, busy, () => fn(note), () => {
          close();
          onDone();
        });
      });
    },
  });
}

function promptQuarantine(recallId, onDone) {
  modal({
    title: "Quarantine an asset",
    body: `
      <div class="form-grid">
        <div class="field"><label>Asset type</label><select class="select" id="q-type">${assetTypeOptionsHtml()}</select></div>
        <div class="field"><label>Asset ID *</label><input class="input" id="q-id" placeholder="BATCH-001" /></div>
        <div class="field span-2"><label>Reason</label><input class="input" id="q-reason" placeholder="optional" /></div>
      </div>`,
    footer: `<button class="btn btn-primary" data-action="go">Quarantine</button>`,
    onMount: (el, close) => {
      onClick(el, "go", (btn) => {
        const type = normalizeAssetType(el.querySelector("#q-type").value);
        const aid = el.querySelector("#q-id").value.trim();
        const reason = el.querySelector("#q-reason").value.trim();
        if (!aid) {
          toast("Asset ID is required", "err");
          return;
        }
        runAction(btn, "Quarantining", () => api.quarantineAsset(recallId, type, aid, reason), () => {
          close();
          onDone();
        });
      });
    },
  });
}

function promptAffected(recallId, onDone) {
  modal({
    title: "Add affected assets",
    body: `
      <div class="field"><label>Asset type</label><select class="select" id="a-type">${assetTypeOptionsHtml()}</select></div>
      <div class="field" style="margin-top:12px"><label>Asset IDs *</label><textarea class="textarea" id="a-ids" placeholder="BATCH-001, BATCH-002"></textarea><span class="hint">Comma or new line separated, or a JSON array.</span></div>`,
    footer: `<button class="btn btn-primary" data-action="go">Add</button>`,
    onMount: (el, close) => {
      onClick(el, "go", (btn) => {
        const type = normalizeAssetType(el.querySelector("#a-type").value);
        const ids = parseIdList(el.querySelector("#a-ids").value);
        if (!ids.length) {
          toast("Enter at least one asset ID", "err");
          return;
        }
        runAction(btn, "Adding", () => api.addAffectedAssets(recallId, type, ids), () => {
          close();
          onDone();
        });
      });
    },
  });
}

function promptClear(onDone) {
  modal({
    title: "Clear a quarantine",
    body: `
      <div class="form-grid">
        <div class="field"><label>Asset type</label><select class="select" id="c-type">${assetTypeOptionsHtml()}</select></div>
        <div class="field"><label>Asset ID *</label><input class="input" id="c-id" placeholder="BATCH-001" /></div>
      </div>`,
    footer: `<button class="btn btn-primary" data-action="go">Clear</button>`,
    onMount: (el, close) => {
      onClick(el, "go", (btn) => {
        const type = normalizeAssetType(el.querySelector("#c-type").value);
        const aid = el.querySelector("#c-id").value.trim();
        if (!aid) {
          toast("Asset ID is required", "err");
          return;
        }
        runAction(btn, "Clearing", () => api.clearQuarantine(type, aid), () => {
          close();
          onDone();
        });
      });
    },
  });
}

function parseIdList(text) {
  const t = (text || "").trim();
  if (!t) return [];
  if (t.startsWith("[")) {
    try {
      const parsed = JSON.parse(t);
      if (Array.isArray(parsed)) return parsed.map(String).filter(Boolean);
    } catch {
      // fall through to delimiter split
    }
  }
  return t
    .split(/[\n,]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

async function runCheck(root) {
  const type = normalizeAssetType(root.querySelector("#check-type").value);
  const aid = root.querySelector("#check-id").value.trim();
  const result = root.querySelector("#check-result");
  if (!type || !aid) {
    toast("Enter both asset type and asset ID", "err");
    return;
  }
  result.innerHTML = `<div class="loading"><span class="spin"></span><span>Checking…</span></div>`;

  const [activeR, quarR] = await Promise.allSettled([
    api.checkActiveRecall(type, aid),
    api.getQuarantine(type, aid),
  ]);

  const parts = [];
  if (activeR.status === "fulfilled") {
    const active = pick(activeR.value, ["active", "Active"], false);
    parts.push(`
      <div class="row" style="gap:8px">
        ${active ? icons.alert(16) : icons.check(16)}
        <b style="font-size:.86rem">${active ? "Under an active recall" : "No active recall"}</b>
      </div>`);
  } else {
    parts.push(`<p class="muted" style="margin:0;font-size:.82rem">Recall check failed: ${esc(activeR.reason.message)}</p>`);
  }

  if (quarR.status === "fulfilled") {
    const q = one(quarR.value, "quarantine");
    if (q && typeof q === "object" && Object.keys(q).length) {
      parts.push(`<div style="margin-top:8px">${ledgerBlock(q)}</div>`);
    } else {
      parts.push(`<p class="muted" style="margin:8px 0 0;font-size:.82rem">No quarantine record for this asset.</p>`);
    }
  }

  result.innerHTML = parts.join("");
}
