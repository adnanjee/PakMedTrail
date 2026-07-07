// Network diagnostics. This page uses the API diagnostics endpoint added during
// review. DRAP sees every configured organisation. Other roles see only their own
// gateway. It checks TCP reachability, the peer gateway, plus the five chaincodes used by the app.

import { icons } from "../icons.js";
import { api } from "../api.js";
import { escape as esc, badge, fmtDate } from "../ui.js";

export default async function networkPage(root) {
  root.innerHTML = `
    <div class="content">
      <div class="row between">
        <p class="muted" style="margin:0">Checks the API, peer TCP reachability, gateway, channel, and the chaincodes this console uses.</p>
        <button class="btn btn-outline btn-sm" data-action="reload">${icons.refresh(16)} Run checks</button>
      </div>
      <div class="card">
        <div class="card-head"><div class="card-title">${icons.shield(16)} Fabric diagnostics</div></div>
        <div class="card-body" id="diag-body">
          <div class="loading"><span class="spin"></span><span>Checking peers and chaincodes…</span></div>
        </div>
      </div>
    </div>`;

  root.querySelector('[data-action="reload"]').addEventListener("click", () => networkPage(root));
  const body = root.querySelector("#diag-body");

  let health;
  try {
    health = await api.health();
  } catch (err) {
    body.innerHTML = `
      <div class="row" style="gap:10px;color:hsl(var(--destructive))">${icons.alert(20)}<b>API is offline</b></div>
      <p class="muted">${esc(err.message)}</p>`;
    return;
  }

  try {
    const diag = await api.diagnostics();
    body.innerHTML = renderDiagnostics(health, diag);
  } catch (err) {
    body.innerHTML = `
      <div class="grid-2">
        <div class="stat">
          <div class="stat-ic">${icons.check(18)}</div>
          <div class="stat-label">API</div>
          <div class="stat-value">online</div>
          <div class="stat-foot">${esc(health.service || "pakmedtrail-api")}</div>
        </div>
        <div class="stat">
          <div class="stat-ic">${icons.alert(18)}</div>
          <div class="stat-label">Diagnostics</div>
          <div class="stat-value">unavailable</div>
          <div class="stat-foot">${esc(err.message)}</div>
        </div>
      </div>
      <p class="muted" style="margin:14px 0 0">Your backend may not include the reviewed diagnostics route yet. Use the full reviewed package, or copy the updated <span class="mono">src/routes/fabric.js</span> and <span class="mono">src/services/fabricGateway.js</span>.</p>`;
  }
}

function renderDiagnostics(health, diag) {
  const orgs = Array.isArray(diag.orgs) ? diag.orgs : [];
  return `
    <div class="stat-grid">
      <div class="stat">
        <div class="stat-ic">${icons.check(18)}</div>
        <div class="stat-label">API</div>
        <div class="stat-value">online</div>
        <div class="stat-foot">${esc(health.service || "pakmedtrail-api")}</div>
      </div>
      <div class="stat">
        <div class="stat-ic">${diag.ok ? icons.check(18) : icons.alert(18)}</div>
        <div class="stat-label">Fabric</div>
        <div class="stat-value">${diag.ok ? "ready" : "attention"}</div>
        <div class="stat-foot">${esc(diag.scope || "current scope")}</div>
      </div>
      <div class="stat">
        <div class="stat-ic">${icons.clock(18)}</div>
        <div class="stat-label">Checked</div>
        <div class="stat-value" style="font-size:1rem">${esc(fmtDate(diag.generatedAt))}</div>
        <div class="stat-foot">channel checks below</div>
      </div>
    </div>

    <div class="col" style="gap:14px;margin-top:16px">
      ${orgs.map(renderOrg).join("") || `<p class="muted">No organisation diagnostics returned.</p>`}
    </div>`;
}

function renderOrg(org) {
  return `
    <div class="card" style="box-shadow:none">
      <div class="card-head">
        <div class="card-title">${org.ok ? icons.check(16) : icons.alert(16)} ${esc(org.org || "org")}</div>
        ${badge(org.ok ? "OK" : "FAIL", org.ok ? "badge-success" : "badge-danger")}
      </div>
      <div class="card-body">
        <div class="form-grid">
          <div class="field"><label>MSP</label><div class="mono">${esc(org.mspId || "not loaded")}</div></div>
          <div class="field"><label>Peer endpoint</label><div class="mono">${esc(org.peerEndpoint || "not loaded")}</div></div>
          <div class="field"><label>TLS host override</label><div class="mono">${esc(org.peerHostOverride || "not loaded")}</div></div>
          <div class="field"><label>Channel</label><div class="mono">${esc(org.channel || "")}</div></div>
        </div>
        ${renderPeerTcp(org.peerTcp)}
        ${org.message ? `<p class="muted" style="color:hsl(var(--destructive));margin:12px 0 0">${esc(org.message)}</p>` : ""}
        ${renderChecks(org.checks || [])}
      </div>
    </div>`;
}


function renderPeerTcp(peerTcp) {
  if (!peerTcp) {
    return `<div class="alert-box" style="margin-top:14px">Peer TCP probe was not returned by the API. Update the backend diagnostics route.</div>`;
  }
  return `
    <div class="alert-box ${peerTcp.ok ? "alert-good" : "alert-bad"}" style="margin-top:14px">
      <b>Peer TCP:</b> ${peerTcp.ok ? "reachable" : "not reachable"}
      <span class="mono">${esc(peerTcp.endpoint || "")}</span>
      <span class="muted">${esc(peerTcp.ms || 0)} ms</span>
      ${peerTcp.message ? `<div class="muted">${esc(peerTcp.message)}</div>` : ""}
    </div>`;
}

function renderChecks(checks) {
  if (!checks.length) return "";
  const rows = checks
    .map((c) => `
      <tr>
        <td><span class="mono">${esc(c.chaincode)}</span></td>
        <td><span class="mono">${esc(c.function)}</span></td>
        <td>${badge(c.ok ? "OK" : "FAIL", c.ok ? "badge-success" : "badge-danger")}</td>
        <td>${esc(c.ms || 0)} ms</td>
        <td>${c.count === null || c.count === undefined ? "—" : esc(c.count)}</td>
        <td>${c.ok ? "" : esc(c.message || "failed")}</td>
      </tr>`)
    .join("");
  return `
    <div class="table-wrap" style="margin-top:14px">
      <table class="data">
        <thead><tr><th>Chaincode</th><th>Function</th><th>Status</th><th>Time</th><th>Count</th><th>Message</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}
