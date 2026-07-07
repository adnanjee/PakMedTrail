// Account shows who you are signed in as, straight from the server, plus the
// registered user list when you are DRAP (the only role allowed to read it).

import { icons } from "../icons.js";
import { api } from "../api.js";
import { isRole } from "../store.js";
import { escape as esc, emptyBlock, fmtDate, mspLabel } from "../ui.js";
import { arr, badge } from "./_shared.js";

export default async function accountPage(root) {
  const canListUsers = isRole("drap");
  root.innerHTML = `
    <div class="content">
      <div class="card">
        <div class="card-head"><div class="card-title">${icons.users(16)} Your identity</div></div>
        <div class="card-body" id="me-body">
          <div class="loading"><span class="spin"></span><span>Loading…</span></div>
        </div>
      </div>
      ${canListUsers ? `
      <div class="card">
        <div class="card-head"><div class="card-title">${icons.users(16)} Registered users</div></div>
        <div class="card-body" id="users-body">
          <div class="loading"><span class="spin"></span><span>Loading users…</span></div>
        </div>
      </div>` : ""}
    </div>`;

  const meBody = root.querySelector("#me-body");
  try {
    const me = await api.me();
    meBody.innerHTML = `
      <div class="form-grid">
        <div class="field"><label>Username</label><div><b>${esc(me.username)}</b></div></div>
        <div class="field"><label>Role</label><div>${badge(me.role)}</div></div>
        <div class="field"><label>Organisation</label><div>${esc(mspLabel(me.org))} <span class="mono">(${esc(me.org)})</span></div></div>
      </div>`;
  } catch (err) {
    meBody.innerHTML = `<p class="muted" style="margin:0">Could not load your identity. ${esc(err.message)}</p>`;
  }

  if (canListUsers) {
    const usersBody = root.querySelector("#users-body");
    try {
      const users = arr(await api.listUsers(), "users");
      if (!users.length) {
        usersBody.innerHTML = emptyBlock(icons.users(22), "No users", "Nothing to show.");
      } else {
        usersBody.innerHTML = usersTable(users);
      }
    } catch (err) {
      usersBody.innerHTML = `<p class="muted" style="margin:0">Could not load users. ${esc(err.message)}</p>`;
    }
  }
}

function usersTable(users) {
  const rows = users
    .map(
      (u) => `
        <tr>
          <td><b>${esc(u.username)}</b></td>
          <td>${esc(mspLabel(u.org))}</td>
          <td>${badge(u.role)}</td>
          <td>${esc(fmtDate(u.createdAt))}</td>
        </tr>`
    )
    .join("");
  return `
    <div class="table-wrap">
      <table class="data">
        <thead><tr><th>Username</th><th>Organisation</th><th>Role</th><th>Created</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}
