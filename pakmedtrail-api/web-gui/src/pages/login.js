// Login screen. Split layout: a branded panel that names the supply chain on the
// left, the sign in form on the right. The five seeded accounts are offered as
// one tap fill buttons so the app is easy to demo. A register tab is included
// because the API exposes /api/auth/register.

import { icons } from "../icons.js";
import { api } from "../api.js";
import { setSession } from "../store.js";
import { toast, escape, qs, MSP_OPTIONS } from "../ui.js";

const DEMO_USERS = [
  { username: "supplier_admin", password: "supplier123", role: "supplier" },
  { username: "mfg_admin", password: "mfg123", role: "manufacturer" },
  { username: "dist_admin", password: "dist123", role: "distributor" },
  { username: "retail_admin", password: "retail123", role: "retailer" },
  { username: "drap_admin", password: "drap123", role: "drap" },
];

const FLOW = [
  { icon: "lots", label: "Supplier" },
  { icon: "factory", label: "Manufacturer" },
  { icon: "shield", label: "DRAP" },
  { icon: "truck", label: "Distributor" },
  { icon: "store", label: "Pharmacy" },
  { icon: "patient", label: "Patient" },
];

export default function loginPage(root, onDone) {
  let mode = "login";

  function art() {
    return `
      <div class="auth-art">
        <div class="brand" style="padding:0;color:#fff">
          <div class="brand-mark">${icons.pill(22)}</div>
          <div>
            <div class="brand-name" style="color:#fff">PakMedTrail</div>
            <div class="brand-sub" style="color:hsl(0 0% 100% / .7)">Blockchain drug traceability</div>
          </div>
        </div>
        <div>
          <h2>Track every medicine from raw lot to patient.</h2>
          <p>A permissioned ledger links suppliers, manufacturers, distributors, pharmacies, and the regulator. Every transfer, approval, and recall is recorded and can be traced.</p>
          <div class="auth-flow">
            ${FLOW.map((f) => `<span class="auth-chip">${icons[f.icon](15)}${f.label}</span>`).join("")}
          </div>
        </div>
        <div class="muted" style="color:hsl(0 0% 100% / .6);font-size:.78rem">
          NRPU Project 16777 · PakMedTrail
        </div>
      </div>`;
  }

  function loginForm() {
    return `
      <div class="col">
        <div>
          <h2 style="margin:0 0 4px;font-size:1.5rem;font-weight:800">Welcome back</h2>
          <p class="muted" style="margin:0">Sign in to the supply chain console.</p>
        </div>
        <div class="field">
          <label>Username</label>
          <input class="input" id="username" autocomplete="username" placeholder="e.g. mfg_admin" />
        </div>
        <div class="field">
          <label>Password</label>
          <input class="input" id="password" type="password" autocomplete="current-password" placeholder="••••••••" />
        </div>
        <button class="btn btn-primary" id="submit" style="width:100%">${icons.logout(17)} Sign in</button>
        <div class="divider">demo accounts</div>
        <div class="demo-users">
          ${DEMO_USERS.map(
            (u) => `
            <button class="demo-user" data-user="${u.username}">
              <b>${u.username}</b><span>${u.role}</span>
            </button>`
          ).join("")}
        </div>
        <p class="muted" style="margin:0;font-size:.78rem;text-align:center">
          No account? <a href="#" id="to-register" style="color:hsl(var(--primary));font-weight:600">Register one</a>
        </p>
      </div>`;
  }

  function registerForm() {
    return `
      <div class="col">
        <div>
          <h2 style="margin:0 0 4px;font-size:1.5rem;font-weight:800">Create account</h2>
          <p class="muted" style="margin:0">Register a console user against the API.</p>
        </div>
        <div class="field">
          <label>Username</label>
          <input class="input" id="r-username" placeholder="choose a username" />
        </div>
        <div class="field">
          <label>Password</label>
          <input class="input" id="r-password" type="password" placeholder="choose a password" />
        </div>
        <div class="field">
          <label>Organisation and role</label>
          <select class="select" id="r-org">
            ${MSP_OPTIONS.map((m) => `<option value="${m.value}">${m.label} (${m.value})</option>`).join("")}
          </select>
          <span class="hint">Role is set to match the organisation you pick.</span>
        </div>
        <button class="btn btn-primary" id="r-submit" style="width:100%">${icons.plus(17)} Create account</button>
        <p class="muted" style="margin:0;font-size:.78rem;text-align:center">
          Already have one? <a href="#" id="to-login" style="color:hsl(var(--primary));font-weight:600">Sign in</a>
        </p>
      </div>`;
  }

  function render() {
    root.innerHTML = `
      <div class="auth-wrap">
        ${art()}
        <div class="auth-form-side">
          <div class="auth-card">
            ${mode === "login" ? loginForm() : registerForm()}
          </div>
        </div>
      </div>`;
    bind();
  }

  async function doLogin(username, password) {
    const btn = qs("#submit");
    if (btn) {
      btn.disabled = true;
      btn.innerHTML = `<span class="spin" style="border-top-color:#fff;border-color:rgba(255,255,255,.4)"></span> Signing in…`;
    }
    try {
      const res = await api.login(username, password);
      setSession(res.token, res.user);
      toast(`Signed in as ${res.user.username}`, "ok");
      onDone();
    } catch (err) {
      toast(err.message, "err");
      if (btn) {
        btn.disabled = false;
        btn.innerHTML = `${icons.logout(17)} Sign in`;
      }
    }
  }

  function bind() {
    if (mode === "login") {
      qs("#submit").addEventListener("click", () => {
        const u = qs("#username").value.trim();
        const p = qs("#password").value;
        if (!u || !p) return toast("Enter a username and password", "err", "Missing details");
        doLogin(u, p);
      });
      qs("#password").addEventListener("keydown", (e) => {
        if (e.key === "Enter") qs("#submit").click();
      });
      root.querySelectorAll("[data-user]").forEach((b) => {
        b.addEventListener("click", () => {
          const hit = DEMO_USERS.find((d) => d.username === b.getAttribute("data-user"));
          qs("#username").value = hit.username;
          qs("#password").value = hit.password;
          doLogin(hit.username, hit.password);
        });
      });
      qs("#to-register").addEventListener("click", (e) => {
        e.preventDefault();
        mode = "register";
        render();
      });
    } else {
      qs("#r-submit").addEventListener("click", async () => {
        const username = qs("#r-username").value.trim();
        const password = qs("#r-password").value;
        const org = qs("#r-org").value;
        const roleMap = {
          supplierMSP: "supplier",
          manufacturerMSP: "manufacturer",
          distributorMSP: "distributor",
          retailerMSP: "retailer",
          drapMSP: "drap",
        };
        if (!username || !password) return toast("Enter a username and password", "err", "Missing details");
        try {
          await api.register({ username, password, org, role: roleMap[org] });
          toast("Account created. You can sign in now.", "ok");
          mode = "login";
          render();
          qs("#username").value = username;
        } catch (err) {
          toast(err.message, "err");
        }
      });
      qs("#to-login").addEventListener("click", (e) => {
        e.preventDefault();
        mode = "login";
        render();
      });
    }
  }

  render();
}
