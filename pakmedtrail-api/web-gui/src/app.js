// App bootstrap. Builds the shell once (sidebar + top bar + content area),
// wires global controls, then lets the router swap page content. When signed
// out, the whole screen becomes the login page.

import { icons } from "./icons.js";
import { api } from "./api.js";
import { getUser, isLoggedIn, clearSession, role } from "./store.js";
import { getApiBase, setApiBase, DEFAULT_API_BASE } from "./config.js";
import { toast, modal, escape, onClick } from "./ui.js";
import * as router from "./router.js";

import loginPage from "./pages/login.js";
import dashboardPage from "./pages/dashboard.js";
import lotsPage from "./pages/lots.js";
import batchesPage from "./pages/batches.js";
import formulationsPage from "./pages/formulations.js";
import shipmentsPage from "./pages/shipments.js";
import recallsPage from "./pages/recalls.js";
import verifyPage from "./pages/verify.js";
import accountPage from "./pages/account.js";
import networkPage from "./pages/network.js";

const NAV = [
  { name: "dashboard", label: "Dashboard", icon: "dashboard" },
  { name: "lots", label: "Lots", icon: "lots" },
  { name: "batches", label: "Batches", icon: "factory" },
  { name: "formulations", label: "Formulations", icon: "flask" },
  { name: "shipments", label: "Shipments", icon: "truck" },
  { name: "recalls", label: "Recalls", icon: "alert" },
  { name: "verify", label: "Verify", icon: "qr" },
  { name: "network", label: "Network", icon: "shield" },
  { name: "account", label: "Account", icon: "users" },
];

const PAGE_META = {
  dashboard: { title: "Dashboard", sub: "Live view of the medicine supply chain" },
  lots: { title: "Lots", sub: "Raw material lots created by suppliers" },
  batches: { title: "Batches", sub: "Drug batches produced by manufacturers" },
  formulations: { title: "Formulations", sub: "Recipes that define how a drug is made" },
  shipments: { title: "Shipments", sub: "Distribution, retail, and dispense records" },
  recalls: { title: "Recalls", sub: "DRAP recalls and quarantine status" },
  verify: { title: "Verify", sub: "Trace any lot, batch, shipment, or dispense by ID" },
  network: { title: "Network", sub: "Peer and chaincode diagnostics" },
  account: { title: "Account", sub: "Your identity and registered users" },
};

const PAGES = {
  dashboard: dashboardPage,
  lots: lotsPage,
  batches: batchesPage,
  formulations: formulationsPage,
  shipments: shipmentsPage,
  recalls: recallsPage,
  verify: verifyPage,
  network: networkPage,
  account: accountPage,
};

const THEME_KEY = "pmt_theme";

function getTheme() {
  try {
    return localStorage.getItem(THEME_KEY) || "light";
  } catch {
    return "light";
  }
}

function applyTheme(t) {
  document.documentElement.setAttribute("data-theme", t);
  try {
    localStorage.setItem(THEME_KEY, t);
  } catch {
    // ignore
  }
}

function initials(name) {
  return String(name || "?")
    .split(/[_\s.-]+/)
    .map((p) => p[0])
    .filter(Boolean)
    .slice(0, 2)
    .join("")
    .toUpperCase();
}

const root = document.getElementById("root");

function renderLogin() {
  document.body.classList.add("auth-mode");
  root.innerHTML = "";
  loginPage(root, () => boot());
}

function shellHtml() {
  const user = getUser();
  const navItems = NAV.map(
    (n) => `
      <a class="nav-item" data-nav="${n.name}" href="#/${n.name}">
        ${icons[n.icon](19)}<span>${n.label}</span>
      </a>`
  ).join("");

  return `
    <div class="scrim" id="scrim"></div>
    <div class="shell">
      <aside class="sidebar" id="sidebar">
        <div class="brand">
          <div class="brand-mark">${icons.pill(22)}</div>
          <div>
            <div class="brand-name">PakMedTrail</div>
            <div class="brand-sub">Console</div>
          </div>
        </div>
        <nav class="nav">${navItems}</nav>
        <div class="sidebar-foot">
          <div class="net-status" id="net-status">
            <span class="dot idle"></span><span>Checking network…</span>
          </div>
        </div>
      </aside>

      <div class="main">
        <header class="topbar">
          <div class="row" style="gap:14px">
            <button class="icon-btn menu-toggle" id="menu-toggle" aria-label="Menu">${icons.menu(18)}</button>
            <div class="page-title">
              <h1 id="page-title">Dashboard</h1>
              <p id="page-sub"></p>
            </div>
          </div>
          <div class="topbar-right">
            <button class="icon-btn" id="theme-toggle" title="Toggle theme">${icons.moon(18)}</button>
            <button class="icon-btn" id="api-settings" title="API settings">${icons.gear(18)}</button>
            <div class="who">
              <div class="avatar">${initials(user && user.username)}</div>
              <div class="who-meta">
                <b>${escape(user ? user.username : "")}</b>
                <span>${escape(user ? user.role : "")}</span>
              </div>
              <button class="icon-btn btn-ghost" id="logout" title="Sign out" style="border:none;width:32px;height:32px">${icons.logout(17)}</button>
            </div>
          </div>
        </header>
        <main id="content"></main>
      </div>
    </div>`;
}

function setActiveNav(name) {
  document.querySelectorAll(".nav-item").forEach((el) => {
    el.classList.toggle("active", el.getAttribute("data-nav") === name);
  });
  const meta = PAGE_META[name] || { title: name, sub: "" };
  const t = document.getElementById("page-title");
  const s = document.getElementById("page-sub");
  if (t) t.textContent = meta.title;
  if (s) s.textContent = meta.sub;
  // Close mobile drawer on navigate.
  document.getElementById("sidebar")?.classList.remove("open");
  document.getElementById("scrim")?.classList.remove("show");
}

let netTimer = null;

async function refreshNetStatus() {
  const node = document.getElementById("net-status");
  if (!node) return;
  try {
    await api.health();
    let chain = "API online";
    let cls = "live";
    try {
      await api.ping();
      chain = "Ledger live · API online";
    } catch {
      chain = "API online · ledger unreachable";
      cls = "idle";
    }
    node.innerHTML = `<span class="dot ${cls}"></span><span>${escape(chain)}</span>`;
  } catch {
    node.innerHTML = `<span class="dot down"></span><span>API offline</span>`;
  }
}

function openApiSettings() {
  modal({
    title: "API settings",
    body: `
      <div class="field">
        <label>API base URL</label>
        <input class="input" id="api-base-input" value="${escape(getApiBase())}" placeholder="${DEFAULT_API_BASE}" />
        <span class="hint">The PakMedTrail server address. Default is ${escape(DEFAULT_API_BASE)}.</span>
      </div>`,
    footer: `
      <button class="btn btn-outline" data-action="reset-base">Reset to default</button>
      <button class="btn btn-primary" data-action="save-base">Save</button>`,
    onMount: (el, close) => {
      onClick(el, "save-base", () => {
        const v = el.querySelector("#api-base-input").value.trim();
        setApiBase(v || DEFAULT_API_BASE);
        toast("API URL saved", "ok");
        close();
        refreshNetStatus();
        router.resolve();
      });
      onClick(el, "reset-base", () => {
        setApiBase("");
        el.querySelector("#api-base-input").value = DEFAULT_API_BASE;
        toast("Reset to default", "info");
      });
    },
  });
}

function renderShell() {
  document.body.classList.remove("auth-mode");
  root.innerHTML = shellHtml();

  // theme button reflects current theme
  const themeBtn = document.getElementById("theme-toggle");
  const syncThemeIcon = () => {
    const t = document.documentElement.getAttribute("data-theme");
    themeBtn.innerHTML = t === "dark" ? icons.sun(18) : icons.moon(18);
  };
  syncThemeIcon();
  themeBtn.addEventListener("click", () => {
    const t = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
    applyTheme(t);
    syncThemeIcon();
  });

  document.getElementById("api-settings").addEventListener("click", openApiSettings);

  document.getElementById("logout").addEventListener("click", () => {
    clearSession();
    if (netTimer) clearInterval(netTimer);
    location.hash = "/dashboard";
    boot();
  });

  const sidebar = document.getElementById("sidebar");
  const scrim = document.getElementById("scrim");
  document.getElementById("menu-toggle").addEventListener("click", () => {
    sidebar.classList.toggle("open");
    scrim.classList.toggle("show");
  });
  scrim.addEventListener("click", () => {
    sidebar.classList.remove("open");
    scrim.classList.remove("show");
  });

  router.setOnNavigate(setActiveNav);
  router.setNotFound(() => {
    const c = document.getElementById("content");
    if (c) c.innerHTML = `<div class="content"><div class="card"><div class="card-body">Page not found.</div></div></div>`;
  });

  // Register each route to mount its page into #content.
  Object.entries(PAGES).forEach(([name, pageFn]) => {
    router.register(name, async (params) => {
      const content = document.getElementById("content");
      if (!content) return;
      content.innerHTML = `<div class="content">${loadingCard()}</div>`;
      try {
        await pageFn(content, params);
      } catch (err) {
        content.innerHTML = `<div class="content">${errorCard(err)}</div>`;
      }
    });
  });

  refreshNetStatus();
  netTimer = setInterval(refreshNetStatus, 20000);

  router.start();
  router.resolve();
}

function loadingCard() {
  return `<div class="card"><div class="loading"><span class="spin"></span><span>Loading…</span></div></div>`;
}

function errorCard(err) {
  return `
    <div class="card">
      <div class="card-body">
        <div class="row" style="gap:10px;color:hsl(var(--destructive))">
          ${icons.alert(20)}<b>Could not load this page</b>
        </div>
        <p class="muted" style="margin-top:8px">${escape(err && err.message ? err.message : String(err))}</p>
      </div>
    </div>`;
}

export function boot() {
  applyTheme(getTheme());
  if (isLoggedIn()) renderShell();
  else renderLogin();
}

boot();
