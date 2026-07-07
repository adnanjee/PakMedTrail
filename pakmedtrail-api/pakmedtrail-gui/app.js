/* PakMedTrail Chain Tracker GUI
   Standalone frontend for pakmedtrail-api.
   It does not modify the backend. It only calls the public REST endpoints.
*/
(function () {
  const DEFAULT_API_BASE = localStorage.getItem("pmt_api_base") || "http://localhost:4000";
  const TOKEN_KEY = "pmt_token";
  const USER_KEY = "pmt_user";
  const THEME_KEY = "pmt_theme";

  const ROLES = [
    { role: "supplier", org: "supplierMSP", label: "Supplier", username: "supplier_admin", password: "supplier123", icon: "🧪" },
    { role: "manufacturer", org: "manufacturerMSP", label: "Manufacturer", username: "mfg_admin", password: "mfg123", icon: "🏭" },
    { role: "distributor", org: "distributorMSP", label: "Distributor", username: "dist_admin", password: "dist123", icon: "🚚" },
    { role: "retailer", org: "retailerMSP", label: "Retailer", username: "retail_admin", password: "retail123", icon: "💊" },
    { role: "drap", org: "drapMSP", label: "DRAP", username: "drap_admin", password: "drap123", icon: "🛡️" },
  ];

  const ORGS = ["supplierMSP", "manufacturerMSP", "distributorMSP", "retailerMSP", "drapMSP"];

  const NAV = [
    { path: "dashboard", label: "Dashboard", icon: "📊", roles: ["supplier", "manufacturer", "distributor", "retailer", "drap"] },
    { path: "lots", label: "Raw Lots", icon: "🧪", roles: ["supplier", "manufacturer", "drap"] },
    { path: "batches", label: "Batches", icon: "🏭", roles: ["manufacturer", "distributor", "retailer", "drap"] },
    { path: "shipments", label: "Shipments", icon: "🚚", roles: ["manufacturer", "distributor", "retailer", "drap"] },
    { path: "recalls", label: "Recalls", icon: "🚨", roles: ["supplier", "manufacturer", "distributor", "retailer", "drap"] },
    { path: "verify", label: "Verify", icon: "🔎", roles: ["supplier", "manufacturer", "distributor", "retailer", "drap"] },
    { path: "users", label: "Users", icon: "👥", roles: ["drap"] },
  ];

  const state = {
    apiBase: DEFAULT_API_BASE,
    token: localStorage.getItem(TOKEN_KEY) || "",
    user: safeParse(localStorage.getItem(USER_KEY), null),
    route: getRoute(),
    lastHealth: null,
    sidebarOpen: false,
    cache: { lots: [], batches: [], distShipments: [], retailShipments: [], recalls: [], formulations: [] },
  };

  const app = document.getElementById("app");
  const rootTheme = () => document.documentElement.setAttribute("data-theme", localStorage.getItem(THEME_KEY) || "light");
  rootTheme();

  class ApiClient {
    constructor(getBase, getToken) {
      this.getBase = getBase;
      this.getToken = getToken;
    }

    url(path) {
      const base = String(this.getBase() || "").replace(/\/+$/, "");
      return `${base}${path.startsWith("/") ? path : `/${path}`}`;
    }

    async request(path, opts = {}) {
      const headers = Object.assign({ Accept: "application/json" }, opts.headers || {});
      const token = this.getToken();
      if (token) headers.Authorization = `Bearer ${token}`;
      if (opts.body !== undefined && !(opts.body instanceof FormData)) {
        headers["Content-Type"] = "application/json";
        opts.body = JSON.stringify(opts.body);
      }
      let response;
      let payload;
      try {
        response = await fetch(this.url(path), Object.assign({}, opts, { headers }));
      } catch (err) {
        throw new Error(`Cannot reach API server at ${this.getBase()}. ${err.message || err}`);
      }
      const text = await response.text();
      try { payload = text ? JSON.parse(text) : null; } catch { payload = text; }
      if (!response.ok) {
        const msg = payload && typeof payload === "object"
          ? payload.message || payload.error || JSON.stringify(payload)
          : payload || response.statusText;
        const e = new Error(msg);
        e.status = response.status;
        e.payload = payload;
        throw e;
      }
      return payload;
    }

    health() { return this.request("/health"); }
    login(username, password) { return this.request("/api/auth/login", { method: "POST", body: { username, password } }); }
    register(body) { return this.request("/api/auth/register", { method: "POST", body }); }
    me() { return this.request("/api/auth/me"); }
    users() { return this.request("/api/auth/users"); }
    fabricPing() { return this.request("/api/fabric/ping"); }

    lots() { return this.request("/api/lots"); }
    lot(id) { return this.request(`/api/lots/${encodeURIComponent(id)}`); }
    createLot(body) { return this.request("/api/lots", { method: "POST", body }); }
    approveLot(id, note) { return this.request(`/api/lots/${encodeURIComponent(id)}/drap-approve`, { method: "POST", body: { note } }); }
    proposeLotTransfer(id, proposedOwnerMSP) { return this.request(`/api/lots/${encodeURIComponent(id)}/propose-transfer`, { method: "POST", body: { proposedOwnerMSP } }); }
    acceptLotTransfer(id) { return this.request(`/api/lots/${encodeURIComponent(id)}/accept-transfer`, { method: "POST", body: {} }); }

    formulations() { return this.request("/api/batches/formulations"); }
    formulation(drugCode) { return this.request(`/api/batches/formulations/${encodeURIComponent(drugCode)}`); }
    createFormulation(body) { return this.request("/api/batches/formulations", { method: "POST", body }); }
    batches(owner) { return this.request(owner ? `/api/batches?owner=${encodeURIComponent(owner)}` : "/api/batches"); }
    batch(id) { return this.request(`/api/batches/${encodeURIComponent(id)}`); }
    produceBatch(body) { return this.request("/api/batches", { method: "POST", body }); }
    approveBatch(id, note) { return this.request(`/api/batches/${encodeURIComponent(id)}/drap-approve`, { method: "POST", body: { note } }); }
    proposeBatchTransfer(id, proposedOwnerMSP) { return this.request(`/api/batches/${encodeURIComponent(id)}/propose-transfer`, { method: "POST", body: { proposedOwnerMSP } }); }
    acceptBatchTransfer(id) { return this.request(`/api/batches/${encodeURIComponent(id)}/accept-transfer`, { method: "POST", body: {} }); }

    distributionShipments(party) { return this.request(party ? `/api/shipments/distribution?party=${encodeURIComponent(party)}` : "/api/shipments/distribution"); }
    distributionShipment(id) { return this.request(`/api/shipments/distribution/${encodeURIComponent(id)}`); }
    createDistributionShipment(body) { return this.request("/api/shipments/distribution", { method: "POST", body }); }
    acceptDistributionShipment(id) { return this.request(`/api/shipments/distribution/${encodeURIComponent(id)}/accept`, { method: "POST", body: {} }); }
    deliverDistributionShipment(id) { return this.request(`/api/shipments/distribution/${encodeURIComponent(id)}/deliver`, { method: "POST", body: {} }); }

    retailShipments(party) { return this.request(party ? `/api/shipments/retail?party=${encodeURIComponent(party)}` : "/api/shipments/retail"); }
    retailShipment(id) { return this.request(`/api/shipments/retail/${encodeURIComponent(id)}`); }
    createRetailShipment(body) { return this.request("/api/shipments/retail", { method: "POST", body }); }
    acceptRetailShipment(id) { return this.request(`/api/shipments/retail/${encodeURIComponent(id)}/accept`, { method: "POST", body: {} }); }
    deliverRetailShipment(id) { return this.request(`/api/shipments/retail/${encodeURIComponent(id)}/deliver`, { method: "POST", body: {} }); }
    dispense(body) { return this.request("/api/shipments/dispense", { method: "POST", body }); }
    readDispense(id) { return this.request(`/api/shipments/dispense/${encodeURIComponent(id)}`); }

    recalls() { return this.request("/api/recalls/active"); }
    recall(id) { return this.request(`/api/recalls/${encodeURIComponent(id)}`); }
    createRecall(body) { return this.request("/api/recalls", { method: "POST", body }); }
    addAffectedAssets(id, body) { return this.request(`/api/recalls/${encodeURIComponent(id)}/affected-assets`, { method: "POST", body }); }
    acknowledgeRecall(id, note) { return this.request(`/api/recalls/${encodeURIComponent(id)}/acknowledge`, { method: "POST", body: { note } }); }
    quarantineAsset(recallId, body) { return this.request(`/api/recalls/${encodeURIComponent(recallId)}/quarantine`, { method: "POST", body }); }
    closeRecall(id, note) { return this.request(`/api/recalls/${encodeURIComponent(id)}/close`, { method: "POST", body: { note } }); }
    activeCheck(assetType, assetId) { return this.request(`/api/recalls/active-check/${encodeURIComponent(assetType)}/${encodeURIComponent(assetId)}`); }
    getQuarantine(assetType, assetId) { return this.request(`/api/recalls/quarantine/${encodeURIComponent(assetType)}/${encodeURIComponent(assetId)}`); }
    clearQuarantine(assetType, assetId) { return this.request(`/api/recalls/quarantine/${encodeURIComponent(assetType)}/${encodeURIComponent(assetId)}/clear`, { method: "POST", body: {} }); }
  }

  const api = new ApiClient(() => state.apiBase, () => state.token);

  window.addEventListener("hashchange", () => {
    state.route = getRoute();
    state.sidebarOpen = false;
    render();
  });

  document.addEventListener("click", (event) => {
    const action = event.target.closest("[data-action]");
    if (!action) return;
    const name = action.getAttribute("data-action");
    const args = action.getAttribute("data-args");
    handlers[name]?.(action, args ? safeParse(decodeURIComponent(args), {}) : {});
  });

  document.addEventListener("submit", (event) => {
    const form = event.target.closest("form[data-submit]");
    if (!form) return;
    event.preventDefault();
    const name = form.getAttribute("data-submit");
    handlers[name]?.(form, formToObject(form));
  });

  const handlers = {
    setRoute: (_, args) => go(args.path),
    toggleTheme: () => {
      const next = (localStorage.getItem(THEME_KEY) || "light") === "light" ? "dark" : "light";
      localStorage.setItem(THEME_KEY, next);
      rootTheme();
      render();
    },
    toggleSidebar: () => { state.sidebarOpen = !state.sidebarOpen; render(); },
    logout: () => {
      state.token = "";
      state.user = null;
      localStorage.removeItem(TOKEN_KEY);
      localStorage.removeItem(USER_KEY);
      go("login");
    },
    saveApiBase: () => {
      const input = document.getElementById("apiBaseInput");
      if (!input) return;
      state.apiBase = input.value.trim().replace(/\/+$/, "") || DEFAULT_API_BASE;
      localStorage.setItem("pmt_api_base", state.apiBase);
      toast("API server updated", state.apiBase, "good");
      checkHealth(false);
    },
    loginQuick: (_, args) => {
      const user = ROLES.find(r => r.role === args.role);
      if (!user) return;
      const username = document.getElementById("loginUsername");
      const password = document.getElementById("loginPassword");
      if (username) username.value = user.username;
      if (password) password.value = user.password;
    },
    login: async (form, data) => busy(form, async () => {
      const res = await api.login(data.username, data.password);
      setAuth(res.token, res.user);
      toast("Login successful", `${res.user.username} connected as ${res.user.role}.`, "good");
      go("dashboard");
    }),
    register: async (form, data) => busy(form, async () => {
      await api.register(data);
      toast("User registered", `${data.username} can now log in.`, "good");
      form.reset();
    }),
    refreshDashboard: () => loadDashboard(true),
    showJSON: (_, args) => showDetail(args.title || "Ledger object", args.data || {}),
    readLot: async (_, args) => runAction("Lot loaded", async () => showDetail(`Lot ${args.id}`, await api.lot(args.id))),
    readLotManual: async () => {
      const id = document.getElementById("manualLotId")?.value?.trim();
      if (!id) return toast("Lot ID required", "Enter a lot ID first.", "warn");
      await handlers.readLot(null, { id });
    },
    createLot: async (form, data) => busy(form, async () => {
      data.quantity = numberOrString(data.quantity);
      data.metadata = parseMetadata(data.metadata);
      const res = await api.createLot(data);
      toast("Lot created", data.lotId, "good");
      showDetail("Created lot", res);
      await refreshCurrentPage();
      form.reset();
    }),
    approveLot: async (_, args) => promptRun("Approve lot", "Approval note", async note => api.approveLot(args.id, note), "Lot approved"),
    proposeLotTransfer: async (_, args) => selectRun("Propose lot transfer", "Send to MSP", ["manufacturerMSP"], async msp => api.proposeLotTransfer(args.id, msp), "Transfer proposed"),
    acceptLotTransfer: async (_, args) => runAction("Lot transfer accepted", async () => api.acceptLotTransfer(args.id)),

    createFormulation: async (form, data) => busy(form, async () => {
      data.requirements = parseRequirements(data.requirements);
      const res = await api.createFormulation(data);
      toast("Formulation created", data.drugCode, "good");
      showDetail("Created formulation", res);
      await refreshCurrentPage();
      form.reset();
    }),
    readFormulation: async (_, args) => runAction("Formulation loaded", async () => showDetail(`Formulation ${args.id}`, await api.formulation(args.id))),
    loadBatchesOwner: async () => {
      const owner = document.getElementById("batchOwnerFilter")?.value || state.user?.org || "";
      await loadBatchesPage(owner);
    },
    produceBatch: async (form, data) => busy(form, async () => {
      data.outputQuantity = numberOrString(data.outputQuantity);
      data.inputs = parseInputs(data.inputs);
      const res = await api.produceBatch(data);
      toast("Batch produced", data.batchId, "good");
      showDetail("Produced batch", res);
      await refreshCurrentPage();
      form.reset();
    }),
    approveBatch: async (_, args) => promptRun("Approve batch", "Approval note", async note => api.approveBatch(args.id, note), "Batch approved"),
    proposeBatchTransfer: async (_, args) => selectRun("Propose batch transfer", "Send to MSP", ["distributorMSP"], async msp => api.proposeBatchTransfer(args.id, msp), "Batch transfer proposed"),
    acceptBatchTransfer: async (_, args) => runAction("Batch transfer accepted", async () => api.acceptBatchTransfer(args.id)),
    readBatch: async (_, args) => runAction("Batch loaded", async () => showDetail(`Batch ${args.id}`, await api.batch(args.id))),

    createDistributionShipment: async (form, data) => busy(form, async () => {
      data.quantity = numberOrString(data.quantity);
      data.metadata = parseMetadata(data.metadata);
      const res = await api.createDistributionShipment(data);
      toast("Distribution shipment created", data.shipmentId, "good");
      showDetail("Distribution shipment", res);
      await refreshCurrentPage();
      form.reset();
    }),
    createRetailShipment: async (form, data) => busy(form, async () => {
      data.quantity = numberOrString(data.quantity);
      data.metadata = parseMetadata(data.metadata);
      const res = await api.createRetailShipment(data);
      toast("Retail shipment created", data.shipmentId, "good");
      showDetail("Retail shipment", res);
      await refreshCurrentPage();
      form.reset();
    }),
    acceptDistributionShipment: async (_, args) => runAction("Distribution shipment accepted", async () => api.acceptDistributionShipment(args.id)),
    deliverDistributionShipment: async (_, args) => runAction("Distribution shipment delivered", async () => api.deliverDistributionShipment(args.id)),
    readDistributionShipment: async (_, args) => runAction("Shipment loaded", async () => showDetail(`Distribution shipment ${args.id}`, await api.distributionShipment(args.id))),
    acceptRetailShipment: async (_, args) => runAction("Retail shipment accepted", async () => api.acceptRetailShipment(args.id)),
    deliverRetailShipment: async (_, args) => runAction("Retail shipment delivered", async () => api.deliverRetailShipment(args.id)),
    readRetailShipment: async (_, args) => runAction("Shipment loaded", async () => showDetail(`Retail shipment ${args.id}`, await api.retailShipment(args.id))),
    dispense: async (form, data) => busy(form, async () => {
      data.quantity = numberOrString(data.quantity);
      data.metadata = parseMetadata(data.metadata);
      const res = await api.dispense(data);
      toast("Dispense verified", data.dispenseId, "good");
      showDetail("Dispense record", res);
      form.reset();
    }),
    readDispense: async (_, args) => promptRun("Read dispense", "Dispense ID", async id => api.readDispense(id), "Dispense loaded"),

    createRecall: async (form, data) => busy(form, async () => {
      data.directives = parseDirectives(data.directives);
      const res = await api.createRecall(data);
      toast("Recall initiated", data.recallId, "good");
      showDetail("Recall", res);
      await refreshCurrentPage();
      form.reset();
    }),
    addAffectedAssets: async (form, data) => busy(form, async () => {
      const recallId = data.recallId;
      delete data.recallId;
      data.assetIds = String(data.assetIds || "").split(",").map(x => x.trim()).filter(Boolean);
      const res = await api.addAffectedAssets(recallId, data);
      toast("Affected assets added", recallId, "good");
      showDetail("Affected asset result", res);
      form.reset();
    }),
    acknowledgeRecall: async (_, args) => promptRun("Acknowledge recall", "Note", async note => api.acknowledgeRecall(args.id, note), "Recall acknowledged"),
    closeRecall: async (_, args) => promptRun("Close recall", "Closure note", async note => api.closeRecall(args.id, note), "Recall closed"),
    readRecall: async (_, args) => runAction("Recall loaded", async () => showDetail(`Recall ${args.id}`, await api.recall(args.id))),
    quarantineAsset: async (form, data) => busy(form, async () => {
      const recallId = data.recallId;
      delete data.recallId;
      const res = await api.quarantineAsset(recallId, data);
      toast("Asset quarantined", `${data.assetType || "BATCH"} ${data.assetId}`, "good");
      showDetail("Quarantine", res);
      form.reset();
    }),
    verifyAsset: async (form, data) => busy(form, async () => {
      const out = {};
      if (data.assetKind === "lot") out.lot = await api.lot(data.assetId);
      if (data.assetKind === "batch") out.batch = await api.batch(data.assetId);
      if (data.assetKind === "distributionShipment") out.shipment = await api.distributionShipment(data.assetId);
      if (data.assetKind === "retailShipment") out.shipment = await api.retailShipment(data.assetId);
      if (data.assetKind === "recall") out.recall = await api.recall(data.assetId);
      showDetail("Verification result", out);
      toast("Verification complete", data.assetId, "good");
    }),
    recallCheck: async (form, data) => busy(form, async () => {
      const result = await api.activeCheck(data.assetType, data.assetId);
      showDetail("Recall active check", result);
      toast("Recall check complete", `${data.assetType} ${data.assetId}`, result.active ? "warn" : "good");
    }),
    quarantineCheck: async (form, data) => busy(form, async () => {
      const result = await api.getQuarantine(data.assetType, data.assetId);
      showDetail("Quarantine record", result);
      toast("Quarantine query complete", `${data.assetType} ${data.assetId}`, "good");
    }),
    clearQuarantine: async (form, data) => busy(form, async () => {
      const result = await api.clearQuarantine(data.assetType, data.assetId);
      showDetail("Clear quarantine result", result);
      toast("Quarantine cleared", `${data.assetType} ${data.assetId}`, "good");
    }),
    clearQuarantineFromForm: async (button) => {
      const form = button.closest("form");
      if (!form) return;
      const data = formToObject(form);
      await handlers.clearQuarantine(form, data);
    },
  };

  function setAuth(token, user) {
    state.token = token;
    state.user = user;
    localStorage.setItem(TOKEN_KEY, token);
    localStorage.setItem(USER_KEY, JSON.stringify(user));
  }

  function getRoute() {
    const raw = location.hash.replace(/^#\/?/, "").trim();
    return raw || (localStorage.getItem(TOKEN_KEY) ? "dashboard" : "login");
  }

  function go(path) {
    location.hash = `#/${path}`;
  }

  function render() {
    if (!state.token || state.route === "login") {
      app.innerHTML = loginPage();
      setTimeout(() => checkHealth(false), 10);
      return;
    }
    if (!isAllowed(state.route)) {
      state.route = "dashboard";
      location.hash = "#/dashboard";
      return;
    }
    app.innerHTML = shell(pageHtml());
    setTimeout(() => {
      checkHealth(false);
      mountRouteData();
    }, 10);
  }

  function isAllowed(route) {
    const item = NAV.find(x => x.path === route);
    if (!item) return true;
    return item.roles.includes(state.user?.role);
  }

  function shell(content) {
    const nav = NAV.filter(item => item.roles.includes(state.user?.role)).map(item => `
      <button class="nav-item ${state.route === item.path ? "active" : ""}" data-action="setRoute" data-args="${args({ path: item.path })}">
        <span class="ico">${item.icon}</span><span>${esc(item.label)}</span>
      </button>
    `).join("");
    const healthClass = state.lastHealth?.ok ? "ok" : state.lastHealth?.ok === false ? "bad" : "";
    const healthText = state.lastHealth?.ok ? "API online" : state.lastHealth?.ok === false ? "API offline" : "Checking API";
    return `
      <div class="app-shell">
        <aside class="sidebar ${state.sidebarOpen ? "open" : ""}">
          <div class="brand">
            <div class="logo"><span>✓</span></div>
            <div><h1>PakMedTrail</h1><p>Fabric chain tracker GUI</p></div>
          </div>
          <nav class="nav">${nav}</nav>
          <div class="side-footer">
            <div class="status-line"><span id="healthDot" class="dot ${healthClass}"></span><span id="healthText">${healthText}</span></div>
            <strong>${esc(roleLabel(state.user?.role))}</strong>
            <small>${esc(state.user?.username || "")} · ${esc(state.user?.org || "")}</small>
          </div>
        </aside>
        <main class="main">
          <header class="topbar">
            <button class="btn mobile-menu" data-action="toggleSidebar">☰ Menu</button>
            <div class="topbar-left">
              <div class="eyebrow">${esc(routeEyebrow())}</div>
              <h2>${esc(routeTitle())}</h2>
            </div>
            <div class="top-actions">
              <div class="api-box">
                <label>API</label>
                <input id="apiBaseInput" value="${escAttr(state.apiBase)}" spellcheck="false" />
                <button class="btn small" data-action="saveApiBase">Save</button>
              </div>
              <button class="btn" data-action="toggleTheme">${(localStorage.getItem(THEME_KEY) || "light") === "light" ? "🌙" : "☀️"}</button>
              <button class="btn ghost" data-action="logout">Logout</button>
            </div>
          </header>
          ${content}
        </main>
      </div>
      <div id="detailPanel" class="detail-panel card"></div>
      <div id="toastWrap" class="toast-wrap"></div>
    `;
  }

  function pageHtml() {
    switch (state.route) {
      case "lots": return lotsPage();
      case "batches": return batchesPage();
      case "shipments": return shipmentsPage();
      case "recalls": return recallsPage();
      case "verify": return verifyPage();
      case "users": return usersPage();
      default: return dashboardPage();
    }
  }

  function routeTitle() {
    const map = {
      dashboard: "Supply chain command center",
      lots: "Raw material lots",
      batches: "Formulations and drug batches",
      shipments: "Distribution and retail shipments",
      recalls: "Recall and quarantine control",
      verify: "Verify ledger asset",
      users: "Registered system users",
      login: "Login",
    };
    return map[state.route] || "PakMedTrail";
  }

  function routeEyebrow() {
    return `PakMedTrail · ${state.user?.org || "REST API"}`;
  }

  function loginPage() {
    return `
      <div class="login-wrap">
        <section class="login-visual">
          <div class="brand" style="border:0;padding:0;margin-bottom:24px">
            <div class="logo"><span>✓</span></div>
            <div><h1>PakMedTrail</h1><p>Hyperledger Fabric medicine traceability</p></div>
          </div>
          <h1 class="login-title">Track every medicine movement with <span class="gradient-text">ledger proof.</span></h1>
          <p class="hero p" style="max-width:680px;color:var(--muted);line-height:1.75">This frontend is wired to your real PakMedTrail API. It keeps the PharmaChain look, but it uses your actual lots, batches, shipments, dispense, DRAP approval, and recall endpoints.</p>
          <div class="card" style="margin-top:28px;max-width:760px">
            <div class="pipeline">
              ${pipelineNodes()}
            </div>
          </div>
        </section>
        <section class="login-card">
          <div class="login-panel card">
            <div class="card-title">
              <div><h3>Sign in to API</h3><p>Use seeded demo users or your registered user.</p></div>
              <button class="btn" data-action="toggleTheme">${(localStorage.getItem(THEME_KEY) || "light") === "light" ? "🌙" : "☀️"}</button>
            </div>
            <div class="api-box" style="width:100%;margin-bottom:14px">
              <label>API</label><input id="apiBaseInput" value="${escAttr(state.apiBase)}" /><button class="btn small" data-action="saveApiBase">Save</button>
            </div>
            <form class="form" data-submit="login">
              <div class="field"><label>Username</label><input id="loginUsername" class="input" name="username" required placeholder="mfg_admin" /></div>
              <div class="field"><label>Password</label><input id="loginPassword" class="input" name="password" type="password" required placeholder="mfg123" /></div>
              <button class="btn primary" type="submit">Login</button>
            </form>
            <div class="quick-users">
              ${ROLES.map(r => `<button class="btn small" data-action="loginQuick" data-args="${args({ role: r.role })}">${r.icon} ${r.label}</button>`).join("")}
            </div>
            <hr style="border:0;border-top:1px solid var(--line);margin:18px 0" />
            <details>
              <summary class="badge blue" style="cursor:pointer">Register new local API user</summary>
              <form class="form" data-submit="register" style="margin-top:14px">
                <div class="form-row">
                  <div class="field"><label>Username</label><input class="input" name="username" required /></div>
                  <div class="field"><label>Password</label><input class="input" name="password" type="password" required /></div>
                </div>
                <div class="form-row">
                  <div class="field"><label>Organization MSP</label>${select("org", ORGS)}</div>
                  <div class="field"><label>Role</label>${select("role", ROLES.map(r => r.role))}</div>
                </div>
                <p class="help">The backend stores these users in memory. Restarting the API resets them to the seeded demo users.</p>
                <button class="btn green" type="submit">Create user</button>
              </form>
            </details>
          </div>
        </section>
      </div>
      <div id="toastWrap" class="toast-wrap"></div>
    `;
  }

  function dashboardPage() {
    return `
      <section class="page" id="dashboardPage">
        <div class="hero">
          <div class="eyebrow">Live ledger console</div>
          <h1>One clean screen for lots, batches, DRAP approvals, transfers, shipments, and recalls.</h1>
          <p>The old tracker design was attractive, but its API model was wrong for your backend. This GUI follows your actual PakMedTrail workflow and keeps the same glassmorphism supply chain style.</p>
          <div class="hero-actions">
            <button class="btn primary" data-action="refreshDashboard">Refresh network data</button>
            <button class="btn" data-action="setRoute" data-args="${args({ path: "verify" })}">Verify asset</button>
          </div>
        </div>
        <div class="grid four" id="metricGrid">${metricSkeleton()}</div>
        <div class="card">
          <div class="card-title"><div><h3>Supply chain pipeline</h3><p>Mapped to your API roles and MSP identities.</p></div><span class="badge blue">${esc(state.user?.org || "")}</span></div>
          <div class="pipeline">${pipelineNodes()}</div>
        </div>
        <div class="grid two">
          <div class="card" id="attentionCard">${empty("Loading DRAP and transfer attention items...")}</div>
          <div class="card" id="fabricCard">${empty("Checking Fabric ping...")}</div>
        </div>
      </section>
    `;
  }

  function lotsPage() {
    return `
      <section class="page">
        <div class="grid sidebar-main">
          <div class="grid">
            ${roleGate("supplier", createLotForm())}
            <div class="card">
              <div class="card-title"><div><h3>Read lot by ID</h3><p>Direct lookup through /api/lots/:lotId.</p></div></div>
              <form class="form" onsubmit="event.preventDefault(); document.querySelector('[data-action=readLotManual]').click();">
                <div class="field"><label>Lot ID</label><input class="input" id="manualLotId" placeholder="LOT-001" /></div>
                <button type="button" class="btn" data-action="readLotManual">Read lot</button>
              </form>
            </div>
          </div>
          <div class="card">
            <div class="card-title"><div><h3>Ledger lots</h3><p>Loaded from /api/lots with the current Fabric identity.</p></div><button class="btn small" onclick="loadLotsPage()">Refresh</button></div>
            <div id="lotsTable">${empty("Loading lots...")}</div>
          </div>
        </div>
      </section>
    `;
  }

  function createLotForm() {
    return `
      <div class="card">
        <div class="card-title"><div><h3>Create raw material lot</h3><p>Supplier only. Submits CreateLot through the API.</p></div></div>
        <form class="form" data-submit="createLot">
          <div class="form-row">
            <div class="field"><label>Lot ID</label><input class="input" name="lotId" required placeholder="LOT-API-001" /></div>
            <div class="field"><label>Name</label><input class="input" name="name" required placeholder="Paracetamol API" /></div>
          </div>
          <div class="form-row">
            <div class="field"><label>Batch number</label><input class="input" name="batchNumber" placeholder="BN-001" /></div>
            <div class="field"><label>Unit</label><input class="input" name="unit" required placeholder="kg" /></div>
          </div>
          <div class="form-row">
            <div class="field"><label>Quantity</label><input class="input" name="quantity" required type="number" step="any" /></div>
            <div class="field"><label>Manufacture date</label><input class="input" name="manufactureDate" type="date" /></div>
          </div>
          <div class="field"><label>Expiry date</label><input class="input" name="expiryDate" type="date" /></div>
          <div class="field"><label>Metadata JSON</label><textarea class="textarea" name="metadata" placeholder='{"origin":"Pakistan","certificate":"COA-001"}'></textarea></div>
          <button class="btn primary" type="submit">Create lot</button>
        </form>
      </div>
    `;
  }

  function batchesPage() {
    return `
      <section class="page">
        <div class="grid sidebar-main">
          <div class="grid">
            ${roleGate("manufacturer", createFormulationForm())}
            ${roleGate("manufacturer", produceBatchForm())}
            <div class="card">
              <div class="card-title"><div><h3>Owner filter</h3><p>GET /api/batches?owner=MSP.</p></div></div>
              <div class="form">
                <div class="field"><label>Owner MSP</label>${select("owner", ORGS, state.user?.org || "", "batchOwnerFilter")}</div>
                <button class="btn" data-action="loadBatchesOwner">Load batches</button>
              </div>
            </div>
          </div>
          <div class="grid">
            <div class="card"><div class="card-title"><div><h3>Formulations</h3><p>Drug formulation registry.</p></div><button class="btn small" onclick="loadBatchesPage()">Refresh</button></div><div id="formulationsTable">${empty("Loading formulations...")}</div></div>
            <div class="card"><div class="card-title"><div><h3>Drug batches</h3><p>Batches owned by the selected MSP.</p></div></div><div id="batchesTable">${empty("Loading batches...")}</div></div>
          </div>
        </div>
      </section>
    `;
  }

  function createFormulationForm() {
    return `
      <div class="card">
        <div class="card-title"><div><h3>Create formulation</h3><p>Manufacturer only.</p></div></div>
        <form class="form" data-submit="createFormulation">
          <div class="form-row">
            <div class="field"><label>Drug code</label><input class="input" name="drugCode" required placeholder="PARA-500" /></div>
            <div class="field"><label>Unit</label><input class="input" name="unit" required placeholder="tablet" /></div>
          </div>
          <div class="field"><label>Requirements JSON array</label><textarea class="textarea" name="requirements" placeholder='[{"ingredientName":"Paracetamol API","amount":"0.5"}]'></textarea></div>
          <button class="btn primary" type="submit">Create formulation</button>
        </form>
      </div>
    `;
  }

  function produceBatchForm() {
    return `
      <div class="card">
        <div class="card-title"><div><h3>Produce drug batch</h3><p>Manufacturer only. Uses raw material inputs.</p></div></div>
        <form class="form" data-submit="produceBatch">
          <div class="form-row">
            <div class="field"><label>Batch ID</label><input class="input" name="batchId" required placeholder="BATCH-001" /></div>
            <div class="field"><label>Drug code</label><input class="input" name="drugCode" required placeholder="PARA-500" /></div>
          </div>
          <div class="form-row">
            <div class="field"><label>Output quantity</label><input class="input" name="outputQuantity" required type="number" step="any" /></div>
            <div class="field"><label>Unit</label><input class="input" name="unit" required placeholder="tablet" /></div>
          </div>
          <div class="field"><label>Inputs JSON array</label><textarea class="textarea" name="inputs" placeholder='[{"lotId":"LOT-001","ingredientName":"Paracetamol API","amount":25}]'></textarea></div>
          <button class="btn primary" type="submit">Produce batch</button>
        </form>
      </div>
    `;
  }

  function shipmentsPage() {
    return `
      <section class="page">
        <div class="grid sidebar-main">
          <div class="grid">
            ${roleGate("manufacturer", distributionShipmentForm())}
            ${roleGate("distributor", retailShipmentForm())}
            ${roleGate("retailer", dispenseForm())}
            <div class="card">
              <div class="card-title"><div><h3>Shipment party filter</h3><p>Reads shipments involving this MSP.</p></div></div>
              <div class="form">
                <div class="field"><label>Party MSP</label>${select("party", ORGS, state.user?.org || "", "shipmentPartyFilter")}</div>
                <button class="btn" onclick="loadShipmentsPage(document.getElementById('shipmentPartyFilter').value)">Load shipments</button>
              </div>
            </div>
          </div>
          <div class="grid">
            <div class="card"><div class="card-title"><div><h3>Manufacturer to distributor</h3><p>Distribution chaincode shipments.</p></div><button class="btn small" onclick="loadShipmentsPage()">Refresh</button></div><div id="distShipmentsTable">${empty("Loading distribution shipments...")}</div></div>
            <div class="card"><div class="card-title"><div><h3>Distributor to retailer</h3><p>Retail chaincode shipments.</p></div></div><div id="retailShipmentsTable">${empty("Loading retail shipments...")}</div></div>
          </div>
        </div>
      </section>
    `;
  }

  function distributionShipmentForm() {
    return `
      <div class="card">
        <div class="card-title"><div><h3>Create distribution shipment</h3><p>Manufacturer to distributor.</p></div></div>
        <form class="form" data-submit="createDistributionShipment">
          <div class="form-row"><div class="field"><label>Shipment ID</label><input class="input" name="shipmentId" required /></div><div class="field"><label>Batch ID</label><input class="input" name="batchId" required /></div></div>
          <div class="form-row"><div class="field"><label>To MSP</label>${select("toMSP", ["distributorMSP"])}</div><div class="field"><label>Quantity</label><input class="input" name="quantity" type="number" step="any" required /></div></div>
          <div class="field"><label>Metadata JSON</label><textarea class="textarea" name="metadata" placeholder='{"vehicle":"TRUCK-1","temp":"2-8C"}'></textarea></div>
          <button class="btn primary" type="submit">Create offer</button>
        </form>
      </div>
    `;
  }

  function retailShipmentForm() {
    return `
      <div class="card">
        <div class="card-title"><div><h3>Create retail shipment</h3><p>Distributor to retailer.</p></div></div>
        <form class="form" data-submit="createRetailShipment">
          <div class="form-row"><div class="field"><label>Shipment ID</label><input class="input" name="shipmentId" required /></div><div class="field"><label>Batch ID</label><input class="input" name="batchId" required /></div></div>
          <div class="form-row"><div class="field"><label>To MSP</label>${select("toMSP", ["retailerMSP"])}</div><div class="field"><label>Quantity</label><input class="input" name="quantity" type="number" step="any" required /></div></div>
          <div class="field"><label>Metadata JSON</label><textarea class="textarea" name="metadata" placeholder='{"invoice":"INV-001"}'></textarea></div>
          <button class="btn primary" type="submit">Create retail offer</button>
        </form>
      </div>
    `;
  }

  function dispenseForm() {
    return `
      <div class="card">
        <div class="card-title"><div><h3>Verify dispense</h3><p>Retailer to consumer ledger event.</p></div></div>
        <form class="form" data-submit="dispense">
          <div class="form-row"><div class="field"><label>Dispense ID</label><input class="input" name="dispenseId" required /></div><div class="field"><label>Batch ID</label><input class="input" name="batchId" required /></div></div>
          <div class="field"><label>Quantity</label><input class="input" name="quantity" type="number" step="any" required /></div>
          <div class="field"><label>Metadata JSON</label><textarea class="textarea" name="metadata" placeholder='{"patientRef":"RX-7788"}'></textarea></div>
          <button class="btn primary" type="submit">Verify dispense</button>
        </form>
      </div>
    `;
  }

  function recallsPage() {
    return `
      <section class="page">
        <div class="grid sidebar-main">
          <div class="grid">
            ${roleGate("drap", createRecallForm())}
            ${roleGate("drap", addAffectedAssetsForm())}
            ${quarantineForm()}
          </div>
          <div class="grid">
            <div class="card"><div class="card-title"><div><h3>Active recalls</h3><p>Loaded from /api/recalls/active.</p></div><button class="btn small" onclick="loadRecallsPage()">Refresh</button></div><div id="recallsTable">${empty("Loading active recalls...")}</div></div>
            <div class="grid two">
              ${recallCheckForm()}
              ${quarantineCheckForm()}
            </div>
          </div>
        </div>
      </section>
    `;
  }

  function createRecallForm() {
    return `
      <div class="card">
        <div class="card-title"><div><h3>Initiate recall</h3><p>DRAP only.</p></div></div>
        <form class="form" data-submit="createRecall">
          <div class="form-row"><div class="field"><label>Recall ID</label><input class="input" name="recallId" required /></div><div class="field"><label>Severity</label>${select("severity", ["LOW", "MEDIUM", "HIGH", "CRITICAL"], "HIGH")}</div></div>
          <div class="field"><label>Title</label><input class="input" name="title" required /></div>
          <div class="field"><label>Reason</label><textarea class="textarea" name="reason" required></textarea></div>
          <div class="field"><label>Directives JSON</label><textarea class="textarea" name="directives" placeholder='{"quarantine":"Stop sale and isolate stock"}'></textarea></div>
          <button class="btn red" type="submit">Initiate recall</button>
        </form>
      </div>
    `;
  }

  function addAffectedAssetsForm() {
    return `
      <div class="card">
        <div class="card-title"><div><h3>Add affected assets</h3><p>DRAP recall expansion.</p></div></div>
        <form class="form" data-submit="addAffectedAssets">
          <div class="field"><label>Recall ID</label><input class="input" name="recallId" required /></div>
          <div class="field"><label>Asset type</label>${select("assetType", ["BATCH", "LOT", "SHIPMENT"], "BATCH")}</div>
          <div class="field"><label>Asset IDs, comma separated</label><input class="input" name="assetIds" required placeholder="BATCH-001,BATCH-002" /></div>
          <button class="btn red" type="submit">Add assets</button>
        </form>
      </div>
    `;
  }

  function quarantineForm() {
    return `
      <div class="card">
        <div class="card-title"><div><h3>Quarantine asset</h3><p>Stakeholders can quarantine an affected asset.</p></div></div>
        <form class="form" data-submit="quarantineAsset">
          <div class="form-row"><div class="field"><label>Recall ID</label><input class="input" name="recallId" required /></div><div class="field"><label>Asset type</label>${select("assetType", ["BATCH", "LOT", "SHIPMENT"], "BATCH")}</div></div>
          <div class="field"><label>Asset ID</label><input class="input" name="assetId" required /></div>
          <div class="field"><label>Reason</label><textarea class="textarea" name="reason"></textarea></div>
          <button class="btn amber" type="submit">Quarantine</button>
        </form>
      </div>
    `;
  }

  function recallCheckForm() {
    return `
      <div class="card"><div class="card-title"><div><h3>Active recall check</h3><p>Check whether an asset is under active recall.</p></div></div>
        <form class="form" data-submit="recallCheck">
          <div class="field"><label>Asset type</label>${select("assetType", ["BATCH", "LOT", "SHIPMENT"], "BATCH")}</div>
          <div class="field"><label>Asset ID</label><input class="input" name="assetId" required /></div>
          <button class="btn" type="submit">Check</button>
        </form>
      </div>
    `;
  }

  function quarantineCheckForm() {
    const clear = state.user?.role === "drap" ? `<button class="btn red" type="button" data-action="clearQuarantineFromForm">Clear quarantine</button>` : "";
    return `
      <div class="card"><div class="card-title"><div><h3>Quarantine lookup</h3><p>Read quarantine record by asset.</p></div></div>
        <form class="form" data-submit="quarantineCheck">
          <div class="field"><label>Asset type</label>${select("assetType", ["BATCH", "LOT", "SHIPMENT"], "BATCH")}</div>
          <div class="field"><label>Asset ID</label><input class="input" name="assetId" required /></div>
          <div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn" type="submit">Read quarantine</button>${clear}</div>
        </form>
      </div>
    `;
  }

  function verifyPage() {
    return `
      <section class="page">
        <div class="hero">
          <div class="eyebrow">Asset verification</div>
          <h1>Read one ledger object and inspect its raw JSON.</h1>
          <p>This avoids false certainty. The API returns chaincode data, but field casing can differ across Go structs. This GUI displays the raw payload beside normalized table fields.</p>
        </div>
        <div class="grid two">
          <div class="card">
            <div class="card-title"><div><h3>Verify asset</h3><p>Choose the endpoint family and enter the ID.</p></div></div>
            <form class="form" data-submit="verifyAsset">
              <div class="field"><label>Asset kind</label>${select("assetKind", ["lot", "batch", "distributionShipment", "retailShipment", "recall"])}</div>
              <div class="field"><label>Asset ID</label><input class="input" name="assetId" required /></div>
              <button class="btn primary" type="submit">Verify from ledger</button>
            </form>
          </div>
          <div class="card">
            <div class="card-title"><div><h3>Fabric ping</h3><p>Calls /api/fabric/ping using your current role identity.</p></div><button class="btn small" onclick="loadFabricPing()">Run ping</button></div>
            <div id="verifyPing">${empty("Run ping to verify the gateway and apitransfer chaincode.")}</div>
          </div>
        </div>
      </section>
    `;
  }

  function usersPage() {
    return `
      <section class="page">
        <div class="card">
          <div class="card-title"><div><h3>API users</h3><p>DRAP only. Loaded from /api/auth/users.</p></div><button class="btn small" onclick="loadUsersPage()">Refresh</button></div>
          <div id="usersTable">${empty("Loading users...")}</div>
        </div>
      </section>
    `;
  }

  function mountRouteData() {
    if (state.route === "dashboard") loadDashboard(false);
    if (state.route === "lots") loadLotsPage();
    if (state.route === "batches") loadBatchesPage();
    if (state.route === "shipments") loadShipmentsPage();
    if (state.route === "recalls") loadRecallsPage();
    if (state.route === "users") loadUsersPage();
  }

  async function refreshCurrentPage() {
    if (state.route === "dashboard") return loadDashboard(false);
    if (state.route === "lots") return loadLotsPage();
    if (state.route === "batches") return loadBatchesPage();
    if (state.route === "shipments") return loadShipmentsPage();
    if (state.route === "recalls") return loadRecallsPage();
  }

  async function checkHealth(showToast = false) {
    try {
      const res = await api.health();
      state.lastHealth = { ok: true, data: res };
      updateHealthDom(true, "API online");
      if (showToast) toast("API online", res.service || "pakmedtrail-api", "good");
    } catch (err) {
      state.lastHealth = { ok: false, error: err.message };
      updateHealthDom(false, "API offline");
      if (showToast) toast("API offline", err.message, "bad");
    }
  }

  function updateHealthDom(ok, text) {
    const dot = document.getElementById("healthDot");
    const label = document.getElementById("healthText");
    if (dot) dot.className = `dot ${ok ? "ok" : "bad"}`;
    if (label) label.textContent = text;
  }

  async function loadDashboard(noisy) {
    if (noisy) toast("Refreshing", "Loading current ledger data.");
    const metricGrid = document.getElementById("metricGrid");
    const attention = document.getElementById("attentionCard");
    const fabric = document.getElementById("fabricCard");
    if (metricGrid) metricGrid.innerHTML = metricSkeleton();

    const settled = await Promise.allSettled([
      api.lots(),
      api.formulations(),
      api.batches(state.user?.org),
      api.distributionShipments(state.user?.org),
      api.retailShipments(state.user?.org),
      api.recalls(),
      api.fabricPing(),
    ]);

    const [lots, formulations, batches, dist, retail, recalls, ping] = settled.map(s => s.status === "fulfilled" ? s.value : { __error: s.reason?.message || String(s.reason) });
    state.cache.lots = normalizeArray(lots.lots || lots.result || lots);
    state.cache.formulations = normalizeArray(formulations.formulations || formulations);
    state.cache.batches = normalizeArray(batches.batches || batches);
    state.cache.distShipments = normalizeArray(dist.shipments || dist);
    state.cache.retailShipments = normalizeArray(retail.shipments || retail);
    state.cache.recalls = normalizeArray(recalls.recalls || recalls);

    if (metricGrid) metricGrid.innerHTML = [
      metricCard("🧪", state.cache.lots.length, "Raw material lots"),
      metricCard("🏭", state.cache.batches.length, "Drug batches for current MSP"),
      metricCard("🚚", state.cache.distShipments.length + state.cache.retailShipments.length, "Shipments involving current MSP"),
      metricCard("🚨", state.cache.recalls.length, "Active recalls"),
    ].join("");

    if (attention) attention.innerHTML = attentionHtml();
    if (fabric) fabric.innerHTML = fabricCardHtml(ping, settled[6].status === "rejected" ? settled[6].reason : null);
  }

  async function loadFabricPing() {
    const el = document.getElementById("verifyPing");
    if (el) el.innerHTML = empty("Running Fabric ping...");
    try {
      const res = await api.fabricPing();
      if (el) el.innerHTML = `<pre>${esc(JSON.stringify(res, null, 2))}</pre>`;
    } catch (err) {
      if (el) el.innerHTML = errorBox(err);
    }
  }

  async function loadLotsPage() {
    const el = document.getElementById("lotsTable");
    if (!el) return;
    el.innerHTML = empty("Loading lots...");
    try {
      const data = await api.lots();
      const rows = normalizeArray(data.lots || data.result || data);
      state.cache.lots = rows;
      el.innerHTML = lotsTable(rows);
    } catch (err) {
      el.innerHTML = errorBox(err);
    }
  }

  async function loadBatchesPage(owner) {
    const formulationsEl = document.getElementById("formulationsTable");
    const batchesEl = document.getElementById("batchesTable");
    if (formulationsEl) formulationsEl.innerHTML = empty("Loading formulations...");
    if (batchesEl) batchesEl.innerHTML = empty("Loading batches...");
    const ownerMSP = owner || document.getElementById("batchOwnerFilter")?.value || state.user?.org;
    const [f, b] = await Promise.allSettled([api.formulations(), api.batches(ownerMSP)]);
    if (formulationsEl) {
      if (f.status === "fulfilled") {
        const rows = normalizeArray(f.value.formulations || f.value);
        state.cache.formulations = rows;
        formulationsEl.innerHTML = formulationsTable(rows);
      } else formulationsEl.innerHTML = errorBox(f.reason);
    }
    if (batchesEl) {
      if (b.status === "fulfilled") {
        const rows = normalizeArray(b.value.batches || b.value);
        state.cache.batches = rows;
        batchesEl.innerHTML = batchesTable(rows);
      } else batchesEl.innerHTML = errorBox(b.reason);
    }
  }

  async function loadShipmentsPage(party) {
    const partyMSP = party || document.getElementById("shipmentPartyFilter")?.value || state.user?.org;
    const distEl = document.getElementById("distShipmentsTable");
    const retailEl = document.getElementById("retailShipmentsTable");
    if (distEl) distEl.innerHTML = empty("Loading distribution shipments...");
    if (retailEl) retailEl.innerHTML = empty("Loading retail shipments...");
    const [d, r] = await Promise.allSettled([api.distributionShipments(partyMSP), api.retailShipments(partyMSP)]);
    if (distEl) {
      if (d.status === "fulfilled") {
        const rows = normalizeArray(d.value.shipments || d.value);
        state.cache.distShipments = rows;
        distEl.innerHTML = shipmentsTable(rows, "distribution");
      } else distEl.innerHTML = errorBox(d.reason);
    }
    if (retailEl) {
      if (r.status === "fulfilled") {
        const rows = normalizeArray(r.value.shipments || r.value);
        state.cache.retailShipments = rows;
        retailEl.innerHTML = shipmentsTable(rows, "retail");
      } else retailEl.innerHTML = errorBox(r.reason);
    }
  }

  async function loadRecallsPage() {
    const el = document.getElementById("recallsTable");
    if (!el) return;
    el.innerHTML = empty("Loading active recalls...");
    try {
      const data = await api.recalls();
      const rows = normalizeArray(data.recalls || data);
      state.cache.recalls = rows;
      el.innerHTML = recallsTable(rows);
    } catch (err) {
      el.innerHTML = errorBox(err);
    }
  }

  async function loadUsersPage() {
    const el = document.getElementById("usersTable");
    if (!el) return;
    el.innerHTML = empty("Loading users...");
    try {
      const data = await api.users();
      el.innerHTML = usersTable(normalizeArray(data.users || data));
    } catch (err) {
      el.innerHTML = errorBox(err);
    }
  }

  function lotsTable(rows) {
    if (!rows.length) return empty("No lots returned for this identity.");
    return table(["Lot ID", "Name", "Owner", "Status", "Qty", "Dates", "Actions"], rows.map(row => {
      const id = idOf(row, ["lotId", "LotID", "id", "ID"]);
      const owner = field(row, ["ownerMSP", "OwnerMSP", "owner", "Owner"]);
      const status = field(row, ["status", "Status", "state", "State"]);
      return [
        strong(id),
        esc(field(row, ["name", "Name", "materialName", "MaterialName"]) || "—"),
        badge(owner || "—", "blue"),
        statusBadge(status),
        esc(`${field(row, ["quantity", "Quantity"]) || "—"} ${field(row, ["unit", "Unit"]) || ""}`),
        `<small>${esc(field(row, ["manufactureDate", "ManufactureDate"]) || "")}${field(row, ["expiryDate", "ExpiryDate"]) ? " → " + esc(field(row, ["expiryDate", "ExpiryDate"])) : ""}</small>`,
        lotActions(id, row),
      ];
    }));
  }

  function lotActions(id, row) {
    const actions = [`<button class="btn small" data-action="readLot" data-args="${args({ id })}">Read</button>`, jsonButton(row)];
    if (state.user?.role === "drap") actions.push(`<button class="btn small green" data-action="approveLot" data-args="${args({ id })}">DRAP approve</button>`);
    if (state.user?.role === "supplier") actions.push(`<button class="btn small amber" data-action="proposeLotTransfer" data-args="${args({ id })}">Propose to manufacturer</button>`);
    if (state.user?.role === "manufacturer") actions.push(`<button class="btn small green" data-action="acceptLotTransfer" data-args="${args({ id })}">Accept transfer</button>`);
    return `<div class="table-actions">${actions.join("")}</div>`;
  }

  function formulationsTable(rows) {
    if (!rows.length) return empty("No formulations returned.");
    return table(["Drug code", "Unit", "Requirements", "Actions"], rows.map(row => {
      const id = idOf(row, ["drugCode", "DrugCode", "code", "Code"]);
      return [strong(id), esc(field(row, ["unit", "Unit"]) || "—"), smallJson(field(row, ["requirements", "Requirements"]) || []), `<div class="table-actions"><button class="btn small" data-action="readFormulation" data-args="${args({ id })}">Read</button>${jsonButton(row)}</div>`];
    }));
  }

  function batchesTable(rows) {
    if (!rows.length) return empty("No batches returned for selected owner.");
    return table(["Batch ID", "Drug", "Owner", "Status", "Qty", "Actions"], rows.map(row => {
      const id = idOf(row, ["batchId", "BatchID", "id", "ID"]);
      const status = field(row, ["status", "Status", "state", "State"]);
      const actions = [`<button class="btn small" data-action="readBatch" data-args="${args({ id })}">Read</button>`, jsonButton(row)];
      if (state.user?.role === "drap") actions.push(`<button class="btn small green" data-action="approveBatch" data-args="${args({ id })}">DRAP approve</button>`);
      if (state.user?.role === "manufacturer") actions.push(`<button class="btn small amber" data-action="proposeBatchTransfer" data-args="${args({ id })}">Propose to distributor</button>`);
      if (state.user?.role === "distributor") actions.push(`<button class="btn small green" data-action="acceptBatchTransfer" data-args="${args({ id })}">Accept transfer</button>`);
      return [
        strong(id),
        esc(field(row, ["drugCode", "DrugCode", "drug", "Drug"]) || "—"),
        badge(field(row, ["ownerMSP", "OwnerMSP", "owner", "Owner"]) || "—", "blue"),
        statusBadge(status),
        esc(`${field(row, ["outputQuantity", "OutputQuantity", "quantity", "Quantity"]) || "—"} ${field(row, ["unit", "Unit"]) || ""}`),
        `<div class="table-actions">${actions.join("")}</div>`,
      ];
    }));
  }

  function shipmentsTable(rows, type) {
    if (!rows.length) return empty(`No ${type} shipments returned for selected party.`);
    return table(["Shipment ID", "Batch", "From", "To", "Status", "Qty", "Actions"], rows.map(row => {
      const id = idOf(row, ["shipmentId", "ShipmentID", "id", "ID"]);
      const status = field(row, ["status", "Status", "state", "State"]);
      const actions = [
        `<button class="btn small" data-action="${type === "distribution" ? "readDistributionShipment" : "readRetailShipment"}" data-args="${args({ id })}">Read</button>`,
        jsonButton(row)
      ];
      if (type === "distribution" && state.user?.role === "distributor") {
        actions.push(`<button class="btn small green" data-action="acceptDistributionShipment" data-args="${args({ id })}">Accept</button>`);
        actions.push(`<button class="btn small amber" data-action="deliverDistributionShipment" data-args="${args({ id })}">Deliver</button>`);
      }
      if (type === "retail" && state.user?.role === "retailer") {
        actions.push(`<button class="btn small green" data-action="acceptRetailShipment" data-args="${args({ id })}">Accept</button>`);
        actions.push(`<button class="btn small amber" data-action="deliverRetailShipment" data-args="${args({ id })}">Deliver</button>`);
      }
      return [
        strong(id),
        esc(field(row, ["batchId", "BatchID", "batch", "Batch"]) || "—"),
        badge(field(row, ["fromMSP", "FromMSP", "from", "From", "shipperMSP", "ShipperMSP"]) || "—", "purple"),
        badge(field(row, ["toMSP", "ToMSP", "to", "To", "receiverMSP", "ReceiverMSP"]) || "—", "blue"),
        statusBadge(status),
        esc(`${field(row, ["quantity", "Quantity"]) || "—"}`),
        `<div class="table-actions">${actions.join("")}</div>`,
      ];
    }));
  }

  function recallsTable(rows) {
    if (!rows.length) return empty("No active recalls returned.");
    return table(["Recall ID", "Title", "Severity", "Reason", "Actions"], rows.map(row => {
      const id = idOf(row, ["recallId", "RecallID", "id", "ID"]);
      const actions = [`<button class="btn small" data-action="readRecall" data-args="${args({ id })}">Read</button>`, jsonButton(row), `<button class="btn small amber" data-action="acknowledgeRecall" data-args="${args({ id })}">Acknowledge</button>`];
      if (state.user?.role === "drap") actions.push(`<button class="btn small red" data-action="closeRecall" data-args="${args({ id })}">Close</button>`);
      return [strong(id), esc(field(row, ["title", "Title"]) || "—"), statusBadge(field(row, ["severity", "Severity"]) || "HIGH"), esc(field(row, ["reason", "Reason"]) || "—"), `<div class="table-actions">${actions.join("")}</div>`];
    }));
  }

  function usersTable(rows) {
    if (!rows.length) return empty("No users returned.");
    return table(["Username", "Role", "Organization", "Created"], rows.map(u => [strong(u.username), badge(u.role, "blue"), esc(u.org || "—"), esc(u.createdAt || "—")]));
  }

  function attentionHtml() {
    const pendingWords = ["PENDING", "PENDING_DRAP", "AWAITING_DRAP", "CREATED", "PROPOSED", "OFFERED"];
    const all = [
      ...state.cache.lots.map(x => ({ type: "Lot", id: idOf(x, ["lotId", "LotID", "id", "ID"]), status: field(x, ["status", "Status", "state", "State"]), data: x })),
      ...state.cache.batches.map(x => ({ type: "Batch", id: idOf(x, ["batchId", "BatchID", "id", "ID"]), status: field(x, ["status", "Status", "state", "State"]), data: x })),
      ...state.cache.distShipments.map(x => ({ type: "Distribution shipment", id: idOf(x, ["shipmentId", "ShipmentID", "id", "ID"]), status: field(x, ["status", "Status", "state", "State"]), data: x })),
      ...state.cache.retailShipments.map(x => ({ type: "Retail shipment", id: idOf(x, ["shipmentId", "ShipmentID", "id", "ID"]), status: field(x, ["status", "Status", "state", "State"]), data: x })),
    ].filter(x => pendingWords.some(w => String(x.status || "").toUpperCase().includes(w)));
    if (!all.length) return `<div class="card-title"><div><h3>Attention queue</h3><p>No obvious pending item was found in the records returned to this MSP.</p></div></div>${empty("The backend does not expose dedicated pending DRAP endpoints yet, so this card infers pending work from status fields in loaded records.")}`;
    return `<div class="card-title"><div><h3>Attention queue</h3><p>Inferred from status fields in loaded ledger records.</p></div><span class="badge amber">${all.length} item(s)</span></div>${table(["Type", "ID", "Status", "Raw"], all.map(x => [esc(x.type), strong(x.id), statusBadge(x.status), jsonButton(x.data)]))}`;
  }

  function fabricCardHtml(ping, error) {
    if (error || ping.__error) return `<div class="card-title"><div><h3>Fabric gateway ping</h3><p>The API health route may work even when Fabric gateway initialization or chaincode query fails.</p></div></div>${errorBox(error || new Error(ping.__error))}`;
    return `<div class="card-title"><div><h3>Fabric gateway ping</h3><p>/api/fabric/ping called GetAllLots on apitransfer.</p></div><span class="badge green">OK</span></div><pre>${esc(JSON.stringify(ping, null, 2))}</pre>`;
  }

  function metricSkeleton() {
    return ["Lots", "Batches", "Shipments", "Recalls"].map(x => metricCard("⌁", "…", x)).join("");
  }

  function metricCard(icon, number, label) {
    return `<div class="card"><div class="metric"><div class="bubble">${icon}</div><div><strong>${number}</strong><span>${esc(label)}</span></div></div></div>`;
  }

  function pipelineNodes() {
    return [
      ["🧪", "Supplier", "Create raw material lots"],
      ["🏭", "Manufacturer", "Produce drug batches"],
      ["🛡️", "DRAP", "Approve and recall"],
      ["🚚", "Distributor", "Accept and deliver"],
      ["💊", "Retailer", "Retail shipment and dispense"],
      ["✅", "Patient", "Verify medicine trail"],
    ].map(n => `<div class="pipe-node"><div class="pipe-icon">${n[0]}</div><strong>${n[1]}</strong><small>${n[2]}</small></div>`).join("");
  }

  async function busy(el, fn) {
    el.classList.add("loading");
    const buttons = el.querySelectorAll("button");
    buttons.forEach(b => b.disabled = true);
    try { await fn(); }
    catch (err) { toast("Action failed", err.message || String(err), "bad"); showDetail("Error details", err.payload || { message: err.message, status: err.status }); }
    finally { el.classList.remove("loading"); buttons.forEach(b => b.disabled = false); }
  }

  async function runAction(successTitle, fn) {
    try {
      const res = await fn();
      if (res !== undefined) showDetail(successTitle, res);
      toast(successTitle, "Ledger transaction/query completed.", "good");
      await refreshCurrentPage();
    } catch (err) {
      toast("Action failed", err.message || String(err), "bad");
      showDetail("Error details", err.payload || { message: err.message, status: err.status });
    }
  }

  async function promptRun(title, label, fn, successTitle) {
    const value = prompt(`${title}\n${label}:`, "");
    if (value === null) return;
    await runAction(successTitle, async () => fn(value));
  }

  async function selectRun(title, label, values, fn, successTitle) {
    const value = prompt(`${title}\n${label}: ${values.join(", ")}`, values[0]);
    if (!value) return;
    await runAction(successTitle, async () => fn(value));
  }

  function formToObject(form) {
    const data = {};
    for (const [key, value] of new FormData(form).entries()) {
      data[key] = typeof value === "string" ? value.trim() : value;
    }
    return data;
  }

  function parseMetadata(text) {
    if (!text || !String(text).trim()) return undefined;
    try { return JSON.parse(text); } catch { throw new Error("Metadata must be valid JSON."); }
  }
  function parseRequirements(text) {
    if (!text || !String(text).trim()) return [];
    const v = JSON.parse(text);
    if (!Array.isArray(v)) throw new Error("Requirements must be a JSON array.");
    return v;
  }
  function parseInputs(text) {
    if (!text || !String(text).trim()) return [];
    const v = JSON.parse(text);
    if (!Array.isArray(v)) throw new Error("Inputs must be a JSON array.");
    return v;
  }
  function parseDirectives(text) {
    if (!text || !String(text).trim()) return {};
    return JSON.parse(text);
  }

  function showDetail(title, data) {
    let panel = document.getElementById("detailPanel");
    if (!panel) return;
    panel.className = "detail-panel card open";
    panel.innerHTML = `
      <div class="card-title"><div><h3>${esc(title)}</h3><p>Raw API response. Use this when field names differ from normalized columns.</p></div><button class="btn small" onclick="document.getElementById('detailPanel').className='detail-panel card'">Close</button></div>
      <div class="detail-body"><pre>${esc(JSON.stringify(data, null, 2))}</pre></div>
    `;
  }

  function toast(title, msg, kind = "") {
    let wrap = document.getElementById("toastWrap");
    if (!wrap) {
      wrap = document.createElement("div");
      wrap.id = "toastWrap";
      wrap.className = "toast-wrap";
      document.body.appendChild(wrap);
    }
    const item = document.createElement("div");
    item.className = `toast ${kind}`;
    item.innerHTML = `<strong>${esc(title)}</strong><p>${esc(msg || "")}</p>`;
    wrap.appendChild(item);
    setTimeout(() => item.remove(), 5200);
  }

  function roleGate(role, html) {
    return state.user?.role === role ? html : "";
  }

  function select(name, values, selected = "", id = "") {
    return `<select class="select" name="${escAttr(name)}" ${id ? `id="${escAttr(id)}"` : ""}>${values.map(v => `<option value="${escAttr(v)}" ${v === selected ? "selected" : ""}>${esc(v)}</option>`).join("")}</select>`;
  }

  function table(headers, rows) {
    return `<div class="table-wrap"><table><thead><tr>${headers.map(h => `<th>${esc(h)}</th>`).join("")}</tr></thead><tbody>${rows.map(row => `<tr>${row.map(cell => `<td>${cell}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
  }

  function empty(text) { return `<div class="empty">${esc(text)}</div>`; }
  function errorBox(err) { return `<div class="empty"><strong style="color:var(--red)">Request failed</strong><br>${esc(err?.message || String(err))}</div>`; }
  function strong(text) { return `<strong>${esc(text || "—")}</strong>`; }
  function badge(text, color = "") { return `<span class="badge ${color}">${esc(text || "—")}</span>`; }
  function statusBadge(status) {
    const s = String(status || "UNKNOWN");
    const up = s.toUpperCase();
    let color = "blue";
    if (up.includes("APPROV") || up.includes("ACCEPT") || up.includes("DELIVER") || up.includes("ACTIVE")) color = "green";
    if (up.includes("PENDING") || up.includes("PROPOSE") || up.includes("OFFER") || up.includes("CREATED")) color = "amber";
    if (up.includes("REJECT") || up.includes("RECALL") || up.includes("CLOSE") || up.includes("QUAR")) color = "red";
    return badge(s, color);
  }

  function jsonButton(data, title = "Raw") {
    return `<button class="btn small" data-action="showJSON" data-args="${args({ title, data })}">JSON</button>`;
  }

  function smallJson(value) {
    const s = typeof value === "string" ? value : JSON.stringify(value);
    return `<small>${esc(s.length > 120 ? s.slice(0, 120) + "…" : s)}</small>`;
  }

  function field(obj, names) {
    if (!obj || typeof obj !== "object") return undefined;
    for (const name of names) {
      if (obj[name] !== undefined && obj[name] !== null && obj[name] !== "") return obj[name];
    }
    const lower = Object.fromEntries(Object.entries(obj).map(([k, v]) => [k.toLowerCase(), v]));
    for (const name of names) {
      const val = lower[String(name).toLowerCase()];
      if (val !== undefined && val !== null && val !== "") return val;
    }
    return undefined;
  }

  function idOf(obj, names) {
    return String(field(obj, names) || field(obj, ["assetId", "AssetID", "key", "Key"]) || "");
  }

  function normalizeArray(value) {
    if (!value) return [];
    if (Array.isArray(value)) return value;
    if (value.result && Array.isArray(value.result)) return value.result;
    if (value.lots && Array.isArray(value.lots)) return value.lots;
    if (value.batches && Array.isArray(value.batches)) return value.batches;
    if (value.shipments && Array.isArray(value.shipments)) return value.shipments;
    if (value.recalls && Array.isArray(value.recalls)) return value.recalls;
    if (value.formulations && Array.isArray(value.formulations)) return value.formulations;
    return typeof value === "object" && !value.error && !value.__error ? [value] : [];
  }

  function numberOrString(v) {
    if (v === "" || v === undefined || v === null) return v;
    const n = Number(v);
    return Number.isFinite(n) ? n : v;
  }

  function safeParse(text, fallback) {
    try { return text ? JSON.parse(text) : fallback; } catch { return fallback; }
  }

  function args(obj) { return encodeURIComponent(JSON.stringify(obj)); }
  function esc(value) {
    return String(value === undefined || value === null ? "" : value)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
  }
  function escAttr(value) { return esc(value); }
  function roleLabel(role) { return ROLES.find(r => r.role === role)?.label || role || "Not signed in"; }

  // Expose selected loaders for inline onclick buttons used in static markup.
  window.loadLotsPage = loadLotsPage;
  window.loadBatchesPage = loadBatchesPage;
  window.loadShipmentsPage = loadShipmentsPage;
  window.loadRecallsPage = loadRecallsPage;
  window.loadUsersPage = loadUsersPage;
  window.loadFabricPing = loadFabricPing;
  window.handlers = handlers;

  render();
})();
