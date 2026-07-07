// Verify is the public facing trace tool. You type (or scan) an ID and it tries
// every record type the ledger knows about, then shows whatever it finds. An ID
// usually matches one type, but we run them all so a user never has to know
// which kind of record they are holding.

import { icons } from "../icons.js";
import { api } from "../api.js";
import { escape as esc, emptyBlock, modal } from "../ui.js";
import { one, wireGo, ledgerBlock, badge, idOf, statusOf, toast, iconChip } from "./_shared.js";

const LOOKUPS = [
  { type: "Lot", icon: "lots", key: "lot", route: "/lots/", fn: (id) => api.getLot(id) },
  { type: "Batch", icon: "factory", key: "batch", route: "/batches/", fn: (id) => api.getBatch(id) },
  { type: "Distribution shipment", icon: "truck", key: "shipment", fn: (id) => api.getDistributionShipment(id) },
  { type: "Retail shipment", icon: "store", key: "shipment", fn: (id) => api.getRetailShipment(id) },
  { type: "Dispense", icon: "pill", key: "dispense", fn: (id) => api.getDispense(id) },
];

export default async function verifyPage(root) {
  const canScan = "BarcodeDetector" in window;
  root.innerHTML = `
    <div class="content">
      <div class="card">
        <div class="card-body">
          <div class="row" style="gap:12px;margin-bottom:6px">
            ${iconChip("qr")}
            <div class="col" style="gap:2px">
              <b style="font-size:1.05rem">Trace an item</b>
              <span class="muted" style="font-size:.8rem">Enter any lot, batch, shipment, or dispense ID to see its ledger record.</span>
            </div>
          </div>
          <div class="row" style="gap:8px;margin-top:10px">
            <input class="input" id="verify-id" placeholder="LOT-001, BATCH-001, SHIP-001…" style="flex:1" />
            <button class="btn btn-primary" data-action="verify">${icons.search(16)} Verify</button>
            ${canScan ? `<button class="btn btn-outline" data-action="scan">${icons.qr(16)} Scan QR</button>` : ""}
          </div>
        </div>
      </div>
      <div id="verify-result"></div>
    </div>`;

  const input = root.querySelector("#verify-id");
  const result = root.querySelector("#verify-result");
  const run = () => verify(input.value.trim(), result);

  root.querySelector('[data-action="verify"]').addEventListener("click", run);
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") run();
  });
  if (canScan) {
    root.querySelector('[data-action="scan"]').addEventListener("click", () =>
      openScanner((code) => {
        input.value = code;
        verify(code, result);
      })
    );
  }
}

async function verify(id, result) {
  if (!id) {
    toast("Enter an ID to verify", "err");
    return;
  }
  result.innerHTML = `<div class="card"><div class="loading"><span class="spin"></span><span>Searching the ledger…</span></div></div>`;

  const settled = await Promise.allSettled(LOOKUPS.map((l) => l.fn(id)));
  const hits = [];
  settled.forEach((res, i) => {
    if (res.status !== "fulfilled") return;
    const obj = one(res.value, LOOKUPS[i].key);
    if (obj && typeof obj === "object" && Object.keys(obj).length) {
      hits.push({ meta: LOOKUPS[i], obj });
    }
  });

  if (!hits.length) {
    result.innerHTML = `<div class="card"><div class="card-body">${emptyBlock(icons.search(22), "Nothing found", `No record matched "${esc(id)}".`)}</div></div>`;
    return;
  }

  result.innerHTML = `
    <div class="card" style="margin-top:18px">
      <div class="card-head"><div class="card-title">${icons.check(16)} Found ${hits.length} record${hits.length === 1 ? "" : "s"} for "${esc(id)}"</div></div>
      <div class="card-body">
        <div class="trace">
          ${hits.map((h, i) => traceStep(h, i === hits.length - 1, id)).join("")}
        </div>
      </div>
    </div>`;
  wireGo(result);
}

function traceStep(hit, last, id) {
  const { meta, obj } = hit;
  const link = meta.route
    ? `<button class="btn btn-ghost btn-sm" data-go="${meta.route}${encodeURIComponent(idOf(obj, id))}">Open full view</button>`
    : "";
  return `
    <div class="trace-step">
      <div class="trace-rail">
        <div class="trace-dot">${icons[meta.icon](16)}</div>
        ${last ? "" : `<div class="trace-line"></div>`}
      </div>
      <div class="trace-body">
        <div class="row between">
          <h4 style="margin:2px 0">${esc(meta.type)}</h4>
          <div class="row" style="gap:8px">${badge(statusOf(obj) || "n/a")}${link}</div>
        </div>
        ${ledgerBlock(obj)}
      </div>
    </div>`;
}

// ---------- optional QR scanner ----------

function openScanner(onCode) {
  const m = modal({
    title: "Scan a QR code",
    body: `
      <div class="col" style="gap:10px;align-items:center">
        <video id="scan-video" playsinline style="width:100%;max-width:360px;border-radius:12px;background:#000"></video>
        <span class="muted" style="font-size:.8rem">Point the camera at the code. It reads automatically.</span>
      </div>`,
  });

  const video = m.el.querySelector("#scan-video");
  let stream = null;
  let raf = null;
  let stopped = false;

  const detector = new window.BarcodeDetector({ formats: ["qr_code"] });

  const stop = () => {
    stopped = true;
    if (raf) cancelAnimationFrame(raf);
    if (stream) stream.getTracks().forEach((t) => t.stop());
  };

  // Make sure the camera shuts off when the modal closes by any route.
  const origClose = m.close;
  m.close = () => {
    stop();
    origClose();
  };
  m.el.addEventListener("click", (e) => {
    if (e.target === m.el || e.target.closest('[data-action="close-modal"]')) stop();
  });

  navigator.mediaDevices
    .getUserMedia({ video: { facingMode: "environment" } })
    .then((s) => {
      if (stopped) {
        s.getTracks().forEach((t) => t.stop());
        return;
      }
      stream = s;
      video.srcObject = s;
      video.play();
      const tick = async () => {
        if (stopped) return;
        try {
          const codes = await detector.detect(video);
          if (codes && codes.length) {
            const value = codes[0].rawValue;
            m.close();
            if (value) onCode(value.trim());
            return;
          }
        } catch {
          // detect can throw on a not ready frame; just try the next one
        }
        raf = requestAnimationFrame(tick);
      };
      raf = requestAnimationFrame(tick);
    })
    .catch(() => {
      const body = m.el.querySelector(".modal-body");
      if (body) body.innerHTML = `<p class="muted" style="margin:0">Could not open the camera. Check the browser permission, then type the ID by hand.</p>`;
    });
}
