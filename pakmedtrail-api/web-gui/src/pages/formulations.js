// Formulations are the recipes that say how a drug is made. They are keyed by
// drug code. Manufacturers create them; everyone can read them. They have no
// owner or transfer flow, so this page stays simple: a list, a create form,
// and a detail view.

import { icons } from "../icons.js";
import { api } from "../api.js";
import { isRole } from "../store.js";
import { escape as esc, pick, emptyBlock, readForm, modal, onClick } from "../ui.js";
import {
  arr,
  one,
  wireGo,
  backLink,
  parseLoose,
  runAction,
  ledgerBlock,
  iconChip,
  toast,
} from "./_shared.js";

export default async function formulationsPage(root, params) {
  const code = params && params[0];
  if (code) return renderDetail(root, code);
  return renderList(root);
}

async function renderList(root) {
  const canCreate = isRole("manufacturer");
  root.innerHTML = `
    <div class="content">
      <div class="row between">
        <p class="muted" style="margin:0">Recipes that define how each drug is produced. Manufacturers add them before producing a batch.</p>
        ${canCreate ? `<button class="btn btn-primary btn-sm" data-action="new-form">${icons.flask(16)} New formulation</button>` : ""}
      </div>
      <div class="card">
        <div class="card-head"><div class="card-title">${icons.flask(16)} Formulations</div>
          <button class="btn btn-ghost btn-sm" data-action="reload">${icons.refresh(15)} Refresh</button>
        </div>
        <div class="card-body" id="form-body">
          <div class="loading"><span class="spin"></span><span>Loading formulations…</span></div>
        </div>
      </div>
    </div>`;

  root.querySelector('[data-action="reload"]').addEventListener("click", () => renderList(root));
  if (canCreate) {
    root.querySelector('[data-action="new-form"]').addEventListener("click", () => openCreate(root));
  }

  const body = root.querySelector("#form-body");
  try {
    const forms = arr(await api.getFormulations(), "formulations");
    if (!forms.length) {
      body.innerHTML = emptyBlock(icons.flask(22), "No formulations yet", canCreate ? "Add one to define a drug recipe." : "A manufacturer needs to add one first.");
      return;
    }
    body.innerHTML = formsTable(forms);
    wireGo(body);
  } catch (err) {
    body.innerHTML = `<p class="muted" style="margin:0">Could not load formulations. ${esc(err.message)}</p>`;
  }
}

function formsTable(forms) {
  const rows = forms
    .map((f) => {
      const code = pick(f, ["drugCode", "DrugCode", "id", "ID"], "");
      const unit = pick(f, ["unit", "Unit"], "");
      const req = pick(f, ["requirements", "Requirements"], "");
      const reqText = req && typeof req === "object" ? JSON.stringify(req) : req;
      return `
        <tr data-go="/formulations/${encodeURIComponent(code)}" style="cursor:pointer">
          <td><b>${esc(code || "—")}</b></td>
          <td>${esc(unit || "—")}</td>
          <td><span class="mono">${esc(reqText ? String(reqText).slice(0, 60) : "—")}</span></td>
        </tr>`;
    })
    .join("");
  return `
    <div class="table-wrap">
      <table class="data">
        <thead><tr><th>Drug code</th><th>Unit</th><th>Requirements</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

function openCreate(root) {
  modal({
    title: "New formulation",
    body: `
      <div class="form-grid">
        <div class="field"><label>Drug code *</label><input class="input" name="drugCode" placeholder="PARA-500" /></div>
        <div class="field"><label>Unit *</label><input class="input" name="unit" placeholder="tablets" /></div>
        <div class="field span-2"><label>Requirements</label><textarea class="textarea" name="requirements" placeholder='{"api":"paracetamol","strength":"500mg"}'></textarea><span class="hint">Optional. JSON or plain text describing the recipe.</span></div>
      </div>`,
    footer: `<button class="btn btn-primary" data-action="save-form">Create</button>`,
    onMount: (el, close) => {
      onClick(el, "save-form", (btn) => {
        const form = readForm(el);
        if (!form.drugCode || !form.unit) {
          toast("Drug code and unit are required", "err");
          return;
        }
        const payload = {
          drugCode: form.drugCode,
          unit: form.unit,
          requirements: parseLoose(form.requirements),
        };
        runAction(btn, "Creating", () => api.createFormulation(payload), () => {
          close();
          renderList(root);
        });
      });
    },
  });
}

async function renderDetail(root, code) {
  root.innerHTML = `
    <div class="content">
      <div class="row">${backLink("/formulations", "All formulations")}</div>
      <div class="card"><div class="card-body" id="form-detail">
        <div class="loading"><span class="spin"></span><span>Loading formulation…</span></div>
      </div></div>
    </div>`;
  wireGo(root);

  const box = root.querySelector("#form-detail");
  let form;
  try {
    form = one(await api.getFormulation(code), "formulation");
  } catch (err) {
    box.innerHTML = `<p class="muted" style="margin:0">Could not load formulation ${esc(code)}. ${esc(err.message)}</p>`;
    return;
  }
  if (!form || typeof form !== "object") {
    box.innerHTML = emptyBlock(icons.flask(22), "Formulation not found", `No recipe for ${code}.`);
    return;
  }

  box.innerHTML = `
    <div class="row" style="gap:12px">
      ${iconChip("flask")}
      <div class="col" style="gap:2px">
        <b style="font-size:1.05rem">${esc(pick(form, ["drugCode", "DrugCode"], code))}</b>
        <span class="muted" style="font-size:.78rem">Unit: ${esc(pick(form, ["unit", "Unit"], "—"))}</span>
      </div>
    </div>
    ${ledgerBlock(form)}`;
}
