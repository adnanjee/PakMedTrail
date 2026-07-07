# PakMedTrail Chain Tracker GUI

This is a separate frontend for `pakmedtrail-api`. It is intentionally placed in its own folder:

```text
pakmedtrail-api/
  pakmedtrail-gui/
    index.html
    styles.css
    app.js
    README.md
```

No backend source file is modified. The GUI only calls the existing REST API.

## What this GUI does

The uploaded Fabric Chain Tracker frontend had a good visual style, but it was wired to a different API model. It expected a single `medicine` resource, generic shipments, dashboard summary routes, generated workspace clients, Replit Vite plugins, and workspace dependencies.

Your PakMedTrail API is different. It has:

- Raw material lots: `/api/lots`
- Formulations and manufactured batches: `/api/batches`
- Distribution shipments: `/api/shipments/distribution`
- Retail shipments: `/api/shipments/retail`
- Dispense verification: `/api/shipments/dispense`
- DRAP recalls and quarantine: `/api/recalls`
- Authentication and roles: `/api/auth`
- Fabric ping: `/api/fabric/ping`

So this GUI keeps the same visual direction, but uses your real endpoint structure.

## How to run

### 1. Start the API server

From the root API folder:

```bash
cd pakmedtrail-api
npm install
npm start
```

The API must be able to initialize the Fabric gateways from `.env`. If Fabric certificates, peer addresses, or chaincodes are not available, the API will fail before the frontend can use it.

Default API URL used by the GUI:

```text
http://localhost:4000
```

You can change this from the API box in the GUI top bar.

### 2. Start the GUI

Open a second terminal:

```bash
cd pakmedtrail-api/pakmedtrail-gui
python3 -m http.server 5173
```

Then open:

```text
http://localhost:5173
```

No frontend install is required. This is plain HTML, CSS, and JavaScript.

## Seeded login users

These come from `src/services/userStore.js` in your API.

| Role | Username | Password | MSP |
|---|---|---|---|
| Supplier | `supplier_admin` | `supplier123` | `supplierMSP` |
| Manufacturer | `mfg_admin` | `mfg123` | `manufacturerMSP` |
| Distributor | `dist_admin` | `dist123` | `distributorMSP` |
| Retailer | `retail_admin` | `retail123` | `retailerMSP` |
| DRAP | `drap_admin` | `drap123` | `drapMSP` |

## Pages included

- Dashboard with health, Fabric ping, live counts, and pipeline view
- Raw lots page with create, read, DRAP approve, propose transfer, and accept transfer actions
- Batches page with formulation create/read, batch production, DRAP approval, transfer proposal, and transfer acceptance
- Shipments page with distribution shipment, retail shipment, delivery actions, and dispense verification
- Recalls page with initiate recall, affected assets, acknowledge, quarantine, active recall check, quarantine lookup, clear quarantine, and close recall
- Verify page for direct asset lookup
- DRAP users page

## Important limitation

The backend does not expose dedicated dashboard summary routes or dedicated pending DRAP notification routes. The dashboard attention queue therefore infers pending work from status fields in the records available to the logged-in MSP. For a stronger DRAP notification panel, add API routes for these chaincode functions that already exist in `chaincodeConfig.js`:

- `GetLotsPendingDRAPApproval`
- `GetBatchesPendingDRAPApproval`
- `GetBatchesPendingTransfer`

That is a backend enhancement, not a frontend issue.
