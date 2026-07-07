# PakMedTrail Console

A web GUI for the PakMedTrail API. It is a plain browser app. No build step, no
framework, no npm install. It talks to your API over HTTP using the same login
and token the API already expects.

This folder lives inside your API project at `pakmedtrail-api/web-gui/`. It does
not touch or import anything from the API. You can delete it and the API still
runs exactly as before.

## What you need

* Node 18 or newer, only to run the small static server. The server uses the
  Node standard library and nothing else.
* The PakMedTrail API running. By default the console points at
  `http://localhost:4000`.

## Run it

1. Start the PakMedTrail API first, the normal way you start it.

   In the reviewed API package, the API starts even if one peer is down. Login
   and the health check still work. Ledger pages retry the needed peer lazily
   and the Network page shows which peer or chaincode is failing.

2. From inside this `web-gui` folder, start the console server:

   ```bash
   node server.js
   ```

   To use a different port:

   ```bash
   node server.js 8080
   # or
   PORT=8080 node server.js
   ```

3. Open the address it prints, by default:

   ```
   http://localhost:5173
   ```

   Open it over HTTP as shown. Do not double click `index.html` to open it as a
   file. The app loads as native ES modules and browsers block those on the
   `file://` scheme.

## Point it at a different API

The console defaults to `http://localhost:4000`. To change it, click the gear
icon at the top right and enter your API base URL. The value is saved in your
browser. CORS is already enabled on the API, so the console can run on a
different port than the API with no extra setup.

## Demo logins

These are the seeded accounts from the API. Each one lands on the pages and
actions for its role.

| Role         | Username        | Password      |
| ------------ | --------------- | ------------- |
| Supplier     | `supplier_admin`| `supplier123` |
| Manufacturer | `mfg_admin`     | `mfg123`      |
| Distributor  | `dist_admin`    | `dist123`     |
| Retailer     | `retail_admin`  | `retail123`   |
| DRAP         | `drap_admin`    | `drap123`     |

The login screen also has one click buttons for each of these.

## What each page does

* **Dashboard**: a live view of the chain. A pipeline from supplier to patient
  with counts at each stage, plus your own holdings and any active recalls.
* **Lots**: raw material lots. Suppliers create them and hand them to a
  manufacturer. DRAP can approve.
* **Batches**: finished drug batches a manufacturer produces. They move down to
  a distributor. DRAP can approve. The list filters by owner.
* **Formulations**: the recipes that define how each drug is made.
* **Shipments**: three tabs. Distribution (manufacturer to distributor), Retail
  (distributor to retailer), and Dispense (retailer to patient).
* **Recalls**: DRAP opens and closes recalls. Any org can acknowledge a recall
  or quarantine an asset it holds. There is a tool to check whether an asset is
  under recall or quarantine.
* **Verify**: type or scan any ID and see its ledger record. QR scanning shows
  up only on browsers that support it.
* **Network**: checks API health, peer TCP reachability, the peer gateway, and the chaincodes used by
  the console. This is the page to use when Fabric says an endorser peer is unavailable. DRAP can see all organisation checks when the reviewed API route
  is installed. Other roles see their own org.
* **Account**: your identity from the server. DRAP also sees the user list.

## How it is built

* `index.html` loads the styles and the app entry.
* `server.js` is the static file server described above.
* `src/` holds the app. `api.js` is one function per API endpoint, `store.js`
  keeps your session, `router.js` is a small hash router, and `pages/` has one
  file per screen.

Your token is kept in the browser local storage so a reload keeps you signed in
until it expires. Sign out clears it.
