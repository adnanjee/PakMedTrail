# Fabric Pharma API + Frontend (extended)

This project is a **full vertical slice** for our pharma supply chain network with. It shows how to run this api server and front end. The report has the complete
data

- API lots (supplier + DRAP views)
- Manufacturer ↔ Distributor shipments
- Per-organization dashboards (Supplier, Manufacturer, Distributor, Retail, DRAP)
- **PDC-sensitive commercial terms** (price/discount/incoterms in a private data collection)
- DRAP recalls + quarantine flows hooked into the shipment chaincode

The code is split into:

- `backend/` – Node.js + TypeScript + Fabric Gateway
- `frontend/` – React + Vite SPA

## 1. Backend

cd backend
cp .env.example .env
# Edit .env to point at your crypto-config cert/key paths and peer endpoints after running the network and place the material giving in the Network folder on github
npm install
npm run dev

Key environment variables:

- `FABRIC_CHANNEL` – channel name where your chaincodes are deployed (e.g. `rawmaterialsupply`)
- `FABRIC_CHAINCODE_LOTS` – chaincode name for the API lot contract (e.g. `apitransfer`)
- `FABRIC_CHAINCODE_DISTRIBUTION` – chaincode name for the distribution/recall contract (your `main.go`)

Each org has its own peer endpoint + TLS cert + user cert + private key. 

### Auth

- `POST /auth/login` – demo login, issues a JWT for a username + `orgMSP`.
- Org MSPs used:
  - `supplierMSP`
  - `manufacturerMSP`
  - `distributorMSP`
  - `retailerMSP`
  - `drapMSP`

### Lots (API raw material) – `/api/lots`

Our lots chaincode exposes functions like `CreateLot`, `ReadLot`, `QueryLotsBy`, `ApproveLotByDRAP`, etc.

- `GET /api/lots` – list lots (defaults to lots owned by caller org).
- `POST /api/lots` – create new lot (supplier only).
- `GET /api/lots/pending-drap` – lots awaiting DRAP approval.
- `POST /api/lots/:id/drap/approve` – DRAP approve.
- `POST /api/lots/:id/drap/reject` – DRAP reject.
- `POST /api/lots/:id/propose-transfer` – owner proposes transfer to another MSP.
- `POST /api/lots/:id/accept-transfer` – proposed owner accepts.
- `POST /api/lots/:id/reject-transfer` – proposed owner rejects.
- `POST /api/lots/:id/consume` – consume quantity.
- `POST /api/lots/:id/destroy` – destroy lot.
- `GET /api/lots/:id/history` – full key history.

### Shipments + PDC (DistributionContract) – `/api/shipments`

Backed directly by your uploaded `DistributionContract` chaincode (`CreateShipmentOffer`, `AcceptShipment`, `PutSensitive`, `ReadSensitive`, `LinkSensitiveHash`, `InitiateRecallByDRAP`, etc.).

- `GET /api/shipments?partyMSP=X` – list shipments where `fromMSP == X` or `toMSP == X` (defaults to caller orgMSP).
- `GET /api/shipments/:id` – read a single shipment.
- `POST /api/shipments` – manufacturer creates a shipment offer:
  - body includes `shipmentId`, `batchId`, `toMSP`, `quantity`, `metadata`.
  - internally calls `CreateShipmentOffer` (also triggers manufacturing batch transfer proposal).
- `POST /api/shipments/:id/accept` – `AcceptShipment` (proposed receiver).
- `POST /api/shipments/:id/reject` – `RejectShipment` with `reason`.
- `POST /api/shipments/:id/cancel` – `CancelShipment` with `reason` (sender only).
- `POST /api/shipments/:id/delivered` – `MarkDelivered` by the receiver.

**PDC / sensitive commercial terms**

- `PUT /api/shipments/:id/terms` – wraps `PutSensitive`:
  - body: `{ priceAmt, currency, discount, incoterms, notes }`
  - only Manufacturer/Distributor (shipment parties) can write; content is stored in a **private data collection**.
- `GET /api/shipments/:id/terms` – wraps `ReadSensitive`, visible only to the two parties.
- `POST /api/shipments/:id/terms/hash` – wraps `LinkSensitiveHash` to hash the PDC record and store the hash in the public shipment metadata (`sensitiveHash`).

**Quarantine**

- `POST /api/shipments/:id/quarantine` – wraps `QuarantineByRecall`:
  - permitted for the current owner (fromMSP while pending, toMSP after accepted/delivered).
  - requires an ACTIVE recall notice for the shipment's batch, or it will fail.

### Recalls (DRAP) – `/api/recalls`

- `POST /api/recalls` – DRAP `InitiateRecallByDRAP(recallId, batchId, reason)`.
- `POST /api/recalls/:id/close` – DRAP `CloseRecallByDRAP(recallId, note)`.
- `GET /api/recalls?status=ACTIVE&batchId=BATCH123` – wraps `QueryRecalls` with a Couch selector.
- `GET /api/recalls/batch/:batchId?activeOnly=true` – helper to check whether a batch is under active recall.

The front-end uses these to show recall banners and to enable the "Quarantine" button from manufacturer/distributor/retail views.

## 2. Frontend

cd frontend
cp .env.example .env   # adjust VITE_API_URL if needed
npm install
npm run dev

Then open `http://localhost:5173`.

### Org-specific dashboards

Login page lets us pick any org MSP, then the app shows only that org’s views:

- **Supplier (`supplierMSP`)**
  - `/supplier/lots`
  - Create lots
  - See own lots
  - Propose transfers (e.g. to `manufacturerMSP`)

- **Manufacturer (`manufacturerMSP`)**
  - `/manufacturer/shipments`
  - Create shipment offers to distributors (by `batchId`)
  - See all shipments involving manufacturer
  - Edit PDC terms (price / currency / discount / incoterms / notes)
  - Link PDC hash into shipment metadata
  - See recall flag per batch and quarantine shipments

- **Distributor (`distributorMSP`)**
  - `/distributor/shipments`
  - See shipments involving distributor
  - Accept / reject pending offers
  - Mark shipments as delivered
  - Edit PDC terms + link hash
  - See recall status and quarantine shipments under recall

- **Retail (`retailerMSP`)**
  - `/retailer/shipments`
  - Simple view of shipments where retail MSP is party
  - Recall awareness + quarantine button

- **DRAP (`drapMSP`)**
  - `/drap/lots` – approve/reject API lots
  - `/drap/recalls` – create and close recalls, see all recall notices

The UI is intentionally simple (cards + tables + inline forms) so you can extend it with more analytics and visualisations.

## 3. Wiring to your network

1. Drop this repo next to your existing test network.
2. Point `.env` in `backend/` to your real `crypto-config` paths + peer endpoints.
3. Set:
   - `FABRIC_CHANNEL` to the correct channel name.
   - `FABRIC_CHAINCODE_LOTS` to your lots chaincode name.
   - `FABRIC_CHAINCODE_DISTRIBUTION` to the name of the uploaded `DistributionContract` chaincode.
4. Start the backend (`npm run dev`) and frontend (`npm run dev`).

You we have:

- A REST API suitable for integration with other systems.
- A per-organization web UI that exercises your chaincodes end-to-end, including **PDC-sensitive data** and **recall-driven quarantines**.
