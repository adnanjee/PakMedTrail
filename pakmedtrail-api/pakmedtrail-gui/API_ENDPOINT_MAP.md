# PakMedTrail API endpoint map used by the GUI

## Auth

- `POST /api/auth/login`
- `POST /api/auth/register`
- `GET /api/auth/me`
- `GET /api/auth/users` DRAP only

## Health and Fabric

- `GET /health`
- `GET /api/fabric/ping`

## Lots

- `GET /api/lots`
- `GET /api/lots/:lotId`
- `POST /api/lots` supplier only
- `POST /api/lots/:lotId/drap-approve` DRAP only
- `POST /api/lots/:lotId/propose-transfer` supplier only
- `POST /api/lots/:lotId/accept-transfer` manufacturer only

## Formulations and batches

- `GET /api/batches/formulations`
- `GET /api/batches/formulations/:drugCode`
- `POST /api/batches/formulations` manufacturer only
- `GET /api/batches?owner=:ownerMSP`
- `GET /api/batches/:batchId`
- `POST /api/batches` manufacturer only
- `POST /api/batches/:batchId/drap-approve` DRAP only
- `POST /api/batches/:batchId/propose-transfer` manufacturer only
- `POST /api/batches/:batchId/accept-transfer` distributor only

## Shipments and dispense

- `GET /api/shipments/distribution?party=:partyMSP`
- `GET /api/shipments/distribution/:shipmentId`
- `POST /api/shipments/distribution` manufacturer only
- `POST /api/shipments/distribution/:shipmentId/accept` distributor only
- `POST /api/shipments/distribution/:shipmentId/deliver` distributor only
- `GET /api/shipments/retail?party=:partyMSP`
- `GET /api/shipments/retail/:shipmentId`
- `POST /api/shipments/retail` distributor only
- `POST /api/shipments/retail/:shipmentId/accept` retailer only
- `POST /api/shipments/retail/:shipmentId/deliver` retailer only
- `POST /api/shipments/dispense` retailer only
- `GET /api/shipments/dispense/:dispenseId`

## Recalls

- `GET /api/recalls/active`
- `GET /api/recalls/:recallId`
- `POST /api/recalls` DRAP only
- `POST /api/recalls/:recallId/affected-assets` DRAP only
- `POST /api/recalls/:recallId/acknowledge`
- `POST /api/recalls/:recallId/quarantine`
- `POST /api/recalls/:recallId/close` DRAP only
- `GET /api/recalls/active-check/:assetType/:assetId`
- `GET /api/recalls/quarantine/:assetType/:assetId`
- `POST /api/recalls/quarantine/:assetType/:assetId/clear` DRAP only
