// SPDX-License-Identifier: Apache-2.0
//
// Fabric chaincode (Go) — "API Transfer" example
//
// Domain story:
//  - Active Pharmaceutical Ingredients (APIs) are created by a Supplier.
//  - Supplier adds API lots (batches) into stock on the ledger.
//  - Supplier proposes transferring a lot to a Drug Manufacturer.
//  - Manufacturer must explicitly Accept or Reject (no automatic transfer).
//
// Highlights:
//  - Two-step Offer/Accept flow
//  - Access control by invoker's MSPID (supplier/manufacturer orgs)
//  - CouchDB rich queries + optional pagination
//  - Private Data Collection (PDC) for sensitive fields (price, purity, notes)
//  - Chaincode events for UIs
//  - History endpoint for audit
//
// Notes:
//  - For key-level endorsement (SBE), see the optional function stub below.
//  - For indexes and collections config, see JSON snippets at bottom of file (in comments).

package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
	"github.com/hyperledger/fabric-contract-api-go/metadata"
	"github.com/hyperledger/fabric-chaincode-go/shim"
	statebased "github.com/hyperledger/fabric-chaincode-go/pkg/statebased"
)

// ======= Types =======

type ApiStatus string

const (
	StatusInStock        ApiStatus = "IN_STOCK"
	StatusPendingTransfer ApiStatus = "PENDING_TRANSFER"
	StatusAccepted        ApiStatus = "ACCEPTED"
	StatusRejected        ApiStatus = "REJECTED"
	StatusConsumed        ApiStatus = "CONSUMED"
	StatusDestroyed       ApiStatus = "DESTROYED"
)

type ApiLot struct {
	DocType          string            `json:"docType"`
	LotID            string            `json:"lotId"`
	Name             string            `json:"name"`
	BatchNumber      string            `json:"batchNumber"`
	Quantity         float64           `json:"quantity"`
	Unit             string            `json:"unit"`
	ManufactureDate  string            `json:"manufactureDate,omitempty"`
	ExpiryDate       string            `json:"expiryDate,omitempty"`
	OwnerMSP         string            `json:"ownerMSP"`
	SupplierMSP      string            `json:"supplierMSP"`
	ProposedOwnerMSP string            `json:"proposedOwnerMSP,omitempty"`
	Status           ApiStatus         `json:"status"`
	Metadata         map[string]any    `json:"metadata,omitempty"`
	CreatedAt        string            `json:"createdAt"`
	UpdatedAt        string            `json:"updatedAt"`
}

// Sensitive fields stored in private data (PDC)
// NOTE: keep in sync with hashing logic below

type ApiLotSensitive struct {
	DocType       string      `json:"docType"`
	LotID         string      `json:"lotId"`
	Price         *Price      `json:"price,omitempty"`
	PurityPercent *float64    `json:"purityPercent,omitempty"`
	Notes         string      `json:"notes,omitempty"`
	UpdatedAt     string      `json:"updatedAt"`
}

type Price struct {
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

// Private Data Collection name (configure in collections_config.json)
const PDC_APISENSITIVE = "collectionAPISensitive"

// ======= Contract =======

type ApiTransferContract struct {
	contractapi.Contract
}

// -------- Utility helpers --------

func nowISO() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func (c *ApiTransferContract) invokerMSP(ctx contractapi.TransactionContextInterface) (string, error) {
	id, err := ctx.GetClientIdentity().GetMSPID()
	if err != nil { return "", fmt.Errorf("cannot get invoker MSP: %w", err) }
	return id, nil
}

func (c *ApiTransferContract) lotExists(ctx contractapi.TransactionContextInterface, lotID string) (bool, error) {
	bytes, err := ctx.GetStub().GetState(lotID)
	if err != nil { return false, err }
	return bytes != nil && len(bytes) > 0, nil
}

func (c *ApiTransferContract) getLot(ctx contractapi.TransactionContextInterface, lotID string) (*ApiLot, error) {
	bytes, err := ctx.GetStub().GetState(lotID)
	if err != nil { return nil, err }
	if bytes == nil || len(bytes) == 0 { return nil, fmt.Errorf("lot %s does not exist", lotID) }
	var lot ApiLot
	if err := json.Unmarshal(bytes, &lot); err != nil { return nil, err }
	if lot.DocType != "api.lot" { return nil, fmt.Errorf("key %s exists but is not an API lot", lotID) }
	return &lot, nil
}

func (c *ApiTransferContract) putLot(ctx contractapi.TransactionContextInterface, lot *ApiLot) error {
	bytes, err := json.Marshal(lot)
	if err != nil { return err }
	return ctx.GetStub().PutState(lot.LotID, bytes)
}

func (c *ApiTransferContract) assertOwner(ctx contractapi.TransactionContextInterface, lot *ApiLot) error {
	msp, err := c.invokerMSP(ctx)
	if err != nil { return err }
	if msp != lot.OwnerMSP { return fmt.Errorf("ACCESS DENIED: only owner MSP=%s can perform this operation", lot.OwnerMSP) }
	return nil
}

func (c *ApiTransferContract) assertProposedOwner(ctx contractapi.TransactionContextInterface, lot *ApiLot) error {
	if lot.ProposedOwnerMSP == "" { return errors.New("no proposed owner set; nothing to accept/reject") }
	msp, err := c.invokerMSP(ctx)
	if err != nil { return err }
	if msp != lot.ProposedOwnerMSP { return fmt.Errorf("ACCESS DENIED: only proposed owner MSP=%s can perform this operation", lot.ProposedOwnerMSP) }
	return nil
}

func (c *ApiTransferContract) setEvent(ctx contractapi.TransactionContextInterface, name string, payload any) error {
	b, _ := json.Marshal(payload)
	return ctx.GetStub().SetEvent(name, b)
}

// -------- Read-only / queries --------

func (c *ApiTransferContract) LotExists(ctx contractapi.TransactionContextInterface, lotID string) (bool, error) {
	return c.lotExists(ctx, lotID)
}

func (c *ApiTransferContract) ReadLot(ctx contractapi.TransactionContextInterface, lotID string) (*ApiLot, error) {
	return c.getLot(ctx, lotID)
}

func (c *ApiTransferContract) GetAllLots(ctx contractapi.TransactionContextInterface) ([]*ApiLot, error) {
	iter, err := ctx.GetStub().GetStateByRange("", "")
	if err != nil { return nil, err }
	defer iter.Close()
	var out []*ApiLot
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil { return nil, err }
		var lot ApiLot
		if err := json.Unmarshal(kv.Value, &lot); err == nil && lot.DocType == "api.lot" {
			copy := lot
			out = append(out, &copy)
		}
	}
	return out, nil
}

func (c *ApiTransferContract) GetLotsByOwner(ctx contractapi.TransactionContextInterface, ownerMSP string) ([]*ApiLot, error) {
	selector := map[string]any{
		"selector": map[string]any{"docType": "api.lot", "ownerMSP": ownerMSP},
	}
	return c.queryLots(ctx, selector, 0, "")
}

// QueryLots — generic CouchDB selector with optional pagination
func (c *ApiTransferContract) QueryLots(ctx contractapi.TransactionContextInterface, selectorJSON string, pageSize int32, bookmark string) (map[string]any, error) {
	var selector map[string]any
	if err := json.Unmarshal([]byte(selectorJSON), &selector); err != nil {
		return nil, fmt.Errorf("invalid selector JSON: %w", err)
	}
	items, nextBookmark, err := c.queryLotsWithBookmark(ctx, selector, pageSize, bookmark)
	if err != nil { return nil, err }
	return map[string]any{"items": items, "fetched": len(items), "bookmark": nextBookmark}, nil
}

// QueryLotsBy — convenience selector by common fields
func (c *ApiTransferContract) QueryLotsBy(ctx contractapi.TransactionContextInterface, name string, status ApiStatus, ownerMSP string) ([]*ApiLot, error) {
	sel := map[string]any{"selector": map[string]any{"docType": "api.lot"}}
	inner := sel["selector"].(map[string]any)
	if name != "" { inner["name"] = name }
	if status != "" { inner["status"] = status }
	if ownerMSP != "" { inner["ownerMSP"] = ownerMSP }
	return c.queryLots(ctx, sel, 0, "")
}

func (c *ApiTransferContract) queryLots(ctx contractapi.TransactionContextInterface, selector map[string]any, pageSize int32, bookmark string) ([]*ApiLot, error) {
	items, _, err := c.queryLotsWithBookmark(ctx, selector, pageSize, bookmark)
	return items, err
}

func (c *ApiTransferContract) queryLotsWithBookmark(ctx contractapi.TransactionContextInterface, selector map[string]any, pageSize int32, bookmark string) ([]*ApiLot, string, error) {
	query, _ := json.Marshal(selector)
	stub := ctx.GetStub()
	if pageSize > 0 {
		iter, meta, err := stub.GetQueryResultWithPagination(string(query), pageSize, bookmark)
		if err != nil { return nil, "", err }
		defer iter.Close()
		items, err := collectLots(iter)
		if err != nil { return nil, "", err }
		return items, meta.Bookmark, nil
	}
	iter, err := stub.GetQueryResult(string(query))
	if err != nil { return nil, "", err }
	defer iter.Close()
	items, err := collectLots(iter)
	return items, "", err
}

func collectLots(iter shim.StateQueryIteratorInterface) ([]*ApiLot, error) {
	var out []*ApiLot
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil { return nil, err }
		var lot ApiLot
		if err := json.Unmarshal(kv.Value, &lot); err == nil && lot.DocType == "api.lot" {
			copy := lot
			out = append(out, &copy)
		}
	}
	return out, nil
}

// -------- Create & update --------

func (c *ApiTransferContract) CreateLot(ctx contractapi.TransactionContextInterface, lotID, name, batchNumber string, quantity float64, unit, manufactureDate, expiryDate, metadataJSON string) error {
	exists, err := c.lotExists(ctx, lotID)
	if err != nil { return err }
	if exists { return fmt.Errorf("lot %s already exists", lotID) }
	if quantity <= 0 { return errors.New("quantity must be > 0") }

	msp, err := c.invokerMSP(ctx)
	if err != nil { return err }
	now := nowISO()
	var meta map[string]any
	if metadataJSON != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &meta); err != nil {
			return fmt.Errorf("invalid metadata JSON: %w", err)
		}
	}
	lot := &ApiLot{
		DocType:         "api.lot",
		LotID:           lotID,
		Name:            name,
		BatchNumber:     batchNumber,
		Quantity:        quantity,
		Unit:            unit,
		ManufactureDate: manufactureDate,
		ExpiryDate:      expiryDate,
		OwnerMSP:        msp,
		SupplierMSP:     msp,
		Status:          StatusInStock,
		Metadata:        meta,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if err := c.putLot(ctx, lot); err != nil { return err }
	return c.setEvent(ctx, "LotCreated", map[string]any{"lotId": lotID})
}

func (c *ApiTransferContract) UpdateMetadata(ctx contractapi.TransactionContextInterface, lotID, metadataJSON string) error {
	lot, err := c.getLot(ctx, lotID)
	if err != nil { return err }
	if err := c.assertOwner(ctx, lot); err != nil { return err }
	var meta map[string]any
	if metadataJSON != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &meta); err != nil {
			return fmt.Errorf("invalid metadata JSON: %w", err)
		}
	}
	lot.Metadata = meta
	lot.UpdatedAt = nowISO()
	return c.putLot(ctx, lot)
}

func (c *ApiTransferContract) Consume(ctx contractapi.TransactionContextInterface, lotID string, amount float64) error {
	if amount <= 0 { return errors.New("amount must be > 0") }
	lot, err := c.getLot(ctx, lotID)
	if err != nil { return err }
	if err := c.assertOwner(ctx, lot); err != nil { return err }
	if lot.Status == StatusPendingTransfer { return errors.New("cannot consume while transfer is pending; reject/cancel first") }
	if lot.Quantity < amount { return fmt.Errorf("insufficient quantity; available=%.4f requested=%.4f", lot.Quantity, amount) }
	lot.Quantity -= amount
	if lot.Quantity == 0 { lot.Status = StatusConsumed }
	lot.UpdatedAt = nowISO()
	if err := c.putLot(ctx, lot); err != nil { return err }
	return c.setEvent(ctx, "QuantityUpdated", map[string]any{"lotId": lotID, "quantity": lot.Quantity})
}

func (c *ApiTransferContract) Destroy(ctx contractapi.TransactionContextInterface, lotID string) error {
	lot, err := c.getLot(ctx, lotID)
	if err != nil { return err }
	if err := c.assertOwner(ctx, lot); err != nil { return err }
	lot.Quantity = 0
	lot.Status = StatusDestroyed
	lot.UpdatedAt = nowISO()
	if err := c.putLot(ctx, lot); err != nil { return err }
	return c.setEvent(ctx, "LotDestroyed", map[string]any{"lotId": lotID})
}

func (c *ApiTransferContract) DeleteLot(ctx contractapi.TransactionContextInterface, lotID string) error {
	lot, err := c.getLot(ctx, lotID)
	if err != nil { return err }
	if err := c.assertOwner(ctx, lot); err != nil { return err }
	if lot.Status == StatusPendingTransfer { return errors.New("cannot delete while transfer is pending") }
	if err := ctx.GetStub().DelState(lotID); err != nil { return err }
	return c.setEvent(ctx, "LotDeleted", map[string]any{"lotId": lotID})
}

// -------- Transfer flow (Offer/Accept) --------

func (c *ApiTransferContract) ProposeTransfer(ctx contractapi.TransactionContextInterface, lotID, proposedOwnerMSP string) error {
	lot, err := c.getLot(ctx, lotID)
	if err != nil { return err }
	if err := c.assertOwner(ctx, lot); err != nil { return err }
	if lot.Status == StatusPendingTransfer { return errors.New("a transfer is already pending for this lot") }
	if lot.Quantity <= 0 { return errors.New("cannot transfer: quantity is zero") }
	lot.ProposedOwnerMSP = proposedOwnerMSP
	lot.Status = StatusPendingTransfer
	lot.UpdatedAt = nowISO()
	if err := c.putLot(ctx, lot); err != nil { return err }
	// OPTIONAL: set key-level endorsement requiring both current owner and proposed owner orgs on Accept
	// _ = c.setKeyLevelEndorsement(ctx, lotID, []string{lot.OwnerMSP, proposedOwnerMSP})
	// OPTIONAL: set key-level endorsement requiring both current owner and proposed owner orgs on Accept
_ = c.setKeyLevelEndorsement(ctx, lotID, []string{lot.OwnerMSP, proposedOwnerMSP})
return c.setEvent(ctx, "TransferProposed", map[string]any{"lotId": lotID, "from": lot.OwnerMSP, "to": proposedOwnerMSP})
}

func (c *ApiTransferContract) AcceptTransfer(ctx contractapi.TransactionContextInterface, lotID string) error {
	lot, err := c.getLot(ctx, lotID)
	if err != nil { return err }
	if err := c.assertProposedOwner(ctx, lot); err != nil { return err }
	if lot.Status != StatusPendingTransfer { return errors.New("no pending transfer to accept") }
	prev := lot.OwnerMSP
	lot.OwnerMSP = lot.ProposedOwnerMSP
	lot.ProposedOwnerMSP = ""
	lot.Status = StatusAccepted
	lot.UpdatedAt = nowISO()
	if err := c.putLot(ctx, lot); err != nil { return err }
	// OPTIONAL: reset/adjust key-level endorsement for new owner going forward
	return c.setEvent(ctx, "TransferAccepted", map[string]any{"lotId": lotID, "from": prev, "to": lot.OwnerMSP})
}

func (c *ApiTransferContract) RejectTransfer(ctx contractapi.TransactionContextInterface, lotID, reason string) error {
	lot, err := c.getLot(ctx, lotID)
	if err != nil { return err }
	if err := c.assertProposedOwner(ctx, lot); err != nil { return err }
	if lot.Status != StatusPendingTransfer { return errors.New("no pending transfer to reject") }
	from := lot.OwnerMSP
	to := lot.ProposedOwnerMSP
	lot.ProposedOwnerMSP = ""
	lot.Status = StatusRejected
	lot.UpdatedAt = nowISO()
	if err := c.putLot(ctx, lot); err != nil { return err }
	return c.setEvent(ctx, "TransferRejected", map[string]any{"lotId": lotID, "from": from, "to": to, "reason": reason})
}

func (c *ApiTransferContract) CancelTransfer(ctx contractapi.TransactionContextInterface, lotID, reason string) error {
	lot, err := c.getLot(ctx, lotID)
	if err != nil { return err }
	if err := c.assertOwner(ctx, lot); err != nil { return err }
	if lot.Status != StatusPendingTransfer { return errors.New("no pending transfer to cancel") }
	to := lot.ProposedOwnerMSP
	lot.ProposedOwnerMSP = ""
	lot.Status = StatusInStock
	lot.UpdatedAt = nowISO()
	if err := c.putLot(ctx, lot); err != nil { return err }
	return c.setEvent(ctx, "TransferCancelled", map[string]any{"lotId": lotID, "to": to, "reason": reason})
}

// -------- History --------

func (c *ApiTransferContract) GetHistory(ctx contractapi.TransactionContextInterface, lotID string) ([]map[string]any, error) {
	iter, err := ctx.GetStub().GetHistoryForKey(lotID)
	if err != nil { return nil, err }
	defer iter.Close()
	var out []map[string]any
	for iter.HasNext() {
		rec, err := iter.Next()
		if err != nil { return nil, err }
		var val ApiLot
		if len(rec.Value) > 0 {
			_ = json.Unmarshal(rec.Value, &val)
		}
		out = append(out, map[string]any{
			"txId":     rec.TxId,
			"isDelete": rec.IsDelete,
			"timestamp": time.Unix(rec.Timestamp.Seconds, int64(rec.Timestamp.Nanos)).UTC().Format(time.RFC3339Nano),
			"value":    val,
		})
	}
	return out, nil
}

// -------- Private Data (Sensitive) --------

func (c *ApiTransferContract) PutSensitive(ctx contractapi.TransactionContextInterface, lotID string, priceAmount float64, priceCurrency string, hasPrice bool, purityPercent float64, hasPurity bool, notes string) error {
	// NOTE: Go chaincode method signatures are fixed; to allow optional fields we pass flags hasPrice/hasPurity
	lot, err := c.getLot(ctx, lotID)
	if err != nil { return err }
	if err := c.assertOwner(ctx, lot); err != nil { return err }

	existing, _ := c.getSensitive(ctx, lotID)
	rec := &ApiLotSensitive{
		DocType:   "api.lot.sensitive",
		LotID:     lotID,
		UpdatedAt: nowISO(),
	}
	if existing != nil {
		rec.Price = existing.Price
		rec.PurityPercent = existing.PurityPercent
		rec.Notes = existing.Notes
	}
	if hasPrice {
		rec.Price = &Price{Amount: priceAmount, Currency: priceCurrency}
	}
	if hasPurity {
		rec.PurityPercent = &purityPercent
	}
	if notes != "" { rec.Notes = notes }

	b, _ := json.Marshal(rec)
	return ctx.GetStub().PutPrivateData(PDC_APISENSITIVE, lotID, b)
}

func (c *ApiTransferContract) ReadSensitive(ctx contractapi.TransactionContextInterface, lotID string) (*ApiLotSensitive, error) {
	lot, err := c.getLot(ctx, lotID)
	if err != nil { return nil, err }
	viewer, err := c.invokerMSP(ctx)
	if err != nil { return nil, err }
	if viewer != lot.OwnerMSP && viewer != lot.ProposedOwnerMSP {
		return nil, errors.New("ACCESS DENIED: only current owner or proposed owner may read sensitive data")
	}
	rec, err := c.getSensitive(ctx, lotID)
	if err != nil { return nil, err }
	if rec == nil { return nil, errors.New("no sensitive data for this lot") }
	return rec, nil
}

func (c *ApiTransferContract) getSensitive(ctx contractapi.TransactionContextInterface, lotID string) (*ApiLotSensitive, error) {
	b, err := ctx.GetStub().GetPrivateData(PDC_APISENSITIVE, lotID)
	if err != nil { return nil, err }
	if b == nil || len(b) == 0 { return nil, nil }
	var rec ApiLotSensitive
	if err := json.Unmarshal(b, &rec); err != nil { return nil, err }
	return &rec, nil
}

// LinkSensitiveHash publishes a SHA-256 hash of the private record into the public lot metadata
func (c *ApiTransferContract) LinkSensitiveHash(ctx contractapi.TransactionContextInterface, lotID string) error {
	lot, err := c.getLot(ctx, lotID)
	if err != nil { return err }
	viewer, err := c.invokerMSP(ctx)
	if err != nil { return err }
	if viewer != lot.OwnerMSP { return errors.New("only current owner can link sensitive hash") }
	rec, err := c.getSensitive(ctx, lotID)
	if err != nil { return err }
	if rec == nil { return errors.New("no sensitive record exists to hash") }
	hash := hashSensitive(rec)
	if lot.Metadata == nil { lot.Metadata = map[string]any{} }
	lot.Metadata["sensitiveHash"] = hash
	lot.UpdatedAt = nowISO()
	return c.putLot(ctx, lot)
}

func hashSensitive(s *ApiLotSensitive) string {
	canon := struct {
		LotID         string   `json:"lotId"`
		Price         *Price   `json:"price,omitempty"`
		PurityPercent *float64 `json:"purityPercent,omitempty"`
		Notes         string   `json:"notes,omitempty"`
	}{LotID: s.LotID, Price: s.Price, PurityPercent: s.PurityPercent, Notes: s.Notes}
	b, _ := json.Marshal(canon)
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%x", sum[:])
}

// -------- Optional: Key-Level Endorsement (SBE) --------
// Build and set a validation parameter requiring specific MSPs (as members)
// to endorse future updates to a key.
func (c *ApiTransferContract) setKeyLevelEndorsement(ctx contractapi.TransactionContextInterface, key string, msps []string) error {
    ep, err := statebased.NewStateEP(nil) // <- returns (StateEP, error)
    if err != nil {
        return err
    }
    for _, m := range msps {
        if err := ep.AddOrgs(statebased.RoleTypeMember, m); err != nil {
            return err
        }
    }
    polBytes, err := ep.Policy()
    if err != nil {
        return err
    }
    return ctx.GetStub().SetStateValidationParameter(key, polBytes)
}


// ======= Chaincode metadata (optional) =======

func (c *ApiTransferContract) GetMetadata() *metadata.ContractMetadata {
	return &metadata.ContractMetadata{
		Info: &metadata.InfoMetadata{Title: "ApiTransferContract", Description: "API Supplier → Manufacturer two-step transfer with acceptance (Go)"},
	}
}

// ======= Main =======

func main() {
	contract := new(ApiTransferContract)
	cc, err := contractapi.NewChaincode(contract)
	if err != nil { panic(err) }
	if err := cc.Start(); err != nil { panic(err) }
}

/*
==============================
peer CLI Examples (adjust channel, peers, msp IDs, and connection profile):

Assumptions:
- Channel: rawmaterialsupply  (Fabric channel IDs **cannot contain spaces**; if your display name is “raw material supply channel”, choose a channel ID like `rawmaterialsupply` or `raw-material-supply`)
- Chaincode name: apitransfer
- Orderer endpoint etc. are set by your environment (CORE_PEER_* vars)
- JSON args use the Fabric peer CLI pattern: '["Function","arg1","arg2",...]'

# --- CreateLot --- (as Supplier org)
peer chaincode invoke -C rawmaterialsupply -n apitransfer -c '{
  "Args": [
    "CreateLot","LOT1","Paracetamol","BATCH-001","100","kg","2025-01-01","2027-01-01","{\"grade\":\"USP\"}"
  ]
}'

# --- ReadLot ---
peer chaincode query -C rawmaterialsupply -n apitransfer -c '{"Args":["ReadLot","LOT1"]}'

# --- UpdateMetadata --- (owner only)
peer chaincode invoke -C rawmaterialsupply -n apitransfer -c '{"Args":["UpdateMetadata","LOT1","{\"warehouse\":\"SG-01\"}"]}'

# --- Consume --- (owner only)
peer chaincode invoke -C rawmaterialsupply -n apitransfer -c '{"Args":["Consume","LOT1","10"]}'

# --- Destroy --- (owner only)
peer chaincode invoke -C rawmaterialsupply -n apitransfer -c '{"Args":["Destroy","LOT1"]}'

# --- DeleteLot --- (owner only and not pending)
peer chaincode invoke -C rawmaterialsupply -n apitransfer -c '{"Args":["DeleteLot","LOT1"]}'

# --- ProposeTransfer --- (owner proposes to manufacturer MSP)
peer chaincode invoke -C rawmaterialsupply -n apitransfer -c '{"Args":["ProposeTransfer","LOT1","Org2MSP"]}'

# --- AcceptTransfer --- (must be invoked by proposed owner org)
peer chaincode invoke -C rawmaterialsupply -n apitransfer -c '{"Args":["AcceptTransfer","LOT1"]}'

# --- RejectTransfer --- (proposed owner org)
peer chaincode invoke -C rawmaterialsupply -n apitransfer -c '{"Args":["RejectTransfer","LOT1","out-of-spec"]}'

# --- CancelTransfer --- (current owner org)
peer chaincode invoke -C rawmaterialsupply -n apitransfer -c '{"Args":["CancelTransfer","LOT1","timeout"]}'

# --- QueryLots (selector JSON) ---
peer chaincode query -C rawmaterialsupply -n apitransfer -c '{
  "Args":[
    "QueryLots",
    "{\"selector\":{\"docType\":\"api.lot\",\"ownerMSP\":\"Org1MSP\"}}",
    "0",
    ""
  ]
}'

# --- QueryLotsBy ---
peer chaincode query -C rawmaterialsupply -n apitransfer -c '{"Args":["QueryLotsBy","Paracetamol","","Org1MSP"]}'

# --- GetHistory ---
peer chaincode query -C rawmaterialsupply -n apitransfer -c '{"Args":["GetHistory","LOT1"]}'

# --- PutSensitive --- (owner only; flags allow omitted price/purity)
# Example: set price and purity
peer chaincode invoke -C rawmaterialsupply -n apitransfer -c '{
  "Args":["PutSensitive","LOT1","1200","USD","true","99.8","true","Q3 discount"]
}'

# --- ReadSensitive --- (owner or proposed owner)
peer chaincode query -C rawmaterialsupply -n apitransfer -c '{"Args":["ReadSensitive","LOT1"]}'

# --- LinkSensitiveHash --- (owner only)
peer chaincode invoke -C rawmaterialsupply -n apitransfer -c '{"Args":["LinkSensitiveHash","LOT1"]}'

Tips:
- When invoking from different orgs, switch CORE_PEER_* env vars or use --connProfile with --peerAddresses/--tlsRootCertFiles.
- Ensure the PDC collection is installed on both orgs per collections_config.json.
==============================

==============================
CouchDB Indexes (place these files in your chaincode folder):

1) META-INF/statedb/couchdb/indexes/apiLot-index.json
{
  "index": {
    "fields": ["docType", "ownerMSP", "status", "name"]
  },
  "ddoc": "indexApiLotByOwnerStatusName",
  "name": "indexApiLotByOwnerStatusName",
  "type": "json"
}

2) Optional index focused on status then owner:
META-INF/statedb/couchdb/indexes/apiLot-status-owner.json
{
  "index": {
    "fields": ["docType", "status", "ownerMSP"]
  },
  "ddoc": "indexApiLotByStatusOwner",
  "name": "indexApiLotByStatusOwner",
  "type": "json"
}

==============================
Private Data Collections config (collections_config.json):
[
  {
    "name": "collectionAPISensitive",
    "policy": {
      "identities": [
        {"role": {"name": "member", "mspId": "Org1MSP"}},
        {"role": {"name": "member", "mspId": "Org2MSP"}}
      ],
      "policy": {"2-of": [{"signed-by": 0}, {"signed-by": 1}]}
    },
    "requiredPeerCount": 1,
    "maxPeerCount": 2,
    "blockToLive": 0,
    "memberOnlyRead": true,
    "memberOnlyWrite": true
  }
]

Adjust MSP IDs, peer counts, etc. to your network.
*/
