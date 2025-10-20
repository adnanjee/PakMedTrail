package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hyperledger/fabric-chaincode-go/pkg/cid"
	"github.com/hyperledger/fabric-chaincode-go/pkg/statebased"
	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

const (
	DocTypeLot          = "api.lot"
	DocTypeLotSensitive = "api.lot.sensitive"
	CollectionSensitive = "collectionAPISensitive"

	// Business statuses (public)
	StatusInStock         = "IN_STOCK"
	StatusPendingTransfer = "PENDING_TRANSFER"
	StatusAccepted        = "ACCEPTED"
	StatusRejected        = "REJECTED"
	StatusConsumed        = "CONSUMED"
	StatusDestroyed       = "DESTROYED"

	// Events
	EventLotCreated        = "LotCreated"
	EventLotDeleted        = "LotDeleted"
	EventLotDestroyed      = "LotDestroyed"
	EventQuantityUpdated   = "QuantityUpdated"
	EventTransferProposed  = "TransferProposed"
	EventTransferAccepted  = "TransferAccepted"
	EventTransferRejected  = "TransferRejected"
	EventTransferCancelled = "TransferCancelled"

	// New DRAP-related events
	EventDRAPApproved = "DRAPApproved"
	EventDRAPRejected = "DRAPRejected"

	// MSP ID for DRAP (as requested)
	MSP_DRAP = "drapMSP"
)

// ApiLot is the public state of an API lot (non-sensitive)
type ApiLot struct {
	DocType          string  `json:"docType"`
	LotID            string  `json:"lotId"`
	Name             string  `json:"name"`
	BatchNumber      string  `json:"batchNumber"`
	Quantity         float64 `json:"quantity"`
	Unit             string  `json:"unit"`
	ManufactureDate  string  `json:"manufactureDate,omitempty"`
	ExpiryDate       string  `json:"expiryDate,omitempty"`
	OwnerMSP         string  `json:"ownerMSP"`
	SupplierMSP      string  `json:"supplierMSP"`
	ProposedOwnerMSP string  `json:"proposedOwnerMSP,omitempty"`
	Status           string  `json:"status"`

	// DRAP gate: lot must be approved before it can be transferred
	DRAPApproved bool   `json:"drapApproved"`
	DRAPNote     string `json:"drapNote,omitempty"`
	DRAPAt       string `json:"drapAt,omitempty"` // timestamp of last DRAP decision

	Metadata  map[string]string `json:"metadata,omitempty"`
	CreatedAt string            `json:"createdAt"`
	UpdatedAt string            `json:"updatedAt"`
}

// ApiLotSensitive is stored in Private Data Collection
type ApiLotSensitive struct {
	DocType       string   `json:"docType"`
	LotID         string   `json:"lotId"`
	PriceAmount   *float64 `json:"priceAmount,omitempty"`
	PriceCurrency string   `json:"priceCurrency,omitempty"`
	PurityPercent *float64 `json:"purityPercent,omitempty"`
	Notes         string   `json:"notes,omitempty"`
	UpdatedAt     string   `json:"updatedAt"`
}

// ApiTransferContract implements the chaincode
type ApiTransferContract struct {
	contractapi.Contract
}

/* -------------------------------------------------------------------------- */
/*                               Helper functions                             */
/* -------------------------------------------------------------------------- */

func getMSP(ctx contractapi.TransactionContextInterface) (string, error) {
	msp, err := cid.GetMSPID(ctx.GetStub())
	if err != nil {
		return "", fmt.Errorf("unable to get MSP: %w", err)
	}
	return msp, nil
}

func isDRAP(ctx contractapi.TransactionContextInterface) (bool, string, error) {
	msp, err := getMSP(ctx)
	if err != nil {
		return false, "", err
	}
	return msp == MSP_DRAP, msp, nil
}

func nowRFC3339(ctx contractapi.TransactionContextInterface) string {
	ts, err := ctx.GetStub().GetTxTimestamp()
	if err == nil && ts != nil {
		return time.Unix(ts.Seconds, int64(ts.Nanos)).UTC().Format(time.RFC3339)
	}
	return time.Now().UTC().Format(time.RFC3339)
}

func (c *ApiTransferContract) emit(ctx contractapi.TransactionContextInterface, name string, v any) {
	if b, err := json.Marshal(v); err == nil {
		_ = ctx.GetStub().SetEvent(name, b)
	}
}

func (c *ApiTransferContract) readLot(ctx contractapi.TransactionContextInterface, lotID string) (*ApiLot, error) {
	b, err := ctx.GetStub().GetState(lotID)
	if err != nil {
		return nil, fmt.Errorf("read lot %s: %w", lotID, err)
	}
	if len(b) == 0 {
		return nil, fmt.Errorf("lot %s not found", lotID)
	}
	var lot ApiLot
	if err := json.Unmarshal(b, &lot); err != nil {
		return nil, fmt.Errorf("unmarshal lot %s: %w", lotID, err)
	}
	return &lot, nil
}

func (c *ApiTransferContract) putLot(ctx contractapi.TransactionContextInterface, lot *ApiLot) error {
	lot.UpdatedAt = nowRFC3339(ctx)
	b, err := json.Marshal(lot)
	if err != nil {
		return fmt.Errorf("marshal lot %s: %w", lot.LotID, err)
	}
	return ctx.GetStub().PutState(lot.LotID, b)
}

func (c *ApiTransferContract) checkOwner(ctx contractapi.TransactionContextInterface, lot *ApiLot) error {
	msp, err := getMSP(ctx)
	if err != nil {
		return err
	}
	if lot.OwnerMSP != msp {
		return fmt.Errorf("access denied: caller MSP %s is not the owner %s", msp, lot.OwnerMSP)
	}
	return nil
}

func (c *ApiTransferContract) checkProposedOwner(ctx contractapi.TransactionContextInterface, lot *ApiLot) error {
	msp, err := getMSP(ctx)
	if err != nil {
		return err
	}
	if lot.ProposedOwnerMSP != msp {
		return fmt.Errorf("access denied: caller MSP %s is not the proposed owner %s", msp, lot.ProposedOwnerMSP)
	}
	return nil
}

/* --------------------------- State-based endorsement ---------------------- */

func (c *ApiTransferContract) setSBEForKey(ctx contractapi.TransactionContextInterface, key string, orgMSPs ...string) error {
	ep, err := statebased.NewStateEP(nil)
	if err != nil {
		return fmt.Errorf("new SBE: %w", err)
	}
	if len(orgMSPs) > 0 {
		if err := ep.AddOrgs(statebased.RoleTypePeer, orgMSPs...); err != nil {
			return fmt.Errorf("sbe add orgs: %w", err)
		}
	}
	pol, err := ep.Policy()
	if err != nil {
		return fmt.Errorf("sbe policy: %w", err)
	}
	return ctx.GetStub().SetStateValidationParameter(key, pol)
}

func (c *ApiTransferContract) clearSBE(ctx contractapi.TransactionContextInterface, key string) error {
	return ctx.GetStub().SetStateValidationParameter(key, nil)
}

/* -------------------------------------------------------------------------- */
/*                               Public functions                             */
/* -------------------------------------------------------------------------- */

// LotExists returns true if lot exists
func (c *ApiTransferContract) LotExists(ctx contractapi.TransactionContextInterface, lotID string) (bool, error) {
	b, err := ctx.GetStub().GetState(lotID)
	if err != nil {
		return false, err
	}
	return len(b) > 0, nil
}

// CreateLot adds a new lot (owner = creator MSP). DRAP approval required before transfer.
func (c *ApiTransferContract) CreateLot(
	ctx contractapi.TransactionContextInterface,
	lotID, name, batchNumber, quantityStr, unit, manufactureDate, expiryDate, metadataJSON string,
) (*ApiLot, error) {
	exists, err := c.LotExists(ctx, lotID)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("lot %s already exists", lotID)
	}
	qty, err := strconv.ParseFloat(quantityStr, 64)
	if err != nil || qty <= 0 {
		return nil, fmt.Errorf("invalid quantity %q", quantityStr)
	}
	msp, err := getMSP(ctx)
	if err != nil {
		return nil, err
	}
	now := nowRFC3339(ctx)
	lot := &ApiLot{
		DocType:         DocTypeLot,
		LotID:           lotID,
		Name:            name,
		BatchNumber:     batchNumber,
		Quantity:        qty,
		Unit:            unit,
		ManufactureDate: strings.TrimSpace(manufactureDate),
		ExpiryDate:      strings.TrimSpace(expiryDate),
		OwnerMSP:        msp,
		SupplierMSP:     msp,
		Status:          StatusInStock,
		DRAPApproved:    false, // <- gate is closed until DRAP approves
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if strings.TrimSpace(metadataJSON) != "" {
		md := map[string]string{}
		if err := json.Unmarshal([]byte(metadataJSON), &md); err != nil {
			return nil, fmt.Errorf("metadata JSON invalid: %w", err)
		}
		lot.Metadata = md
	}
	if err := c.putLot(ctx, lot); err != nil {
		return nil, err
	}
	c.emit(ctx, EventLotCreated, lot)

	// SBE: require Supplier (owner) + DRAP to endorse the next state update (approval/rejection)
	_ = c.setSBEForKey(ctx, lotID, lot.OwnerMSP, MSP_DRAP)

	return lot, nil
}

// ReadLot returns a lot
func (c *ApiTransferContract) ReadLot(ctx contractapi.TransactionContextInterface, lotID string) (*ApiLot, error) {
	return c.readLot(ctx, lotID)
}

// UpdateMetadata replaces public metadata (owner only)
func (c *ApiTransferContract) UpdateMetadata(ctx contractapi.TransactionContextInterface, lotID, metadataJSON string) (*ApiLot, error) {
	lot, err := c.readLot(ctx, lotID)
	if err != nil {
		return nil, err
	}
	if err := c.checkOwner(ctx, lot); err != nil {
		return nil, err
	}
	md := map[string]string{}
	if strings.TrimSpace(metadataJSON) != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &md); err != nil {
			return nil, fmt.Errorf("invalid metadata JSON: %w", err)
		}
	}
	lot.Metadata = md
	if err := c.putLot(ctx, lot); err != nil {
		return nil, err
	}
	return lot, nil
}

// Consume decreases quantity (owner only). If <=0 -> CONSUMED
func (c *ApiTransferContract) Consume(ctx contractapi.TransactionContextInterface, lotID, amountStr string) (*ApiLot, error) {
	lot, err := c.readLot(ctx, lotID)
	if err != nil {
		return nil, err
	}
	if lot.Status == StatusPendingTransfer {
		return nil, errors.New("cannot consume while transfer is pending")
	}
	if err := c.checkOwner(ctx, lot); err != nil {
		return nil, err
	}
	amt, err := strconv.ParseFloat(amountStr, 64)
	if err != nil || amt <= 0 {
		return nil, fmt.Errorf("invalid amount %q", amountStr)
	}
	if lot.Quantity < amt {
		return nil, fmt.Errorf("insufficient quantity: have %.4f, need %.4f", lot.Quantity, amt)
	}
	lot.Quantity -= amt
	if lot.Quantity == 0 {
		lot.Status = StatusConsumed
	}
	if err := c.putLot(ctx, lot); err != nil {
		return nil, err
	}
	c.emit(ctx, EventQuantityUpdated, map[string]any{"lotId": lot.LotID, "quantity": lot.Quantity})
	return lot, nil
}

// Destroy marks lot as DESTROYED (owner only)
func (c *ApiTransferContract) Destroy(ctx contractapi.TransactionContextInterface, lotID string) (*ApiLot, error) {
	lot, err := c.readLot(ctx, lotID)
	if err != nil {
		return nil, err
	}
	if err := c.checkOwner(ctx, lot); err != nil {
		return nil, err
	}
	lot.Quantity = 0
	lot.Status = StatusDestroyed
	if err := c.putLot(ctx, lot); err != nil {
		return nil, err
	}
	c.emit(ctx, EventLotDestroyed, lot)
	return lot, nil
}

// DeleteLot removes the lot (owner only, not pending)
func (c *ApiTransferContract) DeleteLot(ctx contractapi.TransactionContextInterface, lotID string) error {
	lot, err := c.readLot(ctx, lotID)
	if err != nil {
		return err
	}
	if lot.Status == StatusPendingTransfer {
		return errors.New("cannot delete while transfer pending")
	}
	if err := c.checkOwner(ctx, lot); err != nil {
		return err
	}
	if err := ctx.GetStub().DelState(lotID); err != nil {
		return fmt.Errorf("delete lot %s: %w", lotID, err)
	}
	c.emit(ctx, EventLotDeleted, map[string]any{"lotId": lotID})
	_ = c.clearSBE(ctx, lotID)
	return nil
}

/* ------------------------------- DRAP Gate -------------------------------- */

// ApproveLotByDRAP sets drapApproved=true (DRAP only) and resets SBE to owner-only.
func (c *ApiTransferContract) ApproveLotByDRAP(ctx contractapi.TransactionContextInterface, lotID string, note string) (*ApiLot, error) {
	is, msp, err := isDRAP(ctx)
	if err != nil {
		return nil, err
	}
	if !is {
		return nil, fmt.Errorf("access denied: MSP %s is not DRAP (%s)", msp, MSP_DRAP)
	}
	lot, err := c.readLot(ctx, lotID)
	if err != nil {
		return nil, err
	}
	if lot.DRAPApproved {
		// idempotent ok
		return lot, nil
	}
	lot.DRAPApproved = true
	lot.DRAPNote = strings.TrimSpace(note)
	lot.DRAPAt = nowRFC3339(ctx)

	if err := c.putLot(ctx, lot); err != nil {
		return nil, err
	}
	// After approval, lock SBE to owner only (or clear to revert to chaincode policy)
	if err := c.setSBEForKey(ctx, lotID, lot.OwnerMSP); err != nil {
		return nil, fmt.Errorf("reset SBE to owner: %w", err)
	}
	c.emit(ctx, EventDRAPApproved, map[string]any{
		"lotId": lot.LotID, "note": lot.DRAPNote, "at": lot.DRAPAt,
	})
	return lot, nil
}

// RejectLotByDRAP sets drapApproved=false with note (DRAP only) and resets SBE to owner-only.
func (c *ApiTransferContract) RejectLotByDRAP(ctx contractapi.TransactionContextInterface, lotID string, reason string) (*ApiLot, error) {
	is, msp, err := isDRAP(ctx)
	if err != nil {
		return nil, err
	}
	if !is {
		return nil, fmt.Errorf("access denied: MSP %s is not DRAP (%s)", msp, MSP_DRAP)
	}
	lot, err := c.readLot(ctx, lotID)
	if err != nil {
		return nil, err
	}
	lot.DRAPApproved = false
	lot.DRAPNote = strings.TrimSpace(reason)
	lot.DRAPAt = nowRFC3339(ctx)

	if err := c.putLot(ctx, lot); err != nil {
		return nil, err
	}
	if err := c.setSBEForKey(ctx, lotID, lot.OwnerMSP); err != nil {
		return nil, fmt.Errorf("reset SBE to owner: %w", err)
	}
	c.emit(ctx, EventDRAPRejected, map[string]any{
		"lotId": lot.LotID, "reason": lot.DRAPNote, "at": lot.DRAPAt,
	})
	return lot, nil
}

/* ------------------------------ Transfer Flow ----------------------------- */

// ProposeTransfer initiates a two-step transfer (owner only).
// Requires DRAP approval first (drapApproved==true). Sets SBE to owner + proposed owner.
func (c *ApiTransferContract) ProposeTransfer(ctx contractapi.TransactionContextInterface, lotID, proposedOwnerMSP string) (*ApiLot, error) {
	lot, err := c.readLot(ctx, lotID)
	if err != nil {
		return nil, err
	}
	if err := c.checkOwner(ctx, lot); err != nil {
		return nil, err
	}
	if lot.Status == StatusPendingTransfer {
		return nil, errors.New("transfer already pending")
	}
	if lot.Quantity <= 0 {
		return nil, errors.New("cannot transfer zero quantity lot")
	}
	if !lot.DRAPApproved {
		return nil, errors.New("DRAP approval required before transfer")
	}

	lot.ProposedOwnerMSP = strings.TrimSpace(proposedOwnerMSP)
	lot.Status = StatusPendingTransfer
	if err := c.putLot(ctx, lot); err != nil {
		return nil, err
	}

	// SBE: require owner + proposed owner for the next update
	if err := c.setSBEForKey(ctx, lotID, lot.OwnerMSP, lot.ProposedOwnerMSP); err != nil {
		return nil, fmt.Errorf("set SBE: %w", err)
	}
	c.emit(ctx, EventTransferProposed, lot)
	return lot, nil
}

// AcceptTransfer by proposed owner. Ownership changes to proposed owner. Reset SBE -> new owner only.
func (c *ApiTransferContract) AcceptTransfer(ctx contractapi.TransactionContextInterface, lotID string) (*ApiLot, error) {
	lot, err := c.readLot(ctx, lotID)
	if err != nil {
		return nil, err
	}
	if lot.Status != StatusPendingTransfer {
		return nil, errors.New("no pending transfer")
	}
	if err := c.checkProposedOwner(ctx, lot); err != nil {
		return nil, err
	}
	lot.OwnerMSP = lot.ProposedOwnerMSP
	lot.ProposedOwnerMSP = ""
	lot.Status = StatusAccepted
	if err := c.putLot(ctx, lot); err != nil {
		return nil, err
	}
	if err := c.setSBEForKey(ctx, lotID, lot.OwnerMSP); err != nil {
		return nil, fmt.Errorf("reset SBE to new owner: %w", err)
	}
	c.emit(ctx, EventTransferAccepted, lot)
	return lot, nil
}

// RejectTransfer by proposed owner. Owner unchanged. Reset SBE -> owner only.
func (c *ApiTransferContract) RejectTransfer(ctx contractapi.TransactionContextInterface, lotID, reason string) (*ApiLot, error) {
	lot, err := c.readLot(ctx, lotID)
	if err != nil {
		return nil, err
	}
	if lot.Status != StatusPendingTransfer {
		return nil, errors.New("no pending transfer")
	}
	if err := c.checkProposedOwner(ctx, lot); err != nil {
		return nil, err
	}
	lot.ProposedOwnerMSP = ""
	lot.Status = StatusRejected
	if lot.Metadata == nil {
		lot.Metadata = map[string]string{}
	}
	if strings.TrimSpace(reason) != "" {
		lot.Metadata["lastRejectReason"] = reason
	}
	if err := c.putLot(ctx, lot); err != nil {
		return nil, err
	}
	if err := c.setSBEForKey(ctx, lotID, lot.OwnerMSP); err != nil {
		return nil, fmt.Errorf("reset SBE to owner: %w", err)
	}
	c.emit(ctx, EventTransferRejected, map[string]any{"lotId": lot.LotID, "reason": reason})
	return lot, nil
}

// CancelTransfer by owner. Reset SBE -> owner only.
func (c *ApiTransferContract) CancelTransfer(ctx contractapi.TransactionContextInterface, lotID, reason string) (*ApiLot, error) {
	lot, err := c.readLot(ctx, lotID)
	if err != nil {
		return nil, err
	}
	if err := c.checkOwner(ctx, lot); err != nil {
		return nil, err
	}
	if lot.Status != StatusPendingTransfer {
		return nil, errors.New("no pending transfer to cancel")
	}
	lot.ProposedOwnerMSP = ""
	lot.Status = StatusInStock
	if lot.Metadata == nil {
		lot.Metadata = map[string]string{}
	}
	if strings.TrimSpace(reason) != "" {
		lot.Metadata["lastCancelReason"] = reason
	}
	if err := c.putLot(ctx, lot); err != nil {
		return nil, err
	}
	if err := c.setSBEForKey(ctx, lotID, lot.OwnerMSP); err != nil {
		return nil, fmt.Errorf("reset SBE to owner: %w", err)
	}
	c.emit(ctx, EventTransferCancelled, map[string]any{"lotId": lot.LotID, "reason": reason})
	return lot, nil
}

/* ------------------------------- PDC methods ------------------------------ */

// PutSensitive upserts sensitive fields in private data (owner only).
func (c *ApiTransferContract) PutSensitive(
	ctx contractapi.TransactionContextInterface,
	lotID string,
	priceAmountStr string,
	priceCurrency string,
	hasPriceStr string,
	purityPercentStr string,
	hasPurityStr string,
	notes string,
) error {
	lot, err := c.readLot(ctx, lotID)
	if err != nil {
		return err
	}
	if err := c.checkOwner(ctx, lot); err != nil {
		return err
	}
	var priceAmount *float64
	var purityPercent *float64

	hasPrice := strings.EqualFold(strings.TrimSpace(hasPriceStr), "true")
	hasPurity := strings.EqualFold(strings.TrimSpace(hasPurityStr), "true")

	if hasPrice {
		val, err := strconv.ParseFloat(priceAmountStr, 64)
		if err != nil {
			return fmt.Errorf("invalid priceAmount %q: %w", priceAmountStr, err)
		}
		priceAmount = &val
	}
	if hasPurity {
		val, err := strconv.ParseFloat(purityPercentStr, 64)
		if err != nil {
			return fmt.Errorf("invalid purityPercent %q: %w", purityPercentStr, err)
		}
		purityPercent = &val
	}

	rec := &ApiLotSensitive{
		DocType:       DocTypeLotSensitive,
		LotID:         lotID,
		PriceAmount:   priceAmount,
		PriceCurrency: strings.TrimSpace(priceCurrency),
		PurityPercent: purityPercent,
		Notes:         strings.TrimSpace(notes),
		UpdatedAt:     nowRFC3339(ctx),
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal sensitive: %w", err)
	}
	return ctx.GetStub().PutPrivateData(CollectionSensitive, lotID, b)
}

// ReadSensitive returns PDC record (owner or proposed owner can read)
func (c *ApiTransferContract) ReadSensitive(ctx contractapi.TransactionContextInterface, lotID string) (*ApiLotSensitive, error) {
	lot, err := c.readLot(ctx, lotID)
	if err != nil {
		return nil, err
	}
	// access: owner or proposed owner (if pending)
	msp, err := getMSP(ctx)
	if err != nil {
		return nil, err
	}
	allowed := (lot.OwnerMSP == msp) || (lot.ProposedOwnerMSP == msp && lot.Status == StatusPendingTransfer)
	if !allowed {
		return nil, fmt.Errorf("access denied: MSP %s is not owner or proposed owner", msp)
	}
	b, err := ctx.GetStub().GetPrivateData(CollectionSensitive, lotID)
	if err != nil {
		return nil, fmt.Errorf("get private data: %w", err)
	}
	if len(b) == 0 {
		return nil, fmt.Errorf("no private record for lot %s", lotID)
	}
	var rec ApiLotSensitive
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal private record: %w", err)
	}
	return &rec, nil
}

// LinkSensitiveHash computes SHA-256 over selected private fields and stores hex in public metadata.sensitiveHash
func (c *ApiTransferContract) LinkSensitiveHash(ctx contractapi.TransactionContextInterface, lotID string) (*ApiLot, error) {
	lot, err := c.readLot(ctx, lotID)
	if err != nil {
		return nil, err
	}
	if err := c.checkOwner(ctx, lot); err != nil {
		return nil, err
	}
	b, err := ctx.GetStub().GetPrivateData(CollectionSensitive, lotID)
	if err != nil {
		return nil, fmt.Errorf("get private data: %w", err)
	}
	if len(b) == 0 {
		return nil, fmt.Errorf("no private record to hash for lot %s", lotID)
	}
	var rec ApiLotSensitive
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal private data: %w", err)
	}
	// Deterministic string for hashing
	var fields []string
	fields = append(fields, fmt.Sprintf("lotId=%s", rec.LotID))
	if rec.PriceAmount != nil {
		fields = append(fields, fmt.Sprintf("priceAmount=%.8f", *rec.PriceAmount))
	}
	if rec.PriceCurrency != "" {
		fields = append(fields, fmt.Sprintf("priceCurrency=%s", rec.PriceCurrency))
	}
	if rec.PurityPercent != nil {
		fields = append(fields, fmt.Sprintf("purityPercent=%.6f", *rec.PurityPercent))
	}
	if rec.Notes != "" {
		fields = append(fields, fmt.Sprintf("notes=%s", rec.Notes))
	}
	concat := strings.Join(fields, "|")
	h := sha256.Sum256([]byte(concat))
	hashHex := hex.EncodeToString(h[:])

	if lot.Metadata == nil {
		lot.Metadata = map[string]string{}
	}
	lot.Metadata["sensitiveHash"] = hashHex
	if err := c.putLot(ctx, lot); err != nil {
		return nil, err
	}
	return lot, nil
}

/* ------------------------------- Query/History ---------------------------- */

// GetAllLots (range scan) – demo use only
func (c *ApiTransferContract) GetAllLots(ctx contractapi.TransactionContextInterface) ([]*ApiLot, error) {
	iter, err := ctx.GetStub().GetStateByRange("", "")
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var out []*ApiLot
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		var lot ApiLot
		if err := json.Unmarshal(kv.Value, &lot); err == nil && lot.DocType == DocTypeLot {
			out = append(out, &lot)
		}
	}
	return out, nil
}

// GetLotsByOwner (rich query convenience)
func (c *ApiTransferContract) GetLotsByOwner(ctx contractapi.TransactionContextInterface, ownerMSP string) ([]*ApiLot, error) {
	selector := map[string]any{
		"selector": map[string]any{
			"docType":  DocTypeLot,
			"ownerMSP": ownerMSP,
		},
	}
	qb, _ := json.Marshal(selector)
	return c.queryLotsInternal(ctx, string(qb))
}

// QueryLots executes an arbitrary selector with optional pagination
func (c *ApiTransferContract) QueryLots(ctx contractapi.TransactionContextInterface, selectorJSON string, pageSizeStr string, bookmark string) (map[string]any, error) {
	pageSize := 0
	if strings.TrimSpace(pageSizeStr) != "" {
		n, err := strconv.Atoi(pageSizeStr)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid pageSize %q", pageSizeStr)
		}
		pageSize = n
	}

	var lots []*ApiLot
	var fetched int
	var nextBookmark string
	var err error

	if pageSize == 0 {
		lots, err = c.queryLotsInternal(ctx, selectorJSON)
		if err != nil {
			return nil, err
		}
		fetched = len(lots)
	} else {
		lots, fetched, nextBookmark, err = c.queryLotsPaged(ctx, selectorJSON, int32(pageSize), bookmark)
		if err != nil {
			return nil, err
		}
	}

	resp := map[string]any{
		"items":    lots,
		"fetched":  fetched,
		"bookmark": nextBookmark,
	}
	return resp, nil
}

func (c *ApiTransferContract) QueryLotsBy(ctx contractapi.TransactionContextInterface, name, status, ownerMSP string) ([]*ApiLot, error) {
	sel := map[string]any{
		"selector": map[string]any{
			"docType": DocTypeLot,
		},
	}
	s := sel["selector"].(map[string]any)
	if strings.TrimSpace(name) != "" {
		s["name"] = name
	}
	if strings.TrimSpace(status) != "" {
		s["status"] = status
	}
	if strings.TrimSpace(ownerMSP) != "" {
		s["ownerMSP"] = ownerMSP
	}
	qb, _ := json.Marshal(sel)
	return c.queryLotsInternal(ctx, string(qb))
}

func (c *ApiTransferContract) queryLotsInternal(ctx contractapi.TransactionContextInterface, selectorJSON string) ([]*ApiLot, error) {
	iter, err := ctx.GetStub().GetQueryResult(selectorJSON)
	if err != nil {
		return nil, fmt.Errorf("query result: %w", err)
	}
	defer iter.Close()

	var results []*ApiLot
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		var lot ApiLot
		if err := json.Unmarshal(kv.Value, &lot); err == nil && lot.DocType == DocTypeLot {
			results = append(results, &lot)
		}
	}
	return results, nil
}

func (c *ApiTransferContract) queryLotsPaged(ctx contractapi.TransactionContextInterface, selectorJSON string, pageSize int32, bookmark string) ([]*ApiLot, int, string, error) {
	iter, meta, err := ctx.GetStub().GetQueryResultWithPagination(selectorJSON, pageSize, bookmark)
	if err != nil {
		return nil, 0, "", fmt.Errorf("query paged: %w", err)
	}
	defer iter.Close()

	var results []*ApiLot
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, 0, "", err
		}
		var lot ApiLot
		if err := json.Unmarshal(kv.Value, &lot); err == nil && lot.DocType == DocTypeLot {
			results = append(results, &lot)
		}
	}
	return results, int(meta.FetchedRecordsCount), meta.Bookmark, nil
}

// GetHistory returns chronological history for a lot
func (c *ApiTransferContract) GetHistory(ctx contractapi.TransactionContextInterface, lotID string) ([]map[string]any, error) {
	iter, err := ctx.GetStub().GetHistoryForKey(lotID)
	if err != nil {
		return nil, fmt.Errorf("history: %w", err)
	}
	defer iter.Close()

	var out []map[string]any
	for iter.HasNext() {
		tx, err := iter.Next()
		if err != nil {
			return nil, err
		}
		entry := map[string]any{
			"txId":     tx.TxId,
			"isDelete": tx.IsDelete,
		}
		if tx.Timestamp != nil {
			entry["timestamp"] = time.Unix(tx.Timestamp.Seconds, int64(tx.Timestamp.Nanos)).UTC().Format(time.RFC3339)
		}
		if !tx.IsDelete && len(tx.Value) > 0 {
			var lot ApiLot
			if err := json.Unmarshal(tx.Value, &lot); err == nil {
				entry["value"] = lot
			}
		}
		out = append(out, entry)
	}
	return out, nil
}

/* ---------------------------------- main ---------------------------------- */

func main() {
	chaincode, err := contractapi.NewChaincode(new(ApiTransferContract))
	if err != nil {
		panic(fmt.Errorf("create chaincode: %w", err))
	}
	if err := chaincode.Start(); err != nil {
		panic(fmt.Errorf("start chaincode: %w", err))
	}
}
