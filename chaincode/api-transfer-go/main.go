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
	StatusPendingDRAPApproval = "PENDING_DRAP_APPROVAL" // NEW: Initial status for DRAP approval
	StatusInStock             = "IN_STOCK"
	StatusPendingTransfer     = "PENDING_TRANSFER"
	StatusAccepted            = "ACCEPTED"
	StatusRejected            = "REJECTED"
	StatusConsumed            = "CONSUMED"
	StatusDestroyed           = "DESTROYED"

	// Events
	EventLotCreated        = "LotCreated"
	EventLotDeleted        = "LotDeleted"
	EventLotDestroyed      = "LotDestroyed"
	EventQuantityUpdated   = "QuantityUpdated"
	EventTransferProposed  = "TransferProposed"
	EventTransferAccepted  = "TransferAccepted"
	EventTransferRejected  = "TransferRejected"
	EventTransferCancelled = "TransferCancelled"
	EventTransferExpired   = "TransferExpired"

	// DRAP-related events
	EventDRAPApproved = "DRAPApproved"
	EventDRAPRejected = "DRAPRejected"

	// MSP ID for DRAP
	MSP_DRAP = "drapMSP"

	// Transfer expiry duration (7 days)
	TransferExpiryHours = 168
)

// ApiLot is the public state of an API lot (non-sensitive)
type ApiLot struct {
	DocType            string            `json:"docType"`
	LotID              string            `json:"lotId"`
	Name               string            `json:"name"`
	BatchNumber        string            `json:"batchNumber"`
	Quantity           int64             `json:"quantity"`
	Unit               string            `json:"unit"`
	ManufactureDate    string            `json:"manufactureDate"`
	ExpiryDate         string            `json:"expiryDate"`
	OwnerMSP           string            `json:"ownerMSP"`
	SupplierMSP        string            `json:"supplierMSP"`
	ProposedOwnerMSP   string            `json:"proposedOwnerMSP"`
	Status             string            `json:"status"`
	DRAPApproved       bool              `json:"drapApproved"`
	DRAPNote           string            `json:"drapNote"`
	DRAPAt             string            `json:"drapAt"`
	TransferProposedAt string            `json:"transferProposedAt"`
	TransferExpiresAt  string            `json:"transferExpiresAt"`
	Metadata           map[string]string `json:"metadata"`
	CreatedAt          string            `json:"createdAt"`
	UpdatedAt          string            `json:"updatedAt"`
}

// ApiLotSensitive is stored in Private Data Collection
type ApiLotSensitive struct {
	DocType       string `json:"docType"`
	LotID         string `json:"lotId"`
	PriceAmount   int64  `json:"priceAmount"`
	PriceCurrency string `json:"priceCurrency"`
	PurityPercent int64  `json:"purityPercent"`
	Notes         string `json:"notes"`
	UpdatedAt     string `json:"updatedAt"`
}

// HashStruct for deterministic hashing
type SensitiveHashStruct struct {
	LotID         string `json:"lotId"`
	PriceAmount   int64  `json:"priceAmount"`
	PriceCurrency string `json:"priceCurrency"`
	PurityPercent int64  `json:"purityPercent"`
	Notes         string `json:"notes"`
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

/* --------------------------- Validation helpers --------------------------- */

func isValidDate(dateStr string) bool {
	if strings.TrimSpace(dateStr) == "" {
		return true
	}
	_, err := time.Parse("2006-01-02", strings.TrimSpace(dateStr))
	return err == nil
}

func isValidCurrency(currency string) bool {
	if strings.TrimSpace(currency) == "" {
		return true
	}
	// Basic ISO 4217 validation (3 letters)
	return len(currency) == 3 && strings.ToUpper(currency) == currency
}

func (c *ApiTransferContract) validateStatusTransition(oldStatus, newStatus string) error {
	// UPDATED: Added StatusPendingDRAPApproval transitions
	allowedTransitions := map[string][]string{
		StatusPendingDRAPApproval: {StatusInStock, StatusDestroyed}, // DRAP approval moves to IN_STOCK
		StatusInStock:             {StatusPendingTransfer, StatusConsumed, StatusDestroyed},
		StatusPendingTransfer:     {StatusAccepted, StatusRejected, StatusInStock},
		StatusAccepted:            {StatusInStock, StatusConsumed, StatusDestroyed},
		StatusRejected:            {StatusInStock},
		StatusConsumed:            {},
		StatusDestroyed:           {},
	}

	if validTransitions, exists := allowedTransitions[oldStatus]; exists {
		for _, valid := range validTransitions {
			if valid == newStatus {
				return nil
			}
		}
	}
	return fmt.Errorf("invalid status transition from %s to %s", oldStatus, newStatus)
}

func (c *ApiTransferContract) checkTransferExpiry(lot *ApiLot) error {
	if lot.TransferExpiresAt != "" {
		expiry, err := time.Parse(time.RFC3339, lot.TransferExpiresAt)
		if err != nil {
			return fmt.Errorf("invalid expiry date format: %w", err)
		}
		if time.Now().UTC().After(expiry) {
			return errors.New("transfer proposal has expired")
		}
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

// CreateLot adds a new lot (owner = creator MSP). Starts as PENDING_DRAP_APPROVAL.
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

	// Validate quantity (now in milligrams)
	qty, err := strconv.ParseInt(quantityStr, 10, 64)
	if err != nil || qty <= 0 {
		return nil, fmt.Errorf("invalid quantity %q (must be positive integer)", quantityStr)
	}

	// Validate dates
	if !isValidDate(manufactureDate) {
		return nil, fmt.Errorf("invalid manufacture date format %q (use YYYY-MM-DD)", manufactureDate)
	}
	if !isValidDate(expiryDate) {
		return nil, fmt.Errorf("invalid expiry date format %q (use YYYY-MM-DD)", expiryDate)
	}

	msp, err := getMSP(ctx)
	if err != nil {
		return nil, err
	}

	now := nowRFC3339(ctx)
	// CHANGED: Status starts as PENDING_DRAP_APPROVAL instead of IN_STOCK
	lot := &ApiLot{
		DocType:            DocTypeLot,
		LotID:              lotID,
		Name:               strings.TrimSpace(name),
		BatchNumber:        strings.TrimSpace(batchNumber),
		Quantity:           qty,
		Unit:               strings.TrimSpace(unit),
		ManufactureDate:    strings.TrimSpace(manufactureDate),
		ExpiryDate:         strings.TrimSpace(expiryDate),
		OwnerMSP:           msp,
		SupplierMSP:        msp,
		Status:             StatusPendingDRAPApproval, // CHANGED: Requires DRAP approval
		DRAPApproved:       false,
		DRAPNote:           "",
		DRAPAt:             "",
		ProposedOwnerMSP:   "",
		TransferProposedAt: "",
		TransferExpiresAt:  "",
		Metadata:           make(map[string]string),
		CreatedAt:          now,
		UpdatedAt:          now,
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
	if err := c.setSBEForKey(ctx, lotID, lot.OwnerMSP, MSP_DRAP); err != nil {
		return nil, fmt.Errorf("set initial SBE: %w", err)
	}

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

// Consume decreases quantity (owner only). If <=0 -> CONSUMED. Requires DRAP approval.
func (c *ApiTransferContract) Consume(ctx contractapi.TransactionContextInterface, lotID, amountStr string) (*ApiLot, error) {
	lot, err := c.readLot(ctx, lotID)
	if err != nil {
		return nil, err
	}

	// NEW: Check if lot is DRAP approved
	if !lot.DRAPApproved {
		return nil, errors.New("DRAP approval required before consumption")
	}

	if lot.Status == StatusPendingTransfer {
		return nil, errors.New("cannot consume while transfer is pending")
	}
	if err := c.checkOwner(ctx, lot); err != nil {
		return nil, err
	}

	amt, err := strconv.ParseInt(amountStr, 10, 64)
	if err != nil || amt <= 0 {
		return nil, fmt.Errorf("invalid amount %q (must be positive integer)", amountStr)
	}
	if lot.Quantity < amt {
		return nil, fmt.Errorf("insufficient quantity: have %d, need %d", lot.Quantity, amt)
	}

	lot.Quantity -= amt
	if lot.Quantity == 0 {
		if err := c.validateStatusTransition(lot.Status, StatusConsumed); err != nil {
			return nil, err
		}
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

	if err := c.validateStatusTransition(lot.Status, StatusDestroyed); err != nil {
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

// ApproveLotByDRAP sets drapApproved=true and moves to IN_STOCK (DRAP only)
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

	if lot.Status != StatusPendingDRAPApproval {
		return nil, fmt.Errorf("lot is not pending DRAP approval (current status: %s)", lot.Status)
	}

	if lot.DRAPApproved {
		// idempotent ok
		return lot, nil
	}

	lot.DRAPApproved = true
	lot.DRAPNote = strings.TrimSpace(note)
	lot.DRAPAt = nowRFC3339(ctx)
	lot.Status = StatusInStock // Move to IN_STOCK after approval

	if err := c.putLot(ctx, lot); err != nil {
		return nil, err
	}
	// After approval, lock SBE to owner only
	if err := c.setSBEForKey(ctx, lotID, lot.OwnerMSP); err != nil {
		return nil, fmt.Errorf("reset SBE to owner: %w", err)
	}
	c.emit(ctx, EventDRAPApproved, map[string]any{
		"lotId": lot.LotID, "note": lot.DRAPNote, "at": lot.DRAPAt,
	})
	return lot, nil
}

// RejectLotByDRAP sets drapApproved=false with note (DRAP only) - stays in PENDING_DRAP_APPROVAL
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

	if lot.Status != StatusPendingDRAPApproval {
		return nil, fmt.Errorf("lot is not pending DRAP approval (current status: %s)", lot.Status)
	}

	lot.DRAPApproved = false
	lot.DRAPNote = strings.TrimSpace(reason)
	lot.DRAPAt = nowRFC3339(ctx)
	// Status remains PENDING_DRAP_APPROVAL

	if err := c.putLot(ctx, lot); err != nil {
		return nil, err
	}
	// SBE remains with owner + DRAP for potential re-approval
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
	// UPDATED: Check for IN_STOCK status (after DRAP approval)
	if lot.Status != StatusInStock {
		return nil, fmt.Errorf("lot must be IN_STOCK for transfer (current status: %s)", lot.Status)
	}
	if !lot.DRAPApproved {
		return nil, errors.New("DRAP approval required before transfer")
	}

	if err := c.validateStatusTransition(lot.Status, StatusPendingTransfer); err != nil {
		return nil, err
	}

	now := nowRFC3339(ctx)
	expiry := time.Now().UTC().Add(time.Hour * TransferExpiryHours).Format(time.RFC3339)

	lot.ProposedOwnerMSP = strings.TrimSpace(proposedOwnerMSP)
	lot.Status = StatusPendingTransfer
	lot.TransferProposedAt = now
	lot.TransferExpiresAt = expiry

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
	if err := c.checkTransferExpiry(lot); err != nil {
		return nil, err
	}

	if err := c.validateStatusTransition(lot.Status, StatusAccepted); err != nil {
		return nil, err
	}

	lot.OwnerMSP = lot.ProposedOwnerMSP
	lot.ProposedOwnerMSP = ""
	lot.Status = StatusAccepted
	lot.TransferProposedAt = ""
	lot.TransferExpiresAt = ""

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

	if err := c.validateStatusTransition(lot.Status, StatusRejected); err != nil {
		return nil, err
	}

	lot.ProposedOwnerMSP = ""
	lot.Status = StatusRejected
	lot.TransferProposedAt = ""
	lot.TransferExpiresAt = ""
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

	if err := c.validateStatusTransition(lot.Status, StatusInStock); err != nil {
		return nil, err
	}

	lot.ProposedOwnerMSP = ""
	lot.Status = StatusInStock
	lot.TransferProposedAt = ""
	lot.TransferExpiresAt = ""
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

// ExpireTransfer checks and expires pending transfers (can be called by anyone)
func (c *ApiTransferContract) ExpireTransfer(ctx contractapi.TransactionContextInterface, lotID string) (*ApiLot, error) {
	lot, err := c.readLot(ctx, lotID)
	if err != nil {
		return nil, err
	}
	if lot.Status != StatusPendingTransfer {
		return nil, errors.New("no pending transfer")
	}

	if err := c.checkTransferExpiry(lot); err == nil {
		return nil, errors.New("transfer has not expired yet")
	}

	// Transfer has expired
	lot.ProposedOwnerMSP = ""
	lot.Status = StatusInStock
	lot.TransferProposedAt = ""
	lot.TransferExpiresAt = ""
	if lot.Metadata == nil {
		lot.Metadata = map[string]string{}
	}
	lot.Metadata["expiredAt"] = nowRFC3339(ctx)

	if err := c.putLot(ctx, lot); err != nil {
		return nil, err
	}
	if err := c.setSBEForKey(ctx, lotID, lot.OwnerMSP); err != nil {
		return nil, fmt.Errorf("reset SBE to owner: %w", err)
	}
	c.emit(ctx, EventTransferExpired, map[string]any{"lotId": lot.LotID})
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

	// Validate currency
	if !isValidCurrency(priceCurrency) {
		return fmt.Errorf("invalid currency code %q (must be 3-letter ISO code)", priceCurrency)
	}

	var priceAmount int64 = -1   // Use -1 to represent "not set"
	var purityPercent int64 = -1 // Use -1 to represent "not set"

	hasPrice := strings.EqualFold(strings.TrimSpace(hasPriceStr), "true")
	hasPurity := strings.EqualFold(strings.TrimSpace(hasPurityStr), "true")

	if hasPrice {
		val, err := strconv.ParseInt(priceAmountStr, 10, 64)
		if err != nil || val < 0 {
			return fmt.Errorf("invalid priceAmount %q (must be non-negative integer)", priceAmountStr)
		}
		priceAmount = val
	}
	if hasPurity {
		val, err := strconv.ParseInt(purityPercentStr, 10, 64)
		if err != nil || val < 0 || val > 10000 {
			return fmt.Errorf("invalid purityPercent %q (must be 0-10000 basis points)", purityPercentStr)
		}
		purityPercent = val
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

	// Use deterministic JSON marshaling for hashing
	hashStruct := SensitiveHashStruct{
		LotID:         rec.LotID,
		PriceAmount:   rec.PriceAmount,
		PriceCurrency: rec.PriceCurrency,
		PurityPercent: rec.PurityPercent,
		Notes:         rec.Notes,
	}
	hashBytes, err := json.Marshal(hashStruct)
	if err != nil {
		return nil, fmt.Errorf("marshal hash struct: %w", err)
	}

	h := sha256.Sum256(hashBytes)
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

// GetLotsPendingDRAPApproval - NEW: Get all lots waiting for DRAP approval
func (c *ApiTransferContract) GetLotsPendingDRAPApproval(ctx contractapi.TransactionContextInterface) ([]*ApiLot, error) {
	selector := map[string]any{
		"selector": map[string]any{
			"docType": DocTypeLot,
			"status":  StatusPendingDRAPApproval,
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
