package main

import (
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

/*
   Manufacturing Chaincode (Go)
   ---------------------------------------------
   - Produces finished drug batches from API lots owned by the Manufacturer
   - Validates against a declared formulation (BOM)
   - Consumes raw lots via CC2CC calls to "apitransfer" (same channel)
   - DRAP (drapMSP) must approve the finished batch before transfer
   - Manufacturer -> Distributor transfer with accept/reject + SBE
   - Rich queries + paginated query helper
   - Automatic expiry for pending transfers (7 days)
   - PROPER CHAIN OF CUSTODY: ProducerMSP never changes, CurrentOwnerMSP tracks ownership

   Channel: rawmaterialsupply
   External CC: apitransfer (Chaincode #1 for raw API lots)
*/

// ---------------- Constants & IDs ----------------

const (
	// External chaincode (Chaincode #1 for raw APIs)
	apiCCName  = "apitransfer" // CORRECTED: matches your deployed chaincode name
	apiChannel = ""            // empty => same channel; keep empty for atomic CC2CC

	// MSPs
	mspDRAP = "drapMSP"

	// Generic statuses
	statusStock = "IN_STOCK"

	// Drug batch statuses
	StatusPendingTransfer = "PENDING_TRANSFER"
	StatusAccepted        = "ACCEPTED"
	StatusRejected        = "REJECTED"
	StatusDestroyed       = "DESTROYED"
	StatusExpired         = "EXPIRED" // New status for expired transfers

	// DocTypes
	DocTypeFormulation = "drug.formulation"
	DocTypeBatch       = "drug.batch"

	// Events
	EventFormulationCreated = "FormulationCreated"
	EventFormulationUpdated = "FormulationUpdated"
	EventDrugProduced       = "DrugProduced"
	EventBatchDestroyed     = "BatchDestroyed"
	EventBatchTransferProp  = "BatchTransferProposed"
	EventBatchAccepted      = "BatchTransferAccepted"
	EventBatchRejected      = "BatchTransferRejected"
	EventBatchCancelled     = "BatchTransferCancelled"
	EventBatchExpired       = "BatchTransferExpired" // New event for expired transfers
	EventDRAPApproved       = "DrugDRAPApproved"
	EventDRAPRejected       = "DrugDRAPRejected"

	// Transfer expiry settings
	TransferExpiryDays = 7
)

// ---------------- Data Models ----------------

// DrugFormulation defines required ingredient amounts PER UNIT of final drug.
type DrugFormulation struct {
	DocType      string             `json:"docType"` // "drug.formulation"
	DrugCode     string             `json:"drugCode"`
	Unit         string             `json:"unit"`         // unit for output quantity (e.g., "packs")
	Requirements map[string]float64 `json:"requirements"` // ingredientName -> amount required per 1 output unit
	CreatedAt    string             `json:"createdAt"`
	UpdatedAt    string             `json:"updatedAt"`
	OwnerMSP     string             `json:"ownerMSP"` // who defined/owns this formulation (typically manufacturerMSP)
}

// InputUse records how much of which lot was consumed.
type InputUse struct {
	LotID          string  `json:"lotId"`
	IngredientName string  `json:"ingredientName"`
	Amount         float64 `json:"amount"`
}

// DrugBatch is the produced lot of finished drug.
type DrugBatch struct {
	DocType            string            `json:"docType"` // "drug.batch"
	BatchID            string            `json:"batchId"`
	DrugCode           string            `json:"drugCode"`
	Quantity           float64           `json:"quantity"` // output units
	Unit               string            `json:"unit"`
	ProducerMSP        string            `json:"producerMSP"`     // NEVER CHANGES - who manufactured the drug
	CurrentOwnerMSP    string            `json:"currentOwnerMSP"` // CHANGES - who currently owns the batch
	Status             string            `json:"status"`
	Inputs             []InputUse        `json:"inputs"` // actual raw consumptions
	DRAPApproved       bool              `json:"drapApproved"`
	DRAPNote           string            `json:"drapNote"`
	DRAPAt             string            `json:"drapAt"`
	ProposedOwnerMSP   string            `json:"proposedOwnerMSP"`
	TransferProposedAt string            `json:"transferProposedAt"`
	TransferExpiresAt  string            `json:"transferExpiresAt"`
	Metadata           map[string]string `json:"metadata"`
	CreatedAt          string            `json:"createdAt"`
	UpdatedAt          string            `json:"updatedAt"`
}

// PagedBatchesResult is a helper return type for paginated queries.
type PagedBatchesResult struct {
	Items    []*DrugBatch `json:"items"`
	Fetched  int          `json:"fetched"`
	Bookmark string       `json:"bookmark"`
}

// ---------------- Contract ----------------

type ManufacturingContract struct {
	contractapi.Contract
}

// ---------------- Utility Helpers ----------------

func nowRFC3339(ctx contractapi.TransactionContextInterface) string {
	ts, err := ctx.GetStub().GetTxTimestamp()
	if err == nil && ts != nil {
		return time.Unix(ts.Seconds, int64(ts.Nanos)).UTC().Format(time.RFC3339)
	}
	return time.Now().UTC().Format(time.RFC3339)
}

func getMSP(ctx contractapi.TransactionContextInterface) (string, error) {
	m, err := cid.GetMSPID(ctx.GetStub())
	if err != nil {
		return "", fmt.Errorf("get MSP: %w", err)
	}
	return m, nil
}

func isDRAP(ctx contractapi.TransactionContextInterface) (bool, string, error) {
	msp, err := getMSP(ctx)
	if err != nil {
		return false, "", err
	}
	return msp == mspDRAP, msp, nil
}

func emit(ctx contractapi.TransactionContextInterface, name string, v any) {
	if b, err := json.Marshal(v); err == nil {
		_ = ctx.GetStub().SetEvent(name, b)
	}
}

func (c *ManufacturingContract) setSBE(ctx contractapi.TransactionContextInterface, key string, orgMSPs ...string) error {
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

func (c *ManufacturingContract) clearSBE(ctx contractapi.TransactionContextInterface, key string) error {
	return ctx.GetStub().SetStateValidationParameter(key, nil)
}

// ---------------- CRUD: Formulations ----------------

// CreateFormulation(drugCode, unit, requirementsJSON)
// requirementsJSON example: {"Paracetamol":0.5,"Starch":0.2,"Povidone":0.05}
func (c *ManufacturingContract) CreateFormulation(ctx contractapi.TransactionContextInterface, drugCode, unit, requirementsJSON string) (*DrugFormulation, error) {
	drugCode = strings.TrimSpace(drugCode)
	if drugCode == "" {
		return nil, errors.New("drugCode required")
	}
	key := "FORM_" + drugCode
	exists, err := c.keyExists(ctx, key)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("formulation for %s already exists", drugCode)
	}
	req := map[string]float64{}
	if err := json.Unmarshal([]byte(requirementsJSON), &req); err != nil {
		return nil, fmt.Errorf("requirements JSON invalid: %w", err)
	}
	for k, v := range req {
		if strings.TrimSpace(k) == "" || v <= 0 {
			return nil, fmt.Errorf("invalid requirement entry %q: %.6f", k, v)
		}
	}
	msp, _ := getMSP(ctx)
	now := nowRFC3339(ctx)
	f := &DrugFormulation{
		DocType:      DocTypeFormulation,
		DrugCode:     drugCode,
		Unit:         strings.TrimSpace(unit),
		Requirements: req,
		CreatedAt:    now,
		UpdatedAt:    now,
		OwnerMSP:     msp,
	}
	if err := putJSON(ctx, key, f); err != nil {
		return nil, err
	}
	emit(ctx, EventFormulationCreated, f)
	return f, nil
}

func (c *ManufacturingContract) ReadFormulation(ctx contractapi.TransactionContextInterface, drugCode string) (*DrugFormulation, error) {
	drugCode = strings.TrimSpace(drugCode)
	key := "FORM_" + drugCode
	var f DrugFormulation
	if err := getJSON(ctx, key, &f); err != nil {
		return nil, err
	}
	return &f, nil
}

// UpdateFormulation replaces requirements & unit (owner MSP of formulation only)
func (c *ManufacturingContract) UpdateFormulation(ctx contractapi.TransactionContextInterface, drugCode, unit, requirementsJSON string) (*DrugFormulation, error) {
	f, err := c.ReadFormulation(ctx, drugCode)
	if err != nil {
		return nil, err
	}
	msp, _ := getMSP(ctx)
	if f.OwnerMSP != msp {
		return nil, fmt.Errorf("only owner MSP %s can update (caller %s)", f.OwnerMSP, msp)
	}
	req := map[string]float64{}
	if err := json.Unmarshal([]byte(requirementsJSON), &req); err != nil {
		return nil, fmt.Errorf("requirements JSON invalid: %w", err)
	}
	for k, v := range req {
		if strings.TrimSpace(k) == "" || v <= 0 {
			return nil, fmt.Errorf("invalid requirement %q: %.6f", k, v)
		}
	}
	f.Requirements = req
	f.Unit = strings.TrimSpace(unit)
	f.UpdatedAt = nowRFC3339(ctx)
	if err := putJSON(ctx, "FORM_"+f.DrugCode, f); err != nil {
		return nil, err
	}
	emit(ctx, EventFormulationUpdated, f)
	return f, nil
}

// ---------------- Production ----------------

// ProduceDrug creates a DrugBatch after consuming raw lots via apitransfer chaincode.
// inputsJSON: [{"lotId":"LOT1","ingredientName":"Paracetamol","amount":"50.0"}, ...]
func (c *ManufacturingContract) ProduceDrug(
	ctx contractapi.TransactionContextInterface,
	batchID, drugCode, outputQtyStr, unit, inputsJSON string,
) (*DrugBatch, error) {

	batchID = strings.TrimSpace(batchID)
	if batchID == "" {
		return nil, errors.New("batchID required")
	}
	key := "BATCH_" + batchID
	exists, err := c.keyExists(ctx, key)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("batch %s already exists", batchID)
	}

	f, err := c.ReadFormulation(ctx, drugCode)
	if err != nil {
		return nil, fmt.Errorf("formulation not found for %s: %w", drugCode, err)
	}
	outputQty, err := strconv.ParseFloat(outputQtyStr, 64)
	if err != nil || outputQty <= 0 {
		return nil, fmt.Errorf("invalid output quantity %q", outputQtyStr)
	}

	// Parse inputs
	type inRec struct {
		LotID          string `json:"lotId"`
		IngredientName string `json:"ingredientName"`
		Amount         string `json:"amount"`
	}
	var inList []inRec
	if err := json.Unmarshal([]byte(inputsJSON), &inList); err != nil {
		return nil, fmt.Errorf("inputs JSON invalid: %w", err)
	}
	if len(inList) == 0 {
		return nil, errors.New("inputs required")
	}

	// Aggregate provided amounts by ingredient
	provided := map[string]float64{}
	for _, x := range inList {
		a, err := strconv.ParseFloat(x.Amount, 64)
		if err != nil || a <= 0 {
			return nil, fmt.Errorf("invalid amount for lot %s: %q", x.LotID, x.Amount)
		}
		provided[x.IngredientName] += a
	}

	// Compute required amounts based on formulation
	required := map[string]float64{}
	for ing, perUnit := range f.Requirements {
		required[ing] = perUnit * outputQty
	}

	// Validate that provided >= required for each ingredient in formulation
	const epsilon = 1e-9
	for ing, req := range required {
		if provided[ing] < req-epsilon {
			return nil, fmt.Errorf("insufficient ingredient %s: need %.6f, provided %.6f", ing, req, provided[ing])
		}
	}

	// Verify each lot belongs to caller MSP and is of the correct ingredient; then consume.
	callerMSP, _ := getMSP(ctx)
	var uses []InputUse

	// For each input:
	// 1) apitransfer.ReadLot(lotId) -> check ownerMSP==caller and name==ingredient
	// 2) apitransfer.Consume(lotId, amount)
	for _, x := range inList {
		amount, _ := strconv.ParseFloat(x.Amount, 64)

		// 1) ReadLot
		args := [][]byte{[]byte("ReadLot"), []byte(x.LotID)}
		resp := ctx.GetStub().InvokeChaincode(apiCCName, args, apiChannel)
		if resp.Status != 200 {
			return nil, fmt.Errorf("apitransfer.ReadLot(%s) failed: %s", x.LotID, string(resp.Payload))
		}
		var lot struct {
			DocType      string  `json:"docType"`
			LotID        string  `json:"lotId"`
			Name         string  `json:"name"`
			OwnerMSP     string  `json:"ownerMSP"`
			Quantity     float64 `json:"quantity"`
			Status       string  `json:"status"`
			DRAPApproved bool    `json:"drapApproved"`
		}
		if err := json.Unmarshal(resp.Payload, &lot); err != nil {
			return nil, fmt.Errorf("unmarshal lot %s: %w", x.LotID, err)
		}
		if lot.OwnerMSP != callerMSP {
			return nil, fmt.Errorf("lot %s not owned by caller MSP %s", x.LotID, callerMSP)
		}
		if strings.TrimSpace(lot.Name) != strings.TrimSpace(x.IngredientName) {
			return nil, fmt.Errorf("lot %s ingredient mismatch: lot.Name=%s, expected %s", x.LotID, lot.Name, x.IngredientName)
		}
		if lot.Status == StatusPendingTransfer {
			return nil, fmt.Errorf("lot %s is pending transfer", x.LotID)
		}
		// Check DRAP approval for raw API lot
		if !lot.DRAPApproved {
			return nil, fmt.Errorf("lot %s is not DRAP approved", x.LotID)
		}
		if lot.Quantity < amount-epsilon {
			return nil, fmt.Errorf("lot %s insufficient quantity: have %.6f need %.6f", x.LotID, lot.Quantity, amount)
		}

		// 2) Consume
		args = [][]byte{[]byte("Consume"), []byte(x.LotID), []byte(x.Amount)}
		resp2 := ctx.GetStub().InvokeChaincode(apiCCName, args, apiChannel)
		if resp2.Status != 200 {
			return nil, fmt.Errorf("apitransfer.Consume(%s,%s) failed: %s", x.LotID, x.Amount, string(resp2.Payload))
		}

		uses = append(uses, InputUse{
			LotID:          x.LotID,
			IngredientName: x.IngredientName,
			Amount:         amount,
		})
	}

	// Create batch - FIXED: Initialize all required fields without omitempty
	now := nowRFC3339(ctx)
	b := &DrugBatch{
		DocType:            DocTypeBatch,
		BatchID:            batchID,
		DrugCode:           f.DrugCode,
		Quantity:           outputQty,
		Unit:               strings.TrimSpace(unit),
		ProducerMSP:        callerMSP, // Original manufacturer - NEVER CHANGES
		CurrentOwnerMSP:    callerMSP, // Current owner - starts as manufacturer
		Status:             statusStock,
		Inputs:             uses,
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
	if err := putJSON(ctx, key, b); err != nil {
		return nil, err
	}
	emit(ctx, EventDrugProduced, b)

	// SBE: require Manufacturer (producer) + DRAP to endorse the next decision (approval/rejection)
	if err := c.setSBE(ctx, key, b.CurrentOwnerMSP, mspDRAP); err != nil {
		return nil, fmt.Errorf("set SBE: %w", err)
	}

	return b, nil
}

// ---------------- DRAP Gate for finished drug ----------------

func (c *ManufacturingContract) ApproveDrugBatchByDRAP(ctx contractapi.TransactionContextInterface, batchID, note string) (*DrugBatch, error) {
	is, caller, err := isDRAP(ctx)
	if err != nil {
		return nil, err
	}
	if !is {
		return nil, fmt.Errorf("access denied: MSP %s is not DRAP (%s)", caller, mspDRAP)
	}
	b, err := c.readBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	b.DRAPApproved = true
	b.DRAPNote = strings.TrimSpace(note)
	b.DRAPAt = nowRFC3339(ctx)
	if err := c.putBatch(ctx, b); err != nil {
		return nil, err
	}
	// After DRAP approval, keep both current owner and DRAP in SBE for transfer operations
	if err := c.setSBE(ctx, "BATCH_"+b.BatchID, b.CurrentOwnerMSP, mspDRAP); err != nil {
		return nil, fmt.Errorf("set SBE: %w", err)
	}
	emit(ctx, EventDRAPApproved, map[string]any{"batchId": b.BatchID, "note": b.DRAPNote, "at": b.DRAPAt})
	return b, nil
}

func (c *ManufacturingContract) RejectDrugBatchByDRAP(ctx contractapi.TransactionContextInterface, batchID, reason string) (*DrugBatch, error) {
	is, caller, err := isDRAP(ctx)
	if err != nil {
		return nil, err
	}
	if !is {
		return nil, fmt.Errorf("access denied: MSP %s is not DRAP (%s)", caller, mspDRAP)
	}
	b, err := c.readBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	b.DRAPApproved = false
	b.DRAPNote = strings.TrimSpace(reason)
	b.DRAPAt = nowRFC3339(ctx)
	if err := c.putBatch(ctx, b); err != nil {
		return nil, err
	}
	// After DRAP rejection, keep both current owner and DRAP in SBE
	if err := c.setSBE(ctx, "BATCH_"+b.BatchID, b.CurrentOwnerMSP, mspDRAP); err != nil {
		return nil, fmt.Errorf("set SBE: %w", err)
	}
	emit(ctx, EventDRAPRejected, map[string]any{"batchId": b.BatchID, "reason": b.DRAPNote, "at": b.DRAPAt})
	return b, nil
}

// ---------------- Post-approval Transfer (Manufacturer -> Distributor) ----------------

// ProposeBatchTransfer requires DRAPApproved == true
func (c *ManufacturingContract) ProposeBatchTransfer(ctx contractapi.TransactionContextInterface, batchID, proposedOwnerMSP string) (*DrugBatch, error) {
	b, err := c.readBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if err := c.checkCurrentOwner(ctx, b); err != nil {
		return nil, err
	}
	if !b.DRAPApproved {
		return nil, errors.New("DRAP approval required before transfer to distributor")
	}
	if b.Status == StatusPendingTransfer {
		return nil, errors.New("transfer already pending")
	}

	proposedOwnerMSP = strings.TrimSpace(proposedOwnerMSP)
	if proposedOwnerMSP == "" {
		return nil, errors.New("proposedOwnerMSP cannot be empty")
	}
	if proposedOwnerMSP == b.CurrentOwnerMSP {
		return nil, errors.New("cannot transfer to current owner")
	}

	// Use transaction timestamp for consistency across all peers
	txTime, err := ctx.GetStub().GetTxTimestamp()
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction timestamp: %w", err)
	}

	// Convert to time.Time
	txTimeTime := time.Unix(txTime.Seconds, int64(txTime.Nanos))

	b.ProposedOwnerMSP = proposedOwnerMSP
	b.Status = StatusPendingTransfer
	b.TransferProposedAt = txTimeTime.UTC().Format(time.RFC3339)

	// Set expiry time (7 days from transaction time) - using same timestamp for consistency
	expiryTime := txTimeTime.UTC().Add(time.Hour * 24 * TransferExpiryDays)
	b.TransferExpiresAt = expiryTime.Format(time.RFC3339)

	if err := c.putBatch(ctx, b); err != nil {
		return nil, err
	}
	// SBE: require current owner + proposed owner
	if err := c.setSBE(ctx, "BATCH_"+b.BatchID, b.CurrentOwnerMSP, b.ProposedOwnerMSP); err != nil {
		return nil, fmt.Errorf("set SBE: %w", err)
	}
	emit(ctx, EventBatchTransferProp, b)
	return b, nil
}

func (c *ManufacturingContract) AcceptBatchTransfer(ctx contractapi.TransactionContextInterface, batchID string) (*DrugBatch, error) {
	b, err := c.readBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if b.Status != StatusPendingTransfer {
		return nil, errors.New("no pending transfer")
	}

	// Check if transfer has expired
	if c.isTransferExpired(b) {
		return nil, errors.New("transfer has expired")
	}

	if err := c.checkProposedOwner(ctx, b); err != nil {
		return nil, err
	}

	// CORRECTED: Change current owner, NOT producer
	b.CurrentOwnerMSP = b.ProposedOwnerMSP
	b.ProposedOwnerMSP = ""
	b.Status = StatusAccepted
	b.TransferProposedAt = "" // Clear transfer timestamps
	b.TransferExpiresAt = ""  // Clear transfer timestamps
	if err := c.putBatch(ctx, b); err != nil {
		return nil, err
	}
	if err := c.setSBE(ctx, "BATCH_"+b.BatchID, b.CurrentOwnerMSP); err != nil {
		return nil, fmt.Errorf("reset SBE to new owner: %w", err)
	}
	emit(ctx, EventBatchAccepted, b)
	return b, nil
}

func (c *ManufacturingContract) RejectBatchTransfer(ctx contractapi.TransactionContextInterface, batchID, reason string) (*DrugBatch, error) {
	b, err := c.readBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if b.Status != StatusPendingTransfer {
		return nil, errors.New("no pending transfer")
	}

	// Check if transfer has expired
	if c.isTransferExpired(b) {
		return nil, errors.New("transfer has expired")
	}

	if err := c.checkProposedOwner(ctx, b); err != nil {
		return nil, err
	}
	b.ProposedOwnerMSP = ""
	b.Status = StatusRejected
	b.TransferProposedAt = "" // Clear transfer timestamps
	b.TransferExpiresAt = ""  // Clear transfer timestamps
	if b.Metadata == nil {
		b.Metadata = make(map[string]string)
	}
	if rs := strings.TrimSpace(reason); rs != "" {
		b.Metadata["lastRejectReason"] = rs
	}
	if err := c.putBatch(ctx, b); err != nil {
		return nil, err
	}
	if err := c.setSBE(ctx, "BATCH_"+b.BatchID, b.CurrentOwnerMSP); err != nil {
		return nil, fmt.Errorf("reset SBE to owner: %w", err)
	}
	emit(ctx, EventBatchRejected, map[string]any{"batchId": b.BatchID, "reason": reason})
	return b, nil
}

func (c *ManufacturingContract) CancelBatchTransfer(ctx contractapi.TransactionContextInterface, batchID, reason string) (*DrugBatch, error) {
	b, err := c.readBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if err := c.checkCurrentOwner(ctx, b); err != nil {
		return nil, err
	}
	if b.Status != StatusPendingTransfer {
		return nil, errors.New("no pending transfer to cancel")
	}
	b.ProposedOwnerMSP = ""
	b.Status = statusStock
	b.TransferProposedAt = "" // Clear transfer timestamps
	b.TransferExpiresAt = ""  // Clear transfer timestamps
	if b.Metadata == nil {
		b.Metadata = make(map[string]string)
	}
	if cs := strings.TrimSpace(reason); cs != "" {
		b.Metadata["lastCancelReason"] = cs
	}
	if err := c.putBatch(ctx, b); err != nil {
		return nil, err
	}
	if err := c.setSBE(ctx, "BATCH_"+b.BatchID, b.CurrentOwnerMSP); err != nil {
		return nil, fmt.Errorf("reset SBE to owner: %w", err)
	}
	emit(ctx, EventBatchCancelled, map[string]any{"batchId": b.BatchID, "reason": reason})
	return b, nil
}

// ---------------- Automatic Expiry Functionality ----------------

// ExpireBatchTransfer can be called by anyone to expire a pending transfer that has passed its expiry time
func (c *ManufacturingContract) ExpireBatchTransfer(ctx contractapi.TransactionContextInterface, batchID string) (*DrugBatch, error) {
	b, err := c.readBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}

	// Check if batch is in pending transfer status and has expired
	if b.Status != StatusPendingTransfer {
		return nil, errors.New("batch is not in pending transfer status")
	}

	if !c.isTransferExpired(b) {
		return nil, errors.New("transfer has not expired yet")
	}

	// Mark as expired
	b.Status = StatusExpired
	if b.Metadata == nil {
		b.Metadata = make(map[string]string)
	}
	b.Metadata["expiryReason"] = "Transfer not accepted within 7 days"
	b.ProposedOwnerMSP = ""
	b.TransferProposedAt = "" // Clear transfer timestamps
	b.TransferExpiresAt = ""  // Clear transfer timestamps

	if err := c.putBatch(ctx, b); err != nil {
		return nil, err
	}

	// Reset SBE to only the current owner
	if err := c.setSBE(ctx, "BATCH_"+b.BatchID, b.CurrentOwnerMSP); err != nil {
		return nil, fmt.Errorf("reset SBE to owner: %w", err)
	}

	emit(ctx, EventBatchExpired, map[string]any{
		"batchId": b.BatchID,
		"reason":  "Transfer not accepted within 7 days",
	})

	return b, nil
}

// ExpireAllPendingTransfers can be called by anyone to expire all pending transfers that have passed their expiry time
func (c *ManufacturingContract) ExpireAllPendingTransfers(ctx contractapi.TransactionContextInterface) ([]*DrugBatch, error) {
	// Use transaction timestamp for consistency
	txTime, err := ctx.GetStub().GetTxTimestamp()
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction timestamp: %w", err)
	}
	referenceTime := time.Unix(txTime.Seconds, int64(txTime.Nanos)).UTC()

	// Query for all batches with pending transfer status
	selector := map[string]any{
		"selector": map[string]any{
			"docType": DocTypeBatch,
			"status":  StatusPendingTransfer,
		},
	}
	qb, _ := json.Marshal(selector)

	batches, err := queryBatches(ctx, string(qb))
	if err != nil {
		return nil, fmt.Errorf("failed to query pending transfers: %w", err)
	}

	var expiredBatches []*DrugBatch

	for _, batch := range batches {
		if batch.TransferExpiresAt != "" {
			expiryTime, err := time.Parse(time.RFC3339, batch.TransferExpiresAt)
			if err == nil && expiryTime.Before(referenceTime) {
				// Expire this batch
				batch.Status = StatusExpired
				if batch.Metadata == nil {
					batch.Metadata = make(map[string]string)
				}
				batch.Metadata["expiryReason"] = "Transfer not accepted within 7 days"
				batch.ProposedOwnerMSP = ""
				batch.TransferProposedAt = ""
				batch.TransferExpiresAt = ""

				if err := c.putBatch(ctx, batch); err != nil {
					return nil, fmt.Errorf("failed to expire batch %s: %w", batch.BatchID, err)
				}

				// Reset SBE to only the current owner
				if err := c.setSBE(ctx, "BATCH_"+batch.BatchID, batch.CurrentOwnerMSP); err != nil {
					return nil, fmt.Errorf("reset SBE for batch %s: %w", batch.BatchID, err)
				}

				expiredBatches = append(expiredBatches, batch)

				emit(ctx, EventBatchExpired, map[string]any{
					"batchId": batch.BatchID,
					"reason":  "Transfer not accepted within 7 days",
				})
			}
		}
	}

	return expiredBatches, nil
}

// isTransferExpired checks if a batch transfer has expired
func (c *ManufacturingContract) isTransferExpired(b *DrugBatch) bool {
	if b.TransferExpiresAt == "" {
		return false
	}

	expiryTime, err := time.Parse(time.RFC3339, b.TransferExpiresAt)
	if err != nil {
		return false
	}

	return time.Now().UTC().After(expiryTime)
}

// DestroyBatch – marks batch as destroyed (current owner only)
func (c *ManufacturingContract) DestroyBatch(ctx contractapi.TransactionContextInterface, batchID string) (*DrugBatch, error) {
	b, err := c.readBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if err := c.checkCurrentOwner(ctx, b); err != nil {
		return nil, err
	}
	// Add status validation
	if b.Status == StatusPendingTransfer {
		return nil, errors.New("cannot destroy batch with pending transfer")
	}
	if b.Status == StatusDestroyed {
		return nil, errors.New("batch already destroyed")
	}
	b.Status = StatusDestroyed
	if err := c.putBatch(ctx, b); err != nil {
		return nil, err
	}
	emit(ctx, EventBatchDestroyed, b)
	return b, nil
}

// ---------------- Queries ----------------

func (c *ManufacturingContract) ReadBatch(ctx contractapi.TransactionContextInterface, batchID string) (*DrugBatch, error) {
	return c.readBatch(ctx, batchID)
}

func (c *ManufacturingContract) GetBatchesByOwner(ctx contractapi.TransactionContextInterface, ownerMSP string) ([]*DrugBatch, error) {
	selector := map[string]any{
		"selector": map[string]any{
			"docType":         DocTypeBatch,
			"currentOwnerMSP": ownerMSP,
		},
	}
	qb, _ := json.Marshal(selector)
	return queryBatches(ctx, string(qb))
}

// GetBatchesByProducer returns all batches produced by a specific manufacturer
func (c *ManufacturingContract) GetBatchesByProducer(ctx contractapi.TransactionContextInterface, producerMSP string) ([]*DrugBatch, error) {
	selector := map[string]any{
		"selector": map[string]any{
			"docType":     DocTypeBatch,
			"producerMSP": producerMSP,
		},
	}
	qb, _ := json.Marshal(selector)
	return queryBatches(ctx, string(qb))
}

func (c *ManufacturingContract) QueryBatches(ctx contractapi.TransactionContextInterface, selectorJSON string) ([]*DrugBatch, error) {
	return queryBatches(ctx, selectorJSON)
}

// QueryBatchesPaged(selectorJSON, pageSize, bookmark) -> {items, fetched, bookmark}
func (c *ManufacturingContract) QueryBatchesPaged(
	ctx contractapi.TransactionContextInterface,
	selectorJSON, pageSizeStr, bookmark string,
) (*PagedBatchesResult, error) {

	pageSize, err := strconv.Atoi(strings.TrimSpace(pageSizeStr))
	if err != nil || pageSize <= 0 {
		pageSize = 50 // sensible default
	}
	return queryBatchesPaged(ctx, selectorJSON, int32(pageSize), bookmark)
}

// ---------------- Private helpers (state) ----------------

func (c *ManufacturingContract) keyExists(ctx contractapi.TransactionContextInterface, key string) (bool, error) {
	b, err := ctx.GetStub().GetState(key)
	if err != nil {
		return false, err
	}
	return len(b) > 0, nil
}

func (c *ManufacturingContract) readBatch(ctx contractapi.TransactionContextInterface, batchID string) (*DrugBatch, error) {
	batchID = strings.TrimSpace(batchID)
	var b DrugBatch
	if err := getJSON(ctx, "BATCH_"+batchID, &b); err != nil {
		return nil, err
	}
	return &b, nil
}

func (c *ManufacturingContract) putBatch(ctx contractapi.TransactionContextInterface, b *DrugBatch) error {
	b.UpdatedAt = nowRFC3339(ctx)
	return putJSON(ctx, "BATCH_"+b.BatchID, b)
}

func (c *ManufacturingContract) checkCurrentOwner(ctx contractapi.TransactionContextInterface, b *DrugBatch) error {
	msp, _ := getMSP(ctx)
	if b.CurrentOwnerMSP != msp {
		return fmt.Errorf("access denied: caller MSP %s is not the current owner %s", msp, b.CurrentOwnerMSP)
	}
	return nil
}

func (c *ManufacturingContract) checkProposedOwner(ctx contractapi.TransactionContextInterface, b *DrugBatch) error {
	msp, _ := getMSP(ctx)
	if b.ProposedOwnerMSP != msp {
		return fmt.Errorf("access denied: caller MSP %s is not proposed owner %s", msp, b.ProposedOwnerMSP)
	}
	return nil
}

// ---------------- JSON/state utilities ----------------

func getJSON(ctx contractapi.TransactionContextInterface, key string, v any) error {
	b, err := ctx.GetStub().GetState(key)
	if err != nil {
		return fmt.Errorf("get %s: %w", key, err)
	}
	if len(b) == 0 {
		return fmt.Errorf("key %s not found", key)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("unmarshal %s: %w", key, err)
	}
	return nil
}

func putJSON(ctx contractapi.TransactionContextInterface, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	return ctx.GetStub().PutState(key, b)
}

func queryBatches(ctx contractapi.TransactionContextInterface, selectorJSON string) ([]*DrugBatch, error) {
	iter, err := ctx.GetStub().GetQueryResult(selectorJSON)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer iter.Close()
	var out []*DrugBatch
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		var b DrugBatch
		if err := json.Unmarshal(kv.Value, &b); err == nil && b.DocType == DocTypeBatch {
			out = append(out, &b)
		}
	}
	return out, nil
}

func queryBatchesPaged(ctx contractapi.TransactionContextInterface, selectorJSON string, pageSize int32, bookmark string) (*PagedBatchesResult, error) {
	iter, md, err := ctx.GetStub().GetQueryResultWithPagination(selectorJSON, pageSize, bookmark)
	if err != nil {
		return nil, fmt.Errorf("query with pagination: %w", err)
	}
	defer iter.Close()

	var items []*DrugBatch
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		var b DrugBatch
		if err := json.Unmarshal(kv.Value, &b); err == nil && b.DocType == DocTypeBatch {
			items = append(items, &b)
		}
	}
	out := &PagedBatchesResult{
		Items:    items,
		Fetched:  len(items),
		Bookmark: md.Bookmark,
	}
	return out, nil
}

// ---------------- main ----------------

func main() {
	cc, err := contractapi.NewChaincode(new(ManufacturingContract))
	if err != nil {
		panic(fmt.Errorf("create chaincode: %w", err))
	}
	if err := cc.Start(); err != nil {
		panic(fmt.Errorf("start chaincode: %w", err))
	}
}
