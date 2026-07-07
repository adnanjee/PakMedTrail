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

/*
   Retail / Dispense Chaincode (Go)
   ---------------------------------------------
   Scope:
   - Distributor -> Retail shipment offers (offer / accept / reject / cancel / delivered)
   - CC2CC with "manufacturing" to move batch ownership on accept/reject/cancel
   - DRAP (drapMSP) recalls: initiate/close recalls; owners can quarantine shipments
   - Retail dispense verification (records a dispense event after retailer receives stock)
     NOTE: Patient data is NOT written on-chain; do not store PII
   - PDC for commercial terms (price, incoterms, discount, notes) between distributor & retail
   - Rich queries + paginated queries
   - SBE to enforce dual-endorsement where appropriate

   Channel: pakmedtrail
   External CC: manufacturing (finished drug batches)
*/

// ---------------- Constants & IDs ----------------

const (
	// External manufacturing chaincode
	mfgCCName  = "manufacturing"
	mfgChannel = "" // same channel

	// External recall/compliance chaincode
	recallCCName  = "recall"
	recallChannel = "" // same channel

	// MSP IDs
	mspDRAP        = "drapMSP"
	mspDistributor = "distributorMSP" // informative
	mspRetail      = "retailerMSP"    // informative

	// DocTypes
	DocTypeShipment = "ret.shipment"
	DocTypeRecall   = "ret.recall"
	DocTypeDispense = "ret.dispense"

	// Shipment statuses
	ShipPending     = "PENDING"
	ShipAccepted    = "ACCEPTED"
	ShipRejected    = "REJECTED"
	ShipCancelled   = "CANCELLED"
	ShipDelivered   = "DELIVERED"
	ShipQuarantined = "QUARANTINED"

	// Recall statuses
	RecallActive = "ACTIVE"
	RecallClosed = "CLOSED"

	// Dispense statuses
	DispenseVerified = "VERIFIED"

	// Events
	EvtShipOffered   = "RetailShipmentOffered"
	EvtShipAccepted  = "RetailShipmentAccepted"
	EvtShipRejected  = "RetailShipmentRejected"
	EvtShipCancelled = "RetailShipmentCancelled"
	EvtShipDelivered = "RetailShipmentDelivered"
	EvtShipQuarant   = "RetailShipmentQuarantined"

	EvtRecallInitiated = "RetailRecallInitiated"
	EvtRecallClosed    = "RetailRecallClosed"

	EvtDispenseVerified = "RetailDispenseVerified"
)

// ---------------- Data Models ----------------

type Shipment struct {
	DocType    string            `json:"docType"` // "ret.shipment"
	ShipmentID string            `json:"shipmentId"`
	BatchID    string            `json:"batchId"`
	FromMSP    string            `json:"fromMSP"` // distributor
	ToMSP      string            `json:"toMSP"`   // retail
	Quantity   float64           `json:"quantity"`
	Status     string            `json:"status"`
	Metadata   map[string]string `json:"metadata,omitempty"` // public, may include "sensitiveHash"
	CreatedAt  string            `json:"createdAt"`
	UpdatedAt  string            `json:"updatedAt"`
}

// Commercial terms (PDC)
type ShipmentSensitive struct {
	DocType    string  `json:"docType"` // "ret.shipment.sensitive"
	ShipmentID string  `json:"shipmentId"`
	PriceAmt   float64 `json:"priceAmt,omitempty"`
	Currency   string  `json:"currency,omitempty"`
	Incoterms  string  `json:"incoterms,omitempty"`
	Discount   float64 `json:"discount,omitempty"`
	Notes      string  `json:"notes,omitempty"`
	UpdatedAt  string  `json:"updatedAt"`
}

// Recall notice (kept in this CC; mirrors distribution pattern)
type RecallNotice struct {
	DocType   string `json:"docType"` // "ret.recall"
	RecallID  string `json:"recallId"`
	BatchID   string `json:"batchId"`
	Reason    string `json:"reason"`
	Status    string `json:"status"` // ACTIVE / CLOSED
	IssuerMSP string `json:"issuerMSP"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
}

// Dispense record (verification only; no PII)
type Dispense struct {
	DocType    string            `json:"docType"` // "ret.dispense"
	DispenseID string            `json:"dispenseId"`
	BatchID    string            `json:"batchId"`
	RetailMSP  string            `json:"retailMSP"`
	Quantity   float64           `json:"quantity"`
	Status     string            `json:"status"`             // VERIFIED
	Metadata   map[string]string `json:"metadata,omitempty"` // e.g., POS ref, anonymized token; never PII
	Timestamp  string            `json:"timestamp"`
}

// Paginated result
type PagedShipmentsResult struct {
	Items    []*Shipment `json:"items"`
	Fetched  int         `json:"fetched"`
	Bookmark string      `json:"bookmark"`
}

type PagedDispenseResult struct {
	Items    []*Dispense `json:"items"`
	Fetched  int         `json:"fetched"`
	Bookmark string      `json:"bookmark"`
}

// ---------------- Contract ----------------

type RetailContract struct {
	contractapi.Contract
}

// ---------------- Utils ----------------

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

func putJSON(ctx contractapi.TransactionContextInterface, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	return ctx.GetStub().PutState(key, b)
}

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

func putPrivateJSON(ctx contractapi.TransactionContextInterface, collection, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", key, err)
	}
	return ctx.GetStub().PutPrivateData(collection, key, b)
}

func getPrivateJSON(ctx contractapi.TransactionContextInterface, collection, key string, v any) error {
	b, err := ctx.GetStub().GetPrivateData(collection, key)
	if err != nil {
		return fmt.Errorf("get PDC %s/%s: %w", collection, key, err)
	}
	if len(b) == 0 {
		return fmt.Errorf("PDC key %s/%s not found", collection, key)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("unmarshal PDC %s/%s: %w", collection, key, err)
	}
	return nil
}

func (c *RetailContract) keyExists(ctx contractapi.TransactionContextInterface, key string) (bool, error) {
	b, err := ctx.GetStub().GetState(key)
	if err != nil {
		return false, err
	}
	return len(b) > 0, nil
}

func (c *RetailContract) setSBE(ctx contractapi.TransactionContextInterface, key string, orgMSPs ...string) error {
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

// ---------------- CC2CC helpers (manufacturing) ----------------

// CORRECTED: Fixed mfgBatch struct to match manufacturing chaincode
type mfgBatch struct {
	DocType          string  `json:"docType"`
	BatchID          string  `json:"batchId"`
	DrugCode         string  `json:"drugCode"`
	Quantity         float64 `json:"quantity"`
	Unit             string  `json:"unit"`
	ProducerMSP      string  `json:"producerMSP"`     // NEVER CHANGES
	CurrentOwnerMSP  string  `json:"currentOwnerMSP"` // CHANGES - current owner (CRITICAL FIX)
	Status           string  `json:"status"`
	DRAPApproved     bool    `json:"drapApproved"`
	ProposedOwnerMSP string  `json:"proposedOwnerMSP,omitempty"`
}

func mfgReadBatch(ctx contractapi.TransactionContextInterface, batchID string) (*mfgBatch, error) {
	args := [][]byte{[]byte("ReadBatch"), []byte(batchID)}
	resp := ctx.GetStub().InvokeChaincode(mfgCCName, args, mfgChannel)
	if resp.Status != 200 {
		// IMPROVED: Better error handling
		var errorMsg string
		if len(resp.Payload) > 0 {
			errorMsg = string(resp.Payload)
		} else {
			errorMsg = "no error details provided"
		}
		return nil, fmt.Errorf("manufacturing.ReadBatch(%s) failed: %s (status: %d)", batchID, errorMsg, resp.Status)
	}
	var b mfgBatch
	if err := json.Unmarshal(resp.Payload, &b); err != nil {
		return nil, fmt.Errorf("manufacturing.ReadBatch unmarshal: %w", err)
	}
	return &b, nil
}

// recallIsAssetUnderActiveRecall checks the external recall chaincode for active recall status.
// This connects retail dispense verification to the DRAP recall workflow.
func recallIsAssetUnderActiveRecall(ctx contractapi.TransactionContextInterface, assetType, assetID string) (bool, error) {
	args := [][]byte{[]byte("IsAssetUnderActiveRecall"), []byte(assetType), []byte(assetID)}
	resp := ctx.GetStub().InvokeChaincode(recallCCName, args, recallChannel)
	if resp.Status != 200 {
		var errorMsg string
		if len(resp.Payload) > 0 {
			errorMsg = string(resp.Payload)
		} else {
			errorMsg = "no error details provided"
		}
		return false, fmt.Errorf("recall.IsAssetUnderActiveRecall(%s,%s) failed: %s (status: %d)", assetType, assetID, errorMsg, resp.Status)
	}
	active, err := strconv.ParseBool(strings.TrimSpace(string(resp.Payload)))
	if err != nil {
		return false, fmt.Errorf("recall.IsAssetUnderActiveRecall response invalid: %q", string(resp.Payload))
	}
	return active, nil
}

// recallQuarantineStatusOn checks whether the external recall chaincode has an ON quarantine record.
// A missing quarantine record is treated as not quarantined; other errors are returned.
func recallQuarantineStatusOn(ctx contractapi.TransactionContextInterface, assetType, assetID string) (bool, error) {
	args := [][]byte{[]byte("GetQuarantine"), []byte(assetType), []byte(assetID)}
	resp := ctx.GetStub().InvokeChaincode(recallCCName, args, recallChannel)
	if resp.Status != 200 {
		payload := strings.TrimSpace(string(resp.Payload))
		if strings.Contains(strings.ToLower(payload), "not found") {
			return false, nil
		}
		if payload == "" {
			payload = "no error details provided"
		}
		return false, fmt.Errorf("recall.GetQuarantine(%s,%s) failed: %s (status: %d)", assetType, assetID, payload, resp.Status)
	}
	var q struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(resp.Payload, &q); err != nil {
		return false, fmt.Errorf("recall.GetQuarantine response invalid: %w", err)
	}
	return strings.EqualFold(strings.TrimSpace(q.Status), "ON"), nil
}

func mfgProposeTransfer(ctx contractapi.TransactionContextInterface, batchID, toMSP string) error {
	args := [][]byte{[]byte("ProposeBatchTransfer"), []byte(batchID), []byte(toMSP)}
	resp := ctx.GetStub().InvokeChaincode(mfgCCName, args, mfgChannel)
	if resp.Status != 200 {
		var errorMsg string
		if len(resp.Payload) > 0 {
			errorMsg = string(resp.Payload)
		} else {
			errorMsg = "no error details provided"
		}
		return fmt.Errorf("manufacturing.ProposeBatchTransfer failed: %s (status: %d)", errorMsg, resp.Status)
	}
	return nil
}

func mfgAcceptTransfer(ctx contractapi.TransactionContextInterface, batchID string) error {
	args := [][]byte{[]byte("AcceptBatchTransfer"), []byte(batchID)}
	resp := ctx.GetStub().InvokeChaincode(mfgCCName, args, mfgChannel)
	if resp.Status != 200 {
		var errorMsg string
		if len(resp.Payload) > 0 {
			errorMsg = string(resp.Payload)
		} else {
			errorMsg = "no error details provided"
		}
		return fmt.Errorf("manufacturing.AcceptBatchTransfer failed: %s (status: %d)", errorMsg, resp.Status)
	}
	return nil
}

func mfgRejectTransfer(ctx contractapi.TransactionContextInterface, batchID, reason string) error {
	args := [][]byte{[]byte("RejectBatchTransfer"), []byte(batchID), []byte(reason)}
	resp := ctx.GetStub().InvokeChaincode(mfgCCName, args, mfgChannel)
	if resp.Status != 200 {
		var errorMsg string
		if len(resp.Payload) > 0 {
			errorMsg = string(resp.Payload)
		} else {
			errorMsg = "no error details provided"
		}
		return fmt.Errorf("manufacturing.RejectBatchTransfer failed: %s (status: %d)", errorMsg, resp.Status)
	}
	return nil
}

func mfgCancelTransfer(ctx contractapi.TransactionContextInterface, batchID, reason string) error {
	args := [][]byte{[]byte("CancelBatchTransfer"), []byte(batchID), []byte(reason)}
	resp := ctx.GetStub().InvokeChaincode(mfgCCName, args, mfgChannel)
	if resp.Status != 200 {
		var errorMsg string
		if len(resp.Payload) > 0 {
			errorMsg = string(resp.Payload)
		} else {
			errorMsg = "no error details provided"
		}
		return fmt.Errorf("manufacturing.CancelBatchTransfer failed: %s (status: %d)", errorMsg, resp.Status)
	}
	return nil
}

// ---------------- Status Validation (NEW FUNCTION) ----------------

func (c *RetailContract) validateShipmentStatusTransition(oldStatus, newStatus string) error {
	allowedTransitions := map[string][]string{
		ShipPending:     {ShipAccepted, ShipRejected, ShipCancelled},
		ShipAccepted:    {ShipDelivered, ShipQuarantined},
		ShipDelivered:   {ShipQuarantined},
		ShipRejected:    {},
		ShipCancelled:   {},
		ShipQuarantined: {},
	}

	if validTransitions, exists := allowedTransitions[oldStatus]; exists {
		for _, valid := range validTransitions {
			if valid == newStatus {
				return nil
			}
		}
	}
	return fmt.Errorf("invalid shipment status transition from %s to %s", oldStatus, newStatus)
}

// ---------------- Shipments: Create/Accept/Reject/Cancel/Delivered ----------------

// CreateRetailShipmentOffer(shipmentId, batchId, toMSP, quantityStr, metadataJSON)
// Pre:
//   - Caller (distributor) must be current owner of batch (in manufacturing)
//   - Batch must be DRAPApproved
//   - quantity <= available
//
// Effects:
//   - Create PENDING shipment
//   - mfg.ProposeBatchTransfer(batchId, toMSP)
func (c *RetailContract) CreateRetailShipmentOffer(
	ctx contractapi.TransactionContextInterface,
	shipmentID, batchID, toMSP, quantityStr, metadataJSON string,
) (*Shipment, error) {

	shipmentID = strings.TrimSpace(shipmentID)
	if shipmentID == "" {
		return nil, errors.New("shipmentId required")
	}
	key := "RSHIP_" + shipmentID
	exists, err := c.keyExists(ctx, key)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("shipment %s already exists", shipmentID)
	}

	b, err := mfgReadBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if !b.DRAPApproved {
		return nil, errors.New("batch is not DRAP-approved")
	}
	callerMSP, _ := getMSP(ctx)
	// CRITICAL FIX: Use CurrentOwnerMSP instead of ProducerMSP
	if b.CurrentOwnerMSP != callerMSP {
		return nil, fmt.Errorf("caller MSP %s is not current owner %s of batch %s", callerMSP, b.CurrentOwnerMSP, batchID)
	}

	qty, err := strconv.ParseFloat(strings.TrimSpace(quantityStr), 64)
	if err != nil || qty <= 0 {
		return nil, fmt.Errorf("invalid quantity %q", quantityStr)
	}
	if qty > b.Quantity+1e-9 {
		return nil, fmt.Errorf("quantity %.6f exceeds batch available %.6f", qty, b.Quantity)
	}

	var md map[string]string
	if strings.TrimSpace(metadataJSON) != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &md); err != nil {
			return nil, fmt.Errorf("metadata JSON invalid: %w", err)
		}
	}

	if err := mfgProposeTransfer(ctx, batchID, toMSP); err != nil {
		return nil, err
	}

	now := nowRFC3339(ctx)
	s := &Shipment{
		DocType:    DocTypeShipment,
		ShipmentID: shipmentID,
		BatchID:    batchID,
		FromMSP:    callerMSP,
		ToMSP:      strings.TrimSpace(toMSP),
		Quantity:   qty,
		Status:     ShipPending,
		Metadata:   md,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := putJSON(ctx, key, s); err != nil {
		return nil, err
	}
	// SBE requiring both distributor + retailer for next update
	_ = c.setSBE(ctx, key, s.FromMSP, s.ToMSP)
	emit(ctx, EvtShipOffered, s)
	return s, nil
}

// AcceptRetailShipment(shipmentId) by retail
func (c *RetailContract) AcceptRetailShipment(ctx contractapi.TransactionContextInterface, shipmentID string) (*Shipment, error) {
	s, err := c.readShipment(ctx, shipmentID)
	if err != nil {
		return nil, err
	}
	callerMSP, _ := getMSP(ctx)
	if s.ToMSP != callerMSP {
		return nil, fmt.Errorf("only proposed receiver %s can accept (caller %s)", s.ToMSP, callerMSP)
	}
	if s.Status != ShipPending {
		return nil, errors.New("shipment not in PENDING state")
	}

	// Validate status transition
	if err := c.validateShipmentStatusTransition(s.Status, ShipAccepted); err != nil {
		return nil, err
	}

	if err := mfgAcceptTransfer(ctx, s.BatchID); err != nil {
		return nil, err
	}
	s.Status = ShipAccepted
	if err := c.putShipment(ctx, s); err != nil {
		return nil, err
	}
	// Now only retailer needs to endorse future updates
	_ = c.setSBE(ctx, "RSHIP_"+s.ShipmentID, s.ToMSP)
	emit(ctx, EvtShipAccepted, s)
	return s, nil
}

// RejectRetailShipment(shipmentId, reason) by retail
func (c *RetailContract) RejectRetailShipment(ctx contractapi.TransactionContextInterface, shipmentID, reason string) (*Shipment, error) {
	s, err := c.readShipment(ctx, shipmentID)
	if err != nil {
		return nil, err
	}
	callerMSP, _ := getMSP(ctx)
	if s.ToMSP != callerMSP {
		return nil, fmt.Errorf("only proposed receiver %s can reject (caller %s)", s.ToMSP, callerMSP)
	}
	if s.Status != ShipPending {
		return nil, errors.New("shipment not in PENDING state")
	}

	// Validate status transition
	if err := c.validateShipmentStatusTransition(s.Status, ShipRejected); err != nil {
		return nil, err
	}

	if err := mfgRejectTransfer(ctx, s.BatchID, strings.TrimSpace(reason)); err != nil {
		return nil, err
	}
	s.Status = ShipRejected
	if s.Metadata == nil {
		s.Metadata = map[string]string{}
	}
	if r := strings.TrimSpace(reason); r != "" {
		s.Metadata["rejectReason"] = r
	}
	if err := c.putShipment(ctx, s); err != nil {
		return nil, err
	}
	_ = c.setSBE(ctx, "RSHIP_"+s.ShipmentID, s.FromMSP)
	emit(ctx, EvtShipRejected, map[string]any{"shipmentId": s.ShipmentID, "reason": reason})
	return s, nil
}

// CancelRetailShipment(shipmentId, reason) by distributor
func (c *RetailContract) CancelRetailShipment(ctx contractapi.TransactionContextInterface, shipmentID, reason string) (*Shipment, error) {
	s, err := c.readShipment(ctx, shipmentID)
	if err != nil {
		return nil, err
	}
	callerMSP, _ := getMSP(ctx)
	if s.FromMSP != callerMSP {
		return nil, fmt.Errorf("only shipper %s can cancel (caller %s)", s.FromMSP, callerMSP)
	}
	if s.Status != ShipPending {
		return nil, errors.New("shipment not in PENDING state")
	}

	// Validate status transition
	if err := c.validateShipmentStatusTransition(s.Status, ShipCancelled); err != nil {
		return nil, err
	}

	if err := mfgCancelTransfer(ctx, s.BatchID, strings.TrimSpace(reason)); err != nil {
		return nil, err
	}
	s.Status = ShipCancelled
	if s.Metadata == nil {
		s.Metadata = map[string]string{}
	}
	if r := strings.TrimSpace(reason); r != "" {
		s.Metadata["cancelReason"] = r
	}
	if err := c.putShipment(ctx, s); err != nil {
		return nil, err
	}
	_ = c.setSBE(ctx, "RSHIP_"+s.ShipmentID, s.FromMSP)
	emit(ctx, EvtShipCancelled, map[string]any{"shipmentId": s.ShipmentID, "reason": reason})
	return s, nil
}

// MarkRetailDelivered(shipmentId) by retail after physical receipt
func (c *RetailContract) MarkRetailDelivered(ctx contractapi.TransactionContextInterface, shipmentID string) (*Shipment, error) {
	s, err := c.readShipment(ctx, shipmentID)
	if err != nil {
		return nil, err
	}
	callerMSP, _ := getMSP(ctx)
	if s.ToMSP != callerMSP {
		return nil, fmt.Errorf("only receiver %s can mark delivered (caller %s)", s.ToMSP, callerMSP)
	}
	if s.Status != ShipAccepted {
		return nil, errors.New("shipment must be ACCEPTED before delivery")
	}

	// Validate status transition
	if err := c.validateShipmentStatusTransition(s.Status, ShipDelivered); err != nil {
		return nil, err
	}

	s.Status = ShipDelivered
	if err := c.putShipment(ctx, s); err != nil {
		return nil, err
	}
	_ = c.setSBE(ctx, "RSHIP_"+s.ShipmentID, s.ToMSP)
	emit(ctx, EvtShipDelivered, s)
	return s, nil
}

// ---------------- PDC: Commercial Terms ----------------

const collectionRetailSensitive = "collectionRetailSensitive"

func (c *RetailContract) PutSensitive(
	ctx contractapi.TransactionContextInterface,
	shipmentID, priceAmtStr, currency, hasPriceStr, discountStr, hasDiscountStr, incoterms, notes string,
) (*ShipmentSensitive, error) {

	s, err := c.readShipment(ctx, shipmentID)
	if err != nil {
		return nil, err
	}
	callerMSP, _ := getMSP(ctx)
	if callerMSP != s.FromMSP && callerMSP != s.ToMSP {
		return nil, fmt.Errorf("only %s or %s can write sensitive terms", s.FromMSP, s.ToMSP)
	}

	var priceAmt float64
	if strings.EqualFold(strings.TrimSpace(hasPriceStr), "true") {
		if v, err := strconv.ParseFloat(strings.TrimSpace(priceAmtStr), 64); err == nil {
			priceAmt = v
		} else {
			return nil, fmt.Errorf("invalid price amount %q", priceAmtStr)
		}
	}
	var discount float64
	if strings.EqualFold(strings.TrimSpace(hasDiscountStr), "true") {
		if v, err := strconv.ParseFloat(strings.TrimSpace(discountStr), 64); err == nil {
			discount = v
		} else {
			return nil, fmt.Errorf("invalid discount %q", discountStr)
		}
	}

	rec := &ShipmentSensitive{
		DocType:    "ret.shipment.sensitive",
		ShipmentID: s.ShipmentID,
		PriceAmt:   priceAmt,
		Currency:   strings.TrimSpace(currency),
		Incoterms:  strings.TrimSpace(incoterms),
		Discount:   discount,
		Notes:      strings.TrimSpace(notes),
		UpdatedAt:  nowRFC3339(ctx),
	}
	if err := putPrivateJSON(ctx, collectionRetailSensitive, "PDC_RSHIP_"+s.ShipmentID, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

func (c *RetailContract) ReadSensitive(ctx contractapi.TransactionContextInterface, shipmentID string) (*ShipmentSensitive, error) {
	s, err := c.readShipment(ctx, shipmentID)
	if err != nil {
		return nil, err
	}
	callerMSP, _ := getMSP(ctx)
	if callerMSP != s.FromMSP && callerMSP != s.ToMSP {
		return nil, fmt.Errorf("only %s or %s can read sensitive terms", s.FromMSP, s.ToMSP)
	}
	var rec ShipmentSensitive
	if err := getPrivateJSON(ctx, collectionRetailSensitive, "PDC_RSHIP_"+s.ShipmentID, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// LinkSensitiveHash computes SHA-256 over the PDC record and stores a public anchor
func (c *RetailContract) LinkSensitiveHash(ctx contractapi.TransactionContextInterface, shipmentID string) (*Shipment, error) {
	s, err := c.readShipment(ctx, shipmentID)
	if err != nil {
		return nil, err
	}
	callerMSP, _ := getMSP(ctx)
	if callerMSP != s.FromMSP && callerMSP != s.ToMSP {
		return nil, fmt.Errorf("only %s or %s can link sensitive hash", s.FromMSP, s.ToMSP)
	}
	var rec ShipmentSensitive
	if err := getPrivateJSON(ctx, collectionRetailSensitive, "PDC_RSHIP_"+s.ShipmentID, &rec); err != nil {
		return nil, err
	}
	blob, _ := json.Marshal(rec)
	sum := sha256.Sum256(blob)
	hashHex := hex.EncodeToString(sum[:])
	if s.Metadata == nil {
		s.Metadata = map[string]string{}
	}
	s.Metadata["sensitiveHash"] = hashHex
	if err := c.putShipment(ctx, s); err != nil {
		return nil, err
	}
	return s, nil
}

// ---------------- DRAP Recalls ----------------

func (c *RetailContract) InitiateRecallByDRAP(ctx contractapi.TransactionContextInterface, recallID, batchID, reason string) (*RecallNotice, error) {
	is, caller, err := isDRAP(ctx)
	if err != nil {
		return nil, err
	}
	if !is {
		return nil, fmt.Errorf("access denied: MSP %s is not DRAP", caller)
	}
	recallID = strings.TrimSpace(recallID)
	if recallID == "" {
		return nil, errors.New("recallId required")
	}
	key := "RRECALL_" + recallID
	exists, err := c.keyExists(ctx, key)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("recall %s already exists", recallID)
	}
	now := nowRFC3339(ctx)
	r := &RecallNotice{
		DocType:   DocTypeRecall,
		RecallID:  recallID,
		BatchID:   strings.TrimSpace(batchID),
		Reason:    strings.TrimSpace(reason),
		Status:    RecallActive,
		IssuerMSP: mspDRAP,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := putJSON(ctx, key, r); err != nil {
		return nil, err
	}
	emit(ctx, EvtRecallInitiated, r)
	return r, nil
}

func (c *RetailContract) CloseRecallByDRAP(ctx contractapi.TransactionContextInterface, recallID, note string) (*RecallNotice, error) {
	is, caller, err := isDRAP(ctx)
	if err != nil {
		return nil, err
	}
	if !is {
		return nil, fmt.Errorf("access denied: MSP %s is not DRAP", caller)
	}
	r, err := c.readRecall(ctx, recallID)
	if err != nil {
		return nil, err
	}
	r.Status = RecallClosed
	if note = strings.TrimSpace(note); note != "" {
		r.Reason = note
	}
	r.UpdatedAt = nowRFC3339(ctx)
	if err := putJSON(ctx, "RRECALL_"+r.RecallID, r); err != nil {
		return nil, err
	}
	emit(ctx, EvtRecallClosed, r)
	return r, nil
}

// QuarantineByRecall(shipmentId) — current owner (distributor or retail) can quarantine when recall ACTIVE
func (c *RetailContract) QuarantineByRecall(ctx contractapi.TransactionContextInterface, shipmentID string) (*Shipment, error) {
	s, err := c.readShipment(ctx, shipmentID)
	if err != nil {
		return nil, err
	}
	callerMSP, _ := getMSP(ctx)
	owner := s.FromMSP
	if s.Status == ShipAccepted || s.Status == ShipDelivered {
		owner = s.ToMSP
	}
	if callerMSP != owner {
		return nil, fmt.Errorf("only current owner %s can quarantine", owner)
	}
	active, err := c.findActiveRecallForBatch(ctx, s.BatchID)
	if err != nil {
		return nil, err
	}
	if !active {
		return nil, errors.New("no ACTIVE recall for this batch")
	}

	// Validate status transition
	if err := c.validateShipmentStatusTransition(s.Status, ShipQuarantined); err != nil {
		return nil, err
	}

	s.Status = ShipQuarantined
	if err := c.putShipment(ctx, s); err != nil {
		return nil, err
	}
	emit(ctx, EvtShipQuarant, map[string]any{"shipmentId": s.ShipmentID, "batchId": s.BatchID})
	return s, nil
}

// ---------------- Dispense Verification ----------------

// VerifyDispense(dispenseId, batchId, quantityStr, metadataJSON)
// Pre:
//   - Caller must be retail and current owner of batch in manufacturing
//   - No ACTIVE recall for batch
//   - quantity > 0  (NOTE: This does not decrement manufacturing quantity; it records verified dispense)
//
// Effects:
//   - Writes a verification record (no PII)
func (c *RetailContract) VerifyDispense(
	ctx contractapi.TransactionContextInterface,
	dispenseID, batchID, quantityStr, metadataJSON string,
) (*Dispense, error) {

	dispenseID = strings.TrimSpace(dispenseID)
	if dispenseID == "" {
		return nil, errors.New("dispenseId required")
	}
	key := "RDISP_" + dispenseID
	exists, err := c.keyExists(ctx, key)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("dispense %s already exists", dispenseID)
	}

	b, err := mfgReadBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	callerMSP, _ := getMSP(ctx)
	// CRITICAL FIX: Use CurrentOwnerMSP instead of ProducerMSP
	if b.CurrentOwnerMSP != callerMSP {
		return nil, fmt.Errorf("caller MSP %s is not current owner %s of batch %s", callerMSP, b.CurrentOwnerMSP, batchID)
	}
	// Must not be under active recall or quarantine in the external DRAP recall chaincode.
	externalActiveRecall, err := recallIsAssetUnderActiveRecall(ctx, "BATCH", batchID)
	if err != nil {
		return nil, err
	}
	if externalActiveRecall {
		return nil, fmt.Errorf("batch %s is under ACTIVE recall in recall chaincode; cannot verify dispense", batchID)
	}
	externalQuarantined, err := recallQuarantineStatusOn(ctx, "BATCH", batchID)
	if err != nil {
		return nil, err
	}
	if externalQuarantined {
		return nil, fmt.Errorf("batch %s is quarantined in recall chaincode; cannot verify dispense", batchID)
	}

	// Backward-compatible local retail recall check, retained for older retail-local recall records.
	activeRecall, err := c.findActiveRecallForBatch(ctx, batchID)
	if err != nil {
		return nil, err
	}
	if activeRecall {
		return nil, errors.New("batch is under ACTIVE recall in retail chaincode; cannot verify dispense")
	}

	qty, err := strconv.ParseFloat(strings.TrimSpace(quantityStr), 64)
	if err != nil || qty <= 0 {
		return nil, fmt.Errorf("invalid quantity %q", quantityStr)
	}

	var md map[string]string
	if strings.TrimSpace(metadataJSON) != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &md); err != nil {
			return nil, fmt.Errorf("metadata JSON invalid: %w", err)
		}
	}
	now := nowRFC3339(ctx)
	rec := &Dispense{
		DocType:    DocTypeDispense,
		DispenseID: dispenseID,
		BatchID:    batchID,
		RetailMSP:  callerMSP,
		Quantity:   qty,
		Status:     DispenseVerified,
		Metadata:   md,
		Timestamp:  now,
	}
	if err := putJSON(ctx, key, rec); err != nil {
		return nil, err
	}
	emit(ctx, EvtDispenseVerified, rec)
	return rec, nil
}

// ---------------- NEW MISSING FUNCTIONS ADDED BELOW ----------------

// UpdateShipmentMetadata updates shipment metadata (NEW FUNCTION)
func (c *RetailContract) UpdateShipmentMetadata(ctx contractapi.TransactionContextInterface, shipmentID, metadataJSON string) (*Shipment, error) {
	s, err := c.readShipment(ctx, shipmentID)
	if err != nil {
		return nil, err
	}

	callerMSP, _ := getMSP(ctx)
	if callerMSP != s.FromMSP && callerMSP != s.ToMSP {
		return nil, fmt.Errorf("only %s or %s can update shipment", s.FromMSP, s.ToMSP)
	}

	md := make(map[string]string)
	if strings.TrimSpace(metadataJSON) != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &md); err != nil {
			return nil, fmt.Errorf("invalid metadata JSON: %w", err)
		}
	}
	s.Metadata = md

	if err := c.putShipment(ctx, s); err != nil {
		return nil, err
	}
	return s, nil
}

// GetShipmentsByStatus returns shipments filtered by status (NEW FUNCTION)
func (c *RetailContract) GetShipmentsByStatus(ctx contractapi.TransactionContextInterface, status string) ([]*Shipment, error) {
	selector := map[string]any{
		"selector": map[string]any{
			"docType": DocTypeShipment,
			"status":  status,
		},
	}
	qb, _ := json.Marshal(selector)
	return queryShipments(ctx, string(qb))
}

// GetActiveRecalls returns all active recalls (NEW FUNCTION)
func (c *RetailContract) GetActiveRecalls(ctx contractapi.TransactionContextInterface) ([]*RecallNotice, error) {
	selector := map[string]any{
		"selector": map[string]any{
			"docType": DocTypeRecall,
			"status":  RecallActive,
		},
	}
	qb, _ := json.Marshal(selector)
	return c.QueryRecalls(ctx, string(qb))
}

// ShipmentExists checks if a shipment exists (NEW FUNCTION)
func (c *RetailContract) ShipmentExists(ctx contractapi.TransactionContextInterface, shipmentID string) (bool, error) {
	return c.keyExists(ctx, "RSHIP_"+shipmentID)
}

// GetShipmentHistory returns the transaction history for a shipment (NEW FUNCTION)
func (c *RetailContract) GetShipmentHistory(ctx contractapi.TransactionContextInterface, shipmentID string) ([]map[string]any, error) {
	iter, err := ctx.GetStub().GetHistoryForKey("RSHIP_" + shipmentID)
	if err != nil {
		return nil, fmt.Errorf("get history for shipment %s: %w", shipmentID, err)
	}
	defer iter.Close()

	var history []map[string]any
	for iter.HasNext() {
		tx, err := iter.Next()
		if err != nil {
			return nil, err
		}
		entry := map[string]any{
			"txId":      tx.TxId,
			"isDelete":  tx.IsDelete,
			"timestamp": "",
		}
		if tx.Timestamp != nil {
			entry["timestamp"] = time.Unix(tx.Timestamp.Seconds, int64(tx.Timestamp.Nanos)).UTC().Format(time.RFC3339)
		}
		if !tx.IsDelete && len(tx.Value) > 0 {
			var shipment Shipment
			if err := json.Unmarshal(tx.Value, &shipment); err == nil {
				entry["value"] = shipment
			}
		}
		history = append(history, entry)
	}
	return history, nil
}

// ---------------- Query APIs ----------------

func (c *RetailContract) ReadShipment(ctx contractapi.TransactionContextInterface, shipmentID string) (*Shipment, error) {
	return c.readShipment(ctx, shipmentID)
}

func (c *RetailContract) ReadRecall(ctx contractapi.TransactionContextInterface, recallID string) (*RecallNotice, error) {
	return c.readRecall(ctx, recallID)
}

func (c *RetailContract) ReadDispense(ctx contractapi.TransactionContextInterface, dispenseID string) (*Dispense, error) {
	var d Dispense
	if err := getJSON(ctx, "RDISP_"+strings.TrimSpace(dispenseID), &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (c *RetailContract) GetShipmentsByParty(ctx contractapi.TransactionContextInterface, partyMSP string) ([]*Shipment, error) {
	selector := map[string]any{
		"selector": map[string]any{
			"docType": DocTypeShipment,
			"$or": []map[string]any{
				{"fromMSP": partyMSP},
				{"toMSP": partyMSP},
			},
		},
	}
	qb, _ := json.Marshal(selector)
	return queryShipments(ctx, string(qb))
}

func (c *RetailContract) QueryShipments(ctx contractapi.TransactionContextInterface, selectorJSON string) ([]*Shipment, error) {
	return queryShipments(ctx, selectorJSON)
}

func (c *RetailContract) QueryShipmentsPaged(ctx contractapi.TransactionContextInterface, selectorJSON, pageSizeStr, bookmark string) (*PagedShipmentsResult, error) {
	pageSize, err := strconv.Atoi(strings.TrimSpace(pageSizeStr))
	if err != nil || pageSize <= 0 {
		pageSize = 50
	}
	return queryShipmentsPaged(ctx, selectorJSON, int32(pageSize), bookmark)
}

func (c *RetailContract) QueryDispenses(ctx contractapi.TransactionContextInterface, selectorJSON string) ([]*Dispense, error) {
	iter, err := ctx.GetStub().GetQueryResult(selectorJSON)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer iter.Close()
	var out []*Dispense
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		var d Dispense
		if err := json.Unmarshal(kv.Value, &d); err == nil && d.DocType == DocTypeDispense {
			out = append(out, &d)
		}
	}
	return out, nil
}

func (c *RetailContract) QueryRecalls(ctx contractapi.TransactionContextInterface, selectorJSON string) ([]*RecallNotice, error) {
	iter, err := ctx.GetStub().GetQueryResult(selectorJSON)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer iter.Close()
	var out []*RecallNotice
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		var r RecallNotice
		if err := json.Unmarshal(kv.Value, &r); err == nil && r.DocType == DocTypeRecall {
			out = append(out, &r)
		}
	}
	return out, nil
}

func (c *RetailContract) QueryDispensesPaged(ctx contractapi.TransactionContextInterface, selectorJSON, pageSizeStr, bookmark string) (*PagedDispenseResult, error) {
	pageSize, err := strconv.Atoi(strings.TrimSpace(pageSizeStr))
	if err != nil || pageSize <= 0 {
		pageSize = 50
	}
	iter, md, err := ctx.GetStub().GetQueryResultWithPagination(selectorJSON, int32(pageSize), bookmark)
	if err != nil {
		return nil, fmt.Errorf("query with pagination: %w", err)
	}
	defer iter.Close()
	var items []*Dispense
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		var d Dispense
		if err := json.Unmarshal(kv.Value, &d); err == nil && d.DocType == DocTypeDispense {
			items = append(items, &d)
		}
	}
	return &PagedDispenseResult{
		Items:    items,
		Fetched:  len(items),
		Bookmark: md.Bookmark,
	}, nil
}

// ---------------- Private helpers ----------------

func (c *RetailContract) readShipment(ctx contractapi.TransactionContextInterface, shipmentID string) (*Shipment, error) {
	var s Shipment
	if err := getJSON(ctx, "RSHIP_"+strings.TrimSpace(shipmentID), &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (c *RetailContract) putShipment(ctx contractapi.TransactionContextInterface, s *Shipment) error {
	s.UpdatedAt = nowRFC3339(ctx)
	return putJSON(ctx, "RSHIP_"+s.ShipmentID, s)
}

func (c *RetailContract) readRecall(ctx contractapi.TransactionContextInterface, recallID string) (*RecallNotice, error) {
	var r RecallNotice
	if err := getJSON(ctx, "RRECALL_"+strings.TrimSpace(recallID), &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (c *RetailContract) findActiveRecallForBatch(ctx contractapi.TransactionContextInterface, batchID string) (bool, error) {
	sel := map[string]any{
		"selector": map[string]any{
			"docType": DocTypeRecall,
			"batchID": batchID,
			"status":  RecallActive,
		},
	}
	b, _ := json.Marshal(sel)
	iter, err := ctx.GetStub().GetQueryResult(string(b))
	if err != nil {
		return false, err
	}
	defer iter.Close()
	return iter.HasNext(), nil
}

func queryShipments(ctx contractapi.TransactionContextInterface, selectorJSON string) ([]*Shipment, error) {
	iter, err := ctx.GetStub().GetQueryResult(selectorJSON)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer iter.Close()
	var out []*Shipment
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		var s Shipment
		if err := json.Unmarshal(kv.Value, &s); err == nil && s.DocType == DocTypeShipment {
			out = append(out, &s)
		}
	}
	return out, nil
}

func queryShipmentsPaged(ctx contractapi.TransactionContextInterface, selectorJSON string, pageSize int32, bookmark string) (*PagedShipmentsResult, error) {
	iter, md, err := ctx.GetStub().GetQueryResultWithPagination(selectorJSON, pageSize, bookmark)
	if err != nil {
		return nil, fmt.Errorf("query with pagination: %w", err)
	}
	defer iter.Close()
	var items []*Shipment
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		var s Shipment
		if err := json.Unmarshal(kv.Value, &s); err == nil && s.DocType == DocTypeShipment {
			items = append(items, &s)
		}
	}
	return &PagedShipmentsResult{
		Items:    items,
		Fetched:  len(items),
		Bookmark: md.Bookmark,
	}, nil
}

// ---------------- main ----------------

func main() {
	cc, err := contractapi.NewChaincode(new(RetailContract))
	if err != nil {
		panic(fmt.Errorf("create chaincode: %w", err))
	}
	if err := cc.Start(); err != nil {
		panic(fmt.Errorf("start chaincode: %w", err))
	}
}

/*
---------------------------
CouchDB Index JSON (copy into:
 chaincode/retail-go/META-INF/statedb/couchdb/indexes/)
---------------------------

1) rshipments-by-parties-status.json
{
  "index": { "fields": ["docType", "fromMSP", "toMSP", "status", "batchId", "createdAt"] },
  "ddoc": "indexRShipmentsByPartiesStatus",
  "name": "indexRShipmentsByPartiesStatus",
  "type": "json"
}

2) rshipments-by-createdAt.json
{
  "index": { "fields": ["docType", "createdAt"] },
  "ddoc": "indexRShipmentsByCreatedAt",
  "name": "indexRShipmentsByCreatedAt",
  "type": "json"
}

3) rrecalls-by-batch-status.json
{
  "index": { "fields": ["docType", "batchId", "status", "createdAt"] },
  "ddoc": "indexRRecallsByBatchStatus",
  "name": "indexRRecallsByBatchStatus",
  "type": "json"
}

4) rdispense-by-batch-retail.json
{
  "index": { "fields": ["docType", "batchId", "retailMSP", "timestamp"] },
  "ddoc": "indexRDispenseByBatchRetail",
  "name": "indexRDispenseByBatchRetail",
  "type": "json"
}

---------------------------
PDC collections_config.json (deploy with chaincode)
---------------------------
[
  {
    "name": "collectionRetailSensitive",
    "policy": "OR('distributorMSP.member','retailerMSP.member')",
    "requiredPeerCount": 1,
    "maxPeerCount": 2,
    "blockToLive": 0,
    "memberOnlyRead": true,
    "memberOnlyWrite": true,
    "endorsementPolicy": {
      "signaturePolicy": "AND('distributorMSP.peer','retailerMSP.peer')"
    }
  }
]
*/
