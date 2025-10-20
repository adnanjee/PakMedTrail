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
   Distribution / Wholesale Chaincode (Go)
   ---------------------------------------------
   - Manufacturer -> Distributor shipments (offer / accept / reject / cancel / delivered)
   - CC2CC with "manufacturing" (batch ownership + DRAP gate)
   - DRAP (drapMSP) recalls: create/close recall notices; owners can quarantine shipments
   - PDC for commercial terms (price, incoterms, discount, notes)
   - Rich queries + paginated queries
   - SBE to enforce dual-endorsement where appropriate

   Channel: rawmaterialsupply
   External CC: manufacturing (finished drug batches)
*/

// ---------------- Constants & IDs ----------------

const (
	// External manufacturing chaincode
	mfgCCName  = "manufacturing"
	mfgChannel = "" // empty => same channel

	// MSPs (align to your network)
	mspDRAP         = "drapMSP"
	mspManufacturer = "manufacturerMSP" // informative; not enforced globally (we check caller vs fromMSP)
	mspDistributor  = "distributorMSP"  // informative

	// DocTypes
	DocTypeShipment = "ship.shipment"
	DocTypeRecall   = "ship.recall"

	// Shipment statuses
	ShipPending     = "PENDING"  // offered by manufacturer, awaiting distributor decision
	ShipAccepted    = "ACCEPTED" // distributor accepted; ownership moved in manufacturing
	ShipRejected    = "REJECTED"
	ShipCancelled   = "CANCELLED"
	ShipDelivered   = "DELIVERED"   // distributor marks arrived in warehouse
	ShipQuarantined = "QUARANTINED" // quarantined due to recall

	// Recall statuses
	RecallActive = "ACTIVE"
	RecallClosed = "CLOSED"

	// Events
	EvtShipmentOffered   = "ShipmentOffered"
	EvtShipmentAccepted  = "ShipmentAccepted"
	EvtShipmentRejected  = "ShipmentRejected"
	EvtShipmentCancelled = "ShipmentCancelled"
	EvtShipmentDelivered = "ShipmentDelivered"
	EvtShipmentQuarant   = "ShipmentQuarantined"

	EvtRecallInitiated = "RecallInitiated"
	EvtRecallClosed    = "RecallClosed"
)

// ---------------- Data Models ----------------

// Shipment represents a Manufacturer -> Distributor transfer proposal.
type Shipment struct {
	DocType    string            `json:"docType"` // "ship.shipment"
	ShipmentID string            `json:"shipmentId"`
	BatchID    string            `json:"batchId"`
	FromMSP    string            `json:"fromMSP"` // manufacturer
	ToMSP      string            `json:"toMSP"`   // distributor
	Quantity   float64           `json:"quantity"`
	Status     string            `json:"status"`
	Metadata   map[string]string `json:"metadata,omitempty"` // public, e.g., lot refs; may include "sensitiveHash"
	CreatedAt  string            `json:"createdAt"`
	UpdatedAt  string            `json:"updatedAt"`
}

// ShipmentSensitive lives in a PDC visible to Manufacturer + Distributor
type ShipmentSensitive struct {
	DocType    string   `json:"docType"` // "ship.shipment.sensitive"
	ShipmentID string   `json:"shipmentId"`
	PriceAmt   *float64 `json:"priceAmt,omitempty"`
	Currency   string   `json:"currency,omitempty"`
	Incoterms  string   `json:"incoterms,omitempty"`
	Discount   *float64 `json:"discount,omitempty"`
	Notes      string   `json:"notes,omitempty"`
	UpdatedAt  string   `json:"updatedAt"`
}

// RecallNotice is created/closed by DRAP.
type RecallNotice struct {
	DocType   string `json:"docType"` // "ship.recall"
	RecallID  string `json:"recallId"`
	BatchID   string `json:"batchId"`
	Reason    string `json:"reason"`
	Status    string `json:"status"` // ACTIVE / CLOSED
	IssuerMSP string `json:"issuerMSP"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
}

// PagedShipmentsResult is a helper for paginated queries.
type PagedShipmentsResult struct {
	Items    []*Shipment `json:"items"`
	Fetched  int         `json:"fetched"`
	Bookmark string      `json:"bookmark"`
}

// ---------------- Contract ----------------

type DistributionContract struct {
	contractapi.Contract
}

// ---------------- Util Helpers ----------------

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

func (c *DistributionContract) keyExists(ctx contractapi.TransactionContextInterface, key string) (bool, error) {
	b, err := ctx.GetStub().GetState(key)
	if err != nil {
		return false, err
	}
	return len(b) > 0, nil
}

func (c *DistributionContract) setSBE(ctx contractapi.TransactionContextInterface, key string, orgMSPs ...string) error {
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

// ---------------- CC2CC Helpers (manufacturing) ----------------

type mfgBatch struct {
	DocType      string  `json:"docType"`
	BatchID      string  `json:"batchId"`
	DrugCode     string  `json:"drugCode"`
	Quantity     float64 `json:"quantity"`
	Unit         string  `json:"unit"`
	ProducerMSP  string  `json:"producerMSP"`
	Status       string  `json:"status"`
	DRAPApproved bool    `json:"drapApproved"`
}

func mfgReadBatch(ctx contractapi.TransactionContextInterface, batchID string) (*mfgBatch, error) {
	args := [][]byte{[]byte("ReadBatch"), []byte(batchID)}
	resp := ctx.GetStub().InvokeChaincode(mfgCCName, args, mfgChannel)
	if resp.Status != 200 {
		return nil, fmt.Errorf("manufacturing.ReadBatch(%s) failed: %s", batchID, string(resp.Payload))
	}
	var b mfgBatch
	if err := json.Unmarshal(resp.Payload, &b); err != nil {
		return nil, fmt.Errorf("manufacturing.ReadBatch unmarshal: %w", err)
	}
	return &b, nil
}

func mfgProposeTransfer(ctx contractapi.TransactionContextInterface, batchID, toMSP string) error {
	args := [][]byte{[]byte("ProposeBatchTransfer"), []byte(batchID), []byte(toMSP)}
	resp := ctx.GetStub().InvokeChaincode(mfgCCName, args, mfgChannel)
	if resp.Status != 200 {
		return fmt.Errorf("manufacturing.ProposeBatchTransfer failed: %s", string(resp.Payload))
	}
	return nil
}

func mfgAcceptTransfer(ctx contractapi.TransactionContextInterface, batchID string) error {
	args := [][]byte{[]byte("AcceptBatchTransfer"), []byte(batchID)}
	resp := ctx.GetStub().InvokeChaincode(mfgCCName, args, mfgChannel)
	if resp.Status != 200 {
		return fmt.Errorf("manufacturing.AcceptBatchTransfer failed: %s", string(resp.Payload))
	}
	return nil
}

func mfgRejectTransfer(ctx contractapi.TransactionContextInterface, batchID, reason string) error {
	args := [][]byte{[]byte("RejectBatchTransfer"), []byte(batchID), []byte(reason)}
	resp := ctx.GetStub().InvokeChaincode(mfgCCName, args, mfgChannel)
	if resp.Status != 200 {
		return fmt.Errorf("manufacturing.RejectBatchTransfer failed: %s", string(resp.Payload))
	}
	return nil
}

func mfgCancelTransfer(ctx contractapi.TransactionContextInterface, batchID, reason string) error {
	args := [][]byte{[]byte("CancelBatchTransfer"), []byte(batchID), []byte(reason)}
	resp := ctx.GetStub().InvokeChaincode(mfgCCName, args, mfgChannel)
	if resp.Status != 200 {
		return fmt.Errorf("manufacturing.CancelBatchTransfer failed: %s", string(resp.Payload))
	}
	return nil
}

// ---------------- Shipments: Create/Accept/Reject/Cancel/Delivered ----------------

// CreateShipmentOffer(shipmentId, batchId, toMSP, quantityStr, metadataJSON)
// Preconditions:
//   - Caller must be current owner of batch in manufacturing (ProducerMSP)
//   - Batch must be DRAPApproved
//   - Initiates manufacturing.ProposeBatchTransfer(batchId, toMSP)
func (c *DistributionContract) CreateShipmentOffer(
	ctx contractapi.TransactionContextInterface,
	shipmentID, batchID, toMSP, quantityStr, metadataJSON string,
) (*Shipment, error) {

	shipmentID = strings.TrimSpace(shipmentID)
	if shipmentID == "" {
		return nil, errors.New("shipmentId required")
	}
	key := "SHIP_" + shipmentID
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
	// Must be created by current owner (manufacturer)
	callerMSP, _ := getMSP(ctx)
	if b.ProducerMSP != callerMSP {
		return nil, fmt.Errorf("caller MSP %s is not current owner %s of batch %s", callerMSP, b.ProducerMSP, batchID)
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

	// Propose transfer in manufacturing (sets its own SBE for producer+proposed owner)
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
	// SBE: require FromMSP + ToMSP for next update (accept/reject/cancel)
	_ = c.setSBE(ctx, key, s.FromMSP, s.ToMSP)
	emit(ctx, EvtShipmentOffered, s)
	return s, nil
}

// AcceptShipment(shipmentId) by distributor; calls manufacturing.AcceptBatchTransfer
func (c *DistributionContract) AcceptShipment(ctx contractapi.TransactionContextInterface, shipmentID string) (*Shipment, error) {
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

	if err := mfgAcceptTransfer(ctx, s.BatchID); err != nil {
		return nil, err
	}
	s.Status = ShipAccepted
	if err := c.putShipment(ctx, s); err != nil {
		return nil, err
	}
	// After acceptance, limit SBE to current owner (now distributor)
	_ = c.setSBE(ctx, "SHIP_"+s.ShipmentID, s.ToMSP)
	emit(ctx, EvtShipmentAccepted, s)
	return s, nil
}

// RejectShipment(shipmentId, reason) by distributor; calls manufacturing.RejectBatchTransfer
func (c *DistributionContract) RejectShipment(ctx contractapi.TransactionContextInterface, shipmentID, reason string) (*Shipment, error) {
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
	// After rejection, owner stays manufacturer
	_ = c.setSBE(ctx, "SHIP_"+s.ShipmentID, s.FromMSP)
	emit(ctx, EvtShipmentRejected, map[string]any{"shipmentId": s.ShipmentID, "reason": reason})
	return s, nil
}

// CancelShipment(shipmentId, reason) by manufacturer; calls manufacturing.CancelBatchTransfer
func (c *DistributionContract) CancelShipment(ctx contractapi.TransactionContextInterface, shipmentID, reason string) (*Shipment, error) {
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
	_ = c.setSBE(ctx, "SHIP_"+s.ShipmentID, s.FromMSP)
	emit(ctx, EvtShipmentCancelled, map[string]any{"shipmentId": s.ShipmentID, "reason": reason})
	return s, nil
}

// MarkDelivered(shipmentId) by distributor after physical receipt (post-acceptance)
func (c *DistributionContract) MarkDelivered(ctx contractapi.TransactionContextInterface, shipmentID string) (*Shipment, error) {
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
	s.Status = ShipDelivered
	if err := c.putShipment(ctx, s); err != nil {
		return nil, err
	}
	_ = c.setSBE(ctx, "SHIP_"+s.ShipmentID, s.ToMSP)
	emit(ctx, EvtShipmentDelivered, s)
	return s, nil
}

// ---------------- PDC: Commercial Terms ----------------

const collectionDistSensitive = "collectionDistSensitive"

func (c *DistributionContract) PutSensitive(
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

	var priceAmt *float64
	if strings.EqualFold(strings.TrimSpace(hasPriceStr), "true") {
		if v, err := strconv.ParseFloat(strings.TrimSpace(priceAmtStr), 64); err == nil {
			priceAmt = &v
		} else {
			return nil, fmt.Errorf("invalid price amount %q", priceAmtStr)
		}
	}
	var discount *float64
	if strings.EqualFold(strings.TrimSpace(hasDiscountStr), "true") {
		if v, err := strconv.ParseFloat(strings.TrimSpace(discountStr), 64); err == nil {
			discount = &v
		} else {
			return nil, fmt.Errorf("invalid discount %q", discountStr)
		}
	}

	rec := &ShipmentSensitive{
		DocType:    "ship.shipment.sensitive",
		ShipmentID: s.ShipmentID,
		PriceAmt:   priceAmt,
		Currency:   strings.TrimSpace(currency),
		Incoterms:  strings.TrimSpace(incoterms),
		Discount:   discount,
		Notes:      strings.TrimSpace(notes),
		UpdatedAt:  nowRFC3339(ctx),
	}
	if err := putPrivateJSON(ctx, collectionDistSensitive, "PDC_SHIP_"+s.ShipmentID, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

func (c *DistributionContract) ReadSensitive(ctx contractapi.TransactionContextInterface, shipmentID string) (*ShipmentSensitive, error) {
	s, err := c.readShipment(ctx, shipmentID)
	if err != nil {
		return nil, err
	}
	callerMSP, _ := getMSP(ctx)
	if callerMSP != s.FromMSP && callerMSP != s.ToMSP {
		return nil, fmt.Errorf("only %s or %s can read sensitive terms", s.FromMSP, s.ToMSP)
	}
	var rec ShipmentSensitive
	if err := getPrivateJSON(ctx, collectionDistSensitive, "PDC_SHIP_"+s.ShipmentID, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// LinkSensitiveHash computes SHA-256 of the PDC record and publishes hex hash into public metadata
func (c *DistributionContract) LinkSensitiveHash(ctx contractapi.TransactionContextInterface, shipmentID string) (*Shipment, error) {
	s, err := c.readShipment(ctx, shipmentID)
	if err != nil {
		return nil, err
	}
	callerMSP, _ := getMSP(ctx)
	if callerMSP != s.FromMSP && callerMSP != s.ToMSP {
		return nil, fmt.Errorf("only %s or %s can link sensitive hash", s.FromMSP, s.ToMSP)
	}
	var rec ShipmentSensitive
	if err := getPrivateJSON(ctx, collectionDistSensitive, "PDC_SHIP_"+s.ShipmentID, &rec); err != nil {
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

// ---------------- Recalls (DRAP) ----------------

// InitiateRecallByDRAP(recallId, batchId, reason)
func (c *DistributionContract) InitiateRecallByDRAP(ctx contractapi.TransactionContextInterface, recallID, batchID, reason string) (*RecallNotice, error) {
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
	key := "RECALL_" + recallID
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

// CloseRecallByDRAP(recallId, note)
func (c *DistributionContract) CloseRecallByDRAP(ctx contractapi.TransactionContextInterface, recallID, note string) (*RecallNotice, error) {
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
	r.Reason = strings.TrimSpace(note) // optional: store closure note here
	r.UpdatedAt = nowRFC3339(ctx)
	if err := putJSON(ctx, "RECALL_"+r.RecallID, r); err != nil {
		return nil, err
	}
	emit(ctx, EvtRecallClosed, r)
	return r, nil
}

// QuarantineByRecall(shipmentId) — owner (manufacturer or distributor) quarantines shipment when recall is active
func (c *DistributionContract) QuarantineByRecall(ctx contractapi.TransactionContextInterface, shipmentID string) (*Shipment, error) {
	s, err := c.readShipment(ctx, shipmentID)
	if err != nil {
		return nil, err
	}
	callerMSP, _ := getMSP(ctx)
	// permitted for current owner of the shipment record
	owner := s.FromMSP
	if s.Status == ShipAccepted || s.Status == ShipDelivered {
		owner = s.ToMSP
	}
	if callerMSP != owner {
		return nil, fmt.Errorf("only current owner %s can quarantine shipment", owner)
	}
	// Ensure there exists an ACTIVE recall notice for the shipment's batch
	active, err := c.findActiveRecallForBatch(ctx, s.BatchID)
	if err != nil {
		return nil, err
	}
	if !active {
		return nil, errors.New("no ACTIVE recall for shipment's batch")
	}
	s.Status = ShipQuarantined
	if err := c.putShipment(ctx, s); err != nil {
		return nil, err
	}
	emit(ctx, EvtShipmentQuarant, map[string]any{"shipmentId": s.ShipmentID, "batchId": s.BatchID})
	return s, nil
}

// ---------------- Queries & Pagination ----------------

func (c *DistributionContract) ReadShipment(ctx contractapi.TransactionContextInterface, shipmentID string) (*Shipment, error) {
	return c.readShipment(ctx, shipmentID)
}

func (c *DistributionContract) GetShipmentsByParty(ctx contractapi.TransactionContextInterface, partyMSP string) ([]*Shipment, error) {
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

func (c *DistributionContract) QueryShipments(ctx contractapi.TransactionContextInterface, selectorJSON string) ([]*Shipment, error) {
	return queryShipments(ctx, selectorJSON)
}

// QueryShipmentsPaged(selectorJSON, pageSize, bookmark) -> {items, fetched, bookmark}
func (c *DistributionContract) QueryShipmentsPaged(
	ctx contractapi.TransactionContextInterface,
	selectorJSON, pageSizeStr, bookmark string,
) (*PagedShipmentsResult, error) {
	pageSize, err := strconv.Atoi(strings.TrimSpace(pageSizeStr))
	if err != nil || pageSize <= 0 {
		pageSize = 50
	}
	return queryShipmentsPaged(ctx, selectorJSON, int32(pageSize), bookmark)
}

func (c *DistributionContract) QueryRecalls(ctx contractapi.TransactionContextInterface, selectorJSON string) ([]*RecallNotice, error) {
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

// ---------------- Private Helpers (state) ----------------

func (c *DistributionContract) readShipment(ctx contractapi.TransactionContextInterface, shipmentID string) (*Shipment, error) {
	var s Shipment
	if err := getJSON(ctx, "SHIP_"+strings.TrimSpace(shipmentID), &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (c *DistributionContract) putShipment(ctx contractapi.TransactionContextInterface, s *Shipment) error {
	s.UpdatedAt = nowRFC3339(ctx)
	return putJSON(ctx, "SHIP_"+s.ShipmentID, s)
}

func (c *DistributionContract) readRecall(ctx contractapi.TransactionContextInterface, recallID string) (*RecallNotice, error) {
	var r RecallNotice
	if err := getJSON(ctx, "RECALL_"+strings.TrimSpace(recallID), &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (c *DistributionContract) findActiveRecallForBatch(ctx contractapi.TransactionContextInterface, batchID string) (bool, error) {
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

// ---------------- Private Data utils ----------------

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

// ---------------- main ----------------

func main() {
	cc, err := contractapi.NewChaincode(new(DistributionContract))
	if err != nil {
		panic(fmt.Errorf("create chaincode: %w", err))
	}
	if err := cc.Start(); err != nil {
		panic(fmt.Errorf("start chaincode: %w", err))
	}
}

/*
---------------------------
CouchDB Index JSON (copy into files under:
 chaincode/distribution-go/META-INF/statedb/couchdb/indexes/)
---------------------------

1) shipments-by-parties-status.json
{
  "index": { "fields": ["docType", "fromMSP", "toMSP", "status", "batchId", "createdAt"] },
  "ddoc": "indexShipmentsByPartiesStatus",
  "name": "indexShipmentsByPartiesStatus",
  "type": "json"
}

2) shipments-by-createdAt.json
{
  "index": { "fields": ["docType", "createdAt"] },
  "ddoc": "indexShipmentsByCreatedAt",
  "name": "indexShipmentsByCreatedAt",
  "type": "json"
}

3) recalls-by-batch-status.json
{
  "index": { "fields": ["docType", "batchId", "status", "createdAt"] },
  "ddoc": "indexRecallsByBatchStatus",
  "name": "indexRecallsByBatchStatus",
  "type": "json"
}

---------------------------
PDC collections_config.json (deploy alongside chaincode)
---------------------------
[
  {
    "name": "collectionDistSensitive",
    "policy": {
      "identities": [
        {"role":{"name":"member","mspId":"manufacturerMSP"}},
        {"role":{"name":"member","mspId":"distributorMSP"}}
      ],
      "policy": {"2-of":[{"signed-by":0},{"signed-by":1}]}
    },
    "requiredPeerCount": 1,
    "maxPeerCount": 2,
    "blockToLive": 0,
    "memberOnlyRead": true,
    "memberOnlyWrite": true
  }
]
*/
