package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hyperledger/fabric-chaincode-go/pkg/cid"
	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

/*
RSK Payment Bridge Chaincode (Go)
---------------------------------
Scope:
- Represents off-chain RSK payments as on-ledger "payment intents".
- Links payments to existing business assets (lots, batches, shipments, etc).
- Tracks lifecycle: PENDING -> SENT -> CONFIRMED / FAILED / CANCELLED.
- Stores RSK metadata: network, token, addresses, transaction hash.
- Emits Fabric events consumed by an off-chain bridge service.

Channel: pakmedtrail (shared with other PakMedTrail supply chain chaincodes)
*/

// ---------------- Constants ----------------

const (
	// Document type for payments
	DocPaymentIntent = "pay.intent"

	// Payment status values
	PayStatusPending   = "PENDING"   // created, not yet sent to RSK
	PayStatusSent      = "SENT"      // RSK tx broadcast, waiting confirmations
	PayStatusConfirmed = "CONFIRMED" // final on RSK
	PayStatusFailed    = "FAILED"    // tx reverted / dropped / error
	PayStatusCancelled = "CANCELLED" // cancelled on Fabric

	// Event names used for bridge integration
	EventPaymentCreated   = "PaymentCreated"
	EventPaymentSent      = "PaymentSent"
	EventPaymentConfirmed = "PaymentConfirmed"
	EventPaymentFailed    = "PaymentFailed"
	EventPaymentCancelled = "PaymentCancelled"
)

// ---------------- Data Models ----------------

// PaymentIntent captures everything the bridge and apps need
// to track a payment that is actually executed on RSK.
type PaymentIntent struct {
	DocType string `json:"docType"` // "pay.intent"

	// Business linkage
	PaymentID string `json:"paymentId"` // logical ID from app
	RefType   string `json:"refType"`   // e.g. "RAW_LOT","BATCH","DIST_SHIPMENT","RETAIL_SHIPMENT"
	RefID     string `json:"refId"`     // e.g. "LOT123","BATCH_001","SHIP_001"

	// Parties (Fabric orgs)
	FromMSP string `json:"fromMSP"` // payer org (caller at creation)
	ToMSP   string `json:"toMSP"`   // payee org

	// Amount & token. Do not use omitempty here: Fabric contract-api schema
	// validation expects these fields to be present even when their value is
	// intentionally empty at PENDING state, such as tokenContract for native
	// RBTC payments.
	Amount           string `json:"amount"`        // human amount, e.g. "100.00" or "0.5"
	Currency         string `json:"currency"`      // e.g. "RBTC","rUSD"
	TokenSymbol      string `json:"tokenSymbol"`   // e.g. "RBTC","USDR"
	TokenContract    string `json:"tokenContract"` // RSK ERC-20 address, empty for native RBTC
	TokenDecimalsStr string `json:"tokenDecimals"` // e.g. "18" (string to avoid float issues)
	RskNetwork       string `json:"rskNetwork"`    // e.g. "testnet","mainnet"

	// RSK addresses + tx. These are empty until the bridge broadcasts the RSK
	// transaction, but they must still be returned in the JSON response.
	RskAddressFrom string `json:"rskAddressFrom"` // bridge wallet / payer address
	RskAddressTo   string `json:"rskAddressTo"`   // recipient RSK address
	RskTxHash      string `json:"rskTxHash"`      // set once broadcast

	// Status & metadata
	Status    string            `json:"status"`    // PENDING / SENT / CONFIRMED / FAILED / CANCELLED
	LastError string            `json:"lastError"` // last failure / cancellation reason
	Metadata  map[string]string `json:"metadata"`  // free-form metadata for app/bridge

	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
}

// ---------------- Contract ----------------

type PaymentContract struct {
	contractapi.Contract
}

// ---------------- Helpers ----------------

func nowRFC3339(ctx contractapi.TransactionContextInterface) string {
	ts, err := ctx.GetStub().GetTxTimestamp()
	if err == nil && ts != nil {
		return time.Unix(ts.Seconds, int64(ts.Nanos)).UTC().Format(time.RFC3339)
	}
	return time.Now().UTC().Format(time.RFC3339)
}

func getMSP(ctx contractapi.TransactionContextInterface) (string, error) {
	return cid.GetMSPID(ctx.GetStub())
}

func emit(ctx contractapi.TransactionContextInterface, name string, v any) {
	if b, err := json.Marshal(v); err == nil {
		_ = ctx.GetStub().SetEvent(name, b)
	}
}

func putJSON(ctx contractapi.TransactionContextInterface, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return ctx.GetStub().PutState(key, b)
}

func getJSON(ctx contractapi.TransactionContextInterface, key string, v any) error {
	b, err := ctx.GetStub().GetState(key)
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return fmt.Errorf("key %s not found", key)
	}
	return json.Unmarshal(b, v)
}

func keyExists(ctx contractapi.TransactionContextInterface, key string) (bool, error) {
	b, err := ctx.GetStub().GetState(key)
	if err != nil {
		return false, err
	}
	return len(b) > 0, nil
}

// Key helpers

func paymentKey(paymentID string) string {
	return "PAY_" + strings.TrimSpace(paymentID)
}

// Validate allowed status transitions
func validatePaymentStatusTransition(oldStatus, newStatus string) error {
	allowed := map[string][]string{
		PayStatusPending: {
			PayStatusSent,
			PayStatusCancelled,
		},
		PayStatusSent: {
			PayStatusConfirmed,
			PayStatusFailed,
		},
		PayStatusFailed: {
			PayStatusSent,
			PayStatusCancelled,
		},
		PayStatusConfirmed: {},
		PayStatusCancelled: {},
	}

	if next, ok := allowed[oldStatus]; ok {
		for _, v := range next {
			if v == newStatus {
				return nil
			}
		}
	}
	return fmt.Errorf("invalid payment status transition from %s to %s", oldStatus, newStatus)
}

// Internal read/write helpers
func normalizePayment(p *PaymentIntent) {
	if p.Metadata == nil {
		p.Metadata = map[string]string{}
	}
}

func (c *PaymentContract) readPayment(ctx contractapi.TransactionContextInterface, paymentID string) (*PaymentIntent, error) {
	var p PaymentIntent
	if err := getJSON(ctx, paymentKey(paymentID), &p); err != nil {
		return nil, err
	}
	normalizePayment(&p)
	return &p, nil
}

func (c *PaymentContract) putPayment(ctx contractapi.TransactionContextInterface, p *PaymentIntent) error {
	normalizePayment(p)
	p.UpdatedAt = nowRFC3339(ctx)
	return putJSON(ctx, paymentKey(p.PaymentID), p)
}

// ---------------- Public API ----------------

// InitLedger is kept for consistency; currently a no-op.
func (c *PaymentContract) InitLedger(ctx contractapi.TransactionContextInterface) error {
	return nil
}

// PaymentExists(paymentId) -> bool
func (c *PaymentContract) PaymentExists(ctx contractapi.TransactionContextInterface, paymentID string) (bool, error) {
	return keyExists(ctx, paymentKey(paymentID))
}

// CreatePaymentIntent creates a new payment intent linked to a business object.
//
// Args (all strings):
//   - paymentId: unique payment ID
//   - refType: type of the referenced business asset
//   - refId: ID of the referenced business asset
//   - toMSP: payee MSP
//   - amount: positive decimal string
//   - currency: optional chain-level currency, e.g. "RBTC"
//   - tokenSymbol: token symbol on RSK
//   - tokenContract: RSK token contract address (for ERC-20 style tokens)
//   - tokenDecimalsStr: token decimals as string, e.g. "18"
//   - rskNetwork: e.g. "testnet","mainnet"
//   - rskAddressTo: receiver RSK address
//   - metadataJSON: optional JSON object for app/bridge metadata
func (c *PaymentContract) CreatePaymentIntent(
	ctx contractapi.TransactionContextInterface,
	paymentID, refType, refID, toMSP,
	amount, currency, tokenSymbol, tokenContract, tokenDecimalsStr,
	rskNetwork, rskAddressTo, metadataJSON string,
) (*PaymentIntent, error) {

	paymentID = strings.TrimSpace(paymentID)
	if paymentID == "" {
		return nil, errors.New("paymentId required")
	}
	exists, err := keyExists(ctx, paymentKey(paymentID))
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("payment %s already exists", paymentID)
	}

	refType = strings.ToUpper(strings.TrimSpace(refType))
	refID = strings.TrimSpace(refID)
	if refType == "" || refID == "" {
		return nil, errors.New("refType and refId are required")
	}

	toMSP = strings.TrimSpace(toMSP)
	if toMSP == "" {
		return nil, errors.New("toMSP required")
	}

	amount = strings.TrimSpace(amount)
	if amount == "" {
		return nil, errors.New("amount required")
	}
	// Basic positive-number validation
	if v, err := strconv.ParseFloat(amount, 64); err != nil || v <= 0 {
		return nil, fmt.Errorf("invalid amount %q (must be positive numeric string)", amount)
	}

	rskAddressTo = strings.TrimSpace(rskAddressTo)
	if rskAddressTo == "" {
		return nil, errors.New("rskAddressTo required")
	}

	callerMSP, err := getMSP(ctx)
	if err != nil {
		return nil, err
	}

	md := map[string]string{}
	if strings.TrimSpace(metadataJSON) != "" {
		if err := json.Unmarshal([]byte(metadataJSON), &md); err != nil {
			return nil, fmt.Errorf("metadata JSON invalid: %w", err)
		}
		if md == nil {
			md = map[string]string{}
		}
	}

	now := nowRFC3339(ctx)
	p := &PaymentIntent{
		DocType:   DocPaymentIntent,
		PaymentID: paymentID,
		RefType:   refType,
		RefID:     refID,
		FromMSP:   callerMSP,
		ToMSP:     toMSP,

		Amount:           amount,
		Currency:         strings.ToUpper(strings.TrimSpace(currency)),
		TokenSymbol:      strings.ToUpper(strings.TrimSpace(tokenSymbol)),
		TokenContract:    strings.TrimSpace(tokenContract),
		TokenDecimalsStr: strings.TrimSpace(tokenDecimalsStr),
		RskNetwork:       strings.TrimSpace(rskNetwork),

		RskAddressFrom: "",
		RskAddressTo:   rskAddressTo,
		RskTxHash:      "",

		Status:    PayStatusPending,
		LastError: "",
		Metadata:  md,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := c.putPayment(ctx, p); err != nil {
		return nil, err
	}

	emit(ctx, EventPaymentCreated, p)
	return p, nil
}

// ReadPaymentIntent(paymentId) -> PaymentIntent
func (c *PaymentContract) ReadPaymentIntent(ctx contractapi.TransactionContextInterface, paymentID string) (*PaymentIntent, error) {
	return c.readPayment(ctx, paymentID)
}

// MarkPaymentSent(paymentId, rskTxHash, rskAddressFrom)
//
// Called by the bridge service after RSK tx has been broadcast.
func (c *PaymentContract) MarkPaymentSent(
	ctx contractapi.TransactionContextInterface,
	paymentID, rskTxHash, rskAddressFrom string,
) (*PaymentIntent, error) {

	p, err := c.readPayment(ctx, paymentID)
	if err != nil {
		return nil, err
	}

	if err := validatePaymentStatusTransition(p.Status, PayStatusSent); err != nil {
		return nil, err
	}

	// Only parties of the payment (or a future bridge MSP) should be able to update.
	callerMSP, err := getMSP(ctx)
	if err != nil {
		return nil, err
	}
	if callerMSP != p.FromMSP && callerMSP != p.ToMSP {
		return nil, fmt.Errorf("access denied: caller MSP %s is not FromMSP (%s) or ToMSP (%s)", callerMSP, p.FromMSP, p.ToMSP)
	}

	rskTxHash = strings.TrimSpace(rskTxHash)
	if rskTxHash == "" {
		return nil, errors.New("rskTxHash required")
	}
	p.RskTxHash = rskTxHash

	if s := strings.TrimSpace(rskAddressFrom); s != "" {
		p.RskAddressFrom = s
	}

	p.Status = PayStatusSent
	p.LastError = ""

	if err := c.putPayment(ctx, p); err != nil {
		return nil, err
	}

	emit(ctx, EventPaymentSent, p)
	return p, nil
}

// MarkPaymentConfirmed(paymentId)
//
// Called by the bridge service after enough RSK confirmations.
func (c *PaymentContract) MarkPaymentConfirmed(
	ctx contractapi.TransactionContextInterface,
	paymentID string,
) (*PaymentIntent, error) {

	p, err := c.readPayment(ctx, paymentID)
	if err != nil {
		return nil, err
	}

	if err := validatePaymentStatusTransition(p.Status, PayStatusConfirmed); err != nil {
		return nil, err
	}

	callerMSP, err := getMSP(ctx)
	if err != nil {
		return nil, err
	}
	if callerMSP != p.FromMSP && callerMSP != p.ToMSP {
		return nil, fmt.Errorf("access denied: caller MSP %s is not FromMSP (%s) or ToMSP (%s)", callerMSP, p.FromMSP, p.ToMSP)
	}

	p.Status = PayStatusConfirmed
	p.LastError = ""

	if err := c.putPayment(ctx, p); err != nil {
		return nil, err
	}

	emit(ctx, EventPaymentConfirmed, p)
	return p, nil
}

// MarkPaymentFailed(paymentId, reason)
//
// Called by the bridge service if the RSK tx fails.
func (c *PaymentContract) MarkPaymentFailed(
	ctx contractapi.TransactionContextInterface,
	paymentID, reason string,
) (*PaymentIntent, error) {

	p, err := c.readPayment(ctx, paymentID)
	if err != nil {
		return nil, err
	}

	if err := validatePaymentStatusTransition(p.Status, PayStatusFailed); err != nil {
		return nil, err
	}

	callerMSP, err := getMSP(ctx)
	if err != nil {
		return nil, err
	}
	if callerMSP != p.FromMSP && callerMSP != p.ToMSP {
		return nil, fmt.Errorf("access denied: caller MSP %s is not FromMSP (%s) or ToMSP (%s)", callerMSP, p.FromMSP, p.ToMSP)
	}

	p.Status = PayStatusFailed
	p.LastError = strings.TrimSpace(reason)

	if err := c.putPayment(ctx, p); err != nil {
		return nil, err
	}

	emit(ctx, EventPaymentFailed, p)
	return p, nil
}

// CancelPaymentIntent(paymentId, reason)
//
// Typically called by the payer BEFORE the tx is sent,
// but we also allow cancellation after a FAILED attempt.
func (c *PaymentContract) CancelPaymentIntent(
	ctx contractapi.TransactionContextInterface,
	paymentID, reason string,
) (*PaymentIntent, error) {

	p, err := c.readPayment(ctx, paymentID)
	if err != nil {
		return nil, err
	}

	// Only the payer org can cancel
	callerMSP, err := getMSP(ctx)
	if err != nil {
		return nil, err
	}
	if callerMSP != p.FromMSP {
		return nil, fmt.Errorf("only payer MSP %s can cancel (caller %s)", p.FromMSP, callerMSP)
	}

	if p.Status != PayStatusPending && p.Status != PayStatusFailed {
		return nil, fmt.Errorf("cannot cancel payment in status %s", p.Status)
	}

	if err := validatePaymentStatusTransition(p.Status, PayStatusCancelled); err != nil {
		return nil, err
	}

	p.Status = PayStatusCancelled
	p.LastError = strings.TrimSpace(reason)

	if err := c.putPayment(ctx, p); err != nil {
		return nil, err
	}

	emit(ctx, EventPaymentCancelled, p)
	return p, nil
}

// ---------------- Query Functions ----------------

// GetPaymentsByRef(refType, refId) -> [PaymentIntent]
func (c *PaymentContract) GetPaymentsByRef(
	ctx contractapi.TransactionContextInterface,
	refType, refID string,
) ([]*PaymentIntent, error) {

	selector := map[string]any{
		"selector": map[string]any{
			"docType": DocPaymentIntent,
			"refType": strings.ToUpper(strings.TrimSpace(refType)),
			"refId":   strings.TrimSpace(refID),
		},
	}

	js, _ := json.Marshal(selector)
	iter, err := ctx.GetStub().GetQueryResult(string(js))
	if err != nil {
		return nil, fmt.Errorf("query payments by ref: %w", err)
	}
	defer iter.Close()

	out := []*PaymentIntent{}
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		var p PaymentIntent
		if err := json.Unmarshal(kv.Value, &p); err == nil && p.DocType == DocPaymentIntent {
			normalizePayment(&p)
			out = append(out, &p)
		}
	}
	return out, nil
}

// GetPaymentsByParty(msp, status) -> [PaymentIntent]
//   - msp: if empty, use caller MSP.
//   - status: optional; if empty, no status filter.
func (c *PaymentContract) GetPaymentsByParty(
	ctx contractapi.TransactionContextInterface,
	msp, status string,
) ([]*PaymentIntent, error) {

	if strings.TrimSpace(msp) == "" {
		var err error
		msp, err = getMSP(ctx)
		if err != nil {
			return nil, err
		}
	}
	msp = strings.TrimSpace(msp)
	status = strings.TrimSpace(status)

	selector := map[string]any{
		"selector": map[string]any{
			"docType": DocPaymentIntent,
			"$or": []map[string]any{
				{"fromMSP": msp},
				{"toMSP": msp},
			},
		},
	}

	if status != "" {
		selector["selector"].(map[string]any)["status"] = status
	}

	js, _ := json.Marshal(selector)
	iter, err := ctx.GetStub().GetQueryResult(string(js))
	if err != nil {
		return nil, fmt.Errorf("query payments by party: %w", err)
	}
	defer iter.Close()

	out := []*PaymentIntent{}
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		var p PaymentIntent
		if err := json.Unmarshal(kv.Value, &p); err == nil && p.DocType == DocPaymentIntent {
			normalizePayment(&p)
			out = append(out, &p)
		}
	}
	return out, nil
}

// GetPaymentsByStatus(status) -> [PaymentIntent]
func (c *PaymentContract) GetPaymentsByStatus(
	ctx contractapi.TransactionContextInterface,
	status string,
) ([]*PaymentIntent, error) {

	selector := map[string]any{
		"selector": map[string]any{
			"docType": DocPaymentIntent,
			"status":  strings.TrimSpace(status),
		},
	}

	js, _ := json.Marshal(selector)
	iter, err := ctx.GetStub().GetQueryResult(string(js))
	if err != nil {
		return nil, fmt.Errorf("query payments by status: %w", err)
	}
	defer iter.Close()

	out := []*PaymentIntent{}
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		var p PaymentIntent
		if err := json.Unmarshal(kv.Value, &p); err == nil && p.DocType == DocPaymentIntent {
			normalizePayment(&p)
			out = append(out, &p)
		}
	}
	return out, nil
}

// GetAllPayments() -> [PaymentIntent]
// WARNING: for large networks this may be heavy; use carefully.
func (c *PaymentContract) GetAllPayments(ctx contractapi.TransactionContextInterface) ([]*PaymentIntent, error) {
	selector := map[string]any{
		"selector": map[string]any{
			"docType": DocPaymentIntent,
		},
	}

	js, _ := json.Marshal(selector)
	iter, err := ctx.GetStub().GetQueryResult(string(js))
	if err != nil {
		return nil, fmt.Errorf("query all payments: %w", err)
	}
	defer iter.Close()

	out := []*PaymentIntent{}
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		var p PaymentIntent
		if err := json.Unmarshal(kv.Value, &p); err == nil && p.DocType == DocPaymentIntent {
			normalizePayment(&p)
			out = append(out, &p)
		}
	}
	return out, nil
}

// ---------------- main ----------------

func main() {
	cc, err := contractapi.NewChaincode(new(PaymentContract))
	if err != nil {
		panic(fmt.Errorf("create chaincode: %w", err))
	}
	if err := cc.Start(); err != nil {
		panic(fmt.Errorf("start chaincode: %w", err))
	}
}

/*
---------------------------------------------
CouchDB Index JSON (put under e.g.:
  chaincode/payments-go/META-INF/statedb/couchdb/indexes/)
---------------------------------------------

1) payments-by-ref.json
{
  "index": { "fields": ["docType", "refType", "refId", "createdAt"] },
  "ddoc": "indexPaymentsByRef",
  "name": "indexPaymentsByRef",
  "type": "json"
}

2) payments-by-party-status.json
{
  "index": {
    "fields": ["docType", "fromMSP", "toMSP", "status", "createdAt"]
  },
  "ddoc": "indexPaymentsByPartyStatus",
  "name": "indexPaymentsByPartyStatus",
  "type": "json"
}

3) payments-by-status.json
{
  "index": { "fields": ["docType", "status", "createdAt"] },
  "ddoc": "indexPaymentsByStatus",
  "name": "indexPaymentsByStatus",
  "type": "json"
}
*/
