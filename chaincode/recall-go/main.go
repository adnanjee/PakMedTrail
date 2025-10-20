package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hyperledger/fabric-chaincode-go/pkg/cid"
	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

/*
Recall & Compliance Chaincode (Go)
----------------------------------
Scope:
- DRAP (drapMSP) can create, update, and close recalls/notices.
- Recalls may target assets by type + id (e.g., RAW_LOT:LOT123, BATCH:BATCH_001, SHIPMENT:RSHIP1).
- Any org can check if an asset is affected (CC2CC-friendly).
- Current owner (business CC decides) can quarantine; DRAP (or closed recall condition) can clear.
- Orgs can acknowledge a recall (org-level receipt).

DocTypes:
- "recall.notice"           : the recall itself
- "recall.affect"           : mapping of (assetType, assetId) -> recallId (helps fast lookups)
- "recall.ack"              : org acknowledgment of a recall
- "recall.quarantine"       : per-asset quarantine status

Channel: rawmaterialsupply
*/

// ---------------- Constants ----------------

const (
	mspDRAP = "drapMSP"

	DocRecall     = "recall.notice"
	DocAffect     = "recall.affect"
	DocAck        = "recall.ack"
	DocQuarantine = "recall.quarantine"

	RecallActive = "ACTIVE"
	RecallClosed = "CLOSED"

	QuarantineOn  = "ON"
	QuarantineOff = "OFF"

	// Events
	EvtRecallInitiated = "RecallInitiated"
	EvtRecallAssetsAdd = "RecallAssetsAdded"
	EvtRecallClosed    = "RecallClosed"
	EvtAck             = "RecallAcknowledged"
	EvtQuarantineOn    = "AssetQuarantined"
	EvtQuarantineOff   = "AssetClearQuarantine"
)

// AssetType is a free enum to decouple this CC from others.
// Suggested: RAW_LOT, BATCH, SHIPMENT, RETAIL_UNIT etc.
type AssetType string

// ---------------- Models ----------------

type RecallNotice struct {
	DocType    string            `json:"docType"` // "recall.notice"
	RecallID   string            `json:"recallId"`
	Title      string            `json:"title,omitempty"`
	Reason     string            `json:"reason"`
	Severity   string            `json:"severity,omitempty"`   // e.g., "HIGH","MEDIUM","LOW"
	Directives map[string]string `json:"directives,omitempty"` // free-form instruction map
	Status     string            `json:"status"`               // ACTIVE / CLOSED
	IssuerMSP  string            `json:"issuerMSP"`            // drapMSP
	CreatedAt  string            `json:"createdAt"`
	UpdatedAt  string            `json:"updatedAt"`
}

type RecallAffect struct {
	DocType   string    `json:"docType"` // "recall.affect"
	RecallID  string    `json:"recallId"`
	AssetType AssetType `json:"assetType"`
	AssetID   string    `json:"assetId"`
	CreatedAt string    `json:"createdAt"`
}

type RecallAck struct {
	DocType   string `json:"docType"` // "recall.ack"
	RecallID  string `json:"recallId"`
	OrgMSP    string `json:"orgMSP"`
	Timestamp string `json:"timestamp"`
	Note      string `json:"note,omitempty"`
}

type Quarantine struct {
	DocType   string    `json:"docType"` // "recall.quarantine"
	AssetType AssetType `json:"assetType"`
	AssetID   string    `json:"assetId"`
	Status    string    `json:"status"`   // ON / OFF
	RecallID  string    `json:"recallId"` // which recall triggered ON (informational)
	UpdatedAt string    `json:"updatedAt"`
}

// ---------------- Contract ----------------

type RecallContract struct {
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

func isDRAP(ctx contractapi.TransactionContextInterface) (bool, string, error) {
	msp, err := getMSP(ctx)
	if err != nil {
		return false, "", err
	}
	return msp == mspDRAP, msp, nil
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

func emit(ctx contractapi.TransactionContextInterface, name string, v any) {
	if b, err := json.Marshal(v); err == nil {
		_ = ctx.GetStub().SetEvent(name, b)
	}
}

func affectKey(assetType AssetType, assetID string, recallID string) string {
	return fmt.Sprintf("AFFECT_%s_%s_%s", strings.ToUpper(string(assetType)), strings.TrimSpace(assetID), strings.TrimSpace(recallID))
}

func quarantineKey(assetType AssetType, assetID string) string {
	return fmt.Sprintf("Q_%s_%s", strings.ToUpper(string(assetType)), strings.TrimSpace(assetID))
}

func recallKey(recallID string) string {
	return "RECALL_" + strings.TrimSpace(recallID)
}

func ackKey(recallID, orgMSP string) string {
	return fmt.Sprintf("ACK_%s_%s", strings.TrimSpace(recallID), strings.TrimSpace(orgMSP))
}

// ---------------- DRAP API ----------------

// InitiateRecallByDRAP(recallId, title, reason, severity, directivesJSON)
func (c *RecallContract) InitiateRecallByDRAP(
	ctx contractapi.TransactionContextInterface,
	recallID, title, reason, severity, directivesJSON string,
) (*RecallNotice, error) {

	ok, caller, err := isDRAP(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("access denied: caller MSP %s is not DRAP", caller)
	}

	if recallID = strings.TrimSpace(recallID); recallID == "" {
		return nil, errors.New("recallId required")
	}
	key := recallKey(recallID)
	ex, err := keyExists(ctx, key)
	if err != nil {
		return nil, err
	}
	if ex {
		return nil, fmt.Errorf("recall %s already exists", recallID)
	}

	var dir map[string]string
	if strings.TrimSpace(directivesJSON) != "" {
		if err := json.Unmarshal([]byte(directivesJSON), &dir); err != nil {
			return nil, fmt.Errorf("directives JSON invalid: %w", err)
		}
	}

	now := nowRFC3339(ctx)
	rec := &RecallNotice{
		DocType:    DocRecall,
		RecallID:   recallID,
		Title:      strings.TrimSpace(title),
		Reason:     strings.TrimSpace(reason),
		Severity:   strings.ToUpper(strings.TrimSpace(severity)),
		Directives: dir,
		Status:     RecallActive,
		IssuerMSP:  mspDRAP,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := putJSON(ctx, key, rec); err != nil {
		return nil, err
	}
	emit(ctx, EvtRecallInitiated, rec)
	return rec, nil
}

// AddAffectedAssetsByDRAP(recallId, assetType, csvAssetIds)
// assetType examples: RAW_LOT, BATCH, SHIPMENT, RETAIL_UNIT
func (c *RecallContract) AddAffectedAssetsByDRAP(
	ctx contractapi.TransactionContextInterface,
	recallID, assetTypeStr, csvAssetIDs string,
) (int, error) {

	ok, caller, err := isDRAP(ctx)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("access denied: caller MSP %s is not DRAP", caller)
	}
	rec, err := c.ReadRecall(ctx, recallID)
	if err != nil {
		return 0, err
	}
	if rec.Status != RecallActive {
		return 0, fmt.Errorf("recall %s is not ACTIVE", recallID)
	}

	at := AssetType(strings.ToUpper(strings.TrimSpace(assetTypeStr)))
	if at == "" {
		return 0, errors.New("assetType required")
	}

	ids := strings.Split(csvAssetIDs, ",")
	count := 0
	now := nowRFC3339(ctx)
	for _, raw := range ids {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}
		a := &RecallAffect{
			DocType:   DocAffect,
			RecallID:  rec.RecallID,
			AssetType: at,
			AssetID:   id,
			CreatedAt: now,
		}
		k := affectKey(at, id, rec.RecallID)
		if err := putJSON(ctx, k, a); err != nil {
			return count, err
		}
		count++
	}
	if count > 0 {
		emit(ctx, EvtRecallAssetsAdd, map[string]any{
			"recallId":  rec.RecallID,
			"assetType": at,
			"count":     count,
		})
	}
	return count, nil
}

// CloseRecallByDRAP(recallId, note)
func (c *RecallContract) CloseRecallByDRAP(ctx contractapi.TransactionContextInterface, recallID, note string) (*RecallNotice, error) {
	ok, caller, err := isDRAP(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("access denied: caller MSP %s is not DRAP", caller)
	}
	rec, err := c.ReadRecall(ctx, recallID)
	if err != nil {
		return nil, err
	}
	rec.Status = RecallClosed
	if s := strings.TrimSpace(note); s != "" {
		rec.Reason = s // optional update
	}
	rec.UpdatedAt = nowRFC3339(ctx)
	if err := putJSON(ctx, recallKey(rec.RecallID), rec); err != nil {
		return nil, err
	}
	emit(ctx, EvtRecallClosed, rec)
	return rec, nil
}

// ---------------- Acknowledgements ----------------

// AcknowledgeRecall(recallId, note) — any org confirms receipt/awareness
func (c *RecallContract) AcknowledgeRecall(ctx contractapi.TransactionContextInterface, recallID, note string) (*RecallAck, error) {
	rec, err := c.ReadRecall(ctx, recallID)
	if err != nil {
		return nil, err
	}
	org, err := getMSP(ctx)
	if err != nil {
		return nil, err
	}
	ack := &RecallAck{
		DocType:   DocAck,
		RecallID:  rec.RecallID,
		OrgMSP:    org,
		Timestamp: nowRFC3339(ctx),
		Note:      strings.TrimSpace(note),
	}
	if err := putJSON(ctx, ackKey(rec.RecallID, org), ack); err != nil {
		return nil, err
	}
	emit(ctx, EvtAck, ack)
	return ack, nil
}

// ---------------- Quarantine API ----------------

// QuarantineAsset(assetType, assetId, recallId)
// - Anyone can call, but typical caller is the current asset owner in its domain CC.
// - Requires recall to be ACTIVE and asset marked affected.
func (c *RecallContract) QuarantineAsset(
	ctx contractapi.TransactionContextInterface,
	assetTypeStr, assetID, recallID string,
) (*Quarantine, error) {

	assetType := AssetType(strings.ToUpper(strings.TrimSpace(assetTypeStr)))
	if assetType == "" || strings.TrimSpace(assetID) == "" {
		return nil, errors.New("assetType and assetId are required")
	}
	rec, err := c.ReadRecall(ctx, recallID)
	if err != nil {
		return nil, err
	}
	if rec.Status != RecallActive {
		return nil, fmt.Errorf("recall %s is not ACTIVE", recallID)
	}

	affected, err := c.isAssetAffectedByRecall(ctx, assetType, assetID, recallID)
	if err != nil {
		return nil, err
	}
	if !affected {
		return nil, fmt.Errorf("asset %s:%s is not listed as affected in recall %s", assetType, assetID, recallID)
	}

	q := &Quarantine{
		DocType:   DocQuarantine,
		AssetType: assetType,
		AssetID:   strings.TrimSpace(assetID),
		Status:    QuarantineOn,
		RecallID:  rec.RecallID,
		UpdatedAt: nowRFC3339(ctx),
	}
	if err := putJSON(ctx, quarantineKey(assetType, assetID), q); err != nil {
		return nil, err
	}
	emit(ctx, EvtQuarantineOn, q)
	return q, nil
}

// ClearQuarantine(assetType, assetId)
// - Allowed when: (a) DRAP closes the recall (preferred), or (b) DRAP overrides (caller is DRAP).
func (c *RecallContract) ClearQuarantine(ctx contractapi.TransactionContextInterface, assetTypeStr, assetID string) (*Quarantine, error) {
	assetType := AssetType(strings.ToUpper(strings.TrimSpace(assetTypeStr)))
	if assetType == "" || strings.TrimSpace(assetID) == "" {
		return nil, errors.New("assetType and assetId are required")
	}
	// Read existing quarantine
	var q Quarantine
	if err := getJSON(ctx, quarantineKey(assetType, assetID), &q); err != nil {
		return nil, err
	}
	// If recall is closed, anyone can clear; otherwise only DRAP can override
	rec, err := c.ReadRecall(ctx, q.RecallID)
	if err != nil {
		return nil, err
	}
	if rec.Status != RecallClosed {
		// need DRAP override
		ok, caller, err := isDRAP(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("recall is ACTIVE; only DRAP can clear quarantine (caller: %s)", caller)
		}
	}

	q.Status = QuarantineOff
	q.UpdatedAt = nowRFC3339(ctx)
	if err := putJSON(ctx, quarantineKey(assetType, assetID), &q); err != nil {
		return nil, err
	}
	emit(ctx, EvtQuarantineOff, q)
	return &q, nil
}

// ---------------- Queries & CC2CC helpers ----------------

// ReadRecall(recallId)
func (c *RecallContract) ReadRecall(ctx contractapi.TransactionContextInterface, recallID string) (*RecallNotice, error) {
	var rec RecallNotice
	if err := getJSON(ctx, recallKey(recallID), &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// IsAssetUnderActiveRecall(assetType, assetId) -> bool
// CC2CC-friendly: returns "true" or "false" as a string in payload when called from another CC.
func (c *RecallContract) IsAssetUnderActiveRecall(ctx contractapi.TransactionContextInterface, assetTypeStr, assetID string) (bool, error) {
	assetType := AssetType(strings.ToUpper(strings.TrimSpace(assetTypeStr)))
	if assetType == "" || strings.TrimSpace(assetID) == "" {
		return false, errors.New("assetType and assetId are required")
	}
	// Find affects for this asset, then confirm the recall is ACTIVE
	selector := map[string]any{
		"selector": map[string]any{
			"docType":   DocAffect,
			"assetType": string(assetType),
			"assetId":   strings.TrimSpace(assetID),
		},
	}
	js, _ := json.Marshal(selector)
	iter, err := ctx.GetStub().GetQueryResult(string(js))
	if err != nil {
		return false, fmt.Errorf("query affects: %w", err)
	}
	defer iter.Close()

	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return false, err
		}
		var a RecallAffect
		if err := json.Unmarshal(kv.Value, &a); err == nil && a.DocType == DocAffect {
			rec, err := c.ReadRecall(ctx, a.RecallID)
			if err != nil {
				return false, err
			}
			if rec.Status == RecallActive {
				return true, nil
			}
		}
	}
	return false, nil
}

// ListActiveRecalls() -> []RecallNotice
func (c *RecallContract) ListActiveRecalls(ctx contractapi.TransactionContextInterface) ([]*RecallNotice, error) {
	selector := map[string]any{
		"selector": map[string]any{"docType": DocRecall, "status": RecallActive},
	}
	js, _ := json.Marshal(selector)
	iter, err := ctx.GetStub().GetQueryResult(string(js))
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*RecallNotice
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		var r RecallNotice
		if err := json.Unmarshal(kv.Value, &r); err == nil && r.DocType == DocRecall {
			out = append(out, &r)
		}
	}
	return out, nil
}

// GetQuarantine(assetType, assetId)
func (c *RecallContract) GetQuarantine(ctx contractapi.TransactionContextInterface, assetTypeStr, assetID string) (*Quarantine, error) {
	var q Quarantine
	if err := getJSON(ctx, quarantineKey(AssetType(strings.ToUpper(strings.TrimSpace(assetTypeStr))), assetID), &q); err != nil {
		return nil, err
	}
	return &q, nil
}

// ListAffectsByAsset(assetType, assetId) -> []RecallAffect
func (c *RecallContract) ListAffectsByAsset(ctx contractapi.TransactionContextInterface, assetTypeStr, assetID string) ([]*RecallAffect, error) {
	selector := map[string]any{
		"selector": map[string]any{
			"docType":   DocAffect,
			"assetType": strings.ToUpper(strings.TrimSpace(assetTypeStr)),
			"assetId":   strings.TrimSpace(assetID),
		},
	}
	js, _ := json.Marshal(selector)
	iter, err := ctx.GetStub().GetQueryResult(string(js))
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*RecallAffect
	for iter.HasNext() {
		kv, err := iter.Next()
		if err != nil {
			return nil, err
		}
		var a RecallAffect
		if err := json.Unmarshal(kv.Value, &a); err == nil && a.DocType == DocAffect {
			out = append(out, &a)
		}
	}
	return out, nil
}

// isAssetAffectedByRecall returns true if (assetType, assetID) is explicitly
// listed as affected by the given recallID.
func (c *RecallContract) isAssetAffectedByRecall(
	ctx contractapi.TransactionContextInterface,
	assetType AssetType,
	assetID string,
	recallID string,
) (bool, error) {

	selector := map[string]any{
		"selector": map[string]any{
			"docType":   DocAffect,
			"assetType": string(assetType),
			"assetId":   strings.TrimSpace(assetID),
			"recallId":  strings.TrimSpace(recallID),
		},
	}

	js, _ := json.Marshal(selector)
	iter, err := ctx.GetStub().GetQueryResult(string(js))
	if err != nil {
		return false, fmt.Errorf("query affects: %w", err)
	}
	defer iter.Close()

	// If we get at least one result, the asset is affected by this recall.
	if iter.HasNext() {
		// consume first row to surface any iterator error
		if _, err := iter.Next(); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// ---------------- main ----------------

func main() {
	cc, err := contractapi.NewChaincode(new(RecallContract))
	if err != nil {
		panic(fmt.Errorf("create chaincode: %w", err))
	}
	if err := cc.Start(); err != nil {
		panic(fmt.Errorf("start chaincode: %w", err))
	}
}

/*
---------------------------------------------
CouchDB Index JSON (place under:
 chaincode/recall-go/META-INF/statedb/couchdb/indexes/)
---------------------------------------------

1) recalls-by-status-createdAt.json
{
  "index": { "fields": ["docType", "status", "createdAt"] },
  "ddoc": "indexRecallsByStatusCreatedAt",
  "name": "indexRecallsByStatusCreatedAt",
  "type": "json"
}

2) affects-by-asset.json
{
  "index": { "fields": ["docType", "assetType", "assetId", "recallId", "createdAt"] },
  "ddoc": "indexAffectsByAsset",
  "name": "indexAffectsByAsset",
  "type": "json"
}

3) quarantine-by-asset.json
{
  "index": { "fields": ["docType", "assetType", "assetId", "status", "updatedAt"] },
  "ddoc": "indexQuarantineByAsset",
  "name": "indexQuarantineByAsset",
  "type": "json"
}
*/
