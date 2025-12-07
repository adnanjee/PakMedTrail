import React, { useEffect, useState } from 'react';
import {
  fetchShipmentsForParty,
  acceptShipment,
  rejectShipment,
  markShipmentDelivered,
  getShipmentTerms,
  putShipmentTerms,
  linkShipmentTermsHash,
  quarantineShipment,
} from '../api/shipments';
import { fetchBatchRecalls } from '../api/recalls';
import { useAuth } from '../auth/AuthContext';

export default function DistributorShipmentsPage() {
  const { user } = useAuth();
  const [shipments, setShipments] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [selectedShipment, setSelectedShipment] = useState(null);
  const [terms, setTerms] = useState({
    priceAmt: '',
    currency: 'USD',
    discount: '',
    incoterms: 'CIF',
    notes: '',
  });
  const [termsLoading, setTermsLoading] = useState(false);
  const [recallsForBatch, setRecallsForBatch] = useState({});

  const loadShipments = async () => {
    setLoading(true);
    setError('');
    try {
      const data = await fetchShipmentsForParty(user.orgMSP);
      setShipments(data || []);
    } catch (err) {
      console.error(err);
      setError(err?.response?.data?.error || 'Failed to load shipments');
    } finally {
      setLoading(false);
    }
  };

  const loadRecallForBatch = async (batchId) => {
    if (!batchId || recallsForBatch[batchId] !== undefined) return;
    try {
      const recs = await fetchBatchRecalls(batchId, true);
      setRecallsForBatch((prev) => ({ ...prev, [batchId]: recs.length }));
    } catch (err) {
      console.error('recall fetch error', err);
    }
  };

  useEffect(() => {
    loadShipments();
  }, []);

  useEffect(() => {
    shipments.forEach((s) => {
      loadRecallForBatch(s.batchId);
    });
  }, [shipments]);

  const handleAccept = async (id) => {
    try {
      await acceptShipment(id);
      await loadShipments();
    } catch (err) {
      console.error(err);
      alert(err?.response?.data?.error || 'Failed to accept shipment');
    }
  };

  const handleReject = async (id) => {
    const reason = prompt('Reason for rejection:', '');
    if (!reason) return;
    try {
      await rejectShipment(id, reason);
      await loadShipments();
    } catch (err) {
      console.error(err);
      alert(err?.response?.data?.error || 'Failed to reject shipment');
    }
  };

  const handleDelivered = async (id) => {
    try {
      await markShipmentDelivered(id);
      await loadShipments();
    } catch (err) {
      console.error(err);
      alert(err?.response?.data?.error || 'Failed to mark delivered');
    }
  };

  const openTerms = async (shipment) => {
    setSelectedShipment(shipment);
    setTermsLoading(true);
    try {
      const existing = await getShipmentTerms(shipment.shipmentId);
      if (existing && Object.keys(existing).length > 0) {
        setTerms({
          priceAmt: existing.priceAmt ?? '',
          currency: existing.currency || 'USD',
          discount: existing.discount ?? '',
          incoterms: existing.incoterms || '',
          notes: existing.notes || '',
        });
      } else {
        setTerms({
          priceAmt: '',
          currency: 'USD',
          discount: '',
          incoterms: 'CIF',
          notes: '',
        });
      }
    } catch (err) {
      console.error(err);
    } finally {
      setTermsLoading(false);
    }
  };

  const handleSaveTerms = async () => {
    if (!selectedShipment) return;
    try {
      await putShipmentTerms(selectedShipment.shipmentId, {
        ...terms,
      });
      alert('Terms saved to PDC');
    } catch (err) {
      console.error(err);
      alert(err?.response?.data?.error || 'Failed to save terms');
    }
  };

  const handleLinkHash = async () => {
    if (!selectedShipment) return;
    try {
      await linkShipmentTermsHash(selectedShipment.shipmentId);
      alert('Sensitive hash linked into public shipment metadata');
      await loadShipments();
    } catch (err) {
      console.error(err);
      alert(err?.response?.data?.error || 'Failed to link hash');
    }
  };

  const handleQuarantine = async (id) => {
    try {
      await quarantineShipment(id);
      await loadShipments();
    } catch (err) {
      console.error(err);
      alert(err?.response?.data?.error || 'Failed to quarantine shipment');
    }
  };

  return (
    <div className="page">
      <h1>Distributor: Shipments</h1>
      {error && <div className="error-text">{error}</div>}

      <section className="card">
        <h2 className="card-title">Shipments involving my org</h2>
        {loading ? (
          <div>Loading...</div>
        ) : shipments.length === 0 ? (
          <div>No shipments yet.</div>
        ) : (
          <table className="table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Batch</th>
                <th>From</th>
                <th>To</th>
                <th>Qty</th>
                <th>Status</th>
                <th>Recall</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {shipments.map((s) => (
                <tr key={s.shipmentId}>
                  <td>{s.shipmentId}</td>
                  <td>{s.batchId}</td>
                  <td>{s.fromMSP}</td>
                  <td>{s.toMSP}</td>
                  <td>{s.quantity}</td>
                  <td>{s.status}</td>
                  <td>
                    {recallsForBatch[s.batchId] > 0 ? (
                      <span className="badge badge-warn">Active recall</span>
                    ) : (
                      <span className="badge badge-ok">None</span>
                    )}
                  </td>
                  <td>
                    {s.status === 'PENDING' && s.toMSP === user.orgMSP && (
                      <>
                        <button
                          className="btn btn-small"
                          onClick={() => handleAccept(s.shipmentId)}
                        >
                          Accept
                        </button>
                        <button
                          className="btn btn-small btn-secondary"
                          onClick={() => handleReject(s.shipmentId)}
                        >
                          Reject
                        </button>
                      </>
                    )}
                    {s.status === 'ACCEPTED' && s.toMSP === user.orgMSP && (
                      <button
                        className="btn btn-small"
                        onClick={() => handleDelivered(s.shipmentId)}
                      >
                        Mark delivered
                      </button>
                    )}
                    <button
                      className="btn btn-small"
                      onClick={() => openTerms(s)}
                    >
                      Terms
                    </button>
                    <button
                      className="btn btn-small"
                      onClick={() => handleQuarantine(s.shipmentId)}
                    >
                      Quarantine
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      {selectedShipment && (
        <section className="card mt">
          <h2 className="card-title">
            Terms for shipment {selectedShipment.shipmentId}
          </h2>
          {termsLoading ? (
            <div>Loading terms...</div>
          ) : (
            <div className="grid-form">
              <label className="field">
                <span>Price amount</span>
                <input
                  type="number"
                  value={terms.priceAmt}
                  onChange={(e) => setTerms({ ...terms, priceAmt: e.target.value })}
                  placeholder="e.g. 12.34"
                />
              </label>
              <label className="field">
                <span>Currency</span>
                <input
                  value={terms.currency}
                  onChange={(e) => setTerms({ ...terms, currency: e.target.value })}
                />
              </label>
              <label className="field">
                <span>Discount (%)</span>
                <input
                  type="number"
                  value={terms.discount}
                  onChange={(e) => setTerms({ ...terms, discount: e.target.value })}
                />
              </label>
              <label className="field">
                <span>Incoterms</span>
                <input
                  value={terms.incoterms}
                  onChange={(e) => setTerms({ ...terms, incoterms: e.target.value })}
                />
              </label>
              <label className="field">
                <span>Notes</span>
                <input
                  value={terms.notes}
                  onChange={(e) => setTerms({ ...terms, notes: e.target.value })}
                />
              </label>
              <div className="field">
                <span>&nbsp;</span>
                <div>
                  <button className="btn btn-primary" type="button" onClick={handleSaveTerms}>
                    Save terms
                  </button>
                  <button className="btn btn-small" type="button" onClick={handleLinkHash}>
                    Link hash
                  </button>
                </div>
              </div>
            </div>
          )}
        </section>
      )}
    </div>
  );
}
