import React, { useEffect, useState } from 'react';
import { fetchShipmentsForParty, quarantineShipment } from '../api/shipments';
import { fetchBatchRecalls } from '../api/recalls';
import { useAuth } from '../auth/AuthContext';

export default function RetailerShipmentsPage() {
  const { user } = useAuth();
  const [shipments, setShipments] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
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
    shipments.forEach((s) => loadRecallForBatch(s.batchId));
  }, [shipments]);

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
      <h1>Retail: Shipments</h1>
      {error && <div className="error-text">{error}</div>}

      <section className="card">
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
    </div>
  );
}
