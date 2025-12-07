import React, { useEffect, useState } from 'react';
import { approveLot, fetchPendingDrapLots, rejectLot } from '../api/lots';

export default function DrapLotsPage() {
  const [lots, setLots] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const load = async () => {
    setLoading(true);
    setError('');
    try {
      const data = await fetchPendingDrapLots();
      setLots(data || []);
    } catch (err) {
      console.error(err);
      setError(err?.response?.data?.error || 'Failed to load lots');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, []);

  const handleApprove = async (lotId) => {
    const note = prompt('Enter approval note (optional):', '');
    try {
      await approveLot(lotId, note || '');
      await load();
    } catch (err) {
      console.error(err);
      alert(err?.response?.data?.error || 'Failed to approve lot');
    }
  };

  const handleReject = async (lotId) => {
    const reason = prompt('Enter rejection reason:', '');
    if (!reason) return;
    try {
      await rejectLot(lotId, reason);
      await load();
    } catch (err) {
      console.error(err);
      alert(err?.response?.data?.error || 'Failed to reject lot');
    }
  };

  return (
    <div className="page">
      <h1>DRAP: Pending API Lots</h1>
      {error && <div className="error-text">{error}</div>}
      <section className="card">
        {loading ? (
          <div>Loading...</div>
        ) : lots.length === 0 ? (
          <div>No pending lots.</div>
        ) : (
          <table className="table">
            <thead>
              <tr>
                <th>Lot ID</th>
                <th>Name</th>
                <th>Batch</th>
                <th>Qty</th>
                <th>Supplier</th>
                <th>Created</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {lots.map((lot) => (
                <tr key={lot.lotId}>
                  <td>{lot.lotId}</td>
                  <td>{lot.name}</td>
                  <td>{lot.batchNumber}</td>
                  <td>
                    {lot.quantity} {lot.unit}
                  </td>
                  <td>{lot.ownerMSP}</td>
                  <td>{lot.createdAt}</td>
                  <td>
                    <button
                      className="btn btn-small"
                      onClick={() => handleApprove(lot.lotId)}
                    >
                      Approve
                    </button>
                    <button
                      className="btn btn-small btn-secondary"
                      onClick={() => handleReject(lot.lotId)}
                    >
                      Reject
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
