import React, { useEffect, useState } from 'react';
import { createLot, fetchMyLots, proposeTransfer } from '../api/lots';

export default function SupplierLotsPage() {
  const [lots, setLots] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [form, setForm] = useState({
    lotId: '',
    name: '',
    batchNumber: '',
    quantity: '1000',
    unit: 'kg',
    manufactureDate: '',
    expiryDate: '',
  });

  const load = async () => {
    setLoading(true);
    setError('');
    try {
      const data = await fetchMyLots();
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

  const handleCreate = async (e) => {
    e.preventDefault();
    setError('');
    try {
      const payload = {
        ...form,
        quantity: Number(form.quantity),
        metadata: {},
      };
      await createLot(payload);
      setForm({
        lotId: '',
        name: '',
        batchNumber: '',
        quantity: '1000',
        unit: 'kg',
        manufactureDate: '',
        expiryDate: '',
      });
      await load();
    } catch (err) {
      console.error(err);
      setError(err?.response?.data?.error || 'Failed to create lot');
    }
  };

  const handleProposeTransfer = async (lotId) => {
    const to = prompt('Enter proposed owner MSP (e.g. manufacturerMSP):');
    if (!to) return;
    try {
      await proposeTransfer(lotId, to);
      await load();
    } catch (err) {
      console.error(err);
      alert(err?.response?.data?.error || 'Failed to propose transfer');
    }
  };

  return (
    <div className="page">
      <h1>Supplier: API Lots</h1>

      <section className="card mb">
        <h2 className="card-title">Create new API lot</h2>
        <form className="grid-form" onSubmit={handleCreate}>
          <label className="field">
            <span>Lot ID</span>
            <input
              value={form.lotId}
              onChange={(e) => setForm({ ...form, lotId: e.target.value })}
            />
          </label>
          <label className="field">
            <span>Name</span>
            <input
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
            />
          </label>
          <label className="field">
            <span>Batch number</span>
            <input
              value={form.batchNumber}
              onChange={(e) => setForm({ ...form, batchNumber: e.target.value })}
            />
          </label>
          <label className="field">
            <span>Quantity</span>
            <input
              type="number"
              value={form.quantity}
              onChange={(e) => setForm({ ...form, quantity: e.target.value })}
            />
          </label>
          <label className="field">
            <span>Unit</span>
            <input
              value={form.unit}
              onChange={(e) => setForm({ ...form, unit: e.target.value })}
            />
          </label>
          <label className="field">
            <span>Manufacture date (YYYY-MM-DD)</span>
            <input
              value={form.manufactureDate}
              onChange={(e) => setForm({ ...form, manufactureDate: e.target.value })}
              placeholder="2025-01-01"
            />
          </label>
          <label className="field">
            <span>Expiry date (YYYY-MM-DD)</span>
            <input
              value={form.expiryDate}
              onChange={(e) => setForm({ ...form, expiryDate: e.target.value })}
              placeholder="2027-01-01"
            />
          </label>
          <button className="btn btn-primary" type="submit">
            Create lot
          </button>
        </form>
        {error && <div className="error-text">{error}</div>}
      </section>

      <section className="card">
        <h2 className="card-title">My lots</h2>
        {loading ? (
          <div>Loading...</div>
        ) : lots.length === 0 ? (
          <div>No lots yet.</div>
        ) : (
          <table className="table">
            <thead>
              <tr>
                <th>Lot ID</th>
                <th>Name</th>
                <th>Qty</th>
                <th>Status</th>
                <th>DRAP</th>
                <th>Owner</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {lots.map((lot) => (
                <tr key={lot.lotId}>
                  <td>{lot.lotId}</td>
                  <td>{lot.name}</td>
                  <td>
                    {lot.quantity} {lot.unit}
                  </td>
                  <td>{lot.status}</td>
                  <td>{lot.drapApproved ? 'Approved' : 'Pending'}</td>
                  <td>{lot.ownerMSP}</td>
                  <td>
                    <button
                      className="btn btn-small"
                      onClick={() => handleProposeTransfer(lot.lotId)}
                      disabled={!lot.drapApproved}
                    >
                      Propose transfer
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
