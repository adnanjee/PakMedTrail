import React, { useEffect, useState } from 'react';
import { closeRecall, createRecall, fetchRecalls } from '../api/recalls';

export default function DrapRecallsPage() {
  const [recalls, setRecalls] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [form, setForm] = useState({
    recallId: '',
    batchId: '',
    reason: '',
  });

  const load = async () => {
    setLoading(true);
    setError('');
    try {
      const data = await fetchRecalls();
      setRecalls(data || []);
    } catch (err) {
      console.error(err);
      setError(err?.response?.data?.error || 'Failed to load recalls');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load();
  }, []);

  const handleCreate = async (e) => {
    e.preventDefault();
    if (!form.recallId || !form.batchId || !form.reason) {
      alert('Fill recallId, batchId, reason');
      return;
    }
    try {
      await createRecall(form);
      setForm({ recallId: '', batchId: '', reason: '' });
      await load();
    } catch (err) {
      console.error(err);
      alert(err?.response?.data?.error || 'Failed to create recall');
    }
  };

  const handleClose = async (id) => {
    const note = prompt('Closure note (optional):', '');
    try {
      await closeRecall(id, note || '');
      await load();
    } catch (err) {
      console.error(err);
      alert(err?.response?.data?.error || 'Failed to close recall');
    }
  };

  return (
    <div className="page">
      <h1>DRAP: Recalls</h1>

      <section className="card mb">
        <h2 className="card-title">Create recall</h2>
        <form className="grid-form" onSubmit={handleCreate}>
          <label className="field">
            <span>Recall ID</span>
            <input
              value={form.recallId}
              onChange={(e) => setForm({ ...form, recallId: e.target.value })}
            />
          </label>
          <label className="field">
            <span>Batch ID</span>
            <input
              value={form.batchId}
              onChange={(e) => setForm({ ...form, batchId: e.target.value })}
            />
          </label>
          <label className="field">
            <span>Reason</span>
            <input
              value={form.reason}
              onChange={(e) => setForm({ ...form, reason: e.target.value })}
            />
          </label>
          <button className="btn btn-primary" type="submit">
            Create recall
          </button>
        </form>
      </section>

      <section className="card">
        <h2 className="card-title">All recalls</h2>
        {error && <div className="error-text">{error}</div>}
        {loading ? (
          <div>Loading...</div>
        ) : recalls.length === 0 ? (
          <div>No recalls yet.</div>
        ) : (
          <table className="table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Batch</th>
                <th>Status</th>
                <th>Reason / Note</th>
                <th>Issuer</th>
                <th>Created</th>
                <th>Updated</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {recalls.map((r) => (
                <tr key={r.recallId}>
                  <td>{r.recallId}</td>
                  <td>{r.batchId}</td>
                  <td>{r.status}</td>
                  <td>{r.reason}</td>
                  <td>{r.issuerMSP}</td>
                  <td>{r.createdAt}</td>
                  <td>{r.updatedAt}</td>
                  <td>
                    {r.status === 'ACTIVE' && (
                      <button
                        className="btn btn-small btn-secondary"
                        onClick={() => handleClose(r.recallId)}
                      >
                        Close
                      </button>
                    )}
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
