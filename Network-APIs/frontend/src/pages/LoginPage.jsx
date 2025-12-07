import React, { useState } from 'react';
import { useAuth } from '../auth/AuthContext';

const ORGS = [
  { value: 'supplierMSP', label: 'Supplier' },
  { value: 'manufacturerMSP', label: 'Manufacturer' },
  { value: 'distributorMSP', label: 'Distributor' },
  { value: 'retailerMSP', label: 'Retailer' },
  { value: 'drapMSP', label: 'DRAP' },
];

export default function LoginPage() {
  const { login } = useAuth();
  const [username, setUsername] = useState('alice');
  const [orgMSP, setOrgMSP] = useState('supplierMSP');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      await login(username, orgMSP);
    } catch (err) {
      console.error(err);
      setError(err?.response?.data?.error || 'Login failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="login-wrapper">
      <form className="card" onSubmit={handleSubmit}>
        <h1 className="card-title">Sign in</h1>
        <label className="field">
          <span>Username</span>
          <input
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="alice"
          />
        </label>
        <label className="field">
          <span>Organization (MSP)</span>
          <select value={orgMSP} onChange={(e) => setOrgMSP(e.target.value)}>
            {ORGS.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label} ({o.value})
              </option>
            ))}
          </select>
        </label>
        {error && <div className="error-text">{error}</div>}
        <button className="btn btn-primary" type="submit" disabled={loading}>
          {loading ? 'Signing in...' : 'Sign in'}
        </button>
        <p className="hint">
          Demo login only – issues a JWT for the selected org, no real password checks.
        </p>
      </form>
    </div>
  );
}
