import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import { useAuth } from '../auth/AuthContext';

export default function Layout({ children }) {
  const { user, logout } = useAuth();
  const location = useLocation();

  const navLinks = [];

  if (user?.orgMSP === 'supplierMSP') {
    navLinks.push({ to: '/supplier/lots', label: 'Supplier Lots' });
  }
  if (user?.orgMSP === 'manufacturerMSP') {
    navLinks.push({ to: '/manufacturer/shipments', label: 'Manufacturer Shipments' });
  }
  if (user?.orgMSP === 'distributorMSP') {
    navLinks.push({ to: '/distributor/shipments', label: 'Distributor Shipments' });
  }
  if (user?.orgMSP === 'retailerMSP') {
    navLinks.push({ to: '/retailer/shipments', label: 'Retail Shipments' });
  }
  if (user?.orgMSP === 'drapMSP') {
    navLinks.push({ to: '/drap/lots', label: 'DRAP Lots' });
    navLinks.push({ to: '/drap/recalls', label: 'DRAP Recalls' });
  }

  return (
    <div className="app-shell">
      <header className="app-header">
        <div className="app-header-left">
          <span className="app-title">Fabric Pharma Demo</span>
          {navLinks.map((link) => (
            <Link
              key={link.to}
              to={link.to}
              className={
                location.pathname === link.to ? 'nav-link nav-link-active' : 'nav-link'
              }
            >
              {link.label}
            </Link>
          ))}
        </div>
        <div className="app-header-right">
          {user && (
            <>
              <span className="badge">{user.orgMSP}</span>
              <button className="btn btn-ghost" onClick={logout}>
                Logout
              </button>
            </>
          )}
        </div>
      </header>
      <main className="app-main">{children}</main>
    </div>
  );
}
