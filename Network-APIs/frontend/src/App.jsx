import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider, useAuth } from './auth/AuthContext';
import LoginPage from './pages/LoginPage';
import SupplierLotsPage from './pages/SupplierLotsPage';
import DrapLotsPage from './pages/DrapLotsPage';
import ManufacturerShipmentsPage from './pages/ManufacturerShipmentsPage';
import DistributorShipmentsPage from './pages/DistributorShipmentsPage';
import RetailerShipmentsPage from './pages/RetailerShipmentsPage';
import DrapRecallsPage from './pages/DrapRecallsPage';
import Layout from './components/Layout';

function AuthedApp() {
  const { user } = useAuth();

  if (!user) {
    return <LoginPage />;
  }

  return (
    <Layout>
      <Routes>
        <Route
          path="/"
          element={<Navigate to="/dashboard" />}
        />
        <Route
          path="/dashboard"
          element={
            <div className="page">
              <h1>Dashboard</h1>
              <p>
                Welcome, {user.sub}. You are logged in as <strong>{user.orgMSP}</strong>.
              </p>
            </div>
          }
        />
        {user.orgMSP === 'supplierMSP' && (
          <Route path="/supplier/lots" element={<SupplierLotsPage />} />
        )}
        {user.orgMSP === 'manufacturerMSP' && (
          <Route path="/manufacturer/shipments" element={<ManufacturerShipmentsPage />} />
        )}
        {user.orgMSP === 'distributorMSP' && (
          <Route path="/distributor/shipments" element={<DistributorShipmentsPage />} />
        )}
        {user.orgMSP === 'retailerMSP' && (
          <Route path="/retailer/shipments" element={<RetailerShipmentsPage />} />
        )}
        {user.orgMSP === 'drapMSP' && (
          <>
            <Route path="/drap/lots" element={<DrapLotsPage />} />
            <Route path="/drap/recalls" element={<DrapRecallsPage />} />
          </>
        )}
        <Route path="*" element={<Navigate to="/dashboard" />} />
      </Routes>
    </Layout>
  );
}

export default function App() {
  return (
    <AuthProvider>
      <AuthedApp />
    </AuthProvider>
  );
}
