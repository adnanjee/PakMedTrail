import api from './client';

export function fetchShipmentsForParty(partyMSP) {
  const params = {};
  if (partyMSP) params.partyMSP = partyMSP;
  return api.get('/api/shipments', { params }).then((res) => res.data);
}

export function fetchShipment(id) {
  return api.get(`/api/shipments/${id}`).then((res) => res.data);
}

export function createShipment(payload) {
  return api.post('/api/shipments', payload).then((res) => res.data);
}

export function acceptShipment(id) {
  return api.post(`/api/shipments/${id}/accept`).then((res) => res.data);
}

export function rejectShipment(id, reason) {
  return api.post(`/api/shipments/${id}/reject`, { reason }).then((res) => res.data);
}

export function cancelShipment(id, reason) {
  return api.post(`/api/shipments/${id}/cancel`, { reason }).then((res) => res.data);
}

export function markShipmentDelivered(id) {
  return api.post(`/api/shipments/${id}/delivered`).then((res) => res.data);
}

export function getShipmentTerms(id) {
  return api.get(`/api/shipments/${id}/terms`).then((res) => res.data);
}

export function putShipmentTerms(id, terms) {
  return api.put(`/api/shipments/${id}/terms`, terms).then((res) => res.data);
}

export function linkShipmentTermsHash(id) {
  return api.post(`/api/shipments/${id}/terms/hash`).then((res) => res.data);
}

export function quarantineShipment(id) {
  return api.post(`/api/shipments/${id}/quarantine`).then((res) => res.data);
}
