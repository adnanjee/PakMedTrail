import api from './client';

export function fetchMyLots(params = {}) {
  return api.get('/api/lots', { params }).then((res) => res.data);
}

export function fetchLot(lotId) {
  return api.get(`/api/lots/${lotId}`).then((res) => res.data);
}

export function createLot(payload) {
  return api.post('/api/lots', payload).then((res) => res.data);
}

export function approveLot(lotId, note) {
  return api.post(`/api/lots/${lotId}/drap/approve`, { note }).then((res) => res.data);
}

export function rejectLot(lotId, reason) {
  return api.post(`/api/lots/${lotId}/drap/reject`, { reason }).then((res) => res.data);
}

export function fetchPendingDrapLots() {
  return api.get('/api/lots/pending-drap').then((res) => res.data);
}

export function proposeTransfer(lotId, proposedOwnerMSP) {
  return api
    .post(`/api/lots/${lotId}/propose-transfer`, { proposedOwnerMSP })
    .then((res) => res.data);
}
