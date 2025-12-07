import api from './client';

export function fetchRecalls(params = {}) {
  return api.get('/api/recalls', { params }).then((res) => res.data);
}

export function createRecall(payload) {
  return api.post('/api/recalls', payload).then((res) => res.data);
}

export function closeRecall(recallId, note) {
  return api.post(`/api/recalls/${recallId}/close`, { note }).then((res) => res.data);
}

export function fetchBatchRecalls(batchId, activeOnly = true) {
  const params = { activeOnly };
  return api.get(`/api/recalls/batch/${batchId}`, { params }).then((res) => res.data);
}
