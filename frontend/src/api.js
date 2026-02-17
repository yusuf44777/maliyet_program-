import axios from 'axios';

const RAW_API_BASE = (import.meta.env.VITE_API_URL || '').trim();
const API_BASE = RAW_API_BASE ? RAW_API_BASE.replace(/\/+$/, '') : '/api';

const api = axios.create({
  baseURL: API_BASE,
  timeout: 30000,
});

const AUTH_TOKEN_KEY = 'maliyet_auth_token';

export const getAuthToken = () => localStorage.getItem(AUTH_TOKEN_KEY) || '';
export const setAuthToken = (token) => {
  if (token) localStorage.setItem(AUTH_TOKEN_KEY, token);
};
export const clearAuthToken = () => {
  localStorage.removeItem(AUTH_TOKEN_KEY);
};

api.interceptors.request.use((config) => {
  const token = getAuthToken();
  if (token) {
    config.headers = config.headers || {};
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

api.interceptors.response.use(
  (response) => response,
  (error) => {
    const status = error?.response?.status;
    const url = error?.config?.url || '';
    if (status === 401 && !url.includes('/auth/login')) {
      clearAuthToken();
      window.dispatchEvent(new CustomEvent('auth:unauthorized'));
    }
    return Promise.reject(error);
  },
);

// ─── Auth ───
export const loginAuth = (data) => api.post('/auth/login', data).then(r => r.data);
export const getMe = () => api.get('/auth/me').then(r => r.data);
export const changePassword = (data) => api.post('/auth/change-password', data).then(r => r.data);
export const getUsers = () => api.get('/auth/users').then(r => r.data);
export const createUser = (data) => api.post('/auth/users', data).then(r => r.data);
export const updateUser = (id, data) => api.put(`/auth/users/${id}`, data).then(r => r.data);
export const deleteUser = (id) => api.delete(`/auth/users/${id}`).then(r => r.data);
export const getAuditLogs = (params) => api.get('/auth/audit-logs', { params }).then(r => r.data);

// ─── Stats ───
export const getStats = () => api.get('/stats').then(r => r.data);

// ─── Products ───
export const getProducts = (params) => api.get('/products', { params }).then(r => r.data);
export const getProduct = (sku) => api.get(`/products/${encodeURIComponent(sku)}`).then(r => r.data);
export const getProductGroups = (kategori) =>
  api.get('/product-groups', { params: kategori ? { kategori } : {} }).then(r => r.data);

// ─── Materials ───
export const getMaterials = () => api.get('/materials').then(r => r.data);
export const createMaterial = (data) => api.post('/materials', data).then(r => r.data);
export const updateMaterial = (id, data) => api.put(`/materials/${id}`, data).then(r => r.data);
export const deleteMaterial = (id) => api.delete(`/materials/${id}`).then(r => r.data);

// ─── Product Materials ───
export const setProductMaterial = (data) => api.post('/product-materials', data).then(r => r.data);
export const setProductMaterialBulk = (data) => api.post('/product-materials/bulk', data).then(r => r.data);
export const getProductMaterials = (sku) =>
  api.get(`/product-materials/${encodeURIComponent(sku)}`).then(r => r.data);

// ─── Costs ───
export const getCostNames = () => api.get('/cost-names').then(r => r.data);
export const getCostDefinitions = (params) => api.get('/cost-definitions', { params }).then(r => r.data);
export const createCostDefinition = (data) => api.post('/cost-definitions', data).then(r => r.data);
export const updateCostDefinition = (id, data) => api.put(`/cost-definitions/${id}`, data).then(r => r.data);
export const deleteCostDefinition = (id) => api.delete(`/cost-definitions/${id}`).then(r => r.data);
export const getKargoOptions = () => api.get('/kargo-options').then(r => r.data);
export const getKaplamaSuggestions = (parentName) =>
  api.get('/kaplama-suggestions', { params: { parent_name: parentName } }).then(r => r.data);
export const getKaplamaNameSuggestions = (parentName) =>
  api.get('/kaplama-name-suggestions', { params: { parent_name: parentName } }).then(r => r.data);
export const setProductCost = (data) => api.post('/product-costs', data).then(r => r.data);

// ─── Inheritance ───
export const applyInheritance = (data) => api.post('/inherit', data).then(r => r.data);

// ─── Export ───
export const exportExcel = async (skus) => {
  const response = await api.post('/export', {
    child_skus: skus,
    include_materials: true,
    include_costs: true,
  }, { responseType: 'blob' });

  const url = window.URL.createObjectURL(new Blob([response.data]));
  const link = document.createElement('a');
  link.href = url;
  link.setAttribute('download', `maliyet_export_${Date.now()}.xlsx`);
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.URL.revokeObjectURL(url);
};

// ─── Template ───
export const getTemplateStructure = () => api.get('/template-structure').then(r => r.data);

// ─── DB Management ───
export const reloadDB = () => api.post('/reload-db').then(r => r.data);

export default api;
