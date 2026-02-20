import { useState, useEffect, useCallback, useRef } from 'react';
import { getProducts, getProductGroups, exportExcel } from '../api';
import { CATEGORY_OPTIONS, getCategoryBadgeClass } from '../categoryUtils';
import toast from 'react-hot-toast';
import {
  Search, ChevronLeft, ChevronRight, Filter, Download,
  ArrowUpDown, Eye, Package,
} from 'lucide-react';
import HelpTip from './HelpTip';

const PAGE_SIZE_OPTIONS = [25, 50, 100];

export default function ProductBrowser({ onSelectProduct, onRefresh }) {
  const [products, setProducts] = useState([]);
  const [groups, setGroups] = useState([]);
  const [viewMode, setViewMode] = useState('table'); // 'table' | 'groups'
  const [loading, setLoading] = useState(false);
  const [groupsLoading, setGroupsLoading] = useState(false);
  const [pagination, setPagination] = useState({ page: 1, total: 0, total_pages: 0, page_size: 50 });
  const [groupPagination, setGroupPagination] = useState({ page: 1, total: 0, total_pages: 0, page_size: 50 });

  // Filtreler
  const [filters, setFilters] = useState({
    kategori: '',
    search: '',
    has_dims: '',
    product_identifier: '',
  });

  const [selectedSkus, setSelectedSkus] = useState(new Set());
  const [debouncedSearch, setDebouncedSearch] = useState('');
  const productsAbortRef = useRef(null);
  const groupsAbortRef = useRef(null);

  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearch(filters.search.trim());
    }, 350);
    return () => clearTimeout(timer);
  }, [filters.search]);

  useEffect(() => {
    return () => {
      if (productsAbortRef.current) productsAbortRef.current.abort();
      if (groupsAbortRef.current) groupsAbortRef.current.abort();
    };
  }, []);

  const fetchProducts = useCallback(async () => {
    setLoading(true);
    if (productsAbortRef.current) productsAbortRef.current.abort();
    const controller = new AbortController();
    productsAbortRef.current = controller;
    try {
      const params = {
        ...filters,
        search: debouncedSearch,
        page: pagination.page,
        page_size: pagination.page_size,
      };
      // Temizle boş parametreleri
      Object.keys(params).forEach(k => {
        if (params[k] === '' || params[k] === null) delete params[k];
      });
      if (params.has_dims === 'true') params.has_dims = true;
      else if (params.has_dims === 'false') params.has_dims = false;
      else delete params.has_dims;

      const data = await getProducts(params, { signal: controller.signal });
      if (controller.signal.aborted) return;
      setProducts(data.products);
      setPagination(prev => ({
        ...prev,
        total: data.total,
        total_pages: data.total_pages,
        page_size: data.page_size || prev.page_size,
      }));
    } catch (err) {
      if (err?.code === 'ERR_CANCELED' || err?.name === 'CanceledError') return;
      toast.error('Ürünler yüklenemedi');
    } finally {
      if (productsAbortRef.current === controller) {
        productsAbortRef.current = null;
        setLoading(false);
      }
    }
  }, [
    filters.kategori,
    filters.has_dims,
    filters.product_identifier,
    debouncedSearch,
    pagination.page,
    pagination.page_size,
  ]);

  const fetchGroups = useCallback(async () => {
    setGroupsLoading(true);
    if (groupsAbortRef.current) groupsAbortRef.current.abort();
    const controller = new AbortController();
    groupsAbortRef.current = controller;
    try {
      const params = {
        page: groupPagination.page,
        page_size: groupPagination.page_size,
      };
      if (filters.kategori) params.kategori = filters.kategori;
      if (debouncedSearch) params.search = debouncedSearch;

      const data = await getProductGroups(params, { signal: controller.signal });
      if (controller.signal.aborted) return;
      if (Array.isArray(data)) {
        setGroups(data);
        setGroupPagination(prev => ({
          ...prev,
          total: data.length,
          total_pages: Math.max(1, Math.ceil(data.length / prev.page_size)),
        }));
      } else {
        setGroups(data.groups || []);
        setGroupPagination(prev => ({
          ...prev,
          total: data.total || 0,
          total_pages: data.total_pages || 0,
          page_size: data.page_size || prev.page_size,
        }));
      }
    } catch (err) {
      if (err?.code === 'ERR_CANCELED' || err?.name === 'CanceledError') return;
      toast.error('Gruplar yüklenemedi');
    } finally {
      if (groupsAbortRef.current === controller) {
        groupsAbortRef.current = null;
        setGroupsLoading(false);
      }
    }
  }, [filters.kategori, debouncedSearch, groupPagination.page, groupPagination.page_size]);

  useEffect(() => {
    fetchProducts();
  }, [fetchProducts]);

  useEffect(() => {
    if (viewMode === 'groups') fetchGroups();
  }, [viewMode, fetchGroups]);

  const handleExportSelected = async () => {
    if (selectedSkus.size === 0) {
      toast.error('Lütfen en az bir ürün seçin');
      return;
    }
    try {
      await exportExcel([...selectedSkus]);
      toast.success(`${selectedSkus.size} ürün export edildi`);
    } catch (err) {
      toast.error('Export hatası');
    }
  };

  const toggleSelect = (sku) => {
    setSelectedSkus(prev => {
      const next = new Set(prev);
      if (next.has(sku)) next.delete(sku);
      else next.add(sku);
      return next;
    });
  };

  const selectAll = () => {
    if (selectedSkus.size === products.length) {
      setSelectedSkus(new Set());
    } else {
      setSelectedSkus(new Set(products.map(p => p.child_sku)));
    }
  };

  return (
    <div className="space-y-4">
      {/* Filtreler */}
      <div className="bg-white rounded-xl border border-gray-200 p-4 shadow-sm">
        <div className="flex items-center gap-2 mb-3">
          <span className="text-sm font-semibold text-gray-800">Filtreler ve Liste</span>
          <HelpTip
            title="Nasıl kullanılır?"
            text="Önce arama ve kategori filtresi verin. Sonra Tablo/Gruplar görünümü arasında geçip doğru kayıtları bulun."
          />
        </div>
        <div className="flex flex-wrap gap-3 items-end">
          {/* Arama */}
          <div className="flex-1 min-w-[200px]">
            <label className="text-xs text-gray-500 mb-1 block">Arama</label>
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
              <input
                type="text"
                value={filters.search}
                onChange={e => {
                  setFilters(f => ({ ...f, search: e.target.value }));
                  setPagination(p => ({ ...p, page: 1 }));
                  setGroupPagination(p => ({ ...p, page: 1 }));
                }}
                placeholder="SKU, isim veya kod..."
                className="w-full pl-9 pr-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
          </div>

          {/* Kategori */}
          <div>
            <label className="text-xs text-gray-500 mb-1 block">Kategori</label>
            <select
              value={filters.kategori}
              onChange={e => {
                setFilters(f => ({ ...f, kategori: e.target.value }));
                setPagination(p => ({ ...p, page: 1 }));
                setGroupPagination(p => ({ ...p, page: 1 }));
              }}
              className="px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="">Tümü</option>
              {CATEGORY_OPTIONS.map((cat) => (
                <option key={cat.value} value={cat.value}>{cat.label}</option>
              ))}
            </select>
          </div>

          {/* Boyut filtresi */}
          <div>
            <label className="text-xs text-gray-500 mb-1 block">Boyut</label>
            <select
              value={filters.has_dims}
              onChange={e => {
                setFilters(f => ({ ...f, has_dims: e.target.value }));
                setPagination(p => ({ ...p, page: 1 }));
              }}
              className="px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="">Tümü</option>
              <option value="true">Boyutlu</option>
              <option value="false">Boyutsuz</option>
            </select>
          </div>

          <div>
            <label className="text-xs text-gray-500 mb-1 block">Satır / Sayfa</label>
            <select
              value={viewMode === 'groups' ? groupPagination.page_size : pagination.page_size}
              onChange={e => {
                const nextSize = Number(e.target.value) || 50;
                if (viewMode === 'groups') {
                  setGroupPagination(p => ({ ...p, page: 1, page_size: nextSize }));
                } else {
                  setPagination(p => ({ ...p, page: 1, page_size: nextSize }));
                }
              }}
              className="px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {PAGE_SIZE_OPTIONS.map(size => (
                <option key={size} value={size}>{size}</option>
              ))}
            </select>
          </div>

          {/* Görünüm */}
          <div className="flex gap-1">
            <button
              onClick={() => setViewMode('table')}
              className={`px-3 py-2 rounded-lg text-sm ${viewMode === 'table' ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-600'}`}
            >
              Tablo
            </button>
            <button
              onClick={() => setViewMode('groups')}
              className={`px-3 py-2 rounded-lg text-sm ${viewMode === 'groups' ? 'bg-blue-100 text-blue-700' : 'bg-gray-100 text-gray-600'}`}
            >
              Gruplar
            </button>
            <HelpTip
              title="Tablo vs Gruplar"
              text="Tablo tek tek ürünleri gösterir. Gruplar görünümü aynı parent altında kaç varyant olduğunu özetler."
              placement="bottom"
            />
          </div>

          {/* Export */}
          {selectedSkus.size > 0 && (
            <button
              onClick={handleExportSelected}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 text-sm"
            >
              <Download className="w-4 h-4" />
              {selectedSkus.size} ürün export
            </button>
          )}
        </div>
      </div>

      {/* Grup Görünümü */}
      {viewMode === 'groups' && (
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm">
          <div className="table-container">
            <table>
              <thead>
                <tr>
                  <th>Ürün Kodu</th>
                  <th>Parent İsim</th>
                  <th>Kategori</th>
                  <th>Varyant Sayısı</th>
                  <th>En (min-max)</th>
                  <th>Boy (min-max)</th>
                  <th>Alan m² (min-max)</th>
                </tr>
              </thead>
              <tbody>
                {groupsLoading ? (
                  <tr>
                    <td colSpan={7} className="text-center py-8 text-gray-400">
                      Yükleniyor...
                    </td>
                  </tr>
                ) : groups.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="text-center py-8 text-gray-400">
                      Grup bulunamadı
                    </td>
                  </tr>
                ) : (
                  groups.map((g, i) => (
                    <tr key={i} className="cursor-pointer" onClick={() => {
                      setFilters(f => ({ ...f, product_identifier: g.product_identifier }));
                      setViewMode('table');
                      setPagination(p => ({ ...p, page: 1 }));
                    }}>
                      <td className="font-mono font-medium">{g.product_identifier}</td>
                      <td className="max-w-xs truncate">{g.parent_name}</td>
                      <td>
                        <span className={`badge ${getCategoryBadgeClass(g.kategori)}`}>
                          {g.kategori}
                        </span>
                      </td>
                      <td className="text-center">{g.variant_count}</td>
                      <td>{g.min_en ?? '—'}{g.min_en !== g.max_en ? ` – ${g.max_en}` : ''}</td>
                      <td>{g.min_boy ?? '—'}{g.min_boy !== g.max_boy ? ` – ${g.max_boy}` : ''}</td>
                      <td>
                        <span className="alan-value">
                          {g.min_alan != null ? g.min_alan.toFixed(4) : '—'}
                          {g.min_alan !== g.max_alan ? ` – ${g.max_alan?.toFixed(4)}` : ''}
                        </span>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
          <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100">
            <span className="text-sm text-gray-500">
              Toplam {groupPagination.total.toLocaleString()} grup
            </span>
            <div className="flex items-center gap-2">
              <button
                onClick={() => setGroupPagination(p => ({ ...p, page: Math.max(1, p.page - 1) }))}
                disabled={groupPagination.page <= 1}
                className="p-1 rounded hover:bg-gray-100 disabled:opacity-30"
              >
                <ChevronLeft className="w-5 h-5" />
              </button>
              <span className="text-sm text-gray-600">
                {groupPagination.page} / {Math.max(1, groupPagination.total_pages || 0)}
              </span>
              <button
                onClick={() => setGroupPagination(p => ({ ...p, page: Math.min(Math.max(1, p.total_pages || 1), p.page + 1) }))}
                disabled={groupPagination.page >= Math.max(1, groupPagination.total_pages || 1)}
                className="p-1 rounded hover:bg-gray-100 disabled:opacity-30"
              >
                <ChevronRight className="w-5 h-5" />
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Tablo Görünümü */}
      {viewMode === 'table' && (
        <>
          {/* Aktif filtre badge */}
          {filters.product_identifier && (
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-500">Filtre:</span>
              <span className="badge bg-blue-100 text-blue-700">
                {filters.product_identifier}
                <button
                  className="ml-1 text-blue-400 hover:text-blue-600"
                  onClick={() => setFilters(f => ({ ...f, product_identifier: '' }))}
                >×</button>
              </span>
            </div>
          )}

          <div className="bg-white rounded-xl border border-gray-200 shadow-sm">
            <div className="table-container">
              <table>
                <thead>
                  <tr>
                    <th className="w-8">
                      <input
                        type="checkbox"
                        checked={selectedSkus.size === products.length && products.length > 0}
                        onChange={selectAll}
                        className="rounded"
                      />
                    </th>
                    <th>SKU</th>
                    <th>Ürün Adı</th>
                    <th>Kategori</th>
                    <th>Kod</th>
                    <th>En (cm)</th>
                    <th>Boy (cm)</th>
                    <th className="text-blue-700">Alan (m²)</th>
                    <th>Boyut</th>
                    <th>Renk</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {loading ? (
                    <tr>
                      <td colSpan={11} className="text-center py-8 text-gray-400">
                        Yükleniyor...
                      </td>
                    </tr>
                  ) : products.length === 0 ? (
                    <tr>
                      <td colSpan={11} className="text-center py-8 text-gray-400">
                        Ürün bulunamadı
                      </td>
                    </tr>
                  ) : (
                    products.map(p => (
                      <tr key={p.child_sku}>
                        <td>
                          <input
                            type="checkbox"
                            checked={selectedSkus.has(p.child_sku)}
                            onChange={() => toggleSelect(p.child_sku)}
                            className="rounded"
                          />
                        </td>
                        <td className="font-mono text-xs">{p.child_sku}</td>
                        <td className="max-w-xs truncate">{p.child_name}</td>
                        <td>
                          <span className={`badge ${getCategoryBadgeClass(p.kategori)}`}>
                            {p.kategori}
                          </span>
                        </td>
                        <td className="font-mono">{p.child_code}</td>
                        <td className="text-right font-mono">{p.en ?? '—'}</td>
                        <td className="text-right font-mono">{p.boy ?? '—'}</td>
                        <td className="text-right">
                          {p.alan_m2 != null ? (
                            <span className="alan-value">{p.alan_m2.toFixed(4)}</span>
                          ) : '—'}
                        </td>
                        <td className="text-xs">{p.variation_size || '—'}</td>
                        <td className="text-xs">{p.variation_color || '—'}</td>
                        <td>
                          <button
                            onClick={() => onSelectProduct(p.child_sku)}
                            className="p-1 text-gray-400 hover:text-blue-600 transition-colors"
                            title="Detay"
                          >
                            <Eye className="w-4 h-4" />
                          </button>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>

            {/* Sayfalama */}
            <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100">
              <span className="text-sm text-gray-500">
                Toplam {pagination.total.toLocaleString()} ürün
                {selectedSkus.size > 0 && ` · ${selectedSkus.size} seçili`}
              </span>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setPagination(p => ({ ...p, page: Math.max(1, p.page - 1) }))}
                  disabled={pagination.page <= 1}
                  className="p-1 rounded hover:bg-gray-100 disabled:opacity-30"
                >
                  <ChevronLeft className="w-5 h-5" />
                </button>
                <span className="text-sm text-gray-600">
                  {pagination.page} / {Math.max(1, pagination.total_pages || 0)}
                </span>
                <button
                  onClick={() => setPagination(p => ({ ...p, page: Math.min(Math.max(1, p.total_pages || 1), p.page + 1) }))}
                  disabled={pagination.page >= Math.max(1, pagination.total_pages || 1)}
                  className="p-1 rounded hover:bg-gray-100 disabled:opacity-30"
                >
                  <ChevronRight className="w-5 h-5" />
                </button>
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
