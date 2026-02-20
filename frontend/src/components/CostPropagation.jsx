import { useEffect, useMemo, useRef, useState } from 'react';
import toast from 'react-hot-toast';
import {
  Search,
  Loader2,
  Plus,
  Trash2,
  ArrowRightLeft,
  CheckCircle2,
} from 'lucide-react';
import { applyCostPropagation, searchParentProducts } from '../api';
import HelpTip from './HelpTip';

const EMPTY_DYNAMIC_ROW = { key: '', value: '' };

function toPayloadValue(rawValue) {
  const value = String(rawValue ?? '').trim();
  if (!value) return null;
  const normalized = value.replace(',', '.');
  const numeric = Number(normalized);
  if (Number.isFinite(numeric)) return numeric;
  return value;
}

function buildParentLabel(parent) {
  const sku = String(parent?.parent_sku || '').trim();
  const name = String(parent?.parent_name || '').trim();
  if (sku && name) return `${sku} - ${name}`;
  if (sku) return sku;
  return name;
}

export default function CostPropagation() {
  const [searchText, setSearchText] = useState('');
  const [debouncedSearch, setDebouncedSearch] = useState('');
  const [searchLoading, setSearchLoading] = useState(false);
  const [parentOptions, setParentOptions] = useState([]);
  const [selectedParent, setSelectedParent] = useState(null);
  const [isDropdownOpen, setIsDropdownOpen] = useState(false);

  const [rawMaterial, setRawMaterial] = useState('');
  const [coating, setCoating] = useState('');
  const [shipping, setShipping] = useState('');
  const [dynamicRows, setDynamicRows] = useState([EMPTY_DYNAMIC_ROW]);

  const [applying, setApplying] = useState(false);
  const [lastResult, setLastResult] = useState(null);
  const abortRef = useRef(null);

  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedSearch(searchText.trim());
    }, 300);
    return () => clearTimeout(timer);
  }, [searchText]);

  useEffect(() => {
    if (abortRef.current) abortRef.current.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setSearchLoading(true);
    searchParentProducts(
      { q: debouncedSearch, limit: 30 },
      { signal: controller.signal },
    )
      .then((rows) => {
        if (controller.signal.aborted) return;
        setParentOptions(Array.isArray(rows) ? rows : []);
      })
      .catch((err) => {
        if (err?.code === 'ERR_CANCELED' || err?.name === 'CanceledError') return;
        toast.error('Parent listesi yüklenemedi');
      })
      .finally(() => {
        if (abortRef.current === controller) {
          abortRef.current = null;
          setSearchLoading(false);
        }
      });

    return () => controller.abort();
  }, [debouncedSearch]);

  const payloadPreview = useMemo(() => {
    const payload = {};
    const raw = toPayloadValue(rawMaterial);
    const coat = toPayloadValue(coating);
    const ship = toPayloadValue(shipping);
    if (raw != null) payload.raw_material = raw;
    if (coat != null) payload.coating_plating = coat;
    if (ship != null) payload.shipping = ship;

    for (const row of dynamicRows) {
      const key = String(row?.key || '').trim();
      const value = toPayloadValue(row?.value);
      if (!key || value == null) continue;
      payload[key] = value;
    }
    return payload;
  }, [rawMaterial, coating, shipping, dynamicRows]);

  const handleSelectParent = (parent) => {
    setSelectedParent(parent);
    setSearchText(buildParentLabel(parent));
    setIsDropdownOpen(false);
  };

  const handleDynamicChange = (index, field, value) => {
    setDynamicRows((prev) => prev.map((row, i) => (
      i === index ? { ...row, [field]: value } : row
    )));
  };

  const handleAddDynamic = () => {
    setDynamicRows((prev) => [...prev, EMPTY_DYNAMIC_ROW]);
  };

  const handleRemoveDynamic = (index) => {
    setDynamicRows((prev) => {
      const next = prev.filter((_, i) => i !== index);
      return next.length > 0 ? next : [EMPTY_DYNAMIC_ROW];
    });
  };

  const handleApply = async () => {
    if (!selectedParent) {
      toast.error('Önce bir parent ürün seçin');
      return;
    }
    if (Object.keys(payloadPreview).length === 0) {
      toast.error('En az bir maliyet alanı girin');
      return;
    }

    setApplying(true);
    try {
      const result = await applyCostPropagation({
        parent_id: selectedParent.parent_id,
        parent_name: selectedParent.parent_name,
        parent_sku: selectedParent.parent_sku,
        cost_breakdown: payloadPreview,
      });
      setLastResult(result);
      if ((result?.children_updated || 0) > 0) {
        toast.success(`${result.children_updated} child ürüne maliyet aktarıldı`);
      } else {
        toast.success('Parent maliyet profili güncellendi (aktif child yok)');
      }
    } catch (err) {
      toast.error(err?.response?.data?.detail || 'Maliyet aktarımı başarısız');
    }
    setApplying(false);
  };

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm">
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold text-gray-900">Parent Ürün Seçimi</h3>
          <HelpTip
            title="Neden parent seçiyoruz?"
            text="Maliyet kırılımı önce parent ürüne yazılır, sonra o parent altındaki tüm child ürünlere aynı değerler yayılır."
          />
        </div>

        <div className="relative">
          <label className="text-xs text-gray-500 mb-1 block">
            SKU veya Ürün Adına Göre Ara
          </label>
          <Search className="absolute left-3 top-[38px] w-4 h-4 text-gray-400" />
          <input
            type="text"
            value={searchText}
            onFocus={() => setIsDropdownOpen(true)}
            onChange={(e) => {
              setSearchText(e.target.value);
              setIsDropdownOpen(true);
            }}
            placeholder="Örn: MTL-001 veya ürün adı"
            className="w-full pl-9 pr-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />

          {isDropdownOpen && (
            <div className="absolute z-20 mt-2 w-full max-h-72 overflow-auto rounded-lg border border-gray-200 bg-white shadow-lg">
              {searchLoading && (
                <div className="px-3 py-3 text-sm text-gray-500 flex items-center gap-2">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Parent ürünler yükleniyor...
                </div>
              )}

              {!searchLoading && parentOptions.length === 0 && (
                <div className="px-3 py-3 text-sm text-gray-500">
                  Eşleşen parent ürün bulunamadı
                </div>
              )}

              {!searchLoading && parentOptions.map((parent) => {
                const selected = selectedParent?.parent_id === parent.parent_id;
                return (
                  <button
                    key={`${parent.parent_id}-${parent.parent_name}`}
                    onClick={() => handleSelectParent(parent)}
                    className={`w-full text-left px-3 py-2 border-b last:border-b-0 transition-colors ${
                      selected ? 'bg-blue-50 text-blue-700' : 'hover:bg-gray-50'
                    }`}
                  >
                    <p className="text-sm font-medium">{buildParentLabel(parent) || '(İsimsiz Parent)'}</p>
                    <p className="text-xs text-gray-500 mt-0.5">
                      Parent ID: {parent.parent_id} | Child: {parent.child_count}
                    </p>
                  </button>
                );
              })}
            </div>
          )}
        </div>

        {selectedParent && (
          <div className="mt-4 rounded-lg border border-blue-100 bg-blue-50 px-3 py-2">
            <p className="text-sm text-blue-800 font-medium">
              Seçili Parent: {buildParentLabel(selectedParent)}
            </p>
            <p className="text-xs text-blue-600 mt-0.5">
              Parent ID: {selectedParent.parent_id} | Toplam Child: {selectedParent.child_count}
            </p>
          </div>
        )}
      </div>

      <div className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm space-y-5">
        <div className="flex items-center gap-2">
          <h3 className="text-lg font-semibold text-gray-900">Maliyet Kırılımı</h3>
          <HelpTip
            title="Neyi doldurmalıyım?"
            text="Sabit alanları ve gerekiyorsa dinamik alanları girin. Sayısal değerleri sayı formatında girerseniz sistem doğru hesaplar."
          />
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <div>
            <label className="text-xs text-gray-500 mb-1 block">Raw Material</label>
            <input
              type="text"
              value={rawMaterial}
              onChange={(e) => setRawMaterial(e.target.value)}
              placeholder="örn: 125.50"
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>

          <div>
            <label className="text-xs text-gray-500 mb-1 block">Coating / Plating</label>
            <input
              type="text"
              value={coating}
              onChange={(e) => setCoating(e.target.value)}
              placeholder="örn: 22.75"
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>

          <div>
            <label className="text-xs text-gray-500 mb-1 block">Shipping</label>
            <input
              type="text"
              value={shipping}
              onChange={(e) => setShipping(e.target.value)}
              placeholder="örn: 15"
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </div>

        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <label className="text-sm font-medium text-gray-700">Dinamik Maliyet Değişkenleri</label>
            <button
              onClick={handleAddDynamic}
              className="inline-flex items-center gap-1 px-2 py-1 rounded border border-gray-200 text-xs text-gray-600 hover:bg-gray-50"
            >
              <Plus className="w-3.5 h-3.5" />
              Alan Ekle
            </button>
          </div>

          {dynamicRows.map((row, idx) => (
            <div key={`dynamic-${idx}`} className="grid grid-cols-12 gap-2">
              <input
                type="text"
                value={row.key}
                onChange={(e) => handleDynamicChange(idx, 'key', e.target.value)}
                placeholder="Alan adı (örn: labor_cost)"
                className="col-span-5 px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <input
                type="text"
                value={row.value}
                onChange={(e) => handleDynamicChange(idx, 'value', e.target.value)}
                placeholder="Değer"
                className="col-span-6 px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <button
                onClick={() => handleRemoveDynamic(idx)}
                className="col-span-1 inline-flex items-center justify-center rounded-lg border border-gray-200 text-gray-500 hover:bg-red-50 hover:text-red-600"
                title="Satırı sil"
              >
                <Trash2 className="w-4 h-4" />
              </button>
            </div>
          ))}
        </div>

        <div className="rounded-lg border border-gray-200 bg-gray-50 p-3">
          <p className="text-xs text-gray-500 mb-2">Gönderilecek payload önizlemesi</p>
          <pre className="text-xs text-gray-700 overflow-auto">
            {JSON.stringify(payloadPreview, null, 2)}
          </pre>
        </div>

        <div className="flex items-center gap-3">
          <button
            onClick={handleApply}
            disabled={applying || !selectedParent}
            className="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50"
          >
            {applying ? <Loader2 className="w-4 h-4 animate-spin" /> : <ArrowRightLeft className="w-4 h-4" />}
            {applying ? 'Aktarılıyor...' : 'Apply / Transfer Costs'}
          </button>
          <HelpTip
            title="Bu işlem ne yapar?"
            text="Parent kaydını günceller ve bağlı child ürünlerin maliyet kırılımını aynı payload ile toplu günceller."
            placement="bottom"
          />

          {!selectedParent && (
            <span className="text-xs text-gray-500">Devam etmek için parent ürün seçin</span>
          )}
        </div>
      </div>

      {lastResult && (
        <div className="bg-white rounded-xl border border-green-200 p-4 shadow-sm">
          <div className="flex items-start gap-2">
            <CheckCircle2 className="w-5 h-5 text-green-600 mt-0.5" />
            <div>
              <p className="text-sm font-semibold text-green-700">Maliyet aktarımı tamamlandı</p>
              <p className="text-sm text-gray-700 mt-1">
                Parent <strong>{lastResult.parent_name}</strong> için
                {' '}
                <strong>{lastResult.children_updated}</strong> child ürün güncellendi.
              </p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
