import { useState, useEffect } from 'react';
import { getProduct, getMaterials, setProductMaterial, setProductMaterialBulk, setProductCost, getCostNames, exportExcel, getProducts } from '../api';
import toast from 'react-hot-toast';
import {
  ArrowLeft, Package, Ruler, Square, Palette, Save,
  Download, Copy, Hammer, Layers,
} from 'lucide-react';

export default function ProductDetail({ sku, onBack, onRefresh }) {
  const [product, setProduct] = useState(null);
  const [allMaterials, setAllMaterials] = useState([]);
  const [costNames, setCostNames] = useState([]);
  const [siblings, setSiblings] = useState([]); // Aynı parent'ın diğer çocukları
  const [materialInputs, setMaterialInputs] = useState({}); // { material_id: quantity }
  const [selectedCost, setSelectedCost] = useState('');
  const [applyToSiblings, setApplyToSiblings] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadData();
  }, [sku]);

  const loadData = async () => {
    setLoading(true);
    try {
      const [prod, mats, costs] = await Promise.all([
        getProduct(sku),
        getMaterials(),
        getCostNames(),
      ]);
      setProduct(prod);
      setAllMaterials(mats);
      setCostNames(costs);

      // Mevcut hammadde miktarlarını inputlara yükle
      const inputs = {};
      (prod.materials || []).forEach(m => {
        inputs[m.material_id] = m.quantity;
      });
      setMaterialInputs(inputs);

      // Mevcut maliyet atamasını bul
      const activeCost = (prod.costs || []).find(c => c.assigned);
      if (activeCost) setSelectedCost(activeCost.cost_name);

      // Aynı parent altındaki diğer ürünleri bul
      if (prod.product_identifier) {
        const sibData = await getProducts({
          product_identifier: prod.product_identifier,
          page_size: 100,
        });
        setSiblings(sibData.products.filter(p => p.child_sku !== sku));
      }
    } catch (err) {
      toast.error('Ürün yüklenemedi');
    }
    setLoading(false);
  };

  const handleSaveMaterial = async (materialId) => {
    const quantity = materialInputs[materialId];
    if (quantity === undefined || quantity === '') return;

    try {
      if (applyToSiblings && siblings.length > 0) {
        const allSkus = [sku, ...siblings.map(s => s.child_sku)];
        await setProductMaterialBulk({
          child_skus: allSkus,
          material_id: materialId,
          quantity: parseFloat(quantity),
        });
        toast.success(`${allSkus.length} ürüne uygulandı`);
      } else {
        await setProductMaterial({
          child_sku: sku,
          material_id: materialId,
          quantity: parseFloat(quantity),
        });
        toast.success('Kaydedildi');
      }
      loadData();
    } catch (err) {
      toast.error('Kaydetme hatası');
    }
  };

  const handleSaveAllMaterials = async () => {
    const entries = Object.entries(materialInputs).filter(([_, q]) => q > 0);
    if (entries.length === 0) return;

    const allSkus = applyToSiblings ? [sku, ...siblings.map(s => s.child_sku)] : [sku];

    for (const [matId, qty] of entries) {
      try {
        if (allSkus.length > 1) {
          await setProductMaterialBulk({
            child_skus: allSkus,
            material_id: parseInt(matId),
            quantity: parseFloat(qty),
          });
        } else {
          await setProductMaterial({
            child_sku: sku,
            material_id: parseInt(matId),
            quantity: parseFloat(qty),
          });
        }
      } catch (err) {
        console.error(err);
      }
    }
    toast.success(`${entries.length} hammadde ${allSkus.length} ürüne kaydedildi`);
    loadData();
    onRefresh();
  };

  const handleSaveCost = async () => {
    if (!selectedCost) return;
    const allSkus = applyToSiblings ? [sku, ...siblings.map(s => s.child_sku)] : [sku];

    for (const s of allSkus) {
      try {
        await setProductCost({ child_sku: s, cost_name: selectedCost, assigned: true });
      } catch (err) {
        console.error(err);
      }
    }
    toast.success(`Maliyet ataması kaydedildi (${allSkus.length} ürün)`);
    loadData();
  };

  const handleExport = async () => {
    const allSkus = [sku, ...siblings.map(s => s.child_sku)];
    try {
      await exportExcel(allSkus);
      toast.success('Excel indirildi');
    } catch (err) {
      toast.error('Export hatası');
    }
  };

  if (loading || !product) {
    return (
      <div className="max-w-7xl mx-auto px-4 py-6">
        <button onClick={onBack} className="flex items-center gap-2 text-gray-500 hover:text-gray-700 mb-4">
          <ArrowLeft className="w-4 h-4" /> Geri
        </button>
        <div className="text-center py-12 text-gray-400">Yükleniyor...</div>
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto px-4 py-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <button onClick={onBack} className="flex items-center gap-2 text-gray-500 hover:text-gray-700">
          <ArrowLeft className="w-4 h-4" /> Ürün Listesi
        </button>
        <button
          onClick={handleExport}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 text-sm"
        >
          <Download className="w-4 h-4" />
          Export ({1 + siblings.length} ürün)
        </button>
      </div>

      {/* Ürün Bilgileri */}
      <div className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm">
        <div className="flex items-start justify-between">
          <div>
            <div className="flex items-center gap-3 mb-2">
              <Package className="w-6 h-6 text-blue-600" />
              <h2 className="text-xl font-bold text-gray-900">{product.child_name}</h2>
            </div>
            <div className="flex items-center gap-4 text-sm text-gray-500">
              <span className="font-mono">{product.child_sku}</span>
              <span className={`badge ${product.kategori === 'metal' ? 'badge-metal' : 'badge-ahsap'}`}>
                {product.kategori}
              </span>
              <span className="font-mono">{product.child_code}</span>
            </div>
          </div>
        </div>

        {/* Boyut & Alan Bilgileri */}
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mt-6">
          <div className="bg-gray-50 rounded-lg p-3">
            <div className="text-xs text-gray-500 mb-1">En (cm)</div>
            <div className="text-lg font-bold font-mono">{product.en ?? '—'}</div>
          </div>
          <div className="bg-gray-50 rounded-lg p-3">
            <div className="text-xs text-gray-500 mb-1">Boy (cm)</div>
            <div className="text-lg font-bold font-mono">{product.boy ?? '—'}</div>
          </div>
          <div className="bg-blue-50 rounded-lg p-3 border border-blue-200">
            <div className="text-xs text-blue-600 mb-1 flex items-center gap-1">
              <Square className="w-3 h-3" /> Alan (m²)
            </div>
            <div className="text-lg font-bold font-mono text-blue-700">
              {product.alan_m2 != null ? product.alan_m2.toFixed(4) : '—'}
            </div>
            {product.en && product.boy && (
              <div className="text-xs text-blue-500 mt-1">
                {product.en} × {product.boy} / 10000
              </div>
            )}
          </div>
          <div className="bg-gray-50 rounded-lg p-3">
            <div className="text-xs text-gray-500 mb-1">Boyut</div>
            <div className="text-sm font-medium">{product.variation_size || '—'}</div>
          </div>
          <div className="bg-gray-50 rounded-lg p-3">
            <div className="text-xs text-gray-500 mb-1 flex items-center gap-1">
              <Palette className="w-3 h-3" /> Renk
            </div>
            <div className="text-sm font-medium">{product.variation_color || '—'}</div>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mt-4">
          <div className="bg-amber-50 rounded-lg p-3 border border-amber-200">
            <div className="text-xs text-amber-700 mb-1">Kargo Kodu</div>
            <div className="text-sm font-mono font-bold text-amber-800">{product.kargo_kodu || '—'}</div>
          </div>
          <div className="bg-amber-50 rounded-lg p-3 border border-amber-200">
            <div className="text-xs text-amber-700 mb-1">Kargo Ölçü (E×B×Y)</div>
            <div className="text-sm font-mono font-bold text-amber-800">
              {product.kargo_en != null && product.kargo_boy != null && product.kargo_yukseklik != null
                ? `${product.kargo_en}×${product.kargo_boy}×${product.kargo_yukseklik}`
                : '—'}
            </div>
          </div>
          <div className="bg-amber-50 rounded-lg p-3 border border-amber-200">
            <div className="text-xs text-amber-700 mb-1">Kargo Ağırlık</div>
            <div className="text-sm font-mono font-bold text-amber-800">
              {product.kargo_agirlik != null ? Number(product.kargo_agirlik).toFixed(3) : '—'}
            </div>
          </div>
          <div className="bg-indigo-50 rounded-lg p-3 border border-indigo-200">
            <div className="text-xs text-indigo-700 mb-1">Desi</div>
            <div className="text-sm font-mono font-bold text-indigo-800">
              {product.kargo_desi != null ? Number(product.kargo_desi).toFixed(3) : '—'}
            </div>
          </div>
        </div>
      </div>

      {/* Kardeş Ürünler (siblings) */}
      {siblings.length > 0 && (
        <div className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <Layers className="w-5 h-5 text-purple-600" />
              <h3 className="font-semibold">Renk Varyantları ({siblings.length + 1} toplam)</h3>
            </div>
            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={applyToSiblings}
                onChange={e => setApplyToSiblings(e.target.checked)}
                className="rounded"
              />
              <Copy className="w-4 h-4 text-purple-500" />
              Tüm varyantlara uygula
            </label>
          </div>
          <div className="flex flex-wrap gap-2">
            {siblings.map(s => (
              <span key={s.child_sku} className="text-xs bg-gray-100 px-2 py-1 rounded font-mono">
                {s.variation_color || s.child_sku}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Maliyet (ambalaj) Ataması */}
      <div className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm">
        <div className="flex items-center gap-2 mb-4">
          <Package className="w-5 h-5 text-orange-600" />
          <h3 className="font-semibold">Maliyet / Ambalaj Ataması</h3>
        </div>
        <div className="flex gap-3 items-end">
          <div className="flex-1">
            <select
              value={selectedCost}
              onChange={e => setSelectedCost(e.target.value)}
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="">Seçiniz...</option>
              {costNames.map(name => (
                <option key={name} value={name}>{name}</option>
              ))}
            </select>
          </div>
          <button
            onClick={handleSaveCost}
            disabled={!selectedCost}
            className="flex items-center gap-2 px-4 py-2 bg-orange-600 text-white rounded-lg hover:bg-orange-700 text-sm disabled:opacity-50"
          >
            <Save className="w-4 h-4" />
            Ata
          </button>
        </div>
      </div>

      {/* Hammadde Girişi */}
      <div className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <Hammer className="w-5 h-5 text-green-600" />
            <h3 className="font-semibold">Hammadde Miktarları</h3>
          </div>
          <button
            onClick={handleSaveAllMaterials}
            className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 text-sm"
          >
            <Save className="w-4 h-4" />
            Tümünü Kaydet
          </button>
        </div>

        {product.alan_m2 && (
          <div className="mb-4 p-3 bg-blue-50 rounded-lg border border-blue-200 text-sm text-blue-700">
            ℹ️ Bu ürünün alanı <strong className="font-mono">{product.alan_m2.toFixed(4)} m²</strong> — 
            m² bazlı hammaddeler için bu değeri referans alabilirsiniz.
          </div>
        )}

        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th className="w-8">#</th>
                <th>Hammadde</th>
                <th>Birim</th>
                <th className="w-32">Birim Fiyat</th>
                <th className="w-40">Miktar</th>
                <th className="w-20"></th>
              </tr>
            </thead>
            <tbody>
              {allMaterials.map((mat, idx) => {
                const currentQty = materialInputs[mat.id] ?? '';
                const isM2 = mat.unit === 'm2';
                return (
                  <tr key={mat.id} className={isM2 ? 'bg-blue-50/30' : ''}>
                    <td className="text-gray-400 text-xs">{idx + 1}</td>
                    <td className="font-medium">
                      {mat.name}
                      {isM2 && <span className="ml-2 text-xs text-blue-500">m²</span>}
                    </td>
                    <td>
                      <span className="badge bg-gray-100 text-gray-600">{mat.unit}</span>
                    </td>
                    <td className="font-mono text-sm text-gray-500">
                      {mat.unit_price > 0 ? `${mat.unit_price.toFixed(2)} ${mat.currency}` : '—'}
                    </td>
                    <td>
                      <input
                        type="number"
                        step="0.0001"
                        value={currentQty}
                        onChange={e => setMaterialInputs(prev => ({
                          ...prev,
                          [mat.id]: e.target.value === '' ? '' : parseFloat(e.target.value),
                        }))}
                        placeholder={isM2 && product.alan_m2 ? product.alan_m2.toFixed(4) : '0'}
                        className="w-full px-2 py-1 border border-gray-200 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono"
                      />
                    </td>
                    <td>
                      <button
                        onClick={() => handleSaveMaterial(mat.id)}
                        className="p-1 text-green-600 hover:bg-green-50 rounded"
                        title="Kaydet"
                      >
                        <Save className="w-4 h-4" />
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
