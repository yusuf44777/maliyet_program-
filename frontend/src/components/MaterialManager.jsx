import { useState, useEffect } from 'react';
import {
  getMaterials, createMaterial, updateMaterial, deleteMaterial,
  getCostDefinitions, createCostDefinition, updateCostDefinition, deleteCostDefinition,
} from '../api';
import toast from 'react-hot-toast';
import { Save, DollarSign, Search, Plus, Trash2, Package } from 'lucide-react';

export default function MaterialManager({ onRefresh }) {
  const [materials, setMaterials] = useState([]);
  const [search, setSearch] = useState('');
  const [editing, setEditing] = useState({});
  const [newMaterial, setNewMaterial] = useState({
    name: '', unit: '', unit_price: '', currency: 'TRY',
  });

  const [costDefinitions, setCostDefinitions] = useState([]);
  const [costSearch, setCostSearch] = useState('');
  const [costFilter, setCostFilter] = useState('');
  const [costEditing, setCostEditing] = useState({});
  const [newCost, setNewCost] = useState({
    name: '',
    category: 'kaplama',
    kargo_code: '',
    is_active: true,
  });

  useEffect(() => {
    loadAll();
  }, []);

  const loadAll = async () => {
    try {
      const [mats, defs] = await Promise.all([
        getMaterials(),
        getCostDefinitions({ include_inactive: true }),
      ]);
      setMaterials(mats || []);
      setCostDefinitions(defs || []);
    } catch (err) {
      toast.error('Veriler yüklenemedi');
    }
  };

  const handleEdit = (mat) => {
    setEditing(prev => ({
      ...prev,
      [mat.id]: { unit_price: mat.unit_price, currency: mat.currency },
    }));
  };

  const handleSave = async (id) => {
    const data = editing[id];
    if (!data) return;
    try {
      await updateMaterial(id, data);
      toast.success('Hammadde güncellendi');
      setEditing(prev => {
        const next = { ...prev };
        delete next[id];
        return next;
      });
      await loadAll();
      onRefresh();
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Güncelleme hatası');
    }
  };

  const handleSaveAll = async () => {
    const ids = Object.keys(editing);
    if (ids.length === 0) return;

    for (const id of ids) {
      try {
        await updateMaterial(parseInt(id, 10), editing[id]);
      } catch {
        toast.error(`Hata (ID: ${id})`);
      }
    }
    toast.success(`${ids.length} hammadde güncellendi`);
    setEditing({});
    await loadAll();
    onRefresh();
  };

  const handleCreateMaterial = async () => {
    const name = (newMaterial.name || '').trim();
    const unit = (newMaterial.unit || '').trim();
    if (!name || !unit) {
      toast.error('Hammadde adı ve birim zorunlu');
      return;
    }
    try {
      await createMaterial({
        name,
        unit,
        unit_price: Number(newMaterial.unit_price || 0),
        currency: newMaterial.currency || 'TRY',
      });
      toast.success('Hammadde eklendi');
      setNewMaterial({ name: '', unit: '', unit_price: '', currency: 'TRY' });
      await loadAll();
      onRefresh();
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Hammadde eklenemedi');
    }
  };

  const handleDeleteMaterial = async (mat) => {
    if (!window.confirm(`${mat.name} hammaddesini silmek istiyor musunuz?`)) return;
    try {
      await deleteMaterial(mat.id);
      toast.success('Hammadde silindi');
      await loadAll();
      onRefresh();
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Silme hatası');
    }
  };

  const handleCostEdit = (def) => {
    setCostEditing(prev => ({
      ...prev,
      [def.id]: {
        name: def.name,
        category: def.category,
        kargo_code: def.kargo_code || '',
        is_active: Number(def.is_active ?? 1) === 1,
      },
    }));
  };

  const handleSaveCost = async (id) => {
    const data = costEditing[id];
    if (!data) return;
    const payload = {
      name: (data.name || '').trim(),
      category: data.category,
      is_active: !!data.is_active,
      kargo_code: data.category === 'kargo' ? (data.kargo_code || '').trim() : null,
    };
    if (!payload.name) {
      toast.error('Maliyet adı boş olamaz');
      return;
    }

    try {
      await updateCostDefinition(id, payload);
      toast.success('Maliyet kalemi güncellendi');
      setCostEditing(prev => {
        const next = { ...prev };
        delete next[id];
        return next;
      });
      await loadAll();
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Maliyet güncelleme hatası');
    }
  };

  const handleSaveAllCosts = async () => {
    const ids = Object.keys(costEditing);
    if (ids.length === 0) return;

    for (const id of ids) {
      const data = costEditing[id];
      try {
        await updateCostDefinition(parseInt(id, 10), {
          name: (data.name || '').trim(),
          category: data.category,
          is_active: !!data.is_active,
          kargo_code: data.category === 'kargo' ? (data.kargo_code || '').trim() : null,
        });
      } catch {
        toast.error(`Maliyet güncellenemedi (ID: ${id})`);
      }
    }
    toast.success(`${ids.length} maliyet kalemi güncellendi`);
    setCostEditing({});
    await loadAll();
  };

  const handleCreateCost = async () => {
    const name = (newCost.name || '').trim();
    if (!name) {
      toast.error('Maliyet adı zorunlu');
      return;
    }

    try {
      await createCostDefinition({
        name,
        category: newCost.category,
        kargo_code: newCost.category === 'kargo' ? (newCost.kargo_code || '').trim() : null,
        is_active: !!newCost.is_active,
      });
      toast.success('Maliyet kalemi eklendi');
      setNewCost({ name: '', category: 'kaplama', kargo_code: '', is_active: true });
      await loadAll();
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Maliyet kalemi eklenemedi');
    }
  };

  const handleDeleteCost = async (def) => {
    if (!window.confirm(`${def.name} maliyet kalemini silmek istiyor musunuz?`)) return;
    try {
      await deleteCostDefinition(def.id);
      toast.success('Maliyet kalemi silindi');
      await loadAll();
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Maliyet silme hatası');
    }
  };

  const filteredMaterials = materials.filter(m =>
    m.name.toLowerCase().includes(search.toLowerCase()) ||
    m.unit.toLowerCase().includes(search.toLowerCase())
  );

  const filteredCosts = costDefinitions.filter(def => {
    if (costFilter && def.category !== costFilter) return false;
    if (!costSearch) return true;
    const q = costSearch.toLowerCase();
    return (
      (def.name || '').toLowerCase().includes(q) ||
      (def.kargo_code || '').toLowerCase().includes(q)
    );
  });

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-xl border border-gray-200 p-4 shadow-sm">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <DollarSign className="w-5 h-5 text-green-600" />
            <h2 className="text-lg font-semibold">Hammadde Birim Fiyatları</h2>
          </div>
          <div className="flex items-center gap-3">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
              <input
                type="text"
                value={search}
                onChange={e => setSearch(e.target.value)}
                placeholder="Hammadde ara..."
                className="pl-9 pr-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            {Object.keys(editing).length > 0 && (
              <button
                onClick={handleSaveAll}
                className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 text-sm"
              >
                <Save className="w-4 h-4" />
                {Object.keys(editing).length} değişikliği kaydet
              </button>
            )}
          </div>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 p-4 shadow-sm">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Yeni Hammadde Ekle</h3>
        <div className="grid grid-cols-1 md:grid-cols-5 gap-2">
          <input
            type="text"
            value={newMaterial.name}
            onChange={e => setNewMaterial(prev => ({ ...prev, name: e.target.value }))}
            placeholder="Hammadde adı"
            className="px-3 py-2 border border-gray-200 rounded-lg text-sm"
          />
          <input
            type="text"
            value={newMaterial.unit}
            onChange={e => setNewMaterial(prev => ({ ...prev, unit: e.target.value }))}
            placeholder="Birim (m2, kg, lt...)"
            className="px-3 py-2 border border-gray-200 rounded-lg text-sm"
          />
          <input
            type="number"
            step="0.01"
            value={newMaterial.unit_price}
            onChange={e => setNewMaterial(prev => ({ ...prev, unit_price: e.target.value }))}
            placeholder="Birim fiyat"
            className="px-3 py-2 border border-gray-200 rounded-lg text-sm"
          />
          <select
            value={newMaterial.currency}
            onChange={e => setNewMaterial(prev => ({ ...prev, currency: e.target.value }))}
            className="px-3 py-2 border border-gray-200 rounded-lg text-sm"
          >
            <option value="TRY">TRY</option>
            <option value="USD">USD</option>
            <option value="EUR">EUR</option>
          </select>
          <button
            onClick={handleCreateMaterial}
            className="flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 text-sm"
          >
            <Plus className="w-4 h-4" /> Ekle
          </button>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 shadow-sm">
        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th className="w-8">#</th>
                <th>Hammadde Adı</th>
                <th>Birim</th>
                <th className="w-40">Birim Fiyat</th>
                <th className="w-24">Para Birimi</th>
                <th className="w-28"></th>
              </tr>
            </thead>
            <tbody>
              {filteredMaterials.map((mat, idx) => {
                const isEditing = editing[mat.id] !== undefined;
                return (
                  <tr key={mat.id} className={isEditing ? 'bg-yellow-50' : ''}>
                    <td className="text-gray-400 text-xs">{idx + 1}</td>
                    <td className="font-medium">{mat.name}</td>
                    <td>
                      <span className="badge bg-gray-100 text-gray-600">{mat.unit}</span>
                    </td>
                    <td>
                      {isEditing ? (
                        <input
                          type="number"
                          step="0.01"
                          value={editing[mat.id].unit_price}
                          onChange={e => setEditing(prev => ({
                            ...prev,
                            [mat.id]: { ...prev[mat.id], unit_price: parseFloat(e.target.value) || 0 },
                          }))}
                          className="w-full px-2 py-1 border border-blue-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                          autoFocus
                        />
                      ) : (
                        <span
                          className={`cursor-pointer ${mat.unit_price > 0 ? 'text-green-700 font-mono' : 'text-gray-300'}`}
                          onClick={() => handleEdit(mat)}
                        >
                          {mat.unit_price > 0 ? mat.unit_price.toFixed(2) : 'Girilmedi'}
                        </span>
                      )}
                    </td>
                    <td>
                      {isEditing ? (
                        <select
                          value={editing[mat.id].currency}
                          onChange={e => setEditing(prev => ({
                            ...prev,
                            [mat.id]: { ...prev[mat.id], currency: e.target.value },
                          }))}
                          className="px-2 py-1 border border-blue-300 rounded text-sm"
                        >
                          <option>TRY</option>
                          <option>USD</option>
                          <option>EUR</option>
                        </select>
                      ) : (
                        <span className="text-gray-500 text-xs">{mat.currency}</span>
                      )}
                    </td>
                    <td>
                      <div className="flex items-center gap-1">
                        {isEditing && (
                          <button
                            onClick={() => handleSave(mat.id)}
                            className="p-1 text-green-600 hover:bg-green-50 rounded"
                            title="Kaydet"
                          >
                            <Save className="w-4 h-4" />
                          </button>
                        )}
                        <button
                          onClick={() => handleDeleteMaterial(mat)}
                          className="p-1 text-red-600 hover:bg-red-50 rounded"
                          title="Sil"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 p-4 shadow-sm">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <Package className="w-5 h-5 text-orange-600" />
            <h2 className="text-lg font-semibold">Kargo / Kaplama Maliyet Kalemleri</h2>
          </div>
          <div className="flex items-center gap-2">
            <select
              value={costFilter}
              onChange={e => setCostFilter(e.target.value)}
              className="px-3 py-2 border border-gray-200 rounded-lg text-sm"
            >
              <option value="">Tümü</option>
              <option value="kargo">Kargo</option>
              <option value="kaplama">Kaplama</option>
            </select>
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
              <input
                type="text"
                value={costSearch}
                onChange={e => setCostSearch(e.target.value)}
                placeholder="Maliyet ara..."
                className="pl-9 pr-3 py-2 border border-gray-200 rounded-lg text-sm"
              />
            </div>
            {Object.keys(costEditing).length > 0 && (
              <button
                onClick={handleSaveAllCosts}
                className="flex items-center gap-2 px-4 py-2 bg-orange-600 text-white rounded-lg hover:bg-orange-700 text-sm"
              >
                <Save className="w-4 h-4" />
                {Object.keys(costEditing).length} değişikliği kaydet
              </button>
            )}
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-5 gap-2 mb-3">
          <input
            type="text"
            value={newCost.name}
            onChange={e => setNewCost(prev => ({ ...prev, name: e.target.value }))}
            placeholder="Maliyet adı"
            className="px-3 py-2 border border-gray-200 rounded-lg text-sm"
          />
          <select
            value={newCost.category}
            onChange={e => setNewCost(prev => ({ ...prev, category: e.target.value }))}
            className="px-3 py-2 border border-gray-200 rounded-lg text-sm"
          >
            <option value="kaplama">Kaplama</option>
            <option value="kargo">Kargo</option>
          </select>
          <input
            type="text"
            value={newCost.kargo_code}
            onChange={e => setNewCost(prev => ({ ...prev, kargo_code: e.target.value }))}
            placeholder="Kargo kodu (örn: M-8)"
            disabled={newCost.category !== 'kargo'}
            className="px-3 py-2 border border-gray-200 rounded-lg text-sm disabled:bg-gray-100"
          />
          <label className="flex items-center gap-2 px-3 py-2 border border-gray-200 rounded-lg text-sm">
            <input
              type="checkbox"
              checked={newCost.is_active}
              onChange={e => setNewCost(prev => ({ ...prev, is_active: e.target.checked }))}
            />
            Aktif
          </label>
          <button
            onClick={handleCreateCost}
            className="flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 text-sm"
          >
            <Plus className="w-4 h-4" /> Ekle
          </button>
        </div>

        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th className="w-8">#</th>
                <th>Maliyet Adı</th>
                <th className="w-28">Tür</th>
                <th className="w-32">Kargo Kodu</th>
                <th className="w-24">Durum</th>
                <th className="w-24"></th>
              </tr>
            </thead>
            <tbody>
              {filteredCosts.map((def, idx) => {
                const isEditing = costEditing[def.id] !== undefined;
                const row = isEditing ? costEditing[def.id] : null;
                return (
                  <tr key={def.id} className={isEditing ? 'bg-yellow-50' : ''}>
                    <td className="text-gray-400 text-xs">{idx + 1}</td>
                    <td>
                      {isEditing ? (
                        <input
                          type="text"
                          value={row.name}
                          onChange={e => setCostEditing(prev => ({
                            ...prev,
                            [def.id]: { ...prev[def.id], name: e.target.value },
                          }))}
                          className="w-full px-2 py-1 border border-blue-300 rounded text-sm"
                        />
                      ) : (
                        <span className="cursor-pointer font-medium" onClick={() => handleCostEdit(def)}>
                          {def.name}
                        </span>
                      )}
                    </td>
                    <td>
                      {isEditing ? (
                        <select
                          value={row.category}
                          onChange={e => setCostEditing(prev => ({
                            ...prev,
                            [def.id]: {
                              ...prev[def.id],
                              category: e.target.value,
                              kargo_code: e.target.value === 'kargo' ? prev[def.id].kargo_code : '',
                            },
                          }))}
                          className="px-2 py-1 border border-blue-300 rounded text-sm"
                        >
                          <option value="kaplama">Kaplama</option>
                          <option value="kargo">Kargo</option>
                        </select>
                      ) : (
                        <span className={`badge ${def.category === 'kargo' ? 'bg-orange-100 text-orange-700' : 'bg-emerald-100 text-emerald-700'}`}>
                          {def.category}
                        </span>
                      )}
                    </td>
                    <td>
                      {isEditing ? (
                        <input
                          type="text"
                          value={row.kargo_code}
                          disabled={row.category !== 'kargo'}
                          onChange={e => setCostEditing(prev => ({
                            ...prev,
                            [def.id]: { ...prev[def.id], kargo_code: e.target.value },
                          }))}
                          placeholder="M-8"
                          className="w-full px-2 py-1 border border-blue-300 rounded text-sm disabled:bg-gray-100"
                        />
                      ) : (
                        <span className="font-mono text-xs text-gray-600">{def.kargo_code || '—'}</span>
                      )}
                    </td>
                    <td>
                      {isEditing ? (
                        <label className="inline-flex items-center gap-1 text-xs">
                          <input
                            type="checkbox"
                            checked={!!row.is_active}
                            onChange={e => setCostEditing(prev => ({
                              ...prev,
                              [def.id]: { ...prev[def.id], is_active: e.target.checked },
                            }))}
                          />
                          Aktif
                        </label>
                      ) : (
                        <span className={`badge ${Number(def.is_active) === 1 ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-500'}`}>
                          {Number(def.is_active) === 1 ? 'Aktif' : 'Pasif'}
                        </span>
                      )}
                    </td>
                    <td>
                      <div className="flex items-center gap-1">
                        {isEditing && (
                          <button
                            onClick={() => handleSaveCost(def.id)}
                            className="p-1 text-green-600 hover:bg-green-50 rounded"
                            title="Kaydet"
                          >
                            <Save className="w-4 h-4" />
                          </button>
                        )}
                        <button
                          onClick={() => handleDeleteCost(def)}
                          className="p-1 text-red-600 hover:bg-red-50 rounded"
                          title="Sil"
                        >
                          <Trash2 className="w-4 h-4" />
                        </button>
                      </div>
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
