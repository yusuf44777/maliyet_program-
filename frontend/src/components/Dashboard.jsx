import { useState } from 'react';
import { reloadDB, exportExcel, getProducts, syncTemplateData } from '../api';
import toast from 'react-hot-toast';
import {
  Package,
  Layers,
  TreePine,
  Ruler,
  AlertCircle,
  Hammer,
  DollarSign,
  RefreshCw,
  Download,
} from 'lucide-react';
import HelpTip from './HelpTip';

const ENABLE_RELOAD_DB = (() => {
  const raw = import.meta.env.VITE_ENABLE_RELOAD_DB;
  if (raw !== undefined) return String(raw).toLowerCase() === 'true';
  return !!import.meta.env.DEV;
})();

function StatCard({ icon: Icon, label, value, sublabel, color = 'blue' }) {
  const colors = {
    blue: 'bg-blue-50 text-blue-600',
    green: 'bg-green-50 text-green-600',
    amber: 'bg-amber-50 text-amber-600',
    slate: 'bg-slate-50 text-slate-600',
    red: 'bg-red-50 text-red-600',
    purple: 'bg-purple-50 text-purple-600',
  };

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-5 shadow-sm">
      <div className="flex items-start justify-between">
        <div>
          <p className="text-sm text-gray-500">{label}</p>
          <p className="text-2xl font-bold text-gray-900 mt-1">
            {value?.toLocaleString() ?? '—'}
          </p>
          {sublabel && <p className="text-xs text-gray-400 mt-1">{sublabel}</p>}
        </div>
        <div className={`p-2 rounded-lg ${colors[color]}`}>
          <Icon className="w-5 h-5" />
        </div>
      </div>
    </div>
  );
}

export default function Dashboard({ stats, onRefresh, isAdmin = false }) {
  const [loading, setLoading] = useState(false);

  const handleReload = async () => {
    if (!confirm('Veritabanı sıfırlanıp yeniden yüklenecek. Devam?')) return;
    setLoading(true);
    try {
      const result = await reloadDB();
      toast.success(`${result.products_loaded} ürün yüklendi`);
      onRefresh();
    } catch (err) {
      toast.error('Yükleme hatası: ' + err.message);
    }
    setLoading(false);
  };

  const handleExportAll = async () => {
    setLoading(true);
    try {
      const data = await getProducts({ page_size: 500 });
      const skus = data.products.map(p => p.child_sku);
      if (skus.length === 0) {
        toast.error('Export edilecek ürün yok');
        return;
      }
      await exportExcel(skus);
      toast.success('Excel dosyası indirildi');
    } catch (err) {
      toast.error('Export hatası: ' + err.message);
    }
    setLoading(false);
  };

  const handleTemplateSync = async () => {
    if (!isAdmin) return;
    setLoading(true);
    try {
      const result = await syncTemplateData({
        force_refresh: true,
        sync_materials: true,
        sync_costs: true,
      });
      toast.success(
        `Şablon sync tamamlandı: +${result.materials_added} hammadde, +${result.cost_definitions_added} maliyet`,
      );
      onRefresh();
    } catch (err) {
      toast.error(err?.response?.data?.detail || 'Şablon senkronizasyonu başarısız');
    }
    setLoading(false);
  };

  if (!stats) {
    return (
      <div className="flex items-center justify-center h-64">
        <RefreshCw className="w-6 h-6 animate-spin text-gray-400" />
        <span className="ml-2 text-gray-500">Yükleniyor...</span>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Stat Cards */}
      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-5 gap-4">
        <StatCard icon={Package} label="Toplam Ürün" value={stats.total_products} color="blue" />
        <StatCard icon={Layers} label="Metal Ürünler" value={stats.metal_products} color="slate" />
        <StatCard icon={TreePine} label="Ahşap Ürünler" value={stats.ahsap_products} color="amber" />
        <StatCard icon={Ruler} label="Cam Ürünler" value={stats.cam_products} color="green" />
        <StatCard icon={Layers} label="Harita Ürünler" value={stats.harita_products} color="purple" />
        <StatCard icon={Package} label="Mobilya Ürünler" value={stats.mobilya_products} color="blue" />
        <StatCard icon={Ruler} label="Boyutlu Ürünler" value={stats.products_with_dims}
          sublabel={`${stats.products_without_dims} boyutsuz`} color="green" />
        <StatCard icon={AlertCircle} label="Boyutsuz Ürünler" value={stats.products_without_dims} color="red" />
        <StatCard icon={Hammer} label="Hammadde Tanımı" value={stats.total_materials} color="purple" />
        <StatCard icon={DollarSign} label="Fiyatlı Hammadde" value={stats.materials_with_price}
          sublabel={`${stats.total_materials - stats.materials_with_price} fiyatsız`} color="green" />
      </div>

      {/* Alan Hesaplama Açıklama */}
      <div className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm">
        <div className="flex items-center gap-2 mb-3">
          <h3 className="text-lg font-semibold text-gray-900">Alan Hesaplama Formülü</h3>
          <HelpTip
            title="Neden önemli?"
            text="Alan değeri, m² bazlı hammaddelerin miktarını otomatik belirler. En ve boy yanlışsa maliyet hesabı da yanlış çıkar."
          />
        </div>
        <div className="bg-blue-50 rounded-lg p-4 border border-blue-200">
          <div className="flex items-center gap-3">
            <Ruler className="w-6 h-6 text-blue-600" />
            <div>
              <p className="text-lg font-mono font-bold text-blue-800">
                Alan (m²) = En (cm) × Boy (cm) / 10.000
              </p>
              <p className="text-sm text-blue-600 mt-1">
                Örnek: En=20cm, Boy=99cm → Alan = 20 × 99 / 10000 = <strong>0.198 m²</strong>
              </p>
              <p className="text-xs text-blue-500 mt-2">
                Bu alan değeri UV baskı, pleksi, saç, MDF, strafor, boya gibi m² bazlı hammadde miktarlarını belirler.
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Aksiyonlar */}
      <div className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm">
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold text-gray-900">Hızlı İşlemler</h3>
          <HelpTip
            title="Bu butonlar ne yapar?"
            text="Senkronizasyon, export ve yeniden yükleme gibi toplu işlemleri tek yerden başlatır. Özellikle şablon değiştiğinde önce senkronizasyon yapın."
          />
        </div>
        <div className="flex gap-3">
          {isAdmin && (
            <button
              onClick={handleTemplateSync}
              disabled={loading}
              className="flex items-center gap-2 px-4 py-2 bg-emerald-100 text-emerald-700 rounded-lg hover:bg-emerald-200 transition-colors disabled:opacity-50"
            >
              <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
              Şablonu Senkronize Et
            </button>
          )}
          {isAdmin && ENABLE_RELOAD_DB && (
            <button
              onClick={handleReload}
              disabled={loading}
              className="flex items-center gap-2 px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors disabled:opacity-50"
            >
              <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
              DB Yeniden Yükle
            </button>
          )}
          <button
            onClick={handleExportAll}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50"
          >
            <Download className="w-4 h-4" />
            Tümünü Export Et
          </button>
        </div>
        {!isAdmin && (
          <p className="text-xs text-gray-500 mt-3">
            DB yeniden yükleme işlemi yalnızca admin kullanıcılar içindir.
          </p>
        )}
        {isAdmin && !ENABLE_RELOAD_DB && (
          <p className="text-xs text-amber-600 mt-3">
            DB yeniden yükleme bu ortamda kapalı.
          </p>
        )}
      </div>
    </div>
  );
}
