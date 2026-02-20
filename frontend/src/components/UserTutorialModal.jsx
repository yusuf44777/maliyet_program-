import { X, BookOpen, CircleHelp } from 'lucide-react';

const BASE_GUIDES = [
  {
    id: 'dashboard',
    title: 'Dashboard',
    reason: 'Sistemin genel durumunu ve veri kalitesini hızlı görmek için.',
    steps: [
      'Kartlardan toplam ürün/hammadde sayılarını kontrol edin.',
      'Boyutsuz ürün varsa önce Ürünler ekranında düzeltin.',
      'Şablon değiştiyse "Şablonu Senkronize Et" butonunu kullanın.',
    ],
  },
  {
    id: 'inheritance',
    title: 'Maliyet Aktarımı',
    reason: 'Bir parent için ayar yapıp tüm child ürünlere otomatik geçirmek için.',
    steps: [
      '1. Parent seçin.',
      '2. Hammadde, kargo, kaplama, ağırlık eşlemesini tamamlayın.',
      '3. "Uygula" ile child ürünlere aktarın.',
    ],
  },
  {
    id: 'products',
    title: 'Ürünler',
    reason: 'Ürünleri bulmak, filtrelemek ve detayına gitmek için.',
    steps: [
      'Arama kutusuna SKU veya isim yazın.',
      'Kategori/Boyut filtrelerini kullanın.',
      'Detay ikonundan tek ürünü açın.',
    ],
  },
];

const ADMIN_GUIDES = [
  {
    id: 'materials',
    title: 'Hammaddeler',
    reason: 'Birim fiyatları ve maliyet kalemlerini yönetmek için.',
    steps: [
      'Hammadde fiyatlarını düzenleyin ve kaydedin.',
      'Yeni hammadde veya maliyet kalemi ekleyin.',
      'Yanlış/eskimiş kayıtları silin veya pasife alın.',
    ],
  },
  {
    id: 'cost-propagation',
    title: 'Maliyet Yayılımı',
    reason: 'Parent maliyet kırılımını tek seferde child ürünlere yansıtmak için.',
    steps: [
      'Parent ürünü arayıp seçin.',
      'Maliyet kırılımı alanlarını doldurun.',
      '"Apply / Transfer Costs" ile aktarımı başlatın.',
    ],
  },
  {
    id: 'users',
    title: 'Kullanıcılar',
    reason: 'Kim neye erişebilir kontrolünü yönetmek için.',
    steps: [
      'Yeni kullanıcı oluşturun.',
      'Rol ve aktiflik durumunu düzenleyin.',
      'Gerekirse kullanıcıyı pasife alın.',
    ],
  },
];

export default function UserTutorialModal({
  open,
  onClose,
  activeTab = '',
  isAdmin = false,
}) {
  if (!open) return null;
  const guides = isAdmin ? [...BASE_GUIDES, ...ADMIN_GUIDES] : BASE_GUIDES;

  return (
    <div className="fixed inset-0 z-50 bg-black/40 flex items-start justify-center p-4 sm:p-6">
      <div className="w-full max-w-3xl max-h-[90vh] overflow-y-auto rounded-xl border border-gray-200 bg-white shadow-2xl">
        <div className="sticky top-0 z-10 flex items-center justify-between border-b border-gray-200 bg-white px-4 py-3">
          <div className="flex items-center gap-2">
            <BookOpen className="w-5 h-5 text-blue-600" />
            <h3 className="text-lg font-semibold text-gray-900">Kısa Kullanım Rehberi</h3>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg p-1 text-gray-500 hover:bg-gray-100 hover:text-gray-700"
            title="Kapat"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="p-4 space-y-3">
          <div className="rounded-lg border border-blue-200 bg-blue-50 p-3 text-sm text-blue-800">
            Bu programda `?` ikonları "neden bu alan var?" sorusuna kısa cevap verir.
          </div>

          {guides.map((guide) => (
            <section
              key={guide.id}
              className={`rounded-lg border p-3 ${
                activeTab === guide.id
                  ? 'border-blue-300 bg-blue-50/60'
                  : 'border-gray-200 bg-white'
              }`}
            >
              <div className="flex items-center gap-2 mb-2">
                <CircleHelp className={`w-4 h-4 ${activeTab === guide.id ? 'text-blue-700' : 'text-gray-500'}`} />
                <h4 className="font-semibold text-gray-900">{guide.title}</h4>
              </div>
              <p className="text-sm text-gray-700 mb-2">{guide.reason}</p>
              <ol className="list-decimal pl-5 text-sm text-gray-700 space-y-1">
                {guide.steps.map((step, idx) => (
                  <li key={`${guide.id}-step-${idx}`}>{step}</li>
                ))}
              </ol>
            </section>
          ))}
        </div>
      </div>
    </div>
  );
}
