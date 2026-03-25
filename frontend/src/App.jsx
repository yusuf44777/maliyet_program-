import { useState, useEffect, useMemo } from 'react';
import { Routes, Route, Navigate, useNavigate, useLocation, useParams } from 'react-router-dom';
import { Toaster } from 'react-hot-toast';
import toast from 'react-hot-toast';
import Dashboard from './components/Dashboard';
import ProductBrowser from './components/ProductBrowser';
import MaterialManager from './components/MaterialManager';
import ParentInheritance from './components/ParentInheritance';
import CostPropagation from './components/CostPropagation';
import ProductDetail from './components/ProductDetail';
import LoginScreen from './components/LoginScreen';
import UserManager from './components/UserManager';
import HelpTip from './components/HelpTip';
import UserTutorialModal from './components/UserTutorialModal';
import {
  AUTH_DISABLED,
  getStats,
  loginAuth,
  getMe,
  changePassword,
  setAuthToken,
  clearAuthToken,
  getAuthToken,
} from './api';
import {
  LayoutDashboard,
  Package,
  Hammer,
  FileSpreadsheet,
  GitBranch,
  ArrowRightLeft,
  Users,
  Shield,
  LogOut,
  KeyRound,
  BookOpen,
} from 'lucide-react';

const OPEN_ACCESS_USER = {
  id: 0,
  username: 'acik-erisim',
  role: 'admin',
  is_active: true,
  created_at: null,
  updated_at: null,
};

// Route → tab id eşlemesi
const ROUTE_TO_TAB = {
  '/': 'dashboard',
  '/inheritance': 'inheritance',
  '/products': 'products',
  '/materials': 'materials',
  '/cost-propagation': 'cost-propagation',
  '/users': 'users',
};

// ProductDetail sarmalayıcı — URL'den sku alır
function ProductDetailRoute({ onRefresh }) {
  const { sku } = useParams();
  const navigate = useNavigate();
  return (
    <ProductDetail
      sku={decodeURIComponent(sku)}
      onBack={() => navigate('/products')}
      onRefresh={onRefresh}
    />
  );
}

// Admin route koruması
function AdminRoute({ isAdmin, children }) {
  if (!isAdmin) return <Navigate to="/" replace />;
  return children;
}

export default function App() {
  const navigate = useNavigate();
  const location = useLocation();

  const [stats, setStats] = useState(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [user, setUser] = useState(null);
  const [tutorialOpen, setTutorialOpen] = useState(false);

  const currentUser = user || (AUTH_DISABLED ? OPEN_ACCESS_USER : null);
  const isAdmin = currentUser?.role === 'admin';

  // Geçerli path'ten aktif tab'ı hesapla
  const activeTab = useMemo(() => {
    const path = location.pathname;
    if (path.startsWith('/products/')) return 'products';
    return ROUTE_TO_TAB[path] || 'dashboard';
  }, [location.pathname]);

  const tabs = useMemo(() => {
    const base = [
      { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard, path: '/' },
      { id: 'inheritance', label: 'Maliyet Aktarımı', icon: GitBranch, path: '/inheritance' },
      { id: 'products', label: 'Ürünler', icon: Package, path: '/products' },
    ];
    if (isAdmin) {
      base.push({ id: 'materials', label: 'Hammaddeler', icon: Hammer, path: '/materials' });
      base.push({ id: 'cost-propagation', label: 'Maliyet Yayılımı', icon: ArrowRightLeft, path: '/cost-propagation' });
      base.push({ id: 'users', label: 'Kullanıcılar', icon: Users, path: '/users' });
    }
    return base;
  }, [isAdmin]);

  // Admin olmayan kullanıcı admin route'taysa ana sayfaya yönlendir
  useEffect(() => {
    const adminPaths = ['/materials', '/cost-propagation', '/users'];
    if (!isAdmin && adminPaths.some(p => location.pathname.startsWith(p))) {
      navigate('/', { replace: true });
    }
  }, [isAdmin, location.pathname, navigate]);

  useEffect(() => {
    const bootstrap = async () => {
      if (AUTH_DISABLED) {
        try {
          const [res, st] = await Promise.all([
            getMe().catch(() => null),
            getStats().catch(() => null),
          ]);
          setUser(res?.user || OPEN_ACCESS_USER);
          setStats(st);
        } catch {
          setUser(OPEN_ACCESS_USER);
          setStats(null);
        }
        setAuthLoading(false);
        return;
      }

      const token = getAuthToken();
      if (!token) {
        setAuthLoading(false);
        return;
      }
      try {
        const [res, st] = await Promise.all([
          getMe(),
          getStats().catch(() => null),
        ]);
        setUser(res.user);
        setStats(st);
      } catch (err) {
        clearAuthToken();
        setUser(null);
      }
      setAuthLoading(false);
    };
    bootstrap();
  }, []);

  useEffect(() => {
    if (AUTH_DISABLED) return undefined;
    const onUnauthorized = () => {
      setUser(null);
      setStats(null);
      toast.error('Oturum süresi doldu, tekrar giriş yapın');
      navigate('/login', { replace: true });
    };
    window.addEventListener('auth:unauthorized', onUnauthorized);
    return () => window.removeEventListener('auth:unauthorized', onUnauthorized);
  }, [navigate]);

  const refreshStats = () => {
    getStats().then(setStats).catch(() => {});
  };

  const handleLogin = async ({ username, password }) => {
    const res = await loginAuth({ username, password });
    setAuthToken(res.access_token);
    setUser(res.user);
    navigate('/', { replace: true });
    getStats().then(setStats).catch(() => setStats(null));
  };

  const handleLogout = () => {
    if (AUTH_DISABLED) return;
    clearAuthToken();
    setUser(null);
    setStats(null);
    navigate('/login', { replace: true });
  };

  const handleChangePassword = async () => {
    if (AUTH_DISABLED) {
      toast.error('Giriş sistemi kapalı olduğu için parola değiştirme devre dışı');
      return;
    }
    const current = window.prompt('Mevcut parola:');
    if (current == null) return;
    const next = window.prompt('Yeni parola (en az 6 karakter):');
    if (next == null) return;
    try {
      await changePassword({ current_password: current, new_password: next });
      toast.success('Parola güncellendi');
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Parola güncellenemedi');
    }
  };

  if (authLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center text-gray-500">
        Yükleniyor...
      </div>
    );
  }

  if (!currentUser) {
    return (
      <div className="min-h-screen">
        <Toaster position="top-right" />
        <Routes>
          <Route path="/login" element={<LoginScreen onLogin={handleLogin} />} />
          <Route path="*" element={<Navigate to="/login" replace />} />
        </Routes>
      </div>
    );
  }

  // Ürün detay sayfası — tam ekran, layout dışında
  if (location.pathname.startsWith('/products/') && location.pathname !== '/products') {
    return (
      <div className="min-h-screen">
        <Toaster position="top-right" />
        <Routes>
          <Route path="/products/:sku" element={<ProductDetailRoute onRefresh={refreshStats} />} />
        </Routes>
      </div>
    );
  }

  return (
    <div className="min-h-screen">
      <Toaster position="top-right" />
      <UserTutorialModal
        open={tutorialOpen}
        onClose={() => setTutorialOpen(false)}
        activeTab={activeTab}
        isAdmin={isAdmin}
      />

      {/* Header */}
      <header className="bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-7xl mx-auto px-4 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <FileSpreadsheet className="w-8 h-8 text-blue-600" />
              <div>
                <div className="flex items-center gap-2">
                  <h1 className="text-xl font-bold text-gray-900">Maliyet Sistemi</h1>
                  <HelpTip
                    title="Bu ekran ne için?"
                    text="Bu başlık altındaki menülerden ürün, maliyet ve aktarım işlemlerini yapabilirsiniz. Kararsız kaldığınız yerde mavi soru işaretine tıklayın."
                    placement="bottom"
                  />
                </div>
                <p className="text-sm text-gray-500">ERP Maliyet Şablonu Yönetimi</p>
              </div>
            </div>
            <div className="flex items-center gap-3 text-sm">
              {stats && (
                <>
                  <span className="text-gray-500">
                    <strong className="text-gray-900">{stats.total_products.toLocaleString()}</strong> ürün
                  </span>
                  <span className="text-gray-500">
                    <strong className="text-gray-900">{stats.total_materials}</strong> hammadde
                  </span>
                </>
              )}
              <span className={`inline-flex items-center gap-1 px-2 py-1 rounded text-xs font-medium ${
                isAdmin ? 'bg-amber-100 text-amber-800' : 'bg-slate-100 text-slate-700'
              }`}>
                <Shield className="w-3 h-3" />
                {currentUser.role}
              </span>
              <span className="text-gray-600">{currentUser.username}</span>
              {AUTH_DISABLED && (
                <span className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-emerald-100 text-emerald-800">
                  Açık erişim
                </span>
              )}
              <button
                type="button"
                onClick={() => setTutorialOpen(true)}
                className="inline-flex items-center gap-1 px-2 py-1 rounded border border-blue-200 text-blue-700 hover:bg-blue-50"
                title="Kullanım rehberini aç"
              >
                <BookOpen className="w-3 h-3" />
                Yardım
              </button>
              {!AUTH_DISABLED && (
                <button
                  onClick={handleChangePassword}
                  className="inline-flex items-center gap-1 px-2 py-1 rounded border border-gray-200 text-gray-600 hover:bg-gray-50"
                  title="Parola değiştir"
                >
                  <KeyRound className="w-3 h-3" />
                  Parola
                </button>
              )}
              {!AUTH_DISABLED && (
                <button
                  onClick={handleLogout}
                  className="inline-flex items-center gap-1 px-2 py-1 rounded border border-gray-200 text-gray-600 hover:bg-gray-50"
                  title="Çıkış yap"
                >
                  <LogOut className="w-3 h-3" />
                  Çıkış
                </button>
              )}
            </div>
          </div>

          {/* Tab Navigation */}
          <nav className="flex gap-1 mt-4">
            {tabs.map(tab => (
              <button
                key={tab.id}
                onClick={() => navigate(tab.path)}
                className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                  activeTab === tab.id
                    ? 'bg-blue-100 text-blue-700'
                    : 'text-gray-600 hover:bg-gray-100'
                }`}
              >
                <tab.icon className="w-4 h-4" />
                {tab.label}
              </button>
            ))}
            <button
              type="button"
              onClick={() => setTutorialOpen(true)}
              className="ml-auto flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium text-blue-700 hover:bg-blue-50 border border-blue-200"
            >
              <BookOpen className="w-4 h-4" />
              Nasıl Kullanılır?
            </button>
          </nav>
        </div>
      </header>

      {/* Content */}
      <main className="max-w-7xl mx-auto px-4 py-6">
        <Routes>
          <Route path="/" element={<Dashboard stats={stats} onRefresh={refreshStats} isAdmin={isAdmin} />} />
          <Route path="/inheritance" element={<ParentInheritance onRefresh={refreshStats} />} />
          <Route path="/products" element={<ProductBrowser onSelectProduct={(sku) => navigate(`/products/${encodeURIComponent(sku)}`)} onRefresh={refreshStats} />} />
          <Route path="/materials" element={<AdminRoute isAdmin={isAdmin}><MaterialManager onRefresh={refreshStats} /></AdminRoute>} />
          <Route path="/cost-propagation" element={<AdminRoute isAdmin={isAdmin}><CostPropagation /></AdminRoute>} />
          <Route path="/users" element={<AdminRoute isAdmin={isAdmin}><UserManager currentUser={currentUser} /></AdminRoute>} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </div>
  );
}
