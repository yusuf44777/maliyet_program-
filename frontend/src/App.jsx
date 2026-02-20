import { useState, useEffect, useMemo } from 'react';
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
import { getStats, loginAuth, getMe, changePassword, setAuthToken, clearAuthToken, getAuthToken } from './api';
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

export default function App() {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [stats, setStats] = useState(null);
  const [selectedProduct, setSelectedProduct] = useState(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [user, setUser] = useState(null);
  const [tutorialOpen, setTutorialOpen] = useState(false);

  const isAdmin = user?.role === 'admin';
  const tabs = useMemo(() => {
    const base = [
      { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
      { id: 'inheritance', label: 'Maliyet Aktarımı', icon: GitBranch },
      { id: 'products', label: 'Ürünler', icon: Package },
    ];
    if (isAdmin) {
      base.push({ id: 'materials', label: 'Hammaddeler', icon: Hammer });
      base.push({ id: 'cost-propagation', label: 'Maliyet Yayılımı', icon: ArrowRightLeft });
      base.push({ id: 'users', label: 'Kullanıcılar', icon: Users });
    }
    return base;
  }, [isAdmin]);

  useEffect(() => {
    if (!tabs.some(t => t.id === activeTab)) {
      setActiveTab('dashboard');
    }
  }, [tabs, activeTab]);

  useEffect(() => {
    const bootstrap = async () => {
      const token = getAuthToken();
      if (!token) {
        setAuthLoading(false);
        return;
      }
      try {
        const res = await getMe();
        setUser(res.user);
        const st = await getStats();
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
    const onUnauthorized = () => {
      setUser(null);
      setSelectedProduct(null);
      setActiveTab('dashboard');
      setStats(null);
      toast.error('Oturum süresi doldu, tekrar giriş yapın');
    };
    window.addEventListener('auth:unauthorized', onUnauthorized);
    return () => window.removeEventListener('auth:unauthorized', onUnauthorized);
  }, []);

  const refreshStats = () => {
    getStats().then(setStats).catch(() => {});
  };

  const handleLogin = async ({ username, password }) => {
    const res = await loginAuth({ username, password });
    setAuthToken(res.access_token);
    setUser(res.user);
    setActiveTab('dashboard');
    setSelectedProduct(null);
    try {
      const st = await getStats();
      setStats(st);
    } catch {
      setStats(null);
    }
  };

  const handleLogout = () => {
    clearAuthToken();
    setUser(null);
    setSelectedProduct(null);
    setStats(null);
    setActiveTab('dashboard');
  };

  const handleChangePassword = async () => {
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

  if (!user) {
    return (
      <div className="min-h-screen">
        <Toaster position="top-right" />
        <LoginScreen onLogin={handleLogin} />
      </div>
    );
  }

  // Ürün detay görünümü
  if (selectedProduct) {
    return (
      <div className="min-h-screen">
        <Toaster position="top-right" />
        <ProductDetail
          sku={selectedProduct}
          onBack={() => setSelectedProduct(null)}
          onRefresh={refreshStats}
        />
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
                {user.role}
              </span>
              <span className="text-gray-600">{user.username}</span>
              <button
                type="button"
                onClick={() => setTutorialOpen(true)}
                className="inline-flex items-center gap-1 px-2 py-1 rounded border border-blue-200 text-blue-700 hover:bg-blue-50"
                title="Kullanım rehberini aç"
              >
                <BookOpen className="w-3 h-3" />
                Yardım
              </button>
              <button
                onClick={handleChangePassword}
                className="inline-flex items-center gap-1 px-2 py-1 rounded border border-gray-200 text-gray-600 hover:bg-gray-50"
                title="Parola değiştir"
              >
                <KeyRound className="w-3 h-3" />
                Parola
              </button>
              <button
                onClick={handleLogout}
                className="inline-flex items-center gap-1 px-2 py-1 rounded border border-gray-200 text-gray-600 hover:bg-gray-50"
                title="Çıkış yap"
              >
                <LogOut className="w-3 h-3" />
                Çıkış
              </button>
            </div>
          </div>

          {/* Tab Navigation */}
          <nav className="flex gap-1 mt-4">
            {tabs.map(tab => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
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
        {activeTab === 'dashboard' && (
          <Dashboard stats={stats} onRefresh={refreshStats} isAdmin={isAdmin} />
        )}
        {activeTab === 'inheritance' && (
          <ParentInheritance onRefresh={refreshStats} />
        )}
        {activeTab === 'products' && (
          <ProductBrowser
            onSelectProduct={setSelectedProduct}
            onRefresh={refreshStats}
          />
        )}
        {activeTab === 'materials' && isAdmin && (
          <MaterialManager onRefresh={refreshStats} />
        )}
        {activeTab === 'cost-propagation' && isAdmin && (
          <CostPropagation />
        )}
        {activeTab === 'users' && isAdmin && (
          <UserManager currentUser={user} />
        )}
      </main>
    </div>
  );
}
