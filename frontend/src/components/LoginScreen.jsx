import { useState } from 'react';
import toast from 'react-hot-toast';
import { LogIn, Shield } from 'lucide-react';

const SHOW_DEFAULT_LOGIN_HINTS = (() => {
  const raw = import.meta.env.VITE_SHOW_DEFAULT_LOGIN_HINTS;
  if (raw !== undefined) return String(raw).toLowerCase() === 'true';
  return !!import.meta.env.DEV;
})();

export default function LoginScreen({ onLogin }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!username.trim() || !password) {
      toast.error('Kullanıcı adı ve parola zorunlu');
      return;
    }
    setLoading(true);
    try {
      await onLogin({ username: username.trim(), password });
      toast.success('Giriş başarılı');
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Giriş başarısız');
    }
    setLoading(false);
  };

  return (
    <div className="min-h-screen flex items-center justify-center px-4 bg-slate-50">
      <div className="w-full max-w-md bg-white border border-gray-200 rounded-2xl shadow-sm p-6">
        <div className="flex items-center gap-2 mb-4">
          <Shield className="w-5 h-5 text-blue-600" />
          <h1 className="text-lg font-semibold text-gray-900">Rol Bazlı Giriş</h1>
        </div>

        <form onSubmit={handleSubmit} className="space-y-3">
          <div>
            <label className="text-xs text-gray-500 block mb-1">Kullanıcı Adı</label>
            <input
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              placeholder="örn: admin"
              autoFocus
            />
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">Parola</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              placeholder="******"
            />
          </div>
          <button
            type="submit"
            disabled={loading}
            className="w-full flex items-center justify-center gap-2 px-4 py-2.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-60"
          >
            <LogIn className="w-4 h-4" />
            {loading ? 'Giriş yapılıyor...' : 'Giriş Yap'}
          </button>
        </form>

        {SHOW_DEFAULT_LOGIN_HINTS && (
          <div className="mt-4 text-xs text-gray-500 bg-gray-50 border border-gray-100 rounded-lg p-3">
            Varsayılan hesaplar:
            <div className="mt-1 font-mono">admin / admin</div>
            <div className="font-mono">user / user123</div>
          </div>
        )}
      </div>
    </div>
  );
}
