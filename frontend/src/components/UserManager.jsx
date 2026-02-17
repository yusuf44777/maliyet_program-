import { useEffect, useState } from 'react';
import toast from 'react-hot-toast';
import { getUsers, createUser, updateUser, deleteUser, getAuditLogs } from '../api';
import { Plus, Save, Trash2, Shield, RefreshCw } from 'lucide-react';

export default function UserManager({ currentUser }) {
  const [users, setUsers] = useState([]);
  const [auditLogs, setAuditLogs] = useState([]);
  const [loading, setLoading] = useState(false);
  const [editing, setEditing] = useState({});
  const [newUser, setNewUser] = useState({
    username: '',
    password: '',
    role: 'user',
    is_active: true,
  });

  const loadAll = async () => {
    setLoading(true);
    try {
      const [u, logs] = await Promise.all([
        getUsers(),
        getAuditLogs({ limit: 100 }),
      ]);
      setUsers(u || []);
      setAuditLogs(logs || []);
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Kullanıcı verileri yüklenemedi');
    }
    setLoading(false);
  };

  useEffect(() => {
    loadAll();
  }, []);

  const handleCreate = async () => {
    const username = newUser.username.trim();
    if (!username || !newUser.password) {
      toast.error('Kullanıcı adı ve parola zorunlu');
      return;
    }
    try {
      await createUser({
        username,
        password: newUser.password,
        role: newUser.role,
        is_active: !!newUser.is_active,
      });
      toast.success('Kullanıcı eklendi');
      setNewUser({ username: '', password: '', role: 'user', is_active: true });
      await loadAll();
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Kullanıcı eklenemedi');
    }
  };

  const handleStartEdit = (u) => {
    setEditing(prev => ({
      ...prev,
      [u.id]: {
        role: u.role,
        is_active: Number(u.is_active) === 1,
        password: '',
      },
    }));
  };

  const handleSaveEdit = async (u) => {
    const row = editing[u.id];
    if (!row) return;
    try {
      await updateUser(u.id, {
        role: row.role,
        is_active: !!row.is_active,
        password: row.password ? row.password : undefined,
      });
      toast.success('Kullanıcı güncellendi');
      setEditing(prev => {
        const next = { ...prev };
        delete next[u.id];
        return next;
      });
      await loadAll();
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Kullanıcı güncellenemedi');
    }
  };

  const handleDelete = async (u) => {
    if (!window.confirm(`${u.username} kullanıcısı silinsin mi?`)) return;
    try {
      await deleteUser(u.id);
      toast.success('Kullanıcı silindi');
      await loadAll();
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Kullanıcı silinemedi');
    }
  };

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-xl border border-gray-200 p-4 shadow-sm">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Shield className="w-5 h-5 text-amber-600" />
            <h2 className="text-lg font-semibold">Kullanıcı ve Rol Yönetimi</h2>
          </div>
          <button
            onClick={loadAll}
            disabled={loading}
            className="inline-flex items-center gap-2 px-3 py-2 text-sm border border-gray-200 rounded-lg hover:bg-gray-50"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
            Yenile
          </button>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 p-4 shadow-sm">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Yeni Kullanıcı</h3>
        <div className="grid grid-cols-1 md:grid-cols-5 gap-2">
          <input
            type="text"
            value={newUser.username}
            onChange={e => setNewUser(prev => ({ ...prev, username: e.target.value }))}
            placeholder="Kullanıcı adı"
            className="px-3 py-2 border border-gray-200 rounded-lg text-sm"
          />
          <input
            type="password"
            value={newUser.password}
            onChange={e => setNewUser(prev => ({ ...prev, password: e.target.value }))}
            placeholder="Parola"
            className="px-3 py-2 border border-gray-200 rounded-lg text-sm"
          />
          <select
            value={newUser.role}
            onChange={e => setNewUser(prev => ({ ...prev, role: e.target.value }))}
            className="px-3 py-2 border border-gray-200 rounded-lg text-sm"
          >
            <option value="user">user</option>
            <option value="admin">admin</option>
          </select>
          <label className="flex items-center gap-2 px-3 py-2 border border-gray-200 rounded-lg text-sm">
            <input
              type="checkbox"
              checked={newUser.is_active}
              onChange={e => setNewUser(prev => ({ ...prev, is_active: e.target.checked }))}
            />
            Aktif
          </label>
          <button
            onClick={handleCreate}
            className="inline-flex items-center justify-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 text-sm"
          >
            <Plus className="w-4 h-4" />
            Ekle
          </button>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 shadow-sm">
        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th className="w-8">#</th>
                <th>Kullanıcı</th>
                <th className="w-28">Rol</th>
                <th className="w-24">Durum</th>
                <th className="w-44">Parola Güncelle</th>
                <th className="w-28"></th>
              </tr>
            </thead>
            <tbody>
              {users.map((u, idx) => {
                const row = editing[u.id];
                const isEditing = !!row;
                return (
                  <tr key={u.id} className={isEditing ? 'bg-yellow-50' : ''}>
                    <td className="text-xs text-gray-400">{idx + 1}</td>
                    <td className="font-medium">
                      {u.username}
                      {currentUser?.id === u.id && (
                        <span className="ml-2 text-[10px] text-blue-600">(sen)</span>
                      )}
                    </td>
                    <td>
                      {isEditing ? (
                        <select
                          value={row.role}
                          onChange={e => setEditing(prev => ({
                            ...prev,
                            [u.id]: { ...prev[u.id], role: e.target.value },
                          }))}
                          className="px-2 py-1 border border-blue-300 rounded text-sm"
                        >
                          <option value="user">user</option>
                          <option value="admin">admin</option>
                        </select>
                      ) : (
                        <span className={`badge ${u.role === 'admin' ? 'bg-amber-100 text-amber-700' : 'bg-slate-100 text-slate-700'}`}>
                          {u.role}
                        </span>
                      )}
                    </td>
                    <td>
                      {isEditing ? (
                        <label className="inline-flex items-center gap-1 text-xs">
                          <input
                            type="checkbox"
                            checked={!!row.is_active}
                            onChange={e => setEditing(prev => ({
                              ...prev,
                              [u.id]: { ...prev[u.id], is_active: e.target.checked },
                            }))}
                          />
                          Aktif
                        </label>
                      ) : (
                        <span className={`badge ${Number(u.is_active) === 1 ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-500'}`}>
                          {Number(u.is_active) === 1 ? 'Aktif' : 'Pasif'}
                        </span>
                      )}
                    </td>
                    <td>
                      {isEditing ? (
                        <input
                          type="password"
                          value={row.password}
                          onChange={e => setEditing(prev => ({
                            ...prev,
                            [u.id]: { ...prev[u.id], password: e.target.value },
                          }))}
                          placeholder="Boşsa değişmez"
                          className="w-full px-2 py-1 border border-blue-300 rounded text-sm"
                        />
                      ) : (
                        <button
                          onClick={() => handleStartEdit(u)}
                          className="text-xs text-blue-600 hover:text-blue-700"
                        >
                          Düzenle
                        </button>
                      )}
                    </td>
                    <td>
                      <div className="flex items-center gap-1">
                        {isEditing && (
                          <button
                            onClick={() => handleSaveEdit(u)}
                            className="p-1 text-green-600 hover:bg-green-50 rounded"
                            title="Kaydet"
                          >
                            <Save className="w-4 h-4" />
                          </button>
                        )}
                        {currentUser?.id !== u.id && (
                          <button
                            onClick={() => handleDelete(u)}
                            className="p-1 text-red-600 hover:bg-red-50 rounded"
                            title="Sil"
                          >
                            <Trash2 className="w-4 h-4" />
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 shadow-sm">
        <div className="p-4 border-b border-gray-100">
          <h3 className="text-sm font-semibold text-gray-700">Audit Log (Son 100)</h3>
        </div>
        <div className="table-container max-h-72 overflow-y-auto">
          <table>
            <thead>
              <tr>
                <th className="w-32">Tarih</th>
                <th className="w-40">Kullanıcı</th>
                <th>Aksiyon</th>
                <th>Hedef</th>
              </tr>
            </thead>
            <tbody>
              {auditLogs.map((log) => (
                <tr key={log.id}>
                  <td className="text-xs font-mono text-gray-500">{log.created_at}</td>
                  <td className="text-xs">{log.username || '—'}</td>
                  <td className="text-xs font-medium">{log.action}</td>
                  <td className="text-xs">{log.target || '—'}</td>
                </tr>
              ))}
              {auditLogs.length === 0 && (
                <tr>
                  <td colSpan={4} className="py-6 text-center text-sm text-gray-400">Kayıt yok</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
