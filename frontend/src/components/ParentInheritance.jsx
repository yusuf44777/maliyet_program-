import { useState, useEffect, useMemo } from 'react';
import {
  getProductGroups, getProducts, getMaterials, getCostDefinitions,
  getKargoOptions, getKaplamaNameSuggestions, applyInheritance, exportExcel,
} from '../api';
import toast from 'react-hot-toast';
import {
  Search, ChevronDown, ChevronRight, GitBranch, Hammer,
  Package, Zap, Download, CheckCircle2, AlertTriangle,
  ArrowRight, Loader2, Ruler, Box,
} from 'lucide-react';

const KAPLAMA_TOKEN_PATTERN = /[a-z0-9çğıöşü]+/gi;
const KAPLAMA_SILVER_TOKENS = new Set(['silver', 'gumus', 'gümüş', 'gümus']);
const KAPLAMA_GOLD_COPPER_TOKENS = new Set([
  'gold', 'altin', 'altın',
  'copper', 'bakir', 'bakır',
  'bronze', 'pirinc', 'pirinç',
  'rosegold',
]);
const KAPLAMA_TIER_ORDER = { silver: 0, gold_copper: 1, other: 2 };

function tokenizeKaplama(value) {
  if (!value) return [];
  return String(value).toLocaleLowerCase('tr').match(KAPLAMA_TOKEN_PATTERN) || [];
}

function detectKaplamaTier(...values) {
  const tokens = new Set(values.flatMap(v => tokenizeKaplama(v)));
  for (const t of tokens) {
    if (KAPLAMA_GOLD_COPPER_TOKENS.has(t)) return 'gold_copper';
  }
  for (const t of tokens) {
    if (KAPLAMA_SILVER_TOKENS.has(t)) return 'silver';
  }
  return 'other';
}

function buildKaplamaGroupKey(name, tier) {
  const normalizedName = String(name || '').trim();
  const normalizedTier = String(tier || 'other').trim().toLowerCase() || 'other';
  return normalizedName ? `${normalizedName}||${normalizedTier}` : '';
}

function kaplamaTierLabel(tier) {
  if (tier === 'silver') return 'silver';
  if (tier === 'gold_copper') return 'gold,copper';
  return 'diğer';
}

function normalizeKaplamaSelection(value) {
  const rawValues = Array.isArray(value) ? value : [value];
  const out = [];
  const seen = new Set();
  for (const raw of rawValues) {
    const name = String(raw || '').trim();
    if (!name) continue;
    const key = name.toLocaleLowerCase('tr');
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(name);
  }
  return out;
}

function hasKaplamaSelection(value) {
  return normalizeKaplamaSelection(value).length > 0;
}

function toggleKaplamaSelection(currentValue, optionName, checked) {
  const option = String(optionName || '').trim();
  const current = normalizeKaplamaSelection(currentValue);
  if (!option) return current;
  const optionKey = option.toLocaleLowerCase('tr');
  const hasOption = current.some(v => v.toLocaleLowerCase('tr') === optionKey);
  if (checked && !hasOption) return [...current, option];
  if (!checked && hasOption) {
    return current.filter(v => v.toLocaleLowerCase('tr') !== optionKey);
  }
  return current;
}

// ─── Step indicator ───
function StepBadge({ number, label, active, done }) {
  return (
    <div className={`flex items-center gap-2 px-3 py-1.5 rounded-full text-sm font-medium transition-all ${
      done ? 'bg-green-100 text-green-700' :
      active ? 'bg-blue-100 text-blue-700' :
      'bg-gray-100 text-gray-400'
    }`}>
      {done ? <CheckCircle2 className="w-4 h-4" /> : (
        <span className="w-5 h-5 rounded-full bg-current/10 flex items-center justify-center text-xs font-bold">
          {number}
        </span>
      )}
      {label}
    </div>
  );
}

export default function ParentInheritance({ onRefresh }) {
  // ─── Data ───
  const [groups, setGroups] = useState([]);
  const [materials, setMaterials] = useState([]);
  const [costDefinitions, setCostDefinitions] = useState([]);
  const [kargoOptionsRaw, setKargoOptionsRaw] = useState([]);
  const [children, setChildren] = useState([]);
  const [kaplamaNameMap, setKaplamaNameMap] = useState({}); // { child_name||tier: [kaplama_cost_names] }
  const [kaplamaSuggestionByName, setKaplamaSuggestionByName] = useState({});

  // ─── Selections ───
  const [selectedGroup, setSelectedGroup] = useState(null);
  const [costMap, setCostMap] = useState({});         // { variation_size: cost_name }
  const [kaplamaMap, setKaplamaMap] = useState({});   // { variation_size: kaplama_cost_name | [kaplama_cost_names] }
  const [weightMap, setWeightMap] = useState({});     // { variation_size: kargo_agirlik }
  const [materialInputs, setMaterialInputs] = useState({}); // { material_id: quantity }
  const [selectedSac, setSelectedSac] = useState(null);     // seçilen Saç material id
  const [selectedMdf, setSelectedMdf] = useState(null);     // seçilen MDF material id
  const [kategoriFilter, setKategoriFilter] = useState('');
  const [groupSearch, setGroupSearch] = useState('');

  // ─── State ───
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [expandedGroup, setExpandedGroup] = useState(null);

  // ─── Load initial data ───
  useEffect(() => {
    Promise.all([
      getProductGroups(),
      getMaterials(),
      getCostDefinitions(),
      getKargoOptions(),
    ]).then(([g, m, cdefs, k]) => {
      setGroups(g);
      setMaterials(m);
      setCostDefinitions(Array.isArray(cdefs) ? cdefs : []);
      setKargoOptionsRaw(k || []);
    }).catch(err => toast.error('Veri yüklenemedi'));
  }, []);

  // ─── Load children when group selected ───
  useEffect(() => {
    if (!selectedGroup) { setChildren([]); return; }
    getProducts({
      parent_name: selectedGroup.parent_name,
      page_size: 500,
    }).then(data => setChildren(data.products))
      .catch(() => {});
  }, [selectedGroup]);

  // ─── Derive unique size groups from children ───
  const sizeGroups = useMemo(() => {
    const map = {};
    for (const c of children) {
      const size = c.variation_size || '(boyutsuz)';
      if (!map[size]) {
        map[size] = {
          size,
          count: 0,
          en: c.en,
          boy: c.boy,
          min_en: c.en ?? null,
          max_en: c.en ?? null,
          min_boy: c.boy ?? null,
          max_boy: c.boy ?? null,
          alan_m2: c.alan_m2,
          items: [],
        };
      }
      map[size].count++;
      if (c.en != null) {
        map[size].min_en = map[size].min_en == null ? c.en : Math.min(map[size].min_en, c.en);
        map[size].max_en = map[size].max_en == null ? c.en : Math.max(map[size].max_en, c.en);
      }
      if (c.boy != null) {
        map[size].min_boy = map[size].min_boy == null ? c.boy : Math.min(map[size].min_boy, c.boy);
        map[size].max_boy = map[size].max_boy == null ? c.boy : Math.max(map[size].max_boy, c.boy);
      }
      map[size].items.push({
        child_sku: c.child_sku,
        child_name: c.child_name,
        variation_color: c.variation_color,
        alan_m2: c.alan_m2,
      });
    }
    return Object.values(map).sort((a, b) => (a.size > b.size ? 1 : -1));
  }, [children]);

  const nameGroups = useMemo(() => {
    const map = {};
    for (const c of children) {
      const name = (c.child_name || c.child_sku || '').trim();
      if (!name) continue;
      const tier = detectKaplamaTier(c.variation_color, c.child_name);
      const key = buildKaplamaGroupKey(name, tier);
      if (!map[key]) {
        map[key] = {
          key,
          name,
          tier,
          count: 0,
          sizes: new Set(),
          colors: new Set(),
        };
      }
      map[key].count++;
      map[key].sizes.add(c.variation_size || '(boyutsuz)');
      const color = (c.variation_color || '').trim();
      if (color) map[key].colors.add(color);
    }
    return Object.values(map)
      .map(v => ({
        ...v,
        sizes: Array.from(v.sizes).sort(),
        colors: Array.from(v.colors).sort(),
      }))
      .sort((a, b) => {
        if (a.name !== b.name) return a.name > b.name ? 1 : -1;
        const ao = KAPLAMA_TIER_ORDER[a.tier] ?? 99;
        const bo = KAPLAMA_TIER_ORDER[b.tier] ?? 99;
        return ao - bo;
      });
  }, [children]);

  // ─── Reset cost/weight map when size groups change ───
  useEffect(() => {
    setCostMap(prev => {
      const newMap = {};
      for (const sg of sizeGroups) {
        newMap[sg.size] = prev[sg.size] || '';
      }
      return newMap;
    });
    setKaplamaMap(prev => {
      const newMap = {};
      for (const sg of sizeGroups) {
        newMap[sg.size] = prev[sg.size] || '';
      }
      return newMap;
    });
    setWeightMap(prev => {
      const newMap = {};
      for (const sg of sizeGroups) {
        newMap[sg.size] = prev[sg.size] ?? '';
      }
      return newMap;
    });
  }, [sizeGroups]);

  useEffect(() => {
    setKaplamaNameMap(prev => {
      const next = {};
      for (const ng of nameGroups) {
        next[ng.key] = normalizeKaplamaSelection(prev[ng.key]);
      }
      return next;
    });
  }, [nameGroups]);

  // ─── Filtered groups ───
  const filteredGroups = groups.filter(g => {
    if (kategoriFilter && g.kategori !== kategoriFilter) return false;
    if (groupSearch) {
      const q = groupSearch.toLowerCase();
      return (
        (g.parent_name || '').toLowerCase().includes(q) ||
        (g.parent_name || '').toLowerCase().includes(q)
      );
    }
    return true;
  });

  // ─── Current step ───
  const allCostsMapped = sizeGroups.length > 0 && sizeGroups.every(sg => costMap[sg.size]);
  const allKaplamaMapped = nameGroups.length > 0 && nameGroups.every(ng => hasKaplamaSelection(kaplamaNameMap[ng.key]));
  const allWeightsMapped = sizeGroups.length > 0 && sizeGroups.every(sg => {
    const v = weightMap[sg.size];
    return v !== '' && v !== undefined && v !== null && Number(v) >= 0;
  });
  const allMappingsReady = allCostsMapped && allKaplamaMapped && allWeightsMapped;
  const step = !selectedGroup ? 1 : !allMappingsReady ? 2 : 3;

  // ─── Helpers ───
  const straforMat = materials.find(m => m.name.toLowerCase().includes('strafor'));
  const boyaMat = materials.find(m => {
    const n = m.name.toLocaleLowerCase('tr');
    return n.includes('boya') && n.includes('işçilik');
  });
  // Saç kalınlık seçenekleri (1.5mm, 2mm, 3mm, 4mm)
  const sacMaterials = materials.filter(m => m.name.toLowerCase().startsWith('saç'));
  const mdfMaterials = materials.filter(m => m.name.toLowerCase().startsWith('mdf'));
  // "Boya" (lt) filtreleme — görüntülenMEyecek
  const visibleMaterials = materials.filter(m => !(m.name === 'Boya' && m.unit === 'lt'));
  const extractKargoCode = (name) => {
    const m = String(name || '').match(/([A-Za-z])\s*-\s*(\d+[A-Za-z]?)/);
    if (!m) return null;
    return `${m[1].toUpperCase()}-${m[2].toUpperCase()}`;
  };
  const parseSizeDims = (sizeLabel) => {
    const raw = String(sizeLabel || '').toLowerCase().replace(/,/g, ".");
    const m = raw.match(/(\d+(?:\.\d+)?)\s*[x×*]\s*(\d+(?:\.\d+)?)/);
    if (!m) return null;
    const a = Number(m[1]);
    const b = Number(m[2]);
    if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
    return {
      long: Math.max(a, b),
      short: Math.min(a, b),
    };
  };

  const kargoByCode = useMemo(() => {
    const map = {};
    for (const k of kargoOptionsRaw || []) {
      if (k?.code) map[k.code] = k;
    }
    return map;
  }, [kargoOptionsRaw]);

  const activeCostDefs = useMemo(
    () => (costDefinitions || []).filter(d => Number(d?.is_active ?? 1) === 1),
    [costDefinitions]
  );
  const costNames = useMemo(
    () => activeCostDefs.map(d => d.name).filter(Boolean),
    [activeCostDefs]
  );

  const kargoCostOptions = useMemo(() => {
    const fromDefs = activeCostDefs
      .filter(d => d.category === 'kargo')
      .map(d => {
        const code = extractKargoCode(d.kargo_code || d.name);
        return {
          name: d.name,
          code,
          meta: code ? (kargoByCode[code] || null) : null,
        };
      })
      .filter(opt => opt.name);
    if (fromDefs.length > 0) return fromDefs;

    const items = [];
    for (const name of costNames) {
      const code = extractKargoCode(name);
      if (!code) continue;
      const meta = kargoByCode[code];
      if (!meta) continue;
      items.push({ name, code, meta });
    }
    return items;
  }, [activeCostDefs, costNames, kargoByCode]);

  const kargoCostNames = kargoCostOptions.map(x => x.name);
  const kargoCostSet = useMemo(() => new Set(kargoCostNames), [kargoCostNames]);
  const kaplamaCostNames = useMemo(() => {
    const fromDefs = activeCostDefs
      .filter(d => d.category === 'kaplama')
      .map(d => d.name)
      .filter(Boolean);
    if (fromDefs.length > 0) return fromDefs;
    return costNames.filter(n => !kargoCostSet.has(n));
  }, [activeCostDefs, costNames, kargoCostSet]);
  const kargoOptions = kargoCostOptions.length > 0 ? kargoCostOptions : costNames.map(name => ({ name, code: extractKargoCode(name), meta: null }));
  const kaplamaOptions = useMemo(() => {
    const base = kaplamaCostNames.length > 0 ? [...kaplamaCostNames] : [...costNames];
    for (const ng of nameGroups) {
      const s = kaplamaSuggestionByName?.[ng.key]?.cost_name || kaplamaSuggestionByName?.[ng.name]?.cost_name;
      if (s && !base.includes(s)) base.push(s);
    }
    return base;
  }, [kaplamaCostNames, costNames, nameGroups, kaplamaSuggestionByName]);
  const kaplamaOptionsByGroup = useMemo(() => {
    const out = {};
    for (const ng of nameGroups) {
      if (ng.tier === 'other') {
        out[ng.key] = kaplamaOptions;
        continue;
      }
      const preferred = [];
      const rest = [];
      for (const option of kaplamaOptions) {
        if (detectKaplamaTier(option) === ng.tier) preferred.push(option);
        else rest.push(option);
      }
      out[ng.key] = [...preferred, ...rest];
    }
    return out;
  }, [nameGroups, kaplamaOptions]);

  const getKaplamaSuggestion = (group) => {
    if (!group) return null;
    return kaplamaSuggestionByName?.[group.key] || kaplamaSuggestionByName?.[group.name] || null;
  };

  const kargoSuggestionBySize = useMemo(() => {
    const out = {};
    const tolCm = 0.5;
    if (!Array.isArray(kargoCostOptions) || kargoCostOptions.length === 0) return out;

    for (const sg of sizeGroups) {
      const parsedDims = parseSizeDims(sg.size);
      const fromProduct = (sg.max_en != null && sg.max_boy != null)
        ? {
            long: Math.max(Number(sg.max_en), Number(sg.max_boy)),
            short: Math.min(Number(sg.max_en), Number(sg.max_boy)),
          }
        : null;
      const longCandidates = [fromProduct?.long, parsedDims?.long].filter(Number.isFinite);
      const shortCandidates = [fromProduct?.short, parsedDims?.short].filter(Number.isFinite);
      if (longCandidates.length === 0 || shortCandidates.length === 0) continue;
      const productLong = Math.max(...longCandidates);
      const productShort = Math.max(...shortCandidates);

      const candidates = [];
      for (const opt of kargoCostOptions) {
        const maxLong = Number(opt.meta?.max_long);
        const maxShort = Number(opt.meta?.max_short);
        if (!Number.isFinite(maxLong) || !Number.isFinite(maxShort)) continue;
        const fits = productLong <= maxLong + tolCm && productShort <= maxShort + tolCm;
        if (!fits) continue;
        const area = maxLong * maxShort;
        candidates.push({
          ...opt,
          maxLong,
          maxShort,
          area,
          slackLong: maxLong - productLong,
          slackShort: maxShort - productShort,
        });
      }

      if (candidates.length === 0) continue;
      candidates.sort((a, b) => {
        if (a.area !== b.area) return a.area - b.area;
        if (a.slackLong !== b.slackLong) return a.slackLong - b.slackLong;
        return a.slackShort - b.slackShort;
      });
      out[sg.size] = candidates[0];
    }
    return out;
  }, [sizeGroups, kargoCostOptions]);

  const applyAutoCargoSuggestions = (overwrite = false) => {
    setCostMap(prev => {
      const next = { ...prev };
      let changed = false;
      for (const sg of sizeGroups) {
        const suggestion = kargoSuggestionBySize[sg.size];
        if (!suggestion?.name) continue;
        if (!overwrite && next[sg.size]) continue;
        if (next[sg.size] !== suggestion.name) {
          next[sg.size] = suggestion.name;
          changed = true;
        }
      }
      return changed ? next : prev;
    });
  };

  const applyAutoKaplamaNameSuggestions = (overwrite = false) => {
    setKaplamaNameMap(prev => {
      const next = { ...prev };
      let changed = false;
      for (const ng of nameGroups) {
        const suggestedName = getKaplamaSuggestion(ng)?.cost_name;
        if (!suggestedName) continue;
        const current = normalizeKaplamaSelection(next[ng.key]);
        if (!overwrite && current.length > 0) continue;
        const suggested = normalizeKaplamaSelection([suggestedName]);
        if (JSON.stringify(current) !== JSON.stringify(suggested)) {
          next[ng.key] = suggested;
          changed = true;
        }
      }
      return changed ? next : prev;
    });
  };

  useEffect(() => {
    if (!selectedGroup?.parent_name) {
      setKaplamaSuggestionByName({});
      return;
    }
    getKaplamaNameSuggestions(selectedGroup.parent_name)
      .then(res => setKaplamaSuggestionByName(res?.suggestions || {}))
      .catch(() => setKaplamaSuggestionByName({}));
  }, [selectedGroup]);

  useEffect(() => {
    if (!selectedGroup) return;
    if (sizeGroups.length === 0) return;
    const hasAnyManual = sizeGroups.some(sg => !!costMap[sg.size]);
    if (hasAnyManual) return;
    applyAutoCargoSuggestions(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedGroup, sizeGroups, kargoSuggestionBySize]);

  useEffect(() => {
    if (!selectedGroup) return;
    if (nameGroups.length === 0) return;
    const hasAnyManual = nameGroups.some(ng => hasKaplamaSelection(kaplamaNameMap[ng.key]));
    if (hasAnyManual) return;
    applyAutoKaplamaNameSuggestions(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedGroup, nameGroups, kaplamaSuggestionByName]);

  const handleApply = async () => {
    if (!selectedGroup || !allMappingsReady) {
      toast.error('Lütfen tüm boyut gruplarına kargo/ağırlık ve tüm ürün adı+renk gruplarına kaplama girin');
      return;
    }
    setLoading(true);
    setResult(null);
    try {
      const cleanWeightMap = {};
      for (const sg of sizeGroups) {
        const val = parseFloat(weightMap[sg.size]);
        if (!Number.isNaN(val) && val >= 0) {
          cleanWeightMap[sg.size] = val;
        }
      }

      const cleanKaplamaNameMap = {};
      for (const ng of nameGroups) {
        const selected = normalizeKaplamaSelection(kaplamaNameMap[ng.key]);
        if (selected.length > 0) {
          cleanKaplamaNameMap[ng.key] = selected;
        }
      }

      const res = await applyInheritance({
        parent_name: selectedGroup.parent_name,
        cost_map: costMap,
        kaplama_map: kaplamaMap,
        kaplama_name_map: cleanKaplamaNameMap,
        weight_map: cleanWeightMap,
        materials: materialInputs,
        sac_material_id: selectedSac || undefined,
        mdf_material_id: selectedMdf || undefined,
      });
      setResult(res);
      toast.success(`${res.children_updated} child güncellendi${res.children_skipped > 0 ? `, ${res.children_skipped} atlandı` : ''}`);
      onRefresh();
      const data = await getProducts({
        parent_name: selectedGroup.parent_name,
        page_size: 500,
      });
      setChildren(data.products);
    } catch (err) {
      toast.error('Uygulama hatası: ' + (err.response?.data?.detail || err.message));
    }
    setLoading(false);
  };

  const handleExportGroup = async () => {
    if (children.length === 0) return;
    try {
      await exportExcel(children.map(c => c.child_sku));
      toast.success('Excel indirildi');
    } catch (err) {
      toast.error('Export hatası');
    }
  };

  return (
    <div className="space-y-5">
      {/* Step Indicators */}
      <div className="flex items-center gap-2 flex-wrap">
        <StepBadge number={1} label="Parent Seç" active={step === 1} done={step > 1} />
        <ArrowRight className="w-4 h-4 text-gray-300" />
        <StepBadge number={2} label="Hammadde + Maliyet" active={step === 2} done={step > 2} />
        <ArrowRight className="w-4 h-4 text-gray-300" />
        <StepBadge number={3} label="Uygula" active={step === 3} done={!!result} />
      </div>

      {/* ═══════════ STEP 1: Select Parent ═══════════ */}
      <div className="bg-white rounded-xl border border-gray-200 p-5 shadow-sm">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <GitBranch className="w-5 h-5 text-indigo-600" />
            <h3 className="font-semibold text-gray-900">1. Parent Seçimi</h3>
          </div>
          <div className="flex gap-2">
            <select
              value={kategoriFilter}
              onChange={e => setKategoriFilter(e.target.value)}
              className="px-3 py-1.5 border border-gray-200 rounded-lg text-sm"
            >
              <option value="">Tüm Kategoriler</option>
              <option value="metal">Metal</option>
              <option value="ahsap">Ahşap</option>
            </select>
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
              <input
                type="text"
                value={groupSearch}
                onChange={e => setGroupSearch(e.target.value)}
                placeholder="Parent ara..."
                className="pl-8 pr-3 py-1.5 border border-gray-200 rounded-lg text-sm w-56 focus:outline-none focus:ring-2 focus:ring-indigo-500"
              />
            </div>
          </div>
        </div>

        {/* Selected parent badge */}
        {selectedGroup && (
          <div className="mb-3 flex items-center gap-3 p-3 bg-indigo-50 rounded-lg border border-indigo-200">
            <CheckCircle2 className="w-5 h-5 text-indigo-600" />
            <div className="flex-1">
              <span className="font-mono font-bold text-indigo-800">{selectedGroup.parent_name}</span>
            </div>
            <span className="text-sm text-indigo-600 font-medium">{selectedGroup.variant_count} varyant</span>
            <button
              onClick={() => {
                setSelectedGroup(null);
                setResult(null);
                setMaterialInputs({});
                setCostMap({});
                setKaplamaMap({});
                setKaplamaNameMap({});
                setWeightMap({});
                setSelectedSac(null);
                setSelectedMdf(null);
              }}
              className="text-indigo-400 hover:text-indigo-700 text-sm"
            >Değiştir</button>
          </div>
        )}

        {/* Group list */}
        {!selectedGroup && (
          <div className="max-h-80 overflow-y-auto border border-gray-100 rounded-lg divide-y divide-gray-100">
            {filteredGroups.length === 0 ? (
              <div className="p-6 text-center text-gray-400 text-sm">Sonuç bulunamadı</div>
            ) : filteredGroups.map(g => (
              <div key={`${g.parent_name}-${g.kategori}`}>
                <button
                  className="w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-gray-50 transition-colors"
                  onClick={() => {
                    if (expandedGroup === g.parent_name) {
                      setSelectedGroup(g);
                      setExpandedGroup(null);
                    } else {
                      setExpandedGroup(g.parent_name);
                    }
                  }}
                >
                  {expandedGroup === g.parent_name
                    ? <ChevronDown className="w-4 h-4 text-gray-400" />
                    : <ChevronRight className="w-4 h-4 text-gray-400" />}
                  <span className="font-medium text-gray-800 flex-1 truncate">{g.parent_name || '(isimsiz)'}</span>
                  <span className={`badge ${g.kategori === 'metal' ? 'badge-metal' : 'badge-ahsap'}`}>{g.kategori}</span>
                  <span className="text-xs text-gray-500">{g.sub_group_count} grp</span>
                  <span className="text-xs text-gray-400">{g.variant_count} ürün</span>
                  {g.min_alan != null && (
                    <span className="alan-value text-xs">
                      {g.min_alan.toFixed(4)}{g.min_alan !== g.max_alan ? `–${g.max_alan?.toFixed(4)}` : ''} m²
                    </span>
                  )}
                </button>
                {expandedGroup === g.parent_name && (
                  <div className="px-12 pb-3 flex items-center gap-3">
                    <span className="text-xs text-gray-500">
                      En: {g.min_en ?? '—'}{g.min_en !== g.max_en ? `–${g.max_en}` : ''} cm |
                      Boy: {g.min_boy ?? '—'}{g.min_boy !== g.max_boy ? `–${g.max_boy}` : ''} cm
                    </span>
                    {g.product_identifiers && (
                      <span className="text-[10px] text-gray-400 truncate max-w-[200px]" title={g.product_identifiers}>
                        {g.product_identifiers}
                      </span>
                    )}
                    <button
                      onClick={() => { setSelectedGroup(g); setExpandedGroup(null); }}
                      className="ml-auto px-3 py-1 bg-indigo-600 text-white rounded text-xs hover:bg-indigo-700"
                    >
                      Bu Parent'ı Seç
                    </button>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* ═══════════ STEP 2: Materials + Cost Category ═══════════ */}
      {selectedGroup && (
        <>
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
          {/* Materials input */}
          <div className="lg:col-span-2 bg-white rounded-xl border border-gray-200 p-5 shadow-sm">
            <div className="flex items-center gap-2 mb-4">
              <Hammer className="w-5 h-5 text-green-600" />
              <h3 className="font-semibold text-gray-900">2. Hammadde Değerleri</h3>
            </div>

            <div className="mb-3 p-2.5 bg-amber-50 rounded-lg border border-amber-200 text-xs text-amber-700 flex items-start gap-2">
              <AlertTriangle className="w-4 h-4 mt-0.5 shrink-0" />
              <div>
                <strong>Strafor</strong>, <strong>Boya + İşçilik</strong> ve seçilen <strong>Saç</strong> otomatik hesaplanır (child alan değerine göre).
              </div>
            </div>

            {/* Saç kalınlık seçici */}
            <div className="mb-3 flex items-center gap-3 p-3 bg-blue-50 rounded-lg border border-blue-200">
              <Ruler className="w-4 h-4 text-blue-600 shrink-0" />
              <span className="text-sm font-medium text-blue-800">Saç Kalınlığı:</span>
              <select
                value={selectedSac || ''}
                onChange={e => setSelectedSac(e.target.value ? parseInt(e.target.value) : null)}
                className="px-3 py-1.5 border border-blue-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="">Seçiniz (opsiyonel)...</option>
                {sacMaterials.map(m => (
                  <option key={m.id} value={m.id}>{m.name}</option>
                ))}
              </select>
              {selectedSac && (
                <span className="text-xs text-blue-600 italic flex items-center gap-1">
                  <Zap className="w-3 h-3" /> = Alan (m²)
                </span>
              )}
            </div>

            {/* MDF seçici */}
            <div className="mb-3 flex items-center gap-3 p-3 bg-emerald-50 rounded-lg border border-emerald-200">
              <Ruler className="w-4 h-4 text-emerald-600 shrink-0" />
              <span className="text-sm font-medium text-emerald-800">MDF:</span>
              <select
                value={selectedMdf || ''}
                onChange={e => setSelectedMdf(e.target.value ? parseInt(e.target.value) : null)}
                className="px-3 py-1.5 border border-emerald-300 rounded-lg text-sm bg-white focus:outline-none focus:ring-2 focus:ring-emerald-500"
              >
                <option value="">Seçiniz (opsiyonel)...</option>
                {mdfMaterials.map(m => (
                  <option key={m.id} value={m.id}>{m.name}</option>
                ))}
              </select>
              {selectedMdf && (
                <span className="text-xs text-emerald-600 italic flex items-center gap-1">
                  <Zap className="w-3 h-3" /> = Alan (m²)
                </span>
              )}
            </div>

            <div className="table-container max-h-96 overflow-y-auto">
              <table>
                <thead className="sticky top-0">
                  <tr>
                    <th>Hammadde</th>
                    <th>Birim</th>
                    <th className="w-36">Miktar</th>
                    <th className="w-20">Oto?</th>
                  </tr>
                </thead>
                <tbody>
                  {visibleMaterials.map(mat => {
                    const isAuto = mat.id === straforMat?.id || mat.id === boyaMat?.id || mat.id === selectedSac || mat.id === selectedMdf;
                    const autoLabel = mat.id === straforMat?.id
                      ? 'Alan × 1.2'
                      : mat.id === boyaMat?.id
                      ? 'Alan × 5'
                      : (mat.id === selectedSac || mat.id === selectedMdf)
                      ? '= Alan'
                      : null;

                    return (
                      <tr key={mat.id} className={isAuto ? 'bg-blue-50/40' : ''}>
                        <td className="font-medium text-sm">{mat.name}</td>
                        <td><span className="badge bg-gray-100 text-gray-600">{mat.unit}</span></td>
                        <td>
                          {isAuto ? (
                            <span className="text-xs text-blue-600 italic">{autoLabel}</span>
                          ) : (
                            <input
                              type="number"
                              step="0.0001"
                              value={materialInputs[mat.id] ?? ''}
                              onChange={e => setMaterialInputs(prev => ({
                                ...prev,
                                [mat.id]: e.target.value === '' ? undefined : parseFloat(e.target.value),
                              }))}
                              placeholder="0"
                              className="w-full px-2 py-1 border border-gray-200 rounded text-sm font-mono focus:outline-none focus:ring-2 focus:ring-green-500"
                            />
                          )}
                        </td>
                        <td className="text-center">
                          {isAuto && (
                            <span className="inline-flex items-center gap-1 text-xs text-blue-600">
                              <Zap className="w-3 h-3" /> Oto
                            </span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>

          {/* Size-based Cost Mapping */}
          <div className="bg-white rounded-xl border border-gray-200 p-5 shadow-sm space-y-5">
            <div className="flex items-center gap-2">
              <Box className="w-5 h-5 text-orange-600" />
              <h3 className="font-semibold text-gray-900">Boyut → Kargo</h3>
              <button
                type="button"
                onClick={() => applyAutoCargoSuggestions(false)}
                className="ml-auto px-2 py-1 text-[11px] rounded border border-blue-200 text-blue-700 hover:bg-blue-50"
                title="Boş kargo alanlarını otomatik öneriyle doldurur"
              >
                Kargoyu Oto Doldur
              </button>
            </div>

            {sizeGroups.length <= 1 && (
              <p className="text-xs text-gray-500">
                Bu parent'ın altında tek boyut grubu var.
              </p>
            )}
            {sizeGroups.length > 1 && (
              <div className="p-2.5 bg-orange-50 rounded-lg border border-orange-200 text-xs text-orange-700">
                Bu parent'ın altında <strong>{sizeGroups.length} farklı boyut</strong> var.
                Her boyut grubuna ayrı kargo maliyeti ve kargo ağırlık girin.
              </div>
            )}

            {/* Per-size selectors */}
            <div className="space-y-3">
              {sizeGroups.map(sg => {
                const hasCost = !!costMap[sg.size];
                const hasWeight = weightMap[sg.size] !== '' && weightMap[sg.size] !== undefined && Number(weightMap[sg.size]) >= 0;
                const ready = hasCost && hasWeight;
                return (
                <div key={sg.size} className={`rounded-lg border p-3 transition-colors ${
                  ready ? 'border-green-200 bg-green-50/30' : 'border-gray-200 bg-gray-50'
                }`}>
                  <div className="flex items-center gap-2 mb-2">
                    <Ruler className="w-4 h-4 text-gray-500" />
                    <span className="font-mono font-bold text-gray-800">{sg.size}</span>
                    <span className="text-[10px] text-gray-400 ml-auto">{sg.count} renk</span>
                  </div>
                  <div className="flex items-center gap-2 text-xs text-gray-500 mb-2">
                    <span>Alan: <strong className="alan-value text-[10px]">{sg.alan_m2 != null ? sg.alan_m2.toFixed(4) : '—'} m²</strong></span>
                  </div>
                  <select
                    value={costMap[sg.size] || ''}
                    onChange={e => setCostMap(prev => ({ ...prev, [sg.size]: e.target.value }))}
                    className={`w-full px-2 py-1.5 border rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-orange-500 ${
                      costMap[sg.size] ? 'border-green-300 bg-white' : 'border-orange-300 bg-white'
                    }`}
                  >
                    <option value="">Kargo maliyeti seçiniz...</option>
                    {kargoOptions.map(opt => (
                      <option key={opt.name} value={opt.name}>
                        {opt.name}
                        {opt.meta?.max_long != null && opt.meta?.max_short != null
                          ? ` (max ${opt.meta.max_long}x${opt.meta.max_short})`
                          : ''}
                      </option>
                    ))}
                  </select>
                  {costMap[sg.size] && (
                    <div className="mt-1.5 text-[10px] text-green-600 flex items-center gap-1">
                      <CheckCircle2 className="w-3 h-3" />
                      Kargo: {costMap[sg.size]}
                    </div>
                  )}
                  {kargoSuggestionBySize[sg.size] && (
                    <div className="mt-1 text-[10px] text-blue-700">
                      Oto öneri: {kargoSuggestionBySize[sg.size].name}
                      {' '}(
                      {Number(kargoSuggestionBySize[sg.size].maxLong).toFixed(1)}x
                      {Number(kargoSuggestionBySize[sg.size].maxShort).toFixed(1)})
                    </div>
                  )}
                  {!kargoSuggestionBySize[sg.size] && (sg.max_en != null && sg.max_boy != null) && (
                    <div className="mt-1 text-[10px] text-amber-700">
                      Bu boyut için otomatik uygun kargo bulunamadı, manuel seçim yapın.
                    </div>
                  )}

                  <div className="mt-2">
                    <label className="block text-[11px] text-gray-600 mb-1">Kargo Ağırlık (kg)</label>
                    <input
                      type="number"
                      step="0.001"
                      min="0"
                      value={weightMap[sg.size] ?? ''}
                      onChange={e => setWeightMap(prev => ({
                        ...prev,
                        [sg.size]: e.target.value === '' ? '' : parseFloat(e.target.value),
                      }))}
                      placeholder="örn: 1.25"
                      className={`w-full px-2 py-1.5 border rounded-lg text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-500 ${
                        hasWeight ? 'border-green-300 bg-white' : 'border-blue-300 bg-white'
                      }`}
                    />
                    {hasWeight && (
                      <div className="mt-1.5 text-[10px] text-blue-600 flex items-center gap-1">
                        <CheckCircle2 className="w-3 h-3" />
                        {Number(weightMap[sg.size]).toFixed(3)} kg
                      </div>
                    )}
                  </div>
                </div>
              )})}
            </div>

            {/* Child breakdown */}
            <div>
              <h4 className="text-xs font-semibold text-gray-500 uppercase mb-2">
                Tüm Child'lar ({children.length})
              </h4>
              <div className="max-h-48 overflow-y-auto space-y-1">
                {sizeGroups.map(sg => (
                  <div key={sg.size}>
                    <div className="text-[10px] font-semibold text-gray-400 mt-1.5 mb-0.5 border-b border-gray-100 pb-0.5">{sg.size} ({sg.count})</div>
                    {sg.items.map((item, i) => (
                      <div key={i} className="flex items-center gap-2 text-xs p-1 rounded hover:bg-gray-50" title={item.child_sku}>
                        <span className="text-gray-700 truncate flex-1">{item.child_name || item.child_sku}</span>
                        <span className="ml-auto alan-value text-[10px] shrink-0">
                          {item.alan_m2 != null ? item.alan_m2.toFixed(4) : '—'} m²
                        </span>
                      </div>
                    ))}
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-5 shadow-sm">
          <div className="flex items-center gap-2 mb-3">
            <Package className="w-5 h-5 text-emerald-600" />
            <h3 className="font-semibold text-gray-900">Ürün Adı + Renk → Kaplama</h3>
            <span className="text-[11px] text-gray-500">{nameGroups.length} grup</span>
            <button
              type="button"
              onClick={() => applyAutoKaplamaNameSuggestions(false)}
              className="ml-auto px-3 py-1.5 text-xs rounded border border-emerald-200 text-emerald-700 hover:bg-emerald-50"
              title="Boş kaplama alanlarını otomatik öneriyle doldurur"
            >
              Kaplamayı Oto Doldur
            </button>
          </div>
          <p className="text-xs text-gray-500 mb-3">
            Bu alanda bir ürün grubu için birden fazla kaplama seçebilirsin.
          </p>

          <div className="grid grid-cols-1 xl:grid-cols-2 gap-3 max-h-[460px] overflow-y-auto pr-1">
            {nameGroups.map(ng => {
              const selectedValues = normalizeKaplamaSelection(kaplamaNameMap[ng.key]);
              const selectedSet = new Set(selectedValues.map(v => v.toLocaleLowerCase('tr')));
              const suggested = getKaplamaSuggestion(ng);
              const options = kaplamaOptionsByGroup[ng.key] || kaplamaOptions;
              const suggestedKey = String(suggested?.cost_name || '').toLocaleLowerCase('tr');
              const hasSuggested = !!suggestedKey && selectedSet.has(suggestedKey);

              return (
                <div key={ng.key} className="border border-gray-200 rounded-lg p-3 bg-gray-50">
                  <div className="flex items-center gap-2 mb-1.5">
                    <span className="text-sm font-medium text-gray-800 truncate" title={ng.name}>{ng.name}</span>
                    <span className={`text-[10px] px-1.5 py-0.5 rounded ${
                      ng.tier === 'silver'
                        ? 'bg-slate-100 text-slate-700'
                        : ng.tier === 'gold_copper'
                          ? 'bg-amber-100 text-amber-700'
                          : 'bg-gray-100 text-gray-600'
                    }`}>
                      {kaplamaTierLabel(ng.tier)}
                    </span>
                    <span className="text-[10px] text-gray-500 ml-auto">{ng.count} ürün</span>
                  </div>

                  {ng.colors?.length > 0 && (
                    <div className="text-[11px] text-gray-500 mb-2 truncate" title={ng.colors.join(', ')}>
                      Renk: {ng.colors.join(', ')}
                    </div>
                  )}

                  <div className={`w-full border rounded-lg p-2.5 bg-white ${
                    selectedValues.length > 0 ? 'border-green-300' : 'border-emerald-300'
                  }`}>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-1.5 max-h-40 overflow-y-auto pr-1">
                      {options.map(name => {
                        const checked = selectedSet.has(String(name).toLocaleLowerCase('tr'));
                        return (
                          <label key={`${ng.key}-${name}`} className="flex items-center gap-2 text-sm text-gray-700 cursor-pointer">
                            <input
                              type="checkbox"
                              checked={checked}
                              onChange={e => {
                                const isChecked = !!e.target.checked;
                                setKaplamaNameMap(prev => ({
                                  ...prev,
                                  [ng.key]: toggleKaplamaSelection(prev[ng.key], name, isChecked),
                                }));
                              }}
                              className="rounded border-gray-300 text-emerald-600 focus:ring-emerald-500"
                            />
                            <span className="truncate" title={name}>{name}</span>
                          </label>
                        );
                      })}
                    </div>
                    {selectedValues.length === 0 && (
                      <div className="mt-2 text-xs text-emerald-700">Kaplama seçiniz...</div>
                    )}
                  </div>

                  {selectedValues.length > 0 && (
                    <div className="mt-2 flex flex-wrap gap-1.5">
                      {selectedValues.map(name => (
                        <span key={`${ng.key}-selected-${name}`} className="badge bg-emerald-100 text-emerald-700 text-[10px]">
                          {name}
                        </span>
                      ))}
                    </div>
                  )}

                  {suggested?.cost_name && (
                    <div className="mt-2 flex items-center gap-2 text-xs text-emerald-700">
                      <span>
                        Oto öneri: {suggested.cost_name}
                        {suggested.confidence ? ` (${suggested.confidence})` : ''}
                      </span>
                      {!hasSuggested && (
                        <button
                          type="button"
                          onClick={() => setKaplamaNameMap(prev => ({
                            ...prev,
                            [ng.key]: toggleKaplamaSelection(prev[ng.key], suggested.cost_name, true),
                          }))}
                          className="px-2 py-0.5 rounded border border-emerald-200 text-emerald-700 hover:bg-emerald-50"
                        >
                          Ekle
                        </button>
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
        </>
      )}

      {/* ═══════════ STEP 3: Apply Button + Results ═══════════ */}
      {selectedGroup && allMappingsReady && (
        <div className="bg-white rounded-xl border border-gray-200 p-5 shadow-sm">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <Zap className="w-5 h-5 text-purple-600" />
              <h3 className="font-semibold text-gray-900">3. Uygula</h3>
            </div>
            <div className="flex gap-2">
              {result && (
                <button
                  onClick={handleExportGroup}
                  className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 text-sm"
                >
                  <Download className="w-4 h-4" />
                  Export ({children.length})
                </button>
              )}
              <button
                onClick={handleApply}
                disabled={loading}
                className="flex items-center gap-2 px-6 py-2.5 bg-purple-600 text-white rounded-lg hover:bg-purple-700 text-sm font-medium disabled:opacity-50 transition-colors"
              >
                {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Zap className="w-4 h-4" />}
                {loading ? 'Uygulanıyor...' : `${children.length} Child'a Uygula`}
              </button>
            </div>
          </div>

          {/* Summary */}
          <div className="grid grid-cols-2 md:grid-cols-6 gap-3 mb-4 text-sm">
            <div className="p-3 bg-gray-50 rounded-lg">
              <div className="text-xs text-gray-500">Parent</div>
              <div className="font-mono font-bold">{selectedGroup.parent_name}</div>
            </div>
            <div className="p-3 bg-gray-50 rounded-lg">
              <div className="text-xs text-gray-500">Boyut Grupları</div>
              <div className="font-bold">{sizeGroups.length} boyut</div>
            </div>
            <div className="p-3 bg-gray-50 rounded-lg">
              <div className="text-xs text-gray-500">Hammadde Girişi</div>
              <div className="font-bold">{Object.values(materialInputs).filter(v => v > 0).length} kalem</div>
            </div>
            <div className="p-3 bg-gray-50 rounded-lg">
              <div className="text-xs text-gray-500">Kaplama (Ürün+Renk)</div>
              <div className="font-bold">
                {nameGroups.filter(ng => hasKaplamaSelection(kaplamaNameMap[ng.key])).length} / {nameGroups.length}
              </div>
            </div>
            <div className="p-3 bg-gray-50 rounded-lg">
              <div className="text-xs text-gray-500">Ağırlık Girişi</div>
              <div className="font-bold">
                {sizeGroups.filter(sg => {
                  const v = weightMap[sg.size];
                  return v !== '' && v !== undefined && v !== null && Number(v) >= 0;
                }).length} / {sizeGroups.length}
              </div>
            </div>
            <div className="p-3 bg-gray-50 rounded-lg">
              <div className="text-xs text-gray-500">Oto-Hesaplanan</div>
              <div className="font-bold text-blue-600">Strafor + Boya</div>
            </div>
          </div>

          {/* Cost map summary */}
          <div className="mb-4 flex flex-wrap gap-2">
            {sizeGroups.map(sg => (
              <div key={sg.size} className="flex items-center gap-2 px-3 py-1.5 bg-orange-50 border border-orange-200 rounded-lg text-xs">
                <span className="font-mono font-bold text-orange-800">{sg.size}</span>
                <ArrowRight className="w-3 h-3 text-orange-400" />
                <span className="text-orange-700">Kargo: {costMap[sg.size]}</span>
                <span className="text-orange-500">Ağırlık: {Number(weightMap[sg.size]).toFixed(3)} kg</span>
                <span className="text-orange-400">({sg.count})</span>
              </div>
            ))}
          </div>

          <div className="mb-4 flex flex-wrap gap-2">
            {nameGroups.map(ng => (
              <div key={ng.key} className="flex items-center gap-2 px-3 py-1.5 bg-emerald-50 border border-emerald-200 rounded-lg text-xs">
                <span className="font-medium text-emerald-800 truncate max-w-[220px]" title={ng.name}>{ng.name}</span>
                <span className="text-[10px] text-emerald-600">[{kaplamaTierLabel(ng.tier)}]</span>
                <ArrowRight className="w-3 h-3 text-emerald-400" />
                <span className="text-emerald-700">Kaplama: {normalizeKaplamaSelection(kaplamaNameMap[ng.key]).join(', ') || '—'}</span>
                <span className="text-emerald-500">({ng.count})</span>
              </div>
            ))}
          </div>

          {/* Result table */}
          {result && (
            <div className="mt-4">
              <div className="flex items-center gap-2 mb-3">
                <CheckCircle2 className="w-5 h-5 text-green-600" />
                <span className="text-sm font-medium text-green-700">
                  {result.children_updated} child güncellendi
                  {result.children_skipped > 0 && (
                    <span className="text-amber-600 ml-2">({result.children_skipped} atlandı)</span>
                  )}
                </span>
              </div>
              <div className="table-container max-h-72 overflow-y-auto">
                <table>
                  <thead className="sticky top-0">
                    <tr>
                      <th>Child SKU</th>
                      <th>Boyut</th>
                      <th>Kargo</th>
                      <th>Kaplama</th>
                      <th>Kargo Kodu</th>
                      <th>Kargo Ölçü (E×B×Y)</th>
                      <th>Ağırlık</th>
                      <th>Desi</th>
                      <th>Alan (m²)</th>
                      <th>Saç (=Alan)</th>
                      <th>MDF (=Alan)</th>
                      <th>Strafor (×1.2)</th>
                      <th>Boya+İşçilik (×5)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.details.map(d => (
                      <tr key={d.child_sku}>
                        <td className="font-mono text-xs">{d.child_sku}</td>
                        <td className="text-xs font-mono">{d.variation_size || '—'}</td>
                        <td>
                          <span className="badge bg-orange-100 text-orange-700 text-[10px]">{d.kargo_cost_name || d.cost_name}</span>
                        </td>
                        <td>
                          <span className="badge bg-emerald-100 text-emerald-700 text-[10px]">
                            {normalizeKaplamaSelection(d.kaplama_cost_names ?? d.kaplama_cost_name).join(', ') || '—'}
                          </span>
                        </td>
                        <td className="font-mono text-xs">{d.kargo_kodu || '—'}</td>
                        <td className="font-mono text-xs">
                          {d.kargo_en != null && d.kargo_boy != null && d.kargo_yukseklik != null
                            ? `${d.kargo_en}×${d.kargo_boy}×${d.kargo_yukseklik}`
                            : '—'}
                        </td>
                        <td className="font-mono text-xs">
                          {d.kargo_agirlik != null ? Number(d.kargo_agirlik).toFixed(3) : '—'}
                        </td>
                        <td className="font-mono text-xs font-semibold text-indigo-700">
                          {d.kargo_desi != null ? Number(d.kargo_desi).toFixed(3) : '—'}
                        </td>
                        <td>
                          <span className="alan-value text-xs">
                            {d.alan_m2 != null ? d.alan_m2.toFixed(4) : '—'}
                          </span>
                        </td>
                        <td className="font-mono text-sm">
                          {d.sac != null ? d.sac.toFixed(4) : '—'}
                        </td>
                        <td className="font-mono text-sm">
                          {d.mdf != null ? d.mdf.toFixed(4) : '—'}
                        </td>
                        <td className="font-mono text-sm">
                          {d.strafor != null ? d.strafor.toFixed(4) : '—'}
                        </td>
                        <td className="font-mono text-sm">
                          {d.boya_iscilik != null ? d.boya_iscilik.toFixed(4) : '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {result.skipped?.length > 0 && (
                <div className="mt-3 p-3 bg-amber-50 rounded-lg border border-amber-200 text-xs text-amber-700">
                  <strong>Atlanan ürünler:</strong>
                  {result.skipped.map(s => (
                    <div key={s.child_sku} className="ml-2">
                      {s.child_sku} — boyut: {s.variation_size || '(boş)'} — {s.reason}
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
