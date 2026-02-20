"""
Maliyet Sistemi - FastAPI Backend
Ana API modülü.
"""

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
from starlette.background import BackgroundTask
from typing import Optional
import os
import csv
import re
import time
import json
import math
import uuid
import threading
import contextvars
import base64
import hmac
import hashlib
import secrets
import logging
import traceback
from pathlib import Path
from collections import defaultdict

logger = logging.getLogger("maliyet")
logging.basicConfig(level=logging.INFO)

from database import (
    get_db, init_db, load_mapped_products, load_default_materials, load_cost_names,
    sync_cost_definitions_from_template, list_cost_definitions,
    canonicalize_kaplama_cost_name, normalize_legacy_gold_silver_names,
    deactivate_shadowed_kaplama_base_names, deactivate_cus_products,
    normalize_product_categories, get_supported_categories,
    DB_BACKEND, DATABASE_URL,
    IntegrityError as DBIntegrityError,
)
from models import (
    ProductResponse, RawMaterialResponse, RawMaterialUpdate, RawMaterialCreate,
    ProductMaterialEntry, ProductMaterialBulk, ProductCostAssignment,
    ExportRequest, StatsResponse, ParentInheritanceRequest,
    ProductSyncRequest, ApprovalReviewRequest,
    CostDefinitionCreate, CostDefinitionUpdate,
    AuthLoginRequest, AuthChangePasswordRequest, AuthUserCreate, AuthUserUpdate,
)
from excel_engine import export_to_template, get_template_structure
from storage_utils import is_http_url, cache_remote_file

app = FastAPI(
    title="Maliyet Sistemi API",
    description="ERP Maliyet Şablonu Yönetim Sistemi",
    version="1.0.0",
)

REQUEST_AUDIT_CONTEXT: contextvars.ContextVar[dict] = contextvars.ContextVar(
    "request_audit_context",
    default={},
)


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


APP_ENV = (os.getenv("APP_ENV") or os.getenv("ENV") or "development").strip().lower()
IS_PRODUCTION = APP_ENV in {"prod", "production"}
IS_VERCEL = env_flag("VERCEL", default=False)

def parse_cors_origins() -> list[str]:
    raw = os.getenv("CORS_ORIGINS", "").strip()
    if raw:
        return [x.strip() for x in raw.split(",") if x.strip()]
    if IS_PRODUCTION:
        return [
            "https://iwamaliyet.vercel.app",
        ]
    return [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ]


ALLOWED_CORS_ORIGINS = parse_cors_origins()
ENABLE_RELOAD_DB = env_flag("ENABLE_RELOAD_DB", default=not IS_PRODUCTION)
ENABLE_PRODUCT_SYNC = env_flag("ENABLE_PRODUCT_SYNC", default=True)
ENABLE_APPROVAL_WORKFLOW = env_flag("ENABLE_APPROVAL_WORKFLOW", default=False)
SEED_DEFAULT_USERS = env_flag("SEED_DEFAULT_USERS", default=True)
# Vercel serverless cold-start'ta ağır bootstrap işlemleri timeout'a yol açabileceği için
# varsayılanı kapatıyoruz. Gerekirse env ile explicit true verilebilir.
ENABLE_STARTUP_DATA_BOOTSTRAP = env_flag(
    "ENABLE_STARTUP_DATA_BOOTSTRAP",
    default=(not IS_PRODUCTION and not IS_VERCEL),
)
ENABLE_STARTUP_TEMPLATE_SYNC = env_flag(
    "ENABLE_STARTUP_TEMPLATE_SYNC",
    default=(not IS_PRODUCTION and not IS_VERCEL),
)
PRODUCT_GROUPS_CACHE_TTL_SECONDS = max(0, int(os.getenv("PRODUCT_GROUPS_CACHE_TTL_SECONDS", "45")))
PRODUCT_GROUPS_CACHE_MAX_ITEMS = max(10, int(os.getenv("PRODUCT_GROUPS_CACHE_MAX_ITEMS", "256")))
_product_groups_cache: dict[str, tuple[float, dict]] = {}
_product_groups_cache_lock = threading.Lock()

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

KARGO_CSV_CANDIDATES = [
    Path(__file__).resolve().parents[2] / "kargo.csv",
    Path(__file__).resolve().parents[1] / "kargo.csv",
    Path(__file__).resolve().parent / "kargo.csv",
]
KARGO_CODE_PATTERN = re.compile(r"([A-Z])\s*-\s*(\d+[A-Z]?)", re.I)
TOKEN_PATTERN = re.compile(r"[a-z0-9çğıöşü]+", re.I)
KAPLAMA_STOP_TOKENS = {
    "cm", "x", "adet", "li", "ve", "ile", "icin", "için",
    "metal", "ahsap", "ahşap", "cam", "boyali", "boyalı", "kaplama",
}
KAPLAMA_SILVER_TOKENS = {"silver", "gumus", "gümüş", "gümus"}
KAPLAMA_GOLD_COPPER_TOKENS = {
    "gold", "altin", "altın",
    "copper", "bakir", "bakır",
    "bronze", "pirinc", "pirinç",
    "rosegold",
}
AUTH_SECRET = os.getenv("AUTH_SECRET", "maliyet-dev-secret-change-me")
AUTH_TOKEN_TTL_SECONDS = int(os.getenv("AUTH_TOKEN_TTL_SECONDS", "43200"))  # 12 saat
AUTH_HASH_ITERATIONS = int(os.getenv("AUTH_HASH_ITERATIONS", "120000"))
WEAK_AUTH_SECRETS = {
    "",
    "maliyet-dev-secret-change-me",
    "change-me",
    "changeme",
    "secret",
}

PUBLIC_API_PATHS = {
    "/api/auth/login",
    "/api/health",
}

ADMIN_ONLY_RULES: list[tuple[str, str]] = [
    ("POST", "/api/materials"),
    ("PUT", "/api/materials/"),
    ("DELETE", "/api/materials/"),
    ("POST", "/api/cost-definitions"),
    ("PUT", "/api/cost-definitions/"),
    ("DELETE", "/api/cost-definitions/"),
    ("POST", "/api/reload-db"),
    ("POST", "/api/sync-products"),
    ("POST", "/api/products/deactivate-cus"),
    ("GET", "/api/quality/report"),
    ("GET", "/api/approvals"),
    ("POST", "/api/approvals/"),
    ("GET", "/api/auth/users"),
    ("POST", "/api/auth/users"),
    ("PUT", "/api/auth/users/"),
    ("DELETE", "/api/auth/users/"),
    ("GET", "/api/auth/audit-logs"),
]


def build_product_groups_cache_key(
    kategori: Optional[str],
    search: Optional[str],
    page: int,
    page_size: int,
) -> str:
    return json.dumps(
        {
            "kategori": (kategori or "").strip().lower(),
            "search": (search or "").strip().lower(),
            "page": int(page),
            "page_size": int(page_size),
        },
        ensure_ascii=False,
        sort_keys=True,
    )


def get_product_groups_cache(cache_key: str) -> dict | None:
    if PRODUCT_GROUPS_CACHE_TTL_SECONDS <= 0:
        return None
    now = time.time()
    with _product_groups_cache_lock:
        cached = _product_groups_cache.get(cache_key)
        if not cached:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            _product_groups_cache.pop(cache_key, None)
            return None
        return payload


def set_product_groups_cache(cache_key: str, payload: dict):
    if PRODUCT_GROUPS_CACHE_TTL_SECONDS <= 0:
        return
    now = time.time()
    expires_at = now + PRODUCT_GROUPS_CACHE_TTL_SECONDS
    with _product_groups_cache_lock:
        _product_groups_cache[cache_key] = (expires_at, payload)
        expired = [key for key, (exp, _) in _product_groups_cache.items() if exp <= now]
        for key in expired:
            _product_groups_cache.pop(key, None)
        overflow = len(_product_groups_cache) - PRODUCT_GROUPS_CACHE_MAX_ITEMS
        if overflow > 0:
            oldest_keys = list(_product_groups_cache.keys())[:overflow]
            for key in oldest_keys:
                _product_groups_cache.pop(key, None)


def invalidate_product_groups_cache():
    with _product_groups_cache_lock:
        _product_groups_cache.clear()


def validate_runtime_security():
    if not IS_PRODUCTION:
        return
    secret = str(AUTH_SECRET or "").strip()
    if secret in WEAK_AUTH_SECRETS or len(secret) < 32:
        logger.warning(
            "AUTH_SECRET production için zayıf görünüyor. En az 32 karakter güçlü secret verin."
        )


def resolve_kargo_csv_path() -> Path:
    csv_env_path = os.getenv("KARGO_CSV_PATH", "").strip()
    csv_env_url = os.getenv("KARGO_CSV_URL", "").strip()

    url = csv_env_url or (csv_env_path if is_http_url(csv_env_path) else "")
    if url:
        cached = cache_remote_file(
            url,
            cache_name="kargo.csv",
            ttl_seconds=int(os.getenv("REMOTE_FILE_CACHE_TTL", "900")),
        )
        if cached and cached.exists():
            return cached

    if csv_env_path and not is_http_url(csv_env_path):
        p = Path(csv_env_path).expanduser()
        if p.exists():
            return p

    for p in KARGO_CSV_CANDIDATES:
        if p.exists():
            return p
    return KARGO_CSV_CANDIDATES[0]


def normalize_kargo_code(value: str | None) -> str | None:
    """M- 13, m-13 gibi formatları M-13 formatına normalize eder."""
    if not value:
        return None
    m = KARGO_CODE_PATTERN.search(str(value).upper())
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}"


def tokenize_text(value: str | None) -> set[str]:
    if not value:
        return set()
    raw = str(value).lower()
    tokens = TOKEN_PATTERN.findall(raw)
    return {t for t in tokens if len(t) > 1 and t not in KAPLAMA_STOP_TOKENS}


def detect_kaplama_tier(*values: str | None) -> str:
    tokens: set[str] = set()
    for value in values:
        tokens.update(tokenize_text(value))
    if tokens & KAPLAMA_GOLD_COPPER_TOKENS:
        return "gold_copper"
    if tokens & KAPLAMA_SILVER_TOKENS:
        return "silver"
    return "other"


def build_kaplama_group_key(name: str | None, tier: str | None) -> str:
    normalized_name = (name or "").strip()
    normalized_tier = (tier or "other").strip().lower() or "other"
    if not normalized_name:
        return ""
    return f"{normalized_name}||{normalized_tier}"


def normalize_cost_name_list(value, canonicalize_kaplama: bool = False) -> list[str]:
    """
    Tek string veya listeyi normalize edilmiş, duplicate'siz cost_name listesine çevirir.
    """
    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        raw_values = value
    else:
        raw_values = [value]

    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        name = str(raw or "").strip()
        if not name:
            continue
        if canonicalize_kaplama:
            name = canonicalize_kaplama_cost_name(name)
            if not name:
                continue
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(raw: str) -> bytes:
    padding = "=" * ((4 - len(raw) % 4) % 4)
    return base64.urlsafe_b64decode(raw + padding)


def hash_password(password: str, salt: str | None = None) -> str:
    password_raw = str(password or "").encode("utf-8")
    salt_raw = (salt or secrets.token_hex(16)).encode("utf-8")
    digest = hashlib.pbkdf2_hmac("sha256", password_raw, salt_raw, AUTH_HASH_ITERATIONS)
    return f"pbkdf2_sha256${AUTH_HASH_ITERATIONS}${salt_raw.decode('utf-8')}${digest.hex()}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algo, iter_text, salt, digest_hex = str(password_hash).split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        iterations = int(iter_text)
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            str(password or "").encode("utf-8"),
            salt.encode("utf-8"),
            iterations,
        )
        return hmac.compare_digest(digest.hex(), digest_hex)
    except Exception:
        return False


def generate_auth_token(user_id: int, username: str, role: str) -> str:
    payload = {
        "uid": int(user_id),
        "sub": str(username),
        "role": str(role),
        "iat": int(time.time()),
        "exp": int(time.time()) + AUTH_TOKEN_TTL_SECONDS,
    }
    payload_text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    payload_b64 = _b64url_encode(payload_text.encode("utf-8"))
    signature = hmac.new(AUTH_SECRET.encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    return f"{payload_b64}.{_b64url_encode(signature)}"


def decode_auth_token(token: str) -> dict | None:
    try:
        payload_b64, sig_b64 = str(token).split(".", 1)
    except ValueError:
        return None

    expected_sig = hmac.new(
        AUTH_SECRET.encode("utf-8"),
        payload_b64.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    try:
        given_sig = _b64url_decode(sig_b64)
    except Exception:
        return None
    if not hmac.compare_digest(expected_sig, given_sig):
        return None

    try:
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
    except Exception:
        return None

    if int(payload.get("exp", 0)) < int(time.time()):
        return None
    if not payload.get("sub") or not payload.get("role"):
        return None
    return payload


def serialize_user(row) -> dict | None:
    if not row:
        return None
    return {
        "id": int(row["id"]),
        "username": row["username"],
        "role": row["role"],
        "is_active": bool(row["is_active"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def row_first_value(row):
    if row is None:
        return None
    if isinstance(row, dict):
        for v in row.values():
            return v
        return None
    try:
        return row[0]
    except Exception:
        pass
    try:
        values = list(getattr(row, "values")())
        return values[0] if values else None
    except Exception:
        return None


def get_user_by_username(username: str):
    conn = get_db()
    try:
        return conn.execute(
            """
            SELECT id, username, password_hash, role, is_active, created_at, updated_at
            FROM users
            WHERE lower(username) = lower(?)
            """,
            (username,),
        ).fetchone()
    finally:
        conn.close()


def get_user_by_id(user_id: int):
    conn = get_db()
    try:
        return conn.execute(
            """
            SELECT id, username, password_hash, role, is_active, created_at, updated_at
            FROM users
            WHERE id = ?
            """,
            (user_id,),
        ).fetchone()
    finally:
        conn.close()


def write_audit_log(
    user: dict | None,
    action: str,
    target: str | None = None,
    details: dict | None = None,
    status: str = "ok",
):
    try:
        ctx = REQUEST_AUDIT_CONTEXT.get({})
        conn = get_db()
        conn.execute(
            """
            INSERT INTO audit_logs (
                user_id, username, action, target, details,
                request_id, method, path, ip_address, user_agent, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user.get("id") if user else None,
                user.get("username") if user else None,
                action,
                target,
                json.dumps(details or {}, ensure_ascii=False),
                ctx.get("request_id"),
                ctx.get("method"),
                ctx.get("path"),
                ctx.get("ip_address"),
                ctx.get("user_agent"),
                str(status or "ok"),
            ),
        )
        conn.commit()
        conn.close()
    except Exception:
        # Audit hiçbir zaman ana akışı düşürmesin.
        pass


def ensure_default_users(force: bool = False):
    if not force and not SEED_DEFAULT_USERS:
        return

    admin_username = os.getenv("DEFAULT_ADMIN_USERNAME", "admin").strip()
    admin_password = os.getenv("DEFAULT_ADMIN_PASSWORD", "admin").strip()
    user_username = os.getenv("DEFAULT_USER_USERNAME", "user").strip()
    user_password = os.getenv("DEFAULT_USER_PASSWORD", "user123").strip()

    defaults = []
    if admin_username and admin_password:
        defaults.append({
            "username": admin_username,
            "password": admin_password,
            "role": "admin",
            "is_active": 1,
        })
    if user_username and user_password:
        defaults.append({
            "username": user_username,
            "password": user_password,
            "role": "user",
            "is_active": 1,
        })

    if not defaults:
        return

    conn = get_db()
    for u in defaults:
        existing = conn.execute(
            "SELECT id, password_hash FROM users WHERE lower(username) = lower(?)",
            (u["username"],),
        ).fetchone()
        if existing:
            # Mevcut kullanıcının parolası varsayılan ile uyuşmuyorsa güncelle
            if not verify_password(u["password"], existing["password_hash"]):
                conn.execute(
                    "UPDATE users SET password_hash = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (hash_password(u["password"]), existing["id"]),
                )
            continue
        conn.execute(
            """
            INSERT INTO users (username, password_hash, role, is_active, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                u["username"],
                hash_password(u["password"]),
                u["role"],
                u["is_active"],
            ),
        )
    conn.commit()
    conn.close()


def is_admin_only_request(method: str, path: str) -> bool:
    method_upper = (method or "").upper()
    for allowed_method, prefix in ADMIN_ONLY_RULES:
        if method_upper == allowed_method and path.startswith(prefix):
            return True
    return False


def require_request_user(request: Request) -> dict:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="Kimlik doğrulaması gerekli")
    return user


def require_admin_user(request: Request) -> dict:
    user = require_request_user(request)
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Bu işlem için admin yetkisi gerekli")
    return user


def create_approval_request(request_type: str, target: str | None, payload: dict, user: dict | None) -> int:
    conn = get_db()
    if DB_BACKEND == "postgres":
        row = conn.execute(
            """
            INSERT INTO approval_requests (request_type, target, payload, status, requested_by, requested_username)
            VALUES (?, ?, ?, 'pending', ?, ?)
            RETURNING id
            """,
            (
                str(request_type or "").strip(),
                target,
                json.dumps(payload or {}, ensure_ascii=False),
                user.get("id") if user else None,
                user.get("username") if user else None,
            ),
        ).fetchone()
        approval_id = row_first_value(row)
    else:
        cur = conn.execute(
            """
            INSERT INTO approval_requests (request_type, target, payload, status, requested_by, requested_username)
            VALUES (?, ?, ?, 'pending', ?, ?)
            """,
            (
                str(request_type or "").strip(),
                target,
                json.dumps(payload or {}, ensure_ascii=False),
                user.get("id") if user else None,
                user.get("username") if user else None,
            ),
        )
        approval_id = getattr(cur, "lastrowid", None)
    conn.commit()
    conn.close()
    return int(approval_id)


def parse_json_text(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except Exception:
        return value


def chunk_list(items: list, chunk_size: int) -> list[list]:
    """Listeyi sabit boyutlu parçalara böler."""
    if chunk_size <= 0:
        return [items]
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def safe_unlink(path: str):
    """Geçici dosyayı sessizce siler."""
    try:
        os.remove(path)
    except Exception:
        pass


def format_approval_row(row):
    item = dict(row)
    item["payload"] = parse_json_text(item.get("payload"))
    item["execution_result"] = parse_json_text(item.get("execution_result"))
    return item


def merge_product_cost_name(conn, old_name: str, new_name: str):
    """
    cost_name rename sırasında product_costs kayıtlarını çakışmasız taşır.
    """
    if old_name == new_name:
        return
    rows = conn.execute(
        "SELECT child_sku, assigned FROM product_costs WHERE cost_name = ?",
        (old_name,),
    ).fetchall()
    for row in rows:
        conn.execute("""
            INSERT INTO product_costs (child_sku, cost_name, assigned)
            VALUES (?, ?, ?)
            ON CONFLICT(child_sku, cost_name) DO UPDATE SET
                assigned = CASE
                    WHEN excluded.assigned > product_costs.assigned THEN excluded.assigned
                    ELSE product_costs.assigned
                END
        """, (row["child_sku"], new_name, row["assigned"]))
    conn.execute("DELETE FROM product_costs WHERE cost_name = ?", (old_name,))


def parse_decimal(value) -> float | None:
    if value is None:
        return None
    raw = str(value).strip().strip('"').replace(",", ".")
    if not raw or raw.upper() == "ÖZEL":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def parse_kargo_dims(value) -> tuple[float | None, float | None, float | None]:
    """en*boy*yukseklik formatını parse eder."""
    if value is None:
        return None, None, None
    raw = str(value).strip().strip('"')
    if not raw or raw.upper() == "ÖZEL":
        return None, None, None

    parts = [p.strip() for p in re.split(r"[*xX×]", raw) if p.strip()]
    nums = [parse_decimal(p) for p in parts]
    nums = [n for n in nums if n is not None]

    if len(nums) >= 3:
        return nums[0], nums[1], nums[2]
    if len(nums) == 2:
        return nums[0], nums[1], None
    return None, None, None


def load_kargo_rows() -> list[dict]:
    """kargo.csv satırlarını normalize edip liste olarak döndürür."""
    rows: list[dict] = []
    csv_path = resolve_kargo_csv_path()
    if not csv_path.exists():
        return rows

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            code = normalize_kargo_code(row.get("kod"))
            if not code:
                continue
            en, boy, yukseklik = parse_kargo_dims(row.get("en*boy*yukseklik"))
            long_side = max(en, boy) if en is not None and boy is not None else None
            short_side = min(en, boy) if en is not None and boy is not None else None
            rows.append({
                "code": code,
                "birim": (row.get("birim") or "").strip() or None,
                "ucret": parse_decimal(row.get("ucret")),
                "en": en,
                "boy": boy,
                "yukseklik": yukseklik,
                "max_long": long_side,
                "max_short": short_side,
                "source_index": idx,
            })
    return rows


def load_kargo_lookup() -> dict[str, dict]:
    """kargo.csv dosyasını kod bazlı lookup tablosu olarak yükler."""
    lookup: dict[str, dict] = {}
    for row in load_kargo_rows():
        lookup[row["code"]] = {
            "kargo_kodu": row["code"],
            "kargo_en": row["en"],
            "kargo_boy": row["boy"],
            "kargo_yukseklik": row["yukseklik"],
        }
    return lookup


def calculate_kargo_desi(
    en: float | None,
    boy: float | None,
    yukseklik: float | None,
    agirlik: float | None,
) -> float | None:
    """
    Desi hesabı:
    max(en*boy*yukseklik/5000, agirlik)
    Sonuç yukarı doğru en yakın 0.5 katına yuvarlanır.
    Örn: 1.3 -> 1.5, 1.8 -> 2.0
    """
    def ceil_to_half(value: float) -> float:
        return math.ceil((value * 2) - 1e-9) / 2.0

    hacim_desi = None
    if en is not None and boy is not None and yukseklik is not None:
        if en > 0 and boy > 0 and yukseklik > 0:
            hacim_desi = (en * boy * yukseklik) / 5000.0

    if agirlik is None and hacim_desi is None:
        return None
    if agirlik is None:
        return ceil_to_half(float(hacim_desi))
    if hacim_desi is None:
        return ceil_to_half(float(agirlik))
    return ceil_to_half(float(max(hacim_desi, agirlik)))


def resolve_request_ip(request: Request) -> str | None:
    forwarded = request.headers.get("x-forwarded-for", "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip() or None
    if request.client and request.client.host:
        return request.client.host
    return None


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    request_id = (request.headers.get("x-request-id", "").strip() or uuid.uuid4().hex)
    context = {
        "request_id": request_id,
        "method": request.method,
        "path": request.url.path,
        "ip_address": resolve_request_ip(request),
        "user_agent": request.headers.get("user-agent"),
    }
    request.state.request_id = request_id
    token = REQUEST_AUDIT_CONTEXT.set(context)
    try:
        response = await call_next(request)
    finally:
        REQUEST_AUDIT_CONTEXT.reset(token)
    response.headers["x-request-id"] = request_id
    return response


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path

    if not path.startswith("/api"):
        return await call_next(request)
    if request.method == "OPTIONS":
        return await call_next(request)
    if path in PUBLIC_API_PATHS or path.rstrip("/") in PUBLIC_API_PATHS:
        return await call_next(request)

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.lower().startswith("bearer "):
        return JSONResponse(status_code=401, content={"detail": "Token gerekli"})

    token = auth_header.split(" ", 1)[1].strip()
    payload = decode_auth_token(token)
    if not payload:
        return JSONResponse(status_code=401, content={"detail": "Geçersiz veya süresi dolmuş token"})

    user_row = get_user_by_id(int(payload["uid"]))
    if not user_row:
        return JSONResponse(status_code=401, content={"detail": "Kullanıcı bulunamadı"})
    if not bool(user_row["is_active"]):
        return JSONResponse(status_code=403, content={"detail": "Kullanıcı pasif"})

    current_user = serialize_user(user_row)
    request.state.user = current_user

    if is_admin_only_request(request.method, path) and current_user["role"] != "admin":
        return JSONResponse(status_code=403, content={"detail": "Bu işlem için admin yetkisi gerekli"})

    return await call_next(request)


# ─────────────────────────── STARTUP ───────────────────────────

_startup_done = False
_startup_error: str | None = None


def _do_startup():
    global _startup_done, _startup_error
    if _startup_done:
        return
    try:
        logger.info("[startup] başlatılıyor...")
        validate_runtime_security()
        init_db()
        logger.info("[startup] init_db tamamlandı")
        ensure_default_users()
        if SEED_DEFAULT_USERS:
            logger.info("[startup] ensure_default_users tamamlandı")
        else:
            conn = get_db()
            try:
                user_count = row_first_value(conn.execute("SELECT COUNT(*) FROM users").fetchone()) or 0
            finally:
                conn.close()
            if not IS_PRODUCTION and user_count == 0:
                ensure_default_users(force=True)
                logger.warning(
                    "[startup] users tablosu boş ve SEED_DEFAULT_USERS=false; "
                    "local erişim kilitlenmesini önlemek için varsayılan kullanıcılar yüklendi"
                )
            else:
                logger.info("[startup] ensure_default_users atlandı (SEED_DEFAULT_USERS=false)")
        conn = get_db()
        try:
            count = row_first_value(conn.execute("SELECT COUNT(*) FROM products").fetchone()) or 0
        finally:
            conn.close()

        if count == 0 and ENABLE_STARTUP_DATA_BOOTSTRAP:
            logger.info("[startup] products boş, bootstrap yükleme başlıyor...")
            try:
                load_mapped_products()
            except Exception as e:
                logger.warning(f"[startup] load_mapped_products başarısız: {e}")
            try:
                load_default_materials()
            except Exception as e:
                logger.warning(f"[startup] load_default_materials başarısız: {e}")
            conn = get_db()
            try:
                # Her zaman: "Boya" (lt) hammaddesini kaldır (sadece "Boya + İşçilik" kalacak)
                try:
                    conn.execute("DELETE FROM raw_materials WHERE name = 'Boya' AND unit = 'lt'")
                    conn.commit()
                except Exception:
                    pass
            finally:
                conn.close()
        elif count == 0:
            logger.info("[startup] products boş ama bootstrap devre dışı (ENABLE_STARTUP_DATA_BOOTSTRAP=false)")

        if ENABLE_STARTUP_TEMPLATE_SYNC:
            # Template'deki maliyet başlıklarını yönetilebilir tabloya senkronize et
            try:
                sync_cost_definitions_from_template()
            except Exception as e:
                logger.warning(f"[startup] sync_cost_definitions başarısız: {e}")
            try:
                normalize_legacy_gold_silver_names()
            except Exception as e:
                logger.warning(f"[startup] normalize_legacy_gold_silver başarısız: {e}")
            try:
                deactivate_shadowed_kaplama_base_names()
            except Exception as e:
                logger.warning(f"[startup] deactivate_shadowed başarısız: {e}")
        else:
            logger.info("[startup] template sync devre dışı (ENABLE_STARTUP_TEMPLATE_SYNC=false)")
        try:
            deactivated = deactivate_cus_products()
            if deactivated:
                logger.info("[startup] CUS kodlu ürün pasife alındı: %s", deactivated)
        except Exception as e:
            logger.warning(f"[startup] deactivate_cus_products başarısız: {e}")
        _startup_done = True
        _startup_error = None
        logger.info("[startup] tamamlandı")
    except Exception as e:
        _startup_error = traceback.format_exc()
        logger.error(f"[startup] HATA: {_startup_error}")
        _startup_done = True  # Tekrar denemeyi engellemek için


@app.on_event("startup")
def startup():
    _do_startup()


@app.middleware("http")
async def ensure_startup_middleware(request: Request, call_next):
    """Vercel serverless ortamında startup event çalışmayabilir. Bu middleware güvence sağlar."""
    _do_startup()
    return await call_next(request)


@app.get("/api/health")
def health_check():
    """Sağlık kontrolü. Startup durumunu görmek için."""
    info = {
        "status": "ok" if _startup_done and not _startup_error else "error",
        "startup_done": _startup_done,
        "db_backend": DB_BACKEND,
        "is_production": IS_PRODUCTION,
        "app_env": APP_ENV,
        "has_database_url": bool(DATABASE_URL),
        "seed_default_users": SEED_DEFAULT_USERS,
        "enable_product_sync": ENABLE_PRODUCT_SYNC,
        "enable_approval_workflow": ENABLE_APPROVAL_WORKFLOW,
        "supported_categories": get_supported_categories(),
        "cors_origins": ALLOWED_CORS_ORIGINS,
    }
    if _startup_error:
        info["startup_error"] = _startup_error
    try:
        conn = get_db()
        user_count = row_first_value(conn.execute("SELECT COUNT(*) FROM users").fetchone()) or 0
        product_count = row_first_value(conn.execute("SELECT COUNT(*) FROM products").fetchone()) or 0
        conn.close()
        info["user_count"] = user_count
        info["product_count"] = product_count
    except Exception as e:
        info["db_error"] = str(e)
    return info


# ─────────────────────────── AUTH ───────────────────────────

@app.post("/api/auth/login")
def login(data: AuthLoginRequest):
    username = (data.username or "").strip()
    if not username or not data.password:
        raise HTTPException(status_code=400, detail="Kullanıcı adı ve parola zorunlu")

    try:
        user_row = get_user_by_username(username)
    except Exception:
        logger.exception("Login sırasında veritabanından kullanıcı çekilemedi: username=%s", username)
        raise HTTPException(
            status_code=503,
            detail="Veritabanına geçici olarak ulaşılamıyor. Lütfen tekrar deneyin.",
        )
    if not user_row:
        raise HTTPException(status_code=401, detail="Kullanıcı adı veya parola hatalı")
    if not bool(user_row["is_active"]):
        raise HTTPException(status_code=403, detail="Kullanıcı pasif")
    if not verify_password(data.password, user_row["password_hash"]):
        raise HTTPException(status_code=401, detail="Kullanıcı adı veya parola hatalı")

    user = serialize_user(user_row)
    token = generate_auth_token(user["id"], user["username"], user["role"])
    write_audit_log(user, "auth.login", target=user["username"])
    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_in": AUTH_TOKEN_TTL_SECONDS,
        "user": user,
    }


@app.get("/api/auth/me")
def auth_me(request: Request):
    return {"user": require_request_user(request)}


@app.post("/api/auth/change-password")
def change_password(data: AuthChangePasswordRequest, request: Request):
    user = require_request_user(request)
    current_password = data.current_password or ""
    new_password = data.new_password or ""
    if len(new_password) < 6:
        raise HTTPException(status_code=400, detail="Yeni parola en az 6 karakter olmalı")

    row = get_user_by_id(user["id"])
    if not row or not verify_password(current_password, row["password_hash"]):
        raise HTTPException(status_code=400, detail="Mevcut parola hatalı")

    conn = get_db()
    conn.execute(
        "UPDATE users SET password_hash = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (hash_password(new_password), user["id"]),
    )
    conn.commit()
    conn.close()

    write_audit_log(user, "auth.change_password", target=user["username"])
    return {"status": "ok"}


@app.get("/api/auth/users")
def list_users(request: Request):
    require_admin_user(request)
    conn = get_db()
    rows = conn.execute(
        """
        SELECT id, username, role, is_active, created_at, updated_at
        FROM users
        ORDER BY role DESC, username
        """
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.post("/api/auth/users")
def create_user(data: AuthUserCreate, request: Request):
    admin = require_admin_user(request)
    username = (data.username or "").strip()
    password = data.password or ""
    if not username:
        raise HTTPException(status_code=400, detail="Kullanıcı adı zorunlu")
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="Parola en az 6 karakter olmalı")

    conn = get_db()
    try:
        cur = conn.execute(
            """
            INSERT INTO users (username, password_hash, role, is_active, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (username, hash_password(password), data.role, int(data.is_active)),
        )
        row = conn.execute(
            "SELECT id, username, role, is_active, created_at, updated_at FROM users WHERE id = ?",
            (cur.lastrowid,),
        ).fetchone()
        conn.commit()
    except DBIntegrityError:
        conn.close()
        raise HTTPException(status_code=409, detail="Bu kullanıcı adı zaten kayıtlı")
    conn.close()

    write_audit_log(admin, "auth.user_create", target=username, details={"role": data.role, "is_active": data.is_active})
    return dict(row)


@app.put("/api/auth/users/{user_id}")
def update_user(user_id: int, data: AuthUserUpdate, request: Request):
    admin = require_admin_user(request)
    conn = get_db()
    existing = conn.execute(
        "SELECT id, username, role, is_active FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="Kullanıcı bulunamadı")

    new_role = data.role if data.role is not None else existing["role"]
    new_is_active = int(data.is_active if data.is_active is not None else bool(existing["is_active"]))
    new_password_hash = hash_password(data.password) if data.password else None

    # En az 1 aktif admin kalsın
    if existing["role"] == "admin" and (new_role != "admin" or new_is_active == 0):
        active_admin_count = row_first_value(conn.execute(
            "SELECT COUNT(*) FROM users WHERE role = 'admin' AND is_active = 1"
        ).fetchone()) or 0
        if active_admin_count <= 1:
            conn.close()
            raise HTTPException(status_code=400, detail="Sistemde en az 1 aktif admin kalmalı")

    if new_password_hash:
        conn.execute(
            """
            UPDATE users
            SET password_hash = ?, role = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (new_password_hash, new_role, new_is_active, user_id),
        )
    else:
        conn.execute(
            """
            UPDATE users
            SET role = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (new_role, new_is_active, user_id),
        )

    row = conn.execute(
        "SELECT id, username, role, is_active, created_at, updated_at FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    conn.commit()
    conn.close()

    write_audit_log(
        admin,
        "auth.user_update",
        target=existing["username"],
        details={"role": new_role, "is_active": bool(new_is_active), "password_updated": bool(new_password_hash)},
    )
    return dict(row)


@app.delete("/api/auth/users/{user_id}")
def delete_user(user_id: int, request: Request):
    admin = require_admin_user(request)
    if user_id == admin["id"]:
        raise HTTPException(status_code=400, detail="Kendi kullanıcınızı silemezsiniz")

    conn = get_db()
    existing = conn.execute(
        "SELECT id, username, role, is_active FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="Kullanıcı bulunamadı")

    if existing["role"] == "admin" and int(existing["is_active"]) == 1:
        active_admin_count = row_first_value(conn.execute(
            "SELECT COUNT(*) FROM users WHERE role = 'admin' AND is_active = 1"
        ).fetchone()) or 0
        if active_admin_count <= 1:
            conn.close()
            raise HTTPException(status_code=400, detail="Sistemde en az 1 aktif admin kalmalı")

    conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()

    write_audit_log(admin, "auth.user_delete", target=existing["username"])
    return {"status": "ok", "deleted_user": existing["username"]}


@app.get("/api/auth/audit-logs")
def list_audit_logs(request: Request, limit: int = Query(200, ge=1, le=1000)):
    require_admin_user(request)
    conn = get_db()
    rows = conn.execute(
        """
        SELECT id, user_id, username, action, target, details,
               request_id, method, path, ip_address, user_agent, status, created_at
        FROM audit_logs
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    out = []
    for row in rows:
        item = dict(row)
        raw_details = item.get("details")
        if raw_details:
            try:
                item["details"] = json.loads(raw_details)
            except Exception:
                pass
        out.append(item)
    return out


@app.get("/api/approvals")
def list_approvals(
    request: Request,
    status: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
):
    require_admin_user(request)
    where_sql = ""
    params: list = []
    if status:
        where_sql = "WHERE status = ?"
        params.append(str(status).strip().lower())
    params.append(limit)

    conn = get_db()
    rows = conn.execute(
        f"""
        SELECT id, request_type, target, payload, status,
               requested_by, requested_username, reviewed_by, reviewed_username,
               review_note, execution_result, created_at, reviewed_at, executed_at
        FROM approval_requests
        {where_sql}
        ORDER BY id DESC
        LIMIT ?
        """,
        params,
    ).fetchall()
    conn.close()
    return [format_approval_row(row) for row in rows]


@app.post("/api/approvals/{approval_id}/review")
def review_approval(
    approval_id: int,
    data: ApprovalReviewRequest,
    request: Request,
):
    admin = require_admin_user(request)
    conn = get_db()
    row = conn.execute(
        """
        SELECT id, request_type, target, status, payload
        FROM approval_requests
        WHERE id = ?
        """,
        (approval_id,),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Onay kaydı bulunamadı")
    if str(row["status"]).lower() != "pending":
        conn.close()
        raise HTTPException(status_code=409, detail="Bu kayıt bekleyen onay durumunda değil")

    new_status = "approved" if bool(data.approve) else "rejected"
    conn.execute(
        """
        UPDATE approval_requests
        SET status = ?,
            reviewed_by = ?,
            reviewed_username = ?,
            review_note = ?,
            reviewed_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            new_status,
            admin["id"],
            admin["username"],
            (data.review_note or "").strip() or None,
            approval_id,
        ),
    )
    conn.commit()
    updated = conn.execute(
        """
        SELECT id, request_type, target, payload, status,
               requested_by, requested_username, reviewed_by, reviewed_username,
               review_note, execution_result, created_at, reviewed_at, executed_at
        FROM approval_requests
        WHERE id = ?
        """,
        (approval_id,),
    ).fetchone()
    conn.close()

    write_audit_log(
        admin,
        f"approval.{new_status}",
        target=str(approval_id),
        details={
            "approval_id": approval_id,
            "request_type": row["request_type"],
            "target": row["target"],
        },
    )
    return format_approval_row(updated)


# ─────────────────────────── QUALITY ───────────────────────────

@app.get("/api/quality/report")
def quality_report(request: Request):
    """
    Veri kalite güvencesi için temel bütünlük kontrollerini raporlar.
    """
    require_admin_user(request)
    conn = get_db()
    checks = {
        "orphan_product_materials": row_first_value(conn.execute(
            """
            SELECT COUNT(*)
            FROM product_materials pm
            LEFT JOIN products p ON p.child_sku = pm.child_sku
            WHERE p.child_sku IS NULL
            """
        ).fetchone()) or 0,
        "orphan_product_costs": row_first_value(conn.execute(
            """
            SELECT COUNT(*)
            FROM product_costs pc
            LEFT JOIN products p ON p.child_sku = pc.child_sku
            WHERE p.child_sku IS NULL
            """
        ).fetchone()) or 0,
        "assigned_costs_without_definition": row_first_value(conn.execute(
            """
            SELECT COUNT(*)
            FROM product_costs pc
            LEFT JOIN cost_definitions cd ON cd.name = pc.cost_name
            WHERE pc.assigned = 1 AND cd.id IS NULL
            """
        ).fetchone()) or 0,
        "assigned_costs_inactive_definition": row_first_value(conn.execute(
            """
            SELECT COUNT(*)
            FROM product_costs pc
            JOIN cost_definitions cd ON cd.name = pc.cost_name
            WHERE pc.assigned = 1 AND COALESCE(cd.is_active, 1) = 0
            """
        ).fetchone()) or 0,
        "products_missing_parent_name": row_first_value(conn.execute(
            "SELECT COUNT(*) FROM products WHERE parent_name IS NULL OR TRIM(parent_name) = ''"
        ).fetchone()) or 0,
        "products_missing_identifier": row_first_value(conn.execute(
            "SELECT COUNT(*) FROM products WHERE product_identifier IS NULL OR TRIM(product_identifier) = ''"
        ).fetchone()) or 0,
        "products_missing_variation_size": row_first_value(conn.execute(
            "SELECT COUNT(*) FROM products WHERE variation_size IS NULL OR TRIM(variation_size) = ''"
        ).fetchone()) or 0,
        "duplicate_users_case_insensitive": row_first_value(conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT LOWER(username) AS k, COUNT(*) AS c
                FROM users
                GROUP BY LOWER(username)
                HAVING COUNT(*) > 1
            ) t
            """
        ).fetchone()) or 0,
        "duplicate_cost_names_case_insensitive": row_first_value(conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT LOWER(name) AS k, COUNT(*) AS c
                FROM cost_definitions
                GROUP BY LOWER(name)
                HAVING COUNT(*) > 1
            ) t
            """
        ).fetchone()) or 0,
    }
    conn.close()

    issue_count = sum(int(v or 0) for v in checks.values())
    return {
        "status": "ok" if issue_count == 0 else "warning",
        "issue_count": issue_count,
        "checks": checks,
    }


# ─────────────────────────── STATS ───────────────────────────

@app.get("/api/stats", response_model=StatsResponse)
def get_stats():
    started_at = time.perf_counter()
    conn = get_db()
    row = conn.execute(
        """
        SELECT
            p.total_products,
            p.metal_products,
            p.ahsap_products,
            p.cam_products,
            p.harita_products,
            p.mobilya_products,
            p.products_with_dims,
            p.products_without_dims,
            m.total_materials,
            m.materials_with_price
        FROM (
            SELECT
                COUNT(*) AS total_products,
                SUM(CASE WHEN kategori = 'metal' THEN 1 ELSE 0 END) AS metal_products,
                SUM(CASE WHEN kategori IN ('ahsap', 'ahşap') THEN 1 ELSE 0 END) AS ahsap_products,
                SUM(CASE WHEN kategori = 'cam' THEN 1 ELSE 0 END) AS cam_products,
                SUM(CASE WHEN kategori = 'harita' THEN 1 ELSE 0 END) AS harita_products,
                SUM(CASE WHEN kategori = 'mobilya' THEN 1 ELSE 0 END) AS mobilya_products,
                SUM(CASE WHEN en IS NOT NULL AND boy IS NOT NULL THEN 1 ELSE 0 END) AS products_with_dims,
                SUM(CASE WHEN en IS NULL OR boy IS NULL THEN 1 ELSE 0 END) AS products_without_dims
            FROM products
            WHERE is_active = 1
        ) p
        CROSS JOIN (
            SELECT
                COUNT(*) AS total_materials,
                SUM(CASE WHEN unit_price > 0 THEN 1 ELSE 0 END) AS materials_with_price
            FROM raw_materials
        ) m
        """
    ).fetchone()
    conn.close()

    stats = {
        "total_products": int((row["total_products"] if row else 0) or 0),
        "metal_products": int((row["metal_products"] if row else 0) or 0),
        "ahsap_products": int((row["ahsap_products"] if row else 0) or 0),
        "cam_products": int((row["cam_products"] if row else 0) or 0),
        "harita_products": int((row["harita_products"] if row else 0) or 0),
        "mobilya_products": int((row["mobilya_products"] if row else 0) or 0),
        "products_with_dims": int((row["products_with_dims"] if row else 0) or 0),
        "products_without_dims": int((row["products_without_dims"] if row else 0) or 0),
        "total_materials": int((row["total_materials"] if row else 0) or 0),
        "materials_with_price": int((row["materials_with_price"] if row else 0) or 0),
    }
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info("[stats] duration_ms=%s", elapsed_ms)
    return stats


# ─────────────────────────── PRODUCTS ───────────────────────────

@app.get("/api/products")
def list_products(
    kategori: Optional[str] = None,
    search: Optional[str] = None,
    parent_name: Optional[str] = None,
    product_identifier: Optional[str] = None,
    has_dims: Optional[bool] = None,
    include_inactive: bool = False,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """Ürünleri listeler, filtreleme ve sayfalama destekler."""
    started_at = time.perf_counter()
    conn = get_db()

    where_clauses = []
    params = []

    if not include_inactive:
        where_clauses.append("is_active = 1")
    if kategori:
        where_clauses.append("kategori = ?")
        params.append(kategori)
    if search:
        where_clauses.append("(child_name LIKE ? OR child_sku LIKE ? OR child_code LIKE ?)")
        params.extend([f"%{search}%"] * 3)
    if parent_name:
        where_clauses.append("parent_name = ?")
        params.append(parent_name)
    if product_identifier:
        where_clauses.append("product_identifier = ?")
        params.append(product_identifier)
    if has_dims is True:
        where_clauses.append("en IS NOT NULL AND boy IS NOT NULL")
    elif has_dims is False:
        where_clauses.append("(en IS NULL OR boy IS NULL)")

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    # Count
    total = row_first_value(conn.execute(f"SELECT COUNT(*) FROM products WHERE {where_sql}", params).fetchone()) or 0

    # Data
    offset = (page - 1) * page_size
    order_sql = "kategori, product_identifier, child_sku"
    # Parent detay ekranında en sık kullanılan senaryo: tek parent altında child listesi
    # Bu senaryoda child_sku sıralaması daha hafif ve indeks dostudur.
    if parent_name and not search and not kategori and not product_identifier:
        order_sql = "child_sku"
    rows = conn.execute(
        f"SELECT * FROM products WHERE {where_sql} ORDER BY {order_sql} LIMIT ? OFFSET ?",
        params + [page_size, offset]
    ).fetchall()

    products = [dict(row) for row in rows]
    conn.close()

    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info(
        "[products] total=%s page=%s size=%s filtered_parent=%s filtered_search=%s duration_ms=%s",
        total,
        page,
        page_size,
        bool(parent_name),
        bool(search),
        elapsed_ms,
    )

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
        "products": products,
    }


@app.get("/api/products/{child_sku}")
def get_product(child_sku: str):
    """Tek bir ürünün detaylarını döndürür (hammaddeler dahil)."""
    conn = get_db()
    product = conn.execute(
        "SELECT * FROM products WHERE child_sku = ? AND is_active = 1",
        (child_sku,),
    ).fetchone()
    if not product:
        conn.close()
        raise HTTPException(status_code=404, detail="Ürün bulunamadı")

    product_dict = dict(product)

    # Hammadde miktarları
    materials = conn.execute("""
        SELECT pm.*, rm.name, rm.unit, rm.unit_price, rm.currency
        FROM product_materials pm
        JOIN raw_materials rm ON pm.material_id = rm.id
        WHERE pm.child_sku = ?
    """, (child_sku,)).fetchall()
    product_dict["materials"] = [dict(m) for m in materials]

    # Maliyet (ambalaj) atamaları
    costs = conn.execute(
        "SELECT * FROM product_costs WHERE child_sku = ?", (child_sku,)
    ).fetchall()
    product_dict["costs"] = [dict(c) for c in costs]

    conn.close()
    return product_dict


@app.get("/api/product-groups")
def list_product_groups(
    kategori: Optional[str] = None,
    search: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """
    Ürünleri parent_name bazında gruplar.
    Aynı parent (maliyet şablonu satırı) altındaki ürünler aynı hammadde yapısını paylaşır.
    """
    started_at = time.perf_counter()
    normalized_search = (search or "").strip()
    cache_key = build_product_groups_cache_key(
        kategori=kategori,
        search=normalized_search,
        page=page,
        page_size=page_size,
    )
    cached = get_product_groups_cache(cache_key)
    if cached is not None:
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        logger.info(
            "[product-groups] cache_hit=1 total=%s page=%s size=%s filtered_kategori=%s filtered_search=%s duration_ms=%s",
            cached.get("total", 0),
            page,
            page_size,
            bool(kategori),
            bool(normalized_search),
            elapsed_ms,
        )
        return cached

    conn = get_db()
    where_clauses = ["is_active = 1"]
    params = []
    if kategori:
        where_clauses.append("kategori = ?")
        params.append(kategori)
    if normalized_search:
        where_clauses.append("LOWER(COALESCE(parent_name, '')) LIKE ?")
        params.append(f"%{normalized_search.lower()}%")
    where_sql = "WHERE " + " AND ".join(where_clauses)

    identifier_agg_sql = (
        "STRING_AGG(DISTINCT product_identifier, ',')"
        if DB_BACKEND == "postgres"
        else "GROUP_CONCAT(DISTINCT product_identifier)"
    )

    offset = (page - 1) * page_size
    rows = conn.execute(
        f"""
        WITH grouped AS (
            SELECT parent_name, kategori,
                   COUNT(*) as variant_count,
                   COUNT(DISTINCT product_identifier) as sub_group_count,
                   {identifier_agg_sql} as product_identifiers,
                   MIN(en) as min_en, MAX(en) as max_en,
                   MIN(boy) as min_boy, MAX(boy) as max_boy,
                   MIN(alan_m2) as min_alan, MAX(alan_m2) as max_alan
            FROM products
            {where_sql}
            GROUP BY parent_name, kategori
        )
        SELECT grouped.*, COUNT(*) OVER() AS total_count
        FROM grouped
        ORDER BY kategori, parent_name
        LIMIT ? OFFSET ?
        """,
        params + [page_size, offset],
    ).fetchall()
    total = 0
    groups: list[dict] = []
    if rows:
        groups = [dict(r) for r in rows]
        total = int(groups[0].get("total_count") or 0)
        for row in groups:
            row.pop("total_count", None)
    elif page > 1:
        total = row_first_value(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT 1
                    FROM products
                    {where_sql}
                    GROUP BY parent_name, kategori
                ) grouped_count
                """,
                params,
            ).fetchone()
        ) or 0
    conn.close()

    response = {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
        "groups": groups,
    }
    set_product_groups_cache(cache_key, response)

    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info(
        "[product-groups] cache_hit=0 total=%s page=%s size=%s filtered_kategori=%s filtered_search=%s duration_ms=%s",
        response["total"],
        page,
        page_size,
        bool(kategori),
        bool(normalized_search),
        elapsed_ms,
    )
    return response


# ─────────────────────────── RAW MATERIALS ───────────────────────────

@app.get("/api/materials")
def list_materials():
    """Tüm hammaddeleri listeler."""
    conn = get_db()
    rows = conn.execute("SELECT * FROM raw_materials ORDER BY name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.post("/api/materials")
def create_material(data: RawMaterialCreate, request: Request):
    """Yeni hammadde tanımı ekler."""
    admin = require_admin_user(request)
    name = (data.name or "").strip()
    unit = (data.unit or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Hammadde adı boş olamaz")
    if not unit:
        raise HTTPException(status_code=400, detail="Birim boş olamaz")

    conn = get_db()
    try:
        cur = conn.execute("""
            INSERT INTO raw_materials (name, unit, unit_price, currency)
            VALUES (?, ?, ?, ?)
        """, (name, unit, float(data.unit_price), data.currency))
        row = conn.execute(
            "SELECT * FROM raw_materials WHERE id = ?",
            (cur.lastrowid,),
        ).fetchone()
        conn.commit()
    except DBIntegrityError:
        conn.close()
        raise HTTPException(status_code=409, detail="Bu hammadde adı zaten kayıtlı")
    conn.close()
    created = dict(row)
    write_audit_log(admin, "materials.create", target=name, details={"unit": unit})
    return created


@app.put("/api/materials/{material_id}")
def update_material(material_id: int, data: RawMaterialUpdate, request: Request):
    """Hammadde birim fiyatını günceller."""
    admin = require_admin_user(request)
    conn = get_db()
    cur = conn.execute(
        "UPDATE raw_materials SET unit_price = ?, currency = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (data.unit_price, data.currency, material_id)
    )
    if cur.rowcount == 0:
        conn.close()
        raise HTTPException(status_code=404, detail="Hammadde bulunamadı")
    conn.commit()
    conn.close()
    write_audit_log(admin, "materials.update", target=str(material_id), details={"unit_price": data.unit_price, "currency": data.currency})
    return {"status": "ok"}


@app.delete("/api/materials/{material_id}")
def delete_material(material_id: int, request: Request):
    """Hammaddeyi ve ilişkili ürün miktar kayıtlarını siler."""
    admin = require_admin_user(request)
    conn = get_db()
    row = conn.execute(
        "SELECT id, name FROM raw_materials WHERE id = ?",
        (material_id,),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Hammadde bulunamadı")

    conn.execute("DELETE FROM product_materials WHERE material_id = ?", (material_id,))
    conn.execute("DELETE FROM raw_materials WHERE id = ?", (material_id,))
    conn.commit()
    conn.close()
    write_audit_log(admin, "materials.delete", target=row["name"])
    return {"status": "ok", "deleted_material": row["name"]}


# ─────────────────────────── PRODUCT MATERIALS ───────────────────────────

@app.post("/api/product-materials")
def set_product_material(entry: ProductMaterialEntry):
    """Bir ürüne hammadde miktarı atar."""
    conn = get_db()
    conn.execute("""
        INSERT INTO product_materials (child_sku, material_id, quantity)
        VALUES (?, ?, ?)
        ON CONFLICT(child_sku, material_id) DO UPDATE SET quantity = ?
    """, (entry.child_sku, entry.material_id, entry.quantity, entry.quantity))
    conn.commit()
    conn.close()
    return {"status": "ok"}


@app.post("/api/product-materials/bulk")
def set_product_material_bulk(entry: ProductMaterialBulk):
    """
    Birden fazla ürüne aynı hammadde miktarı atar.
    Aynı parent altındaki renk varyantları için idealdir.
    """
    conn = get_db()
    for sku in entry.child_skus:
        conn.execute("""
            INSERT INTO product_materials (child_sku, material_id, quantity)
            VALUES (?, ?, ?)
            ON CONFLICT(child_sku, material_id) DO UPDATE SET quantity = ?
        """, (sku, entry.material_id, entry.quantity, entry.quantity))
    conn.commit()
    conn.close()
    return {"status": "ok", "updated": len(entry.child_skus)}


@app.get("/api/product-materials/{child_sku}")
def get_product_materials(child_sku: str):
    """Bir ürünün tüm hammadde miktarlarını döndürür."""
    conn = get_db()
    rows = conn.execute("""
        SELECT pm.*, rm.name, rm.unit, rm.unit_price, rm.currency
        FROM product_materials pm
        JOIN raw_materials rm ON pm.material_id = rm.id
        WHERE pm.child_sku = ?
        ORDER BY rm.name
    """, (child_sku,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─────────────────────────── COST ASSIGNMENTS ───────────────────────────

@app.get("/api/kargo-options")
def get_kargo_options():
    """kargo.csv'deki kargo kutu seçeneklerini döndürür."""
    return load_kargo_rows()


@app.get("/api/cost-definitions")
def get_cost_definitions(
    category: Optional[str] = None,
    include_inactive: bool = False,
):
    """Kargo/Kaplama maliyet tanımlarını döndürür."""
    if category and category not in {"kargo", "kaplama"}:
        raise HTTPException(status_code=400, detail="Geçersiz category")
    return list_cost_definitions(active_only=not include_inactive, category=category)


@app.post("/api/cost-definitions")
def create_cost_definition(data: CostDefinitionCreate, request: Request):
    """Yeni maliyet tanımı ekler."""
    admin = require_admin_user(request)
    name = (data.name or "").strip()
    if data.category == "kaplama":
        name = canonicalize_kaplama_cost_name(name)
    if not name:
        raise HTTPException(status_code=400, detail="Maliyet adı boş olamaz")

    category = data.category
    kargo_code = normalize_kargo_code(data.kargo_code or name) if category == "kargo" else None

    conn = get_db()
    try:
        cur = conn.execute("""
            INSERT INTO cost_definitions (name, category, kargo_code, is_active, source, updated_at)
            VALUES (?, ?, ?, ?, 'manual', CURRENT_TIMESTAMP)
        """, (name, category, kargo_code, int(data.is_active)))
        row = conn.execute("""
            SELECT id, name, category, kargo_code, is_active, source, created_at, updated_at
            FROM cost_definitions
            WHERE id = ?
        """, (cur.lastrowid,)).fetchone()
        conn.commit()
    except DBIntegrityError:
        conn.close()
        raise HTTPException(status_code=409, detail="Bu maliyet adı zaten mevcut")
    conn.close()
    created = dict(row)
    write_audit_log(admin, "costs.create", target=name, details={"category": category, "kargo_code": kargo_code})
    return created


@app.put("/api/cost-definitions/{cost_id}")
def update_cost_definition(cost_id: int, data: CostDefinitionUpdate, request: Request):
    """Maliyet tanımını günceller."""
    admin = require_admin_user(request)
    conn = get_db()
    existing = conn.execute("""
        SELECT id, name, category, kargo_code, is_active
        FROM cost_definitions
        WHERE id = ?
    """, (cost_id,)).fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="Maliyet tanımı bulunamadı")

    new_category = data.category if data.category is not None else existing["category"]
    if new_category not in {"kargo", "kaplama"}:
        conn.close()
        raise HTTPException(status_code=400, detail="Geçersiz category")

    new_name = (data.name.strip() if data.name is not None else existing["name"])
    if new_category == "kaplama":
        new_name = canonicalize_kaplama_cost_name(new_name)
    if not new_name:
        conn.close()
        raise HTTPException(status_code=400, detail="Maliyet adı boş olamaz")

    new_is_active = int(data.is_active if data.is_active is not None else bool(existing["is_active"]))
    if new_category == "kargo":
        source_code = data.kargo_code if data.kargo_code is not None else (existing["kargo_code"] or new_name)
        new_kargo_code = normalize_kargo_code(source_code)
    else:
        new_kargo_code = None

    conflict = conn.execute(
        "SELECT id FROM cost_definitions WHERE name = ? AND id <> ?",
        (new_name, cost_id),
    ).fetchone()
    if conflict:
        conn.close()
        raise HTTPException(status_code=409, detail="Bu maliyet adı zaten mevcut")

    merge_product_cost_name(conn, existing["name"], new_name)
    conn.execute("""
        UPDATE cost_definitions
        SET name = ?, category = ?, kargo_code = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (new_name, new_category, new_kargo_code, new_is_active, cost_id))
    row = conn.execute("""
        SELECT id, name, category, kargo_code, is_active, source, created_at, updated_at
        FROM cost_definitions
        WHERE id = ?
    """, (cost_id,)).fetchone()
    conn.commit()
    conn.close()
    updated = dict(row)
    write_audit_log(admin, "costs.update", target=updated["name"], details={"cost_id": cost_id})
    return updated


@app.delete("/api/cost-definitions/{cost_id}")
def delete_cost_definition(cost_id: int, request: Request):
    """Maliyet tanımını ve ilişkili ürün maliyet atamalarını siler."""
    admin = require_admin_user(request)
    conn = get_db()
    row = conn.execute(
        "SELECT id, name FROM cost_definitions WHERE id = ?",
        (cost_id,),
    ).fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Maliyet tanımı bulunamadı")

    conn.execute("DELETE FROM product_costs WHERE cost_name = ?", (row["name"],))
    conn.execute("DELETE FROM cost_definitions WHERE id = ?", (cost_id,))
    conn.commit()
    conn.close()
    write_audit_log(admin, "costs.delete", target=row["name"])
    return {"status": "ok", "deleted_cost": row["name"]}


@app.get("/api/cost-names")
def get_cost_names():
    """Aktif maliyet (ambalaj) isimlerini döndürür."""
    return load_cost_names()


@app.get("/api/kaplama-suggestions")
def get_kaplama_suggestions(parent_name: str):
    """
    Seçili parent için boyut bazlı kaplama maliyet önerisi üretir.
    Öneri, geçmiş atamalardaki isim benzerliği + boyut eşleşmesi skoruna göre hesaplanır.
    """
    conn = get_db()
    target_rows = conn.execute("""
        SELECT child_sku, child_name, variation_size, kategori
        FROM products
        WHERE parent_name = ? AND is_active = 1
    """, (parent_name,)).fetchall()
    if not target_rows:
        conn.close()
        raise HTTPException(status_code=404, detail="Bu parent altında ürün bulunamadı")

    hist_rows = conn.execute("""
        SELECT p.child_name, p.variation_size, p.kategori, pc.cost_name
        FROM product_costs pc
        JOIN products p ON p.child_sku = pc.child_sku
        JOIN cost_definitions cd ON cd.name = pc.cost_name
        WHERE pc.assigned = 1
          AND cd.category = 'kaplama'
          AND cd.is_active = 1
          AND p.is_active = 1
          AND p.parent_name <> ?
    """, (parent_name,)).fetchall()
    conn.close()

    targets_by_size = {}
    for row in target_rows:
        size = row["variation_size"] or "(boyutsuz)"
        entry = targets_by_size.setdefault(size, {
            "tokens": set(),
            "kategori": row["kategori"],
            "count": 0,
        })
        entry["count"] += 1
        entry["tokens"].update(tokenize_text(row["child_name"]))
        entry["tokens"].update(tokenize_text(size))

    if not hist_rows:
        return {"parent_name": parent_name, "suggestions": {}}

    hist_samples = []
    freq_by_size = defaultdict(lambda: defaultdict(int))
    freq_by_kategori = defaultdict(lambda: defaultdict(int))

    for row in hist_rows:
        h_size = row["variation_size"] or "(boyutsuz)"
        h_kategori = row["kategori"] or ""
        cost_name = row["cost_name"]
        h_tokens = tokenize_text(row["child_name"])
        h_tokens.update(tokenize_text(h_size))
        hist_samples.append((h_size, h_kategori, cost_name, h_tokens))
        freq_by_size[h_size][cost_name] += 1
        freq_by_kategori[h_kategori][cost_name] += 1

    suggestions = {}
    for size, info in targets_by_size.items():
        t_tokens = info["tokens"]
        t_kategori = info["kategori"] or ""
        score_map = defaultdict(lambda: {"score": 0, "hits": 0, "size_hits": 0, "token_hits": 0})

        for h_size, h_kategori, cost_name, h_tokens in hist_samples:
            overlap = len(t_tokens & h_tokens) if t_tokens else 0
            score = 0
            if h_size == size:
                score += 5
            if t_kategori and h_kategori == t_kategori:
                score += 2
            if overlap > 0:
                score += overlap * 3
            if score <= 0:
                continue

            m = score_map[cost_name]
            m["score"] += score
            m["hits"] += 1
            m["token_hits"] += overlap
            if h_size == size:
                m["size_hits"] += 1

        selected_cost = None
        selected_meta = None
        if score_map:
            ranked = sorted(
                score_map.items(),
                key=lambda kv: (
                    kv[1]["score"],
                    kv[1]["size_hits"],
                    kv[1]["token_hits"],
                    kv[1]["hits"],
                ),
                reverse=True,
            )
            selected_cost, selected_meta = ranked[0]
        else:
            if freq_by_size.get(size):
                selected_cost, cnt = max(freq_by_size[size].items(), key=lambda kv: kv[1])
                selected_meta = {"score": cnt * 4, "hits": cnt, "size_hits": cnt, "token_hits": 0}
            elif freq_by_kategori.get(t_kategori):
                selected_cost, cnt = max(freq_by_kategori[t_kategori].items(), key=lambda kv: kv[1])
                selected_meta = {"score": cnt * 2, "hits": cnt, "size_hits": 0, "token_hits": 0}

        if not selected_cost or not selected_meta:
            continue

        confidence = "düşük"
        if selected_meta["score"] >= 18 or selected_meta["size_hits"] >= 3:
            confidence = "yüksek"
        elif selected_meta["score"] >= 8:
            confidence = "orta"

        suggestions[size] = {
            "cost_name": selected_cost,
            "confidence": confidence,
            "score": selected_meta["score"],
            "hits": selected_meta["hits"],
            "size_hits": selected_meta["size_hits"],
            "token_hits": selected_meta["token_hits"],
        }

    return {"parent_name": parent_name, "suggestions": suggestions}


@app.get("/api/kaplama-name-suggestions")
def get_kaplama_name_suggestions(parent_name: str):
    """
    Seçili parent için ürün adına + renk tier'ına göre kaplama maliyet önerisi üretir.
    """
    conn = get_db()
    target_rows = conn.execute("""
        SELECT child_sku, child_name, variation_size, variation_color, kategori
        FROM products
        WHERE parent_name = ? AND is_active = 1
    """, (parent_name,)).fetchall()
    if not target_rows:
        conn.close()
        raise HTTPException(status_code=404, detail="Bu parent altında ürün bulunamadı")

    hist_rows = conn.execute("""
        SELECT p.child_name, p.variation_size, p.variation_color, p.kategori, pc.cost_name
        FROM product_costs pc
        JOIN products p ON p.child_sku = pc.child_sku
        JOIN cost_definitions cd ON cd.name = pc.cost_name
        WHERE pc.assigned = 1
          AND cd.category = 'kaplama'
          AND cd.is_active = 1
          AND p.is_active = 1
          AND p.parent_name <> ?
    """, (parent_name,)).fetchall()
    conn.close()

    kaplama_defs = list_cost_definitions(active_only=True, category="kaplama")
    kaplama_cost_names = [d["name"] for d in kaplama_defs if d.get("name")]
    kaplama_tokens = {name: tokenize_text(name) for name in kaplama_cost_names}
    kaplama_tier_by_cost = {name: detect_kaplama_tier(name) for name in kaplama_cost_names}

    target_by_key = {}
    for row in target_rows:
        name = (row["child_name"] or row["child_sku"] or "").strip()
        if not name:
            continue
        color = (row["variation_color"] or "").strip()
        tier = detect_kaplama_tier(name, color)
        key = build_kaplama_group_key(name, tier)
        entry = target_by_key.setdefault(key, {
            "name": name,
            "tier": tier,
            "tokens": set(),
            "kategori": row["kategori"] or "",
            "sizes": set(),
            "colors": set(),
            "count": 0,
        })
        entry["count"] += 1
        entry["tokens"].update(tokenize_text(name))
        entry["tokens"].update(tokenize_text(color))
        entry["sizes"].add(row["variation_size"] or "(boyutsuz)")
        if color:
            entry["colors"].add(color)

    if not target_by_key:
        return {"parent_name": parent_name, "suggestions": {}}

    hist_samples = []
    freq_by_kategori = defaultdict(lambda: defaultdict(int))
    freq_by_tier = defaultdict(lambda: defaultdict(int))
    for row in hist_rows:
        h_name = (row["child_name"] or "").strip()
        h_size = row["variation_size"] or "(boyutsuz)"
        h_color = (row["variation_color"] or "").strip()
        h_kategori = row["kategori"] or ""
        h_tier = detect_kaplama_tier(h_name, h_color, row["cost_name"])
        cost_name = row["cost_name"]
        h_tokens = tokenize_text(h_name)
        h_tokens.update(tokenize_text(h_size))
        h_tokens.update(tokenize_text(h_color))
        hist_samples.append((h_name, h_size, h_color, h_kategori, h_tier, cost_name, h_tokens))
        freq_by_kategori[h_kategori][cost_name] += 1
        freq_by_tier[h_tier][cost_name] += 1

    suggestions = {}
    for target_key, info in target_by_key.items():
        name = info["name"]
        tier = info["tier"]
        t_tokens = set(info["tokens"])
        for sz in info["sizes"]:
            t_tokens.update(tokenize_text(sz))
        t_kategori = info["kategori"]
        score_map = defaultdict(lambda: {"score": 0, "hits": 0, "direct_hits": 0, "name_hits": 0, "tier_hits": 0})

        # 1) Ürün adı ile maliyet adı token benzerliği
        for cost_name, c_tokens in kaplama_tokens.items():
            overlap = len(t_tokens & c_tokens)
            if overlap <= 0:
                continue
            m = score_map[cost_name]
            m["score"] += overlap * 6
            m["direct_hits"] += overlap
            m["hits"] += 1
            if tier != "other":
                c_tier = kaplama_tier_by_cost.get(cost_name, "other")
                if c_tier == tier:
                    m["score"] += 8
                    m["tier_hits"] += 1
                elif c_tier != "other":
                    m["score"] -= 7

        # 2) Geçmiş ürün atamalarıyla isim benzerliği
        for h_name, h_size, h_color, h_kategori, h_tier, cost_name, h_tokens in hist_samples:
            overlap = len(t_tokens & h_tokens) if t_tokens else 0
            if overlap <= 0 and h_name.lower() != name.lower():
                continue
            m = score_map[cost_name]
            add = overlap * 3
            if h_name.lower() == name.lower():
                add += 10
                m["name_hits"] += 1
            if t_kategori and h_kategori == t_kategori:
                add += 2
            if tier != "other":
                if h_tier == tier:
                    add += 3
                    m["tier_hits"] += 1
                elif h_tier != "other":
                    add -= 2
            m["score"] += add
            m["hits"] += 1

        selected_cost = None
        selected_meta = None

        if score_map:
            ranked = sorted(
                score_map.items(),
                key=lambda kv: (
                    kv[1]["score"],
                    kv[1]["tier_hits"],
                    kv[1]["name_hits"],
                    kv[1]["direct_hits"],
                    kv[1]["hits"],
                ),
                reverse=True,
            )
            selected_cost, selected_meta = ranked[0]
        else:
            # Tier/kategori bazlı fallback
            if tier != "other" and freq_by_tier.get(tier):
                selected_cost, cnt = max(freq_by_tier[tier].items(), key=lambda kv: kv[1])
                selected_meta = {"score": cnt * 3, "hits": cnt, "direct_hits": 0, "name_hits": 0, "tier_hits": cnt}
            elif freq_by_kategori.get(t_kategori):
                selected_cost, cnt = max(freq_by_kategori[t_kategori].items(), key=lambda kv: kv[1])
                selected_meta = {"score": cnt * 2, "hits": cnt, "direct_hits": 0, "name_hits": 0, "tier_hits": 0}

        if not selected_cost or not selected_meta:
            continue

        confidence = "düşük"
        if selected_meta["score"] >= 24 or selected_meta["name_hits"] >= 1 or selected_meta["tier_hits"] >= 2:
            confidence = "yüksek"
        elif selected_meta["score"] >= 10:
            confidence = "orta"

        suggestions[target_key] = {
            "cost_name": selected_cost,
            "confidence": confidence,
            "score": selected_meta["score"],
            "hits": selected_meta["hits"],
            "direct_hits": selected_meta["direct_hits"],
            "name_hits": selected_meta["name_hits"],
            "tier_hits": selected_meta["tier_hits"],
            "tier": tier,
            "group_name": name,
        }

    return {"parent_name": parent_name, "suggestions": suggestions}


@app.post("/api/product-costs")
def set_product_cost(entry: ProductCostAssignment, request: Request):
    """Bir ürüne maliyet (ambalaj) atar."""
    user = require_request_user(request)
    conn = get_db()
    conn.execute("""
        INSERT INTO product_costs (child_sku, cost_name, assigned)
        VALUES (?, ?, ?)
        ON CONFLICT(child_sku, cost_name) DO UPDATE SET assigned = ?
    """, (entry.child_sku, entry.cost_name, int(entry.assigned), int(entry.assigned)))
    conn.commit()
    conn.close()
    write_audit_log(
        user,
        "costs.assign",
        target=entry.child_sku,
        details={
            "child_sku": entry.child_sku,
            "cost_name": entry.cost_name,
            "assigned": bool(entry.assigned),
        },
    )
    return {"status": "ok"}


# ─────────────────────── PARENT-TO-CHILD INHERITANCE ───────────────────────

def _material_flags(name: str | None) -> tuple[bool, bool, bool, bool]:
    folded = str(name or "").strip().casefold()
    boya_iscilik_token = "işçilik".casefold()
    is_strafor = "strafor" in folded
    is_boya_iscilik = "boya" in folded and (boya_iscilik_token in folded or "iscilik" in folded)
    is_sac = folded.startswith("saç".casefold()) or folded.startswith("sac")
    is_mdf = folded.startswith("mdf")
    return is_strafor, is_boya_iscilik, is_sac, is_mdf


@app.get("/api/inherit/prefill")
def get_parent_inheritance_prefill(parent_name: str):
    """
    Parent Inheritance ekranı için mevcut atamaları form formatında döndürür.
    Böylece daha önce yapılmış aktarım "boş ekran" yerine prefill gelir.
    """
    conn = get_db()
    try:
        children = conn.execute(
            """
            SELECT child_sku, child_name, variation_size, variation_color, alan_m2, kargo_agirlik, kargo_kodu
            FROM products
            WHERE parent_name = ? AND is_active = 1
            """,
            (parent_name,),
        ).fetchall()
        if not children:
            raise HTTPException(status_code=404, detail="Bu parent altında ürün bulunamadı")

        child_by_sku = {row["child_sku"]: dict(row) for row in children}
        kargo_counter_by_size: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        kaplama_counter_by_name: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        weight_counter_by_size: dict[str, dict[float, int]] = defaultdict(lambda: defaultdict(int))

        for child in children:
            size = child["variation_size"] or "(boyutsuz)"
            if child["kargo_agirlik"] is None:
                continue
            try:
                weight = round(float(child["kargo_agirlik"]), 6)
            except (TypeError, ValueError):
                continue
            weight_counter_by_size[size][weight] += 1

        cost_rows = conn.execute(
            """
            SELECT pc.child_sku, pc.cost_name, cd.category
            FROM product_costs pc
            LEFT JOIN cost_definitions cd ON cd.name = pc.cost_name
            JOIN products p ON p.child_sku = pc.child_sku
            WHERE p.parent_name = ? AND p.is_active = 1 AND pc.assigned = 1
            """,
            (parent_name,),
        ).fetchall()

        for row in cost_rows:
            sku = row["child_sku"]
            cost_name = str(row["cost_name"] or "").strip()
            if not cost_name:
                continue
            child = child_by_sku.get(sku)
            if not child:
                continue

            size = child["variation_size"] or "(boyutsuz)"
            category = str(row["category"] or "").strip().lower()
            is_kargo = category == "kargo" or bool(normalize_kargo_code(cost_name))
            if is_kargo:
                kargo_counter_by_size[size][cost_name] += 1
                continue

            group_name = child["child_name"] or child["child_sku"] or ""
            tier = detect_kaplama_tier(child["child_name"], child["variation_color"], cost_name)
            group_key = build_kaplama_group_key(group_name, tier)
            if group_key:
                kaplama_counter_by_name[group_key][cost_name] += 1

        fallback_kargo_name_by_code: dict[str, str] = {}
        for row in conn.execute(
            """
            SELECT name, kargo_code
            FROM cost_definitions
            WHERE category = 'kargo'
            """
        ).fetchall():
            name = str(row["name"] or "").strip()
            code = normalize_kargo_code(row["kargo_code"] or row["name"])
            if not name or not code:
                continue
            existing = fallback_kargo_name_by_code.get(code)
            if not existing or name.casefold() < existing.casefold():
                fallback_kargo_name_by_code[code] = name

        for child in children:
            size = child["variation_size"] or "(boyutsuz)"
            if kargo_counter_by_size.get(size):
                continue
            code = normalize_kargo_code(child["kargo_kodu"])
            if not code:
                continue
            fallback_name = fallback_kargo_name_by_code.get(code)
            if fallback_name:
                kargo_counter_by_size[size][fallback_name] += 1

        cost_map: dict[str, str] = {}
        for size, counter in kargo_counter_by_size.items():
            ranked = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0].casefold()))
            if ranked:
                cost_map[size] = ranked[0][0]

        kaplama_name_map: dict[str, list[str]] = {}
        for group_key, counter in kaplama_counter_by_name.items():
            ranked = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0].casefold()))
            names = [name for name, _ in ranked]
            if names:
                kaplama_name_map[group_key] = names

        weight_map: dict[str, float] = {}
        for size, counter in weight_counter_by_size.items():
            ranked = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))
            if ranked:
                weight_map[size] = ranked[0][0]

        material_rows = conn.execute(
            """
            SELECT pm.child_sku, pm.material_id, pm.quantity, rm.name
            FROM product_materials pm
            JOIN raw_materials rm ON rm.id = pm.material_id
            JOIN products p ON p.child_sku = pm.child_sku
            WHERE p.parent_name = ? AND p.is_active = 1
            """,
            (parent_name,),
        ).fetchall()

        sac_presence: dict[int, int] = defaultdict(int)
        mdf_presence: dict[int, int] = defaultdict(int)
        sac_area_match: dict[int, int] = defaultdict(int)
        mdf_area_match: dict[int, int] = defaultdict(int)
        quantity_by_material: dict[int, dict[float, int]] = defaultdict(lambda: defaultdict(int))

        for row in material_rows:
            material_id = int(row["material_id"])
            quantity_raw = row["quantity"]
            if quantity_raw is None:
                continue
            try:
                quantity = round(float(quantity_raw), 6)
            except (TypeError, ValueError):
                continue

            is_strafor, is_boya_iscilik, is_sac, is_mdf = _material_flags(row["name"])
            child = child_by_sku.get(row["child_sku"])
            alan = child.get("alan_m2") if child else None
            try:
                alan_value = float(alan) if alan is not None else None
            except (TypeError, ValueError):
                alan_value = None

            if is_sac:
                sac_presence[material_id] += 1
                if alan_value is not None and math.isclose(quantity, alan_value, rel_tol=0, abs_tol=1e-4):
                    sac_area_match[material_id] += 1
                continue
            if is_mdf:
                mdf_presence[material_id] += 1
                if alan_value is not None and math.isclose(quantity, alan_value, rel_tol=0, abs_tol=1e-4):
                    mdf_area_match[material_id] += 1
                continue
            if is_strafor or is_boya_iscilik:
                continue

            quantity_by_material[material_id][quantity] += 1

        def pick_auto_material(match_counter: dict[int, int], presence_counter: dict[int, int]) -> int | None:
            if match_counter:
                ranked = sorted(
                    match_counter.items(),
                    key=lambda kv: (-kv[1], -presence_counter.get(kv[0], 0), kv[0]),
                )
                return ranked[0][0]
            if len(presence_counter) == 1:
                return next(iter(presence_counter))
            return None

        sac_material_id = pick_auto_material(sac_area_match, sac_presence)
        mdf_material_id = pick_auto_material(mdf_area_match, mdf_presence)

        materials: dict[str, float] = {}
        for material_id, counter in quantity_by_material.items():
            ranked = sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))
            if ranked:
                materials[str(material_id)] = ranked[0][0]

        return {
            "parent_name": parent_name,
            "cost_map": cost_map,
            "kaplama_name_map": kaplama_name_map,
            "weight_map": weight_map,
            "materials": materials,
            "sac_material_id": sac_material_id,
            "mdf_material_id": mdf_material_id,
            "has_prefill": bool(cost_map or kaplama_name_map or weight_map or materials),
        }
    finally:
        conn.close()


@app.post("/api/inherit")
def apply_parent_inheritance(req: ParentInheritanceRequest, request: Request):
    """
    Parent-to-Child Cost Inheritance:
    1. Copy base raw material values to every child of the parent.
    2. Flag kargo and kaplama cost categories per variation_size.
    3. Save cargo package dims from kargo.csv via selected cost code.
    4. Save weight + desi per child:
       - volumetric = en * boy * yukseklik / 5000
       - desi = max(volumetric, kargo_agirlik)
    5. Preserve each child's own Area (alan_m2).
    6. Auto-calculate area-driven materials per child:
       - Strafor  = child.alan_m2 * 1.2
       - Boya + İşçilik = child.alan_m2 * 5
    """
    started_at = time.perf_counter()
    user = require_request_user(request)
    approval_id = req.approval_id
    approval_context = req.model_dump(mode="json", exclude={"approval_id"})

    if ENABLE_APPROVAL_WORKFLOW and user.get("role") != "admin":
        if not approval_id:
            created_approval_id = create_approval_request(
                "inherit.apply",
                req.parent_name,
                approval_context,
                user,
            )
            write_audit_log(
                user,
                "approval.requested",
                target=req.parent_name,
                details={
                    "approval_id": created_approval_id,
                    "request_type": "inherit.apply",
                    "parent_name": req.parent_name,
                },
                status="pending",
            )
            return {
                "status": "pending_approval",
                "approval_id": created_approval_id,
                "parent_name": req.parent_name,
                "message": "İşlem onaya gönderildi. Admin onayından sonra approval_id ile tekrar çalıştırın.",
            }

        approval_conn = get_db()
        try:
            approval_row = approval_conn.execute(
                """
                SELECT id, request_type, status, payload, requested_by
                FROM approval_requests
                WHERE id = ?
                """,
                (approval_id,),
            ).fetchone()
        finally:
            approval_conn.close()
        if not approval_row:
            raise HTTPException(status_code=404, detail="Onay kaydı bulunamadı")
        if str(approval_row["request_type"] or "").strip() != "inherit.apply":
            raise HTTPException(status_code=400, detail="approval_id bu işlem tipiyle uyumlu değil")
        if str(approval_row["status"] or "").strip().lower() != "approved":
            raise HTTPException(status_code=409, detail="İşlem henüz onaylanmamış")
        if approval_row["requested_by"] and int(approval_row["requested_by"]) != int(user["id"]):
            raise HTTPException(status_code=403, detail="Bu onay kaydı başka kullanıcıya ait")

        approved_payload = parse_json_text(approval_row["payload"]) or {}
        if approved_payload != approval_context:
            raise HTTPException(status_code=400, detail="Onay sonrası payload değişmiş. Yeniden onay isteği açın.")

    conn = get_db()
    kargo_lookup = load_kargo_lookup()

    # Fetch all children with their variation_size — use parent_name for grouping
    children = conn.execute(
        """
        SELECT child_sku, child_name, alan_m2, variation_size, variation_color
        FROM products
        WHERE parent_name = ? AND is_active = 1
        """,
        (req.parent_name,)
    ).fetchall()

    if not children:
        conn.close()
        raise HTTPException(status_code=404, detail="Bu parent altında ürün bulunamadı")

    # Resolve material IDs for Strafor and Boya + İşçilik
    strafor_row = conn.execute(
        "SELECT id FROM raw_materials WHERE name LIKE '%Strafor%' LIMIT 1"
    ).fetchone()
    boya_row = conn.execute(
        "SELECT id FROM raw_materials WHERE name LIKE '%Boya%İşçilik%' OR name LIKE '%Boya + İşçilik%' LIMIT 1"
    ).fetchone()

    strafor_id = strafor_row["id"] if strafor_row else None
    boya_id = boya_row["id"] if boya_row else None
    sac_id = req.sac_material_id  # Kullanıcının seçtiği Saç kalınlığı
    mdf_id = req.mdf_material_id  # Kullanıcının seçtiği MDF

    # Auto-calc material IDs set: skip from manual copy
    auto_ids = {strafor_id, boya_id, sac_id, mdf_id} - {None}

    manual_material_assignments: list[tuple[int, float]] = []
    for mat_id_raw, quantity_raw in (req.materials or {}).items():
        try:
            mat_id = int(mat_id_raw)
            quantity = float(quantity_raw)
        except (TypeError, ValueError):
            continue
        # Otomatik hesaplanan materyalleri manuel kalemlerden ayır
        if mat_id in auto_ids:
            continue
        manual_material_assignments.append((mat_id, quantity))

    inherit_detail_limit = max(0, int(os.getenv("INHERIT_RESPONSE_DETAIL_LIMIT", "250")))
    updated_children_count = 0
    skipped_children_count = 0
    updated_children_sample: list[dict] = []
    skipped_children_sample: list[dict] = []
    product_updates: list[tuple] = []
    material_upserts: list[tuple] = []
    cost_upserts: list[tuple] = []

    kaplama_name_map_exact: dict[str, list[str]] = {}
    kaplama_name_map_ci: dict[str, list[str]] = {}
    for k, v in (req.kaplama_name_map or {}).items():
        key = str(k).strip()
        if not key:
            continue
        names = normalize_cost_name_list(v, canonicalize_kaplama=True)
        if not names:
            continue
        kaplama_name_map_exact[key] = names
        kaplama_name_map_ci[key.lower()] = names

    kaplama_map_by_size: dict[str, list[str]] = {}
    for k, v in (req.kaplama_map or {}).items():
        key = str(k).strip()
        if not key:
            continue
        names = normalize_cost_name_list(v, canonicalize_kaplama=True)
        if not names:
            continue
        kaplama_map_by_size[key] = names

    kaplama_fallback_from_cost_map: dict[str, list[str]] = {}
    for k, v in (req.cost_map or {}).items():
        key = str(k).strip()
        if not key:
            continue
        names = normalize_cost_name_list(v, canonicalize_kaplama=True)
        if not names:
            continue
        kaplama_fallback_from_cost_map[key] = names

    for child in children:
        sku = child["child_sku"]
        child_name = (child["child_name"] or "").strip()
        variation_color = (child["variation_color"] or "").strip()
        alan = child["alan_m2"]  # preserved per-child
        size = child["variation_size"] or "(boyutsuz)"

        # Resolve kargo cost_name for this child's size via cost_map
        kargo_cost_name = req.cost_map.get(size)
        if not kargo_cost_name:
            # Try to find a fallback: if cost_map has a "*" / default key
            kargo_cost_name = req.cost_map.get("*")
        if not kargo_cost_name:
            skipped_children_count += 1
            if inherit_detail_limit > 0 and len(skipped_children_sample) < inherit_detail_limit:
                skipped_children_sample.append({"child_sku": sku, "variation_size": size, "reason": "no kargo mapping"})
            continue

        # Resolve kaplama cost_name list:
        # 1) child_name bazlı override (yeni)
        # 2) variation_size bazlı map (geriye dönük)
        # 3) cost_map fallback (eski davranış)
        kaplama_cost_names: list[str] = []
        if child_name:
            lookup_keys = []
            tier_key = build_kaplama_group_key(
                child_name,
                detect_kaplama_tier(child_name, variation_color),
            )
            if tier_key:
                lookup_keys.append(tier_key)
            if variation_color:
                lookup_keys.append(f"{child_name}||{variation_color}")
            lookup_keys.append(child_name)

            seen_keys = set()
            for lookup_key in lookup_keys:
                normalized_lookup = lookup_key.strip()
                if not normalized_lookup:
                    continue
                lowered_lookup = normalized_lookup.lower()
                if lowered_lookup in seen_keys:
                    continue
                seen_keys.add(lowered_lookup)
                kaplama_cost_names = kaplama_name_map_exact.get(normalized_lookup, [])
                if not kaplama_cost_names:
                    kaplama_cost_names = kaplama_name_map_ci.get(lowered_lookup, [])
                if kaplama_cost_names:
                    break
        if not kaplama_cost_names:
            if kaplama_map_by_size:
                kaplama_source_map = kaplama_map_by_size
            elif kaplama_name_map_exact:
                kaplama_source_map = {}
            else:
                kaplama_source_map = kaplama_fallback_from_cost_map
            kaplama_cost_names = kaplama_source_map.get(size, [])
            if not kaplama_cost_names:
                kaplama_cost_names = kaplama_source_map.get("*", [])
        if not kaplama_cost_names:
            if not bool(req.allow_missing_kaplama):
                skipped_children_count += 1
                if inherit_detail_limit > 0 and len(skipped_children_sample) < inherit_detail_limit:
                    skipped_children_sample.append({"child_sku": sku, "variation_size": size, "reason": "no kaplama mapping"})
                continue

        # Resolve cargo weight for this child's size via weight_map
        kargo_agirlik = req.weight_map.get(size)
        if kargo_agirlik is None:
            kargo_agirlik = req.weight_map.get("*")
        if kargo_agirlik is None:
            skipped_children_count += 1
            if inherit_detail_limit > 0 and len(skipped_children_sample) < inherit_detail_limit:
                skipped_children_sample.append({"child_sku": sku, "variation_size": size, "reason": "no weight mapping"})
            continue
        try:
            kargo_agirlik = float(kargo_agirlik)
        except (TypeError, ValueError):
            skipped_children_count += 1
            if inherit_detail_limit > 0 and len(skipped_children_sample) < inherit_detail_limit:
                skipped_children_sample.append({"child_sku": sku, "variation_size": size, "reason": "invalid weight"})
            continue
        if kargo_agirlik < 0:
            skipped_children_count += 1
            if inherit_detail_limit > 0 and len(skipped_children_sample) < inherit_detail_limit:
                skipped_children_sample.append({"child_sku": sku, "variation_size": size, "reason": "negative weight"})
            continue

        kargo_kodu = normalize_kargo_code(kargo_cost_name)
        kargo_meta = kargo_lookup.get(kargo_kodu) if kargo_kodu else None
        kargo_en = kargo_meta["kargo_en"] if kargo_meta else None
        kargo_boy = kargo_meta["kargo_boy"] if kargo_meta else None
        kargo_yukseklik = kargo_meta["kargo_yukseklik"] if kargo_meta else None
        kargo_desi = calculate_kargo_desi(kargo_en, kargo_boy, kargo_yukseklik, kargo_agirlik)

        rounded_agirlik = round(kargo_agirlik, 6)
        product_updates.append((
            kargo_kodu,
            kargo_en,
            kargo_boy,
            kargo_yukseklik,
            rounded_agirlik,
            kargo_desi,
            sku,
        ))

        # 1) Inherit base materials
        for mat_id, quantity in manual_material_assignments:
            material_upserts.append((sku, mat_id, quantity, quantity))

        # 2) Flag kargo + kaplama cost categories for this size
        assigned_costs = [kargo_cost_name, *kaplama_cost_names]
        seen_assigned: set[str] = set()
        for assigned_cost in assigned_costs:
            assigned_cost = str(assigned_cost or "").strip()
            if not assigned_cost:
                continue
            key = assigned_cost.casefold()
            if key in seen_assigned:
                continue
            seen_assigned.add(key)
            cost_upserts.append((sku, assigned_cost))

        # 5-6) Auto-calculate area-driven materials
        child_result = {
            "child_sku": sku, "alan_m2": alan,
            "variation_size": size,
            "cost_name": kargo_cost_name,  # backward compatibility
            "kargo_cost_name": kargo_cost_name,
            "kaplama_cost_name": kaplama_cost_names[0] if kaplama_cost_names else None,
            "kaplama_cost_names": kaplama_cost_names,
            "kargo_kodu": kargo_kodu,
            "kargo_en": kargo_en,
            "kargo_boy": kargo_boy,
            "kargo_yukseklik": kargo_yukseklik,
            "kargo_agirlik": rounded_agirlik,
            "kargo_desi": kargo_desi,
            "strafor": None, "boya_iscilik": None, "sac": None, "mdf": None,
        }

        if alan is not None:
            if strafor_id is not None:
                strafor_qty = round(alan * 1.2, 6)
                material_upserts.append((sku, strafor_id, strafor_qty, strafor_qty))
                child_result["strafor"] = strafor_qty

            if boya_id is not None:
                boya_qty = round(alan * 5, 6)
                material_upserts.append((sku, boya_id, boya_qty, boya_qty))
                child_result["boya_iscilik"] = boya_qty

            if sac_id is not None:
                sac_qty = round(alan, 6)  # Saç = Alan
                material_upserts.append((sku, sac_id, sac_qty, sac_qty))
                child_result["sac"] = sac_qty

            if mdf_id is not None:
                mdf_qty = round(alan, 6)  # MDF = Alan
                material_upserts.append((sku, mdf_id, mdf_qty, mdf_qty))
                child_result["mdf"] = mdf_qty

        updated_children_count += 1
        if inherit_detail_limit > 0 and len(updated_children_sample) < inherit_detail_limit:
            updated_children_sample.append(child_result)

    if product_updates:
        conn.executemany(
            """
            UPDATE products
            SET kargo_kodu = ?,
                kargo_en = ?,
                kargo_boy = ?,
                kargo_yukseklik = ?,
                kargo_agirlik = ?,
                kargo_desi = ?
            WHERE child_sku = ?
            """,
            product_updates,
        )

    if material_upserts:
        conn.executemany(
            """
            INSERT INTO product_materials (child_sku, material_id, quantity)
            VALUES (?, ?, ?)
            ON CONFLICT(child_sku, material_id) DO UPDATE SET quantity = ?
            """,
            material_upserts,
        )

    if cost_upserts:
        conn.executemany(
            """
            INSERT INTO product_costs (child_sku, cost_name, assigned)
            VALUES (?, ?, 1)
            ON CONFLICT(child_sku, cost_name) DO UPDATE SET assigned = 1
            """,
            cost_upserts,
        )

    conn.commit()
    conn.close()
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info(
        "[inherit.apply] parent=%s updated=%s skipped=%s product_updates=%s material_upserts=%s cost_upserts=%s duration_ms=%s",
        req.parent_name,
        updated_children_count,
        skipped_children_count,
        len(product_updates),
        len(material_upserts),
        len(cost_upserts),
        elapsed_ms,
    )
    write_audit_log(
        user,
        "inherit.apply",
        target=req.parent_name,
        details={
            "parent_name": req.parent_name,
            "children_updated": updated_children_count,
            "children_skipped": skipped_children_count,
            "materials_count": len(req.materials or {}),
            "size_cost_map_count": len(req.cost_map or {}),
            "size_kaplama_map_count": len(req.kaplama_map or {}),
            "name_kaplama_map_count": len(req.kaplama_name_map or {}),
            "allow_missing_kaplama": bool(req.allow_missing_kaplama),
            "weight_map_count": len(req.weight_map or {}),
            "approval_id": approval_id,
        },
    )

    if approval_id:
        try:
            conn2 = get_db()
            conn2.execute(
                """
                UPDATE approval_requests
                SET executed_at = CURRENT_TIMESTAMP,
                    execution_result = ?
                WHERE id = ?
                """,
                (
                    json.dumps(
                        {
                            "children_updated": updated_children_count,
                            "children_skipped": skipped_children_count,
                            "parent_name": req.parent_name,
                        },
                        ensure_ascii=False,
                    ),
                    approval_id,
                ),
            )
            conn2.commit()
            conn2.close()
        except Exception:
            pass

    return {
        "status": "ok",
        "parent": req.parent_name,
        "cost_map": req.cost_map,
        "kaplama_map": req.kaplama_map,
        "kaplama_name_map": req.kaplama_name_map,
        "allow_missing_kaplama": bool(req.allow_missing_kaplama),
        "children_updated": updated_children_count,
        "children_skipped": skipped_children_count,
        "details": updated_children_sample,
        "details_truncated": max(0, updated_children_count - len(updated_children_sample)),
        "skipped": skipped_children_sample,
        "skipped_truncated": max(0, skipped_children_count - len(skipped_children_sample)),
    }


# ─────────────────────────── EXPORT ───────────────────────────

@app.post("/api/export")
def export_excel(payload: ExportRequest, request: Request):
    """
    Seçilen ürünleri maliyet_sablonu formatında Excel'e export eder.
    """
    started_at = time.perf_counter()
    user = require_request_user(request)
    conn = get_db()
    query_chunk_size = max(100, int(os.getenv("EXPORT_QUERY_CHUNK_SIZE", "500")))
    requested_skus: list[str] = []
    seen_skus: set[str] = set()
    for raw_sku in payload.child_skus:
        sku = str(raw_sku or "").strip()
        if not sku or sku in seen_skus:
            continue
        seen_skus.add(sku)
        requested_skus.append(sku)

    if not requested_skus:
        conn.close()
        raise HTTPException(status_code=400, detail="Export için SKU listesi boş")

    products_by_sku: dict[str, dict] = {}
    materials_by_sku: dict[str, dict[str, float]] = defaultdict(dict)
    costs_by_sku: dict[str, dict[str, str]] = defaultdict(dict)

    try:
        # Ürünleri toplu çek (N+1 yerine chunked IN sorguları)
        for sku_chunk in chunk_list(requested_skus, query_chunk_size):
            placeholders = ", ".join(["?"] * len(sku_chunk))
            rows = conn.execute(
                f"""
                SELECT child_sku, child_name, variation_color, en, boy,
                       kargo_en, kargo_boy, kargo_yukseklik, kargo_agirlik, kargo_desi
                FROM products
                WHERE is_active = 1
                  AND child_sku IN ({placeholders})
                """,
                sku_chunk,
            ).fetchall()
            for row in rows:
                product = dict(row)
                products_by_sku[product["child_sku"]] = product

        found_skus = [sku for sku in requested_skus if sku in products_by_sku]

        if payload.include_materials and found_skus:
            for sku_chunk in chunk_list(found_skus, query_chunk_size):
                placeholders = ", ".join(["?"] * len(sku_chunk))
                rows = conn.execute(
                    f"""
                    SELECT pm.child_sku, rm.name, pm.quantity
                    FROM product_materials pm
                    JOIN raw_materials rm ON pm.material_id = rm.id
                    WHERE pm.child_sku IN ({placeholders})
                    """,
                    sku_chunk,
                ).fetchall()
                for row in rows:
                    mat = dict(row)
                    materials_by_sku[mat["child_sku"]][mat["name"]] = mat["quantity"]

        if payload.include_costs and found_skus:
            for sku_chunk in chunk_list(found_skus, query_chunk_size):
                placeholders = ", ".join(["?"] * len(sku_chunk))
                rows = conn.execute(
                    f"""
                    SELECT child_sku, cost_name
                    FROM product_costs
                    WHERE assigned = 1
                      AND child_sku IN ({placeholders})
                    """,
                    sku_chunk,
                ).fetchall()
                for row in rows:
                    cost = dict(row)
                    costs_by_sku[cost["child_sku"]][cost["cost_name"]] = "x"
    finally:
        conn.close()

    products_data: list[dict] = []
    for sku in requested_skus:
        p_dict = products_by_sku.get(sku)
        if not p_dict:
            continue
        export_item = {
            "child_sku": p_dict["child_sku"],
            "child_name": p_dict["child_name"],
            "variation_color": p_dict.get("variation_color"),
            "en": p_dict["kargo_en"] if p_dict.get("kargo_en") is not None else p_dict["en"],
            "boy": p_dict["kargo_boy"] if p_dict.get("kargo_boy") is not None else p_dict["boy"],
            "yukseklik": p_dict.get("kargo_yukseklik"),
            "agirlik": p_dict.get("kargo_agirlik"),
            "desi": p_dict.get("kargo_desi"),
        }
        if payload.include_materials:
            export_item["materials"] = materials_by_sku.get(sku, {})
        if payload.include_costs:
            export_item["costs"] = costs_by_sku.get(sku, {})
        products_data.append(export_item)

    if not products_data:
        raise HTTPException(status_code=400, detail="Export edilecek ürün bulunamadı")

    output_path = export_to_template(products_data)
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info(
        "[export] requested=%s exported=%s include_materials=%s include_costs=%s duration_ms=%s",
        len(requested_skus),
        len(products_data),
        bool(payload.include_materials),
        bool(payload.include_costs),
        elapsed_ms,
    )
    write_audit_log(
        user,
        "export.run",
        target=f"{len(products_data)} ürün",
        details={
            "requested_skus": len(requested_skus),
            "exported_skus": len(products_data),
            "include_materials": bool(payload.include_materials),
            "include_costs": bool(payload.include_costs),
            "duration_ms": elapsed_ms,
        },
    )
    return FileResponse(
        output_path,
        filename=os.path.basename(output_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        background=BackgroundTask(safe_unlink, output_path),
    )


@app.get("/api/export/all")
def export_all(request: Request):
    """Tüm ürünleri export eder."""
    conn = get_db()
    all_rows = conn.execute(
        "SELECT child_sku FROM products WHERE is_active = 1 ORDER BY child_sku"
    ).fetchall()
    all_skus = [r.get("child_sku") if isinstance(r, dict) else r["child_sku"] for r in all_rows]
    conn.close()

    return export_excel(ExportRequest(child_skus=all_skus), request)


# ─────────────────────────── TEMPLATE INFO ───────────────────────────

@app.get("/api/template-structure")
def template_structure():
    """Şablonun kolon yapısını döndürür."""
    return get_template_structure()


# ─────────────────────────── DB MANAGEMENT ───────────────────────────

@app.post("/api/sync-products")
def sync_products(data: ProductSyncRequest, request: Request):
    """
    Parent-child listelerini kategori bazlı günceller.
    - categories boş ise tüm kategoriler
    - replace_existing=true ise seçili kategorilerin eski ürünleri silinip yeniden yüklenir
    """
    if not ENABLE_PRODUCT_SYNC:
        raise HTTPException(status_code=403, detail="sync-products bu ortamda kapalı")

    admin = require_admin_user(request)
    try:
        categories = normalize_product_categories(data.categories)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    loaded = load_mapped_products(categories=categories, replace_existing=bool(data.replace_existing))
    deactivated = deactivate_cus_products()
    invalidate_product_groups_cache()
    write_audit_log(
        admin,
        "products.sync",
        details={
            "categories": categories,
            "replace_existing": bool(data.replace_existing),
            "products_loaded": loaded,
            "cus_deactivated": deactivated,
            "supported_categories": get_supported_categories(),
        },
    )
    return {
        "status": "ok",
        "categories": categories,
        "replace_existing": bool(data.replace_existing),
        "products_loaded": loaded,
        "cus_deactivated": deactivated,
    }


@app.post("/api/products/deactivate-cus")
def deactivate_cus_products_api(request: Request):
    """Child_Code'u CUS ile başlayan ürünleri pasife alır."""
    admin = require_admin_user(request)
    deactivated = deactivate_cus_products()
    if deactivated:
        invalidate_product_groups_cache()
    write_audit_log(
        admin,
        "products.deactivate_cus",
        details={"rule": "child_code startswith CUS", "deactivated": deactivated},
    )
    return {
        "status": "ok",
        "rule": "child_code startswith CUS",
        "deactivated": deactivated,
    }


@app.post("/api/reload-db")
def reload_database(request: Request):
    """Veritabanını sıfırdan yeniden yükler."""
    if not ENABLE_RELOAD_DB:
        raise HTTPException(status_code=403, detail="reload-db production ortamında kapalı")
    admin = require_admin_user(request)
    conn = get_db()
    conn.execute("DELETE FROM product_materials")
    conn.execute("DELETE FROM product_costs")
    conn.execute("DELETE FROM cost_definitions")
    conn.execute("DELETE FROM products")
    conn.execute("DELETE FROM raw_materials")
    conn.commit()
    conn.close()

    count = load_mapped_products()
    deactivated = deactivate_cus_products()
    invalidate_product_groups_cache()
    load_default_materials()
    sync_cost_definitions_from_template()
    normalize_legacy_gold_silver_names()
    deactivate_shadowed_kaplama_base_names()
    ensure_default_users()
    write_audit_log(
        admin,
        "db.reload",
        details={"products_loaded": count, "cus_deactivated": deactivated},
    )
    return {"status": "ok", "products_loaded": count, "cus_deactivated": deactivated}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
