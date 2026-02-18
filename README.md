# Maliyet Programi

Urun bazli maliyet hesaplama, hammadde/kaplama/kargo yonetimi, parent-child urun esleme ve Excel export akisini yoneten full-stack bir sistemdir.

## Tech Stack

- Backend: FastAPI (`backend/`)
- Frontend: React + Vite (`frontend/`)
- Veritabani: SQLite (lokal) / PostgreSQL (production onerilen)

## Ozellikler

- Parent altindan child urunlere kalitsal maliyet atama
- Kaplama ve kargo esleme (manuel + otomatik oneriler)
- Desi hesabi: `max(en*boy*yukseklik/5000, agirlik)` ve `.5`'e yuvarlama
- Rol sistemi (`admin` / `user`)
- Excel maliyet exportu (sablon bazli)

## Proje Yapisi

- `backend/main.py`: API endpointleri ve is kurallari
- `backend/database.py`: SQLite/PostgreSQL baglanti katmani
- `backend/excel_engine.py`: Excel export motoru
- `backend/migrate_sqlite_to_postgres.py`: SQLite -> Postgres migration scripti
- `frontend/src/`: React arayuzu

## Lokal Kurulum

### 1) Backend

```bash
cd backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 2) Frontend

```bash
cd frontend
npm install
cp .env.example .env
npm run dev
```

Varsayilan local URL'ler:
- Frontend: `http://localhost:5173`
- Backend: `http://localhost:8000`

## Ortam Degiskenleri

### Backend (`backend/.env`)

Temel alanlar:
- `APP_ENV=development|production`
- `DATABASE_URL=` (PostgreSQL icin production'da zorunlu)
  - Vercel/serverless + Supabase icin `db.<ref>.supabase.co:5432` yerine `pooler.supabase.com:6543` (Transaction Pooler) URL kullanin.
- `AUTH_SECRET=` (production'da guclu ve min 32 karakter)
- `CORS_ORIGINS=https://app.example.com`
- `ENABLE_RELOAD_DB=false`
- `ENABLE_PRODUCT_SYNC=true` (kategori bazli parent-child guncelleme endpointi)
- `ENABLE_APPROVAL_WORKFLOW=false` (inherit islemlerini onaya dusurur)
- `SEED_DEFAULT_USERS=false`
- `PG_POOL_ENABLED=true`
- `PG_POOL_MIN_CONN=1`
- `PG_POOL_MAX_CONN=3`
- `PG_CONNECT_TIMEOUT=10`
- `TEMPLATE_PATH` / `TEMPLATE_URL`
- `KARGO_CSV_PATH` / `KARGO_CSV_URL`
- `KATEGORI_METAL_CSV_PATH`, `KATEGORI_AHSAP_CSV_PATH`, `KATEGORI_CAM_CSV_PATH`, `KATEGORI_HARITA_CSV_PATH`, `KATEGORI_MOBILYA_CSV_PATH` (opsiyonel CSV override)

### Frontend (`frontend/.env`)

- `VITE_API_URL=` (backend ayri hostsa tam API base URL)
- `VITE_SUPABASE_URL=` (Supabase project URL)
- `VITE_SUPABASE_PUBLISHABLE_DEFAULT_KEY=` (Supabase publishable key)
- `VITE_SHOW_DEFAULT_LOGIN_HINTS=false`
- `VITE_ENABLE_RELOAD_DB=false`

## PostgreSQL'e Gecis

1. Postgres provider'dan baglanti string alin (Neon/Supabase vb.).
2. `backend/.env` icinde `DATABASE_URL` tanimlayin.
3. Migration calistirin:

```bash
cd backend
export DATABASE_URL='postgresql://USER:PASSWORD@HOST:5432/DB?sslmode=require'
python migrate_sqlite_to_postgres.py
```

Opsiyonel farkli SQLite dosyasi:

```bash
python migrate_sqlite_to_postgres.py /path/to/maliyet.db
```

## Parent-Child Liste Guncelleme

CSV'leri guncelleyip tum veritabaniyi sifirlamadan sadece secili kategorileri yenileyebilirsiniz:

```bash
curl -X POST "$API_BASE/api/sync-products" \
  -H "Authorization: Bearer <ADMIN_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"categories":["metal","ahsap","cam","harita","mobilya"],"replace_existing":true}'
```

- `categories` bos birakilirsa tum desteklenen kategoriler senkronize edilir.
- `replace_existing=true` secili kategorilerin eski urunlerini silip CSV'den yeniden yukler.
- `mobilya` kategorisi desteklenir.

## DB Performance Paketi

Backend tarafinda su iyilestirmeler aktif:
- PostgreSQL connection pooling (`psycopg2.pool.ThreadedConnectionPool`)
- stale/broken pooled connection eleme ve tekrar baglanma
- sik sorgular icin index paketi:
  - `products(kategori, parent_name, product_identifier, variation_size)`
  - `product_materials(child_sku, material_id)`
  - `product_costs(child_sku, cost_name, assigned)`
  - `cost_definitions(category, is_active, kargo_code)`
  - `audit_logs(created_at, action, request_id)`
  - `approval_requests(status, created_at, request_type, requested_by)`

## Kalite Guvencesi (QA)

`GET /api/quality/report` (admin) endpointi su kontrolleri doner:
- orphan `product_materials`
- orphan `product_costs`
- tanimsiz/inactive `cost_definition` ile assigned maliyetler
- parent/identifier/variation_size eksik urunler
- case-insensitive duplicate user ve cost name kontrolleri

## Audit + Izlenebilirlik

`audit_logs` artik su alanlari da kaydeder:
- `request_id`, `method`, `path`, `ip_address`, `user_agent`, `status`

Request bazli `x-request-id` response header'i set edilir.  
Admin audit listesi: `GET /api/auth/audit-logs`.

## Onay Workflow

`ENABLE_APPROVAL_WORKFLOW=true` oldugunda:
- admin disi kullanicinin `POST /api/inherit` istegi direkt uygulanmaz, `pending_approval` olarak kaydedilir.
- admin `GET /api/approvals` ile bekleyenleri listeler.
- admin `POST /api/approvals/{approval_id}/review` ile `approve/reject` verir.
- ayni payload, approved `approval_id` ile tekrar gonderildiginde inheritance execute edilir ve `executed_at/execution_result` yazilir.

## Production Notlari

- Vercel tarafinda frontend deploy edin.
- Backend'i serverless disinda uygun bir ortama alin (Render/Railway/Fly/VM).
- SQLite yerine PostgreSQL kullanin.
- Varsayilan kullanici bilgilerini production'da kapatin.
- `reload-db` endpointini production'da kapali tutun.

## Gelistirme Notu

Repo'ya `node_modules`, `.venv`, `dist`, lokal DB ve export dosyalari eklenmemelidir; `.gitignore` buna gore duzenlenmistir.

## Yapan

Yapan Mahir Yusuf Açan (Data Scientist & Dentistry Student).

Bu proje iwa, upp ve cfw şirketleri için yapılmıştır.
