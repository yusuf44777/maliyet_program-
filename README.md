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
- `AUTH_SECRET=` (production'da guclu ve min 32 karakter)
- `CORS_ORIGINS=https://app.example.com`
- `ENABLE_RELOAD_DB=false`
- `SEED_DEFAULT_USERS=false`
- `TEMPLATE_PATH` / `TEMPLATE_URL`
- `KARGO_CSV_PATH` / `KARGO_CSV_URL`

### Frontend (`frontend/.env`)

- `VITE_API_URL=` (backend ayri hostsa tam API base URL)
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
