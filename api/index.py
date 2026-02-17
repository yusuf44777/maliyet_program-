"""
Vercel Python Serverless Function entry point.
FastAPI uygulamasını Vercel runtime'a sunar.
"""

import os
import sys
from pathlib import Path

# Backend modüllerini import edebilmek için path'e ekle
_backend_dir = str(Path(__file__).resolve().parent.parent / "backend")
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

# Vercel'de çalışma dizini backend/ olmalı (storage_utils, excel_engine vb.)
if os.path.isdir(_backend_dir):
    os.chdir(_backend_dir)

# FastAPI app'i import et – Vercel Python runtime "app" adını otomatik tanır
from main import app  # noqa: E402, F401

# Vercel handler (bazı runtime sürümleri 'handler' adını da arar)
handler = app
