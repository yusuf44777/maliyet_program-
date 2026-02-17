"""
Remote/local dosya çözümleme yardımcıları.
Vercel gibi ortamlarda /tmp üstüne cache alır.
"""

from __future__ import annotations

import hashlib
import os
import time
import urllib.request
from pathlib import Path


def is_http_url(value: str | None) -> bool:
    raw = str(value or "").strip().lower()
    return raw.startswith("http://") or raw.startswith("https://")


def cache_remote_file(url: str, cache_name: str, ttl_seconds: int = 900) -> Path | None:
    """
    URL'den dosyayı indirip /tmp/maliyet_cache altında cacheler.
    TTL dolmadıysa mevcut cache'i döndürür.
    """
    raw_url = str(url or "").strip()
    if not is_http_url(raw_url):
        return None

    cache_root = Path(os.getenv("REMOTE_FILE_CACHE_DIR", "/tmp/maliyet_cache")).expanduser()
    cache_root.mkdir(parents=True, exist_ok=True)

    key = hashlib.sha1(raw_url.encode("utf-8")).hexdigest()[:12]
    safe_name = "".join(ch for ch in str(cache_name or "file.bin") if ch.isalnum() or ch in "._-") or "file.bin"
    target = cache_root / f"{key}_{safe_name}"

    now = time.time()
    if target.exists() and (now - target.stat().st_mtime) <= max(int(ttl_seconds), 0):
        return target

    tmp_target = target.with_suffix(target.suffix + ".tmp")
    try:
        with urllib.request.urlopen(raw_url, timeout=15) as resp:
            data = resp.read()
        if not data:
            return target if target.exists() else None
        tmp_target.write_bytes(data)
        tmp_target.replace(target)
        return target
    except Exception:
        if tmp_target.exists():
            try:
                tmp_target.unlink()
            except Exception:
                pass
        return target if target.exists() else None
