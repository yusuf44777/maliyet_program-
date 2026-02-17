"""
SQLite -> PostgreSQL veri taşıma scripti.

Kullanım:
  DATABASE_URL='postgresql://...'
  python migrate_sqlite_to_postgres.py [sqlite_db_path]
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

from database import get_db, init_db


TABLES_IN_ORDER = [
    "products",
    "raw_materials",
    "cost_definitions",
    "users",
    "product_materials",
    "product_costs",
    "audit_logs",
]


def load_sqlite_rows(sqlite_conn: sqlite3.Connection, table: str):
    sqlite_conn.row_factory = sqlite3.Row
    rows = sqlite_conn.execute(f"SELECT * FROM {table}").fetchall()
    return [dict(r) for r in rows]


def migrate(sqlite_path: Path):
    if not os.getenv("DATABASE_URL", "").strip():
        raise RuntimeError("DATABASE_URL tanımlı değil. PostgreSQL URL verin.")
    if not sqlite_path.exists():
        raise RuntimeError(f"SQLite dosyası bulunamadı: {sqlite_path}")

    # PG tarafında tabloları hazırla
    init_db()
    pg = get_db()

    # Hedefi temizle
    joined = ", ".join(TABLES_IN_ORDER)
    pg.execute(f"TRUNCATE TABLE {joined} RESTART IDENTITY CASCADE")
    pg.commit()

    sqlite_conn = sqlite3.connect(str(sqlite_path))

    total = 0
    for table in TABLES_IN_ORDER:
        rows = load_sqlite_rows(sqlite_conn, table)
        if not rows:
            print(f"- {table}: 0 satır")
            continue

        cols = list(rows[0].keys())
        col_sql = ", ".join(cols)
        placeholders = ", ".join(["?"] * len(cols))
        insert_sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"

        for row in rows:
            pg.execute(insert_sql, tuple(row.get(c) for c in cols))
            total += 1

        # id sequence'leri MAX(id)'ye çek
        if "id" in cols:
            pg.execute(
                """
                SELECT setval(
                    pg_get_serial_sequence(?, 'id'),
                    COALESCE((SELECT MAX(id) FROM """ + table + """), 1),
                    (SELECT COUNT(*) > 0 FROM """ + table + """)
                )
                """,
                (table,),
            )

        print(f"- {table}: {len(rows)} satır")

    pg.commit()
    pg.close()
    sqlite_conn.close()
    print(f"✅ Migration tamamlandı. Toplam {total} satır taşındı.")


if __name__ == "__main__":
    default_sqlite = Path(__file__).resolve().parent / "maliyet.db"
    sqlite_arg = Path(sys.argv[1]).expanduser() if len(sys.argv) > 1 else default_sqlite
    migrate(sqlite_arg)
