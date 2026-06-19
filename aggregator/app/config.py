"""Konfigurasi service aggregator.

Semua nilai dibaca dari environment variable agar mudah dikonfigurasi melalui
Docker Compose. Tidak ada nilai yang menunjuk ke layanan eksternal publik;
default-nya mengarah ke service internal Compose (storage, broker).
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _normalize_pg_dsn(url: str) -> str:
    """asyncpg menerima skema postgres:// maupun postgresql://.

    Compose biasanya memberi DATABASE_URL berformat ``postgres://...``.
    Kita normalisasi ke ``postgresql://`` agar konsisten.
    """
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://") :]
    return url


@dataclass(frozen=True)
class Settings:
    database_url: str
    broker_url: str

    # Jumlah consumer worker internal yang berjalan paralel di dalam satu
    # proses aggregator. Dipakai untuk membuktikan tidak ada double-process
    # walaupun banyak worker berebut event yang sama.
    consumer_workers: int

    # Konfigurasi Redis Streams (broker internal).
    stream_key: str
    consumer_group: str
    read_count: int          # jumlah pesan diambil per XREADGROUP
    block_ms: int            # blocking timeout XREADGROUP (ms)
    autoclaim_idle_ms: int   # ambang idle untuk reclaim pesan worker yang mati

    # Ukuran connection pool ke Postgres.
    db_pool_min: int
    db_pool_max: int


def load_settings() -> Settings:
    return Settings(
        database_url=_normalize_pg_dsn(
            os.environ.get(
                "DATABASE_URL", "postgres://user:pass@storage:5432/db"
            )
        ),
        broker_url=os.environ.get("BROKER_URL", "redis://broker:6379"),
        consumer_workers=int(os.environ.get("CONSUMER_WORKERS", "4")),
        stream_key=os.environ.get("STREAM_KEY", "events:stream"),
        consumer_group=os.environ.get("CONSUMER_GROUP", "aggregator"),
        read_count=int(os.environ.get("READ_COUNT", "128")),
        block_ms=int(os.environ.get("BLOCK_MS", "2000")),
        autoclaim_idle_ms=int(os.environ.get("AUTOCLAIM_IDLE_MS", "30000")),
        db_pool_min=int(os.environ.get("DB_POOL_MIN", "2")),
        db_pool_max=int(os.environ.get("DB_POOL_MAX", "10")),
    )
