"""Lapisan Postgres: connection pool, skema, dan operasi transaksional.

Inti dedup + idempotency ada di sini:
- Tabel ``processed_events`` punya PRIMARY KEY (topic, event_id) => constraint
  unik yang menjamin satu event hanya bisa tersimpan sekali.
- Pemrosesan event memakai ``INSERT ... ON CONFLICT DO NOTHING`` di dalam satu
  transaksi. Operasi ini ATOMIK di level database, sehingga walau beberapa
  worker memproses event yang sama secara bersamaan, hanya satu yang berhasil
  meng-insert (RETURNING mengembalikan baris); sisanya terdeteksi sebagai
  duplikat. Inilah idempotent write pattern yang bebas race condition.

Isolation level: READ COMMITTED (default Postgres). Untuk dedup, READ COMMITTED
sudah cukup karena keamanan dijamin oleh UNIQUE CONSTRAINT (bukan oleh
serialization). Counter statistik diupdate dengan ``SET col = col + 1`` yang
mengunci baris (row-level lock) sehingga bebas lost-update tanpa perlu
SERIALIZABLE. Trade-off ini dibahas di report.md.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from .models import Event

SCHEMA_SQL = """
-- Dedup store: kombinasi (topic, event_id) wajib unik.
CREATE TABLE IF NOT EXISTS processed_events (
    seq          BIGSERIAL,                 -- counter monotonik urutan ingest
    topic        TEXT        NOT NULL,
    event_id     TEXT        NOT NULL,
    event_ts     TIMESTAMPTZ NOT NULL,      -- timestamp dari event (ordering)
    source       TEXT        NOT NULL,
    payload      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, event_id)
);

CREATE INDEX IF NOT EXISTS idx_processed_topic_seq
    ON processed_events (topic, seq);

-- Statistik global, di-update transaksional agar bebas lost-update.
CREATE TABLE IF NOT EXISTS stats (
    id                INT    PRIMARY KEY DEFAULT 1,
    received          BIGINT NOT NULL DEFAULT 0,
    unique_processed  BIGINT NOT NULL DEFAULT 0,
    duplicate_dropped BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT stats_single_row CHECK (id = 1)
);
INSERT INTO stats (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- Audit log untuk observability / bukti deteksi duplikat.
CREATE TABLE IF NOT EXISTS audit_log (
    id       BIGSERIAL PRIMARY KEY,
    topic    TEXT        NOT NULL,
    event_id TEXT        NOT NULL,
    outcome  TEXT        NOT NULL,  -- 'processed' | 'duplicate'
    worker   TEXT        NOT NULL,
    at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


async def create_pool(dsn: str, min_size: int, max_size: int) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn=dsn, min_size=min_size, max_size=max_size, command_timeout=30
    )


async def init_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)


async def add_received(pool: asyncpg.Pool, count: int) -> None:
    """Tambah counter ``received`` secara atomik (bebas lost-update)."""
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE stats SET received = received + $1 WHERE id = 1", count
        )


async def process_event(pool: asyncpg.Pool, event: Event, worker: str) -> str:
    """Proses satu event secara idempotent di dalam SATU transaksi.

    Mengembalikan 'processed' bila event baru, atau 'duplicate' bila (topic,
    event_id) sudah pernah diproses.
    """
    payload_json = json.dumps(event.payload)
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                INSERT INTO processed_events
                    (topic, event_id, event_ts, source, payload)
                VALUES ($1, $2, $3, $4, $5::jsonb)
                ON CONFLICT (topic, event_id) DO NOTHING
                RETURNING seq
                """,
                event.topic,
                event.event_id,
                event.timestamp,
                event.source,
                payload_json,
            )
            if row is not None:
                await conn.execute(
                    "UPDATE stats SET unique_processed = unique_processed + 1 "
                    "WHERE id = 1"
                )
                outcome = "processed"
            else:
                await conn.execute(
                    "UPDATE stats SET duplicate_dropped = duplicate_dropped + 1 "
                    "WHERE id = 1"
                )
                outcome = "duplicate"
            await conn.execute(
                "INSERT INTO audit_log (topic, event_id, outcome, worker) "
                "VALUES ($1, $2, $3, $4)",
                event.topic,
                event.event_id,
                outcome,
                worker,
            )
            return outcome


async def fetch_events(
    pool: asyncpg.Pool, topic: Optional[str], limit: int, offset: int
) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        if topic:
            rows = await conn.fetch(
                """
                SELECT seq, topic, event_id, event_ts, source, payload,
                       processed_at
                FROM processed_events
                WHERE topic = $1
                ORDER BY seq
                LIMIT $2 OFFSET $3
                """,
                topic,
                limit,
                offset,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT seq, topic, event_id, event_ts, source, payload,
                       processed_at
                FROM processed_events
                ORDER BY seq
                LIMIT $1 OFFSET $2
                """,
                limit,
                offset,
            )
    result: List[Dict[str, Any]] = []
    for r in rows:
        result.append(
            {
                "seq": r["seq"],
                "topic": r["topic"],
                "event_id": r["event_id"],
                "timestamp": r["event_ts"],
                "source": r["source"],
                "payload": json.loads(r["payload"]),
                "processed_at": r["processed_at"],
            }
        )
    return result


async def fetch_stats(pool: asyncpg.Pool) -> Tuple[int, int, int, List[Dict[str, Any]]]:
    async with pool.acquire() as conn:
        srow = await conn.fetchrow(
            "SELECT received, unique_processed, duplicate_dropped "
            "FROM stats WHERE id = 1"
        )
        trows = await conn.fetch(
            "SELECT topic, COUNT(*) AS c FROM processed_events "
            "GROUP BY topic ORDER BY c DESC, topic"
        )
    topics = [{"topic": t["topic"], "count": t["c"]} for t in trows]
    return (
        srow["received"],
        srow["unique_processed"],
        srow["duplicate_dropped"],
        topics,
    )
