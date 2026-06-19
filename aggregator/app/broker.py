"""Broker internal berbasis Redis Streams.

Redis Streams + consumer group dipilih (bukan list biasa) karena memberi
semantik **at-least-once** yang benar:
- XADD menambah event ke stream (durable, tersimpan di volume Redis).
- XREADGROUP membagikan event ke worker dalam satu consumer group.
- Worker WAJIB XACK setelah berhasil memproses. Event yang belum di-ACK tetap
  berada di Pending Entries List (PEL).
- Bila worker crash sebelum ACK, event masih pending dan bisa di-reclaim worker
  lain via XAUTOCLAIM (crash recovery). Karena pemrosesan idempotent, redelivery
  ini aman dan tidak menyebabkan double-process.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

import redis.asyncio as aioredis

from .models import Event


async def create_redis(url: str) -> aioredis.Redis:
    return aioredis.from_url(url, decode_responses=True)


async def ensure_group(redis: aioredis.Redis, stream_key: str, group: str) -> None:
    """Buat consumer group bila belum ada (idempotent)."""
    try:
        await redis.xgroup_create(
            name=stream_key, groupname=group, id="0", mkstream=True
        )
    except aioredis.ResponseError as exc:  # type: ignore[attr-defined]
        if "BUSYGROUP" not in str(exc):
            raise


async def publish(redis: aioredis.Redis, stream_key: str, event: Event) -> str:
    """XADD satu event. Mengembalikan stream message id."""
    data = json.dumps(event.model_dump(mode="json"))
    return await redis.xadd(stream_key, {"data": data})


async def publish_many(
    redis: aioredis.Redis, stream_key: str, events: List[Event]
) -> int:
    """XADD batch via pipeline agar throughput tinggi."""
    pipe = redis.pipeline(transaction=False)
    for ev in events:
        pipe.xadd(stream_key, {"data": json.dumps(ev.model_dump(mode="json"))})
    await pipe.execute()
    return len(events)


def decode_event(fields: Dict[str, str]) -> Event:
    return Event.model_validate(json.loads(fields["data"]))


async def read_group(
    redis: aioredis.Redis,
    stream_key: str,
    group: str,
    consumer: str,
    count: int,
    block_ms: int,
) -> List[Tuple[str, Dict[str, str]]]:
    """XREADGROUP pesan baru ('>'). Mengembalikan list (msg_id, fields)."""
    resp = await redis.xreadgroup(
        groupname=group,
        consumername=consumer,
        streams={stream_key: ">"},
        count=count,
        block=block_ms,
    )
    out: List[Tuple[str, Dict[str, str]]] = []
    for _stream, messages in resp or []:
        for msg_id, fields in messages:
            out.append((msg_id, fields))
    return out


async def autoclaim(
    redis: aioredis.Redis,
    stream_key: str,
    group: str,
    consumer: str,
    min_idle_ms: int,
    count: int = 64,
) -> List[Tuple[str, Dict[str, str]]]:
    """Reclaim pesan pending milik worker yang (kemungkinan) mati."""
    result = await redis.xautoclaim(
        name=stream_key,
        groupname=group,
        consumername=consumer,
        min_idle_time=min_idle_ms,
        start_id="0-0",
        count=count,
    )
    # redis-py mengembalikan (next_cursor, claimed_messages, deleted_ids)
    claimed = result[1] if len(result) > 1 else []
    out: List[Tuple[str, Dict[str, str]]] = []
    for msg_id, fields in claimed:
        if fields:  # pesan yang sudah dihapus bisa muncul tanpa fields
            out.append((msg_id, fields))
    return out


async def ack(redis: aioredis.Redis, stream_key: str, group: str, msg_id: str) -> None:
    await redis.xack(stream_key, group, msg_id)


async def pending_count(redis: aioredis.Redis, stream_key: str, group: str) -> int:
    """Jumlah pesan yang belum di-ACK (in-flight) di consumer group."""
    try:
        info: Dict[str, Any] = await redis.xpending(stream_key, group)
        return int(info.get("pending", 0))
    except aioredis.ResponseError:  # type: ignore[attr-defined]
        return 0
