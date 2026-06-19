"""FastAPI aggregator: API publish/akses event + consumer internal.

Endpoint:
  POST /publish            -> terima single/batch event, validasi, enqueue
  GET  /events?topic=...   -> daftar event unik yang sudah diproses
  GET  /stats              -> received, unique_processed, duplicate_dropped, dst
  GET  /health/live        -> liveness probe
  GET  /health/ready       -> readiness probe (cek DB + Redis)
"""
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from typing import List, Optional, Union

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from . import broker, db
from .config import load_settings
from .consumer import ConsumerManager
from .models import Event, PublishResult, StoredEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("aggregator")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
    app.state.settings = settings
    app.state.start_time = time.time()

    app.state.pool = await db.create_pool(
        settings.database_url, settings.db_pool_min, settings.db_pool_max
    )
    await db.init_schema(app.state.pool)

    app.state.redis = await broker.create_redis(settings.broker_url)
    await app.state.redis.ping()

    app.state.consumer = ConsumerManager(app.state.pool, app.state.redis, settings)
    await app.state.consumer.start()
    log.info("Aggregator siap. workers=%d", settings.consumer_workers)

    try:
        yield
    finally:
        await app.state.consumer.stop()
        await app.state.redis.aclose()
        await app.state.pool.close()
        log.info("Aggregator shutdown selesai")


app = FastAPI(title="Pub-Sub Log Aggregator", version="1.0.0", lifespan=lifespan)


@app.post("/publish", response_model=PublishResult)
async def publish(body: Union[Event, List[Event]]):
    """Terima single event atau batch.

    Kebijakan batch: VALIDASI bersifat atomik. Bila ada satu item yang tidak
    lolos skema, FastAPI/Pydantic menolak seluruh request dengan HTTP 422
    (seluruh batch gagal, tidak ada yang ter-enqueue). Ini menjaga integritas
    boundary batch. Setelah lolos validasi, event di-enqueue ke broker dan
    diproses idempotent oleh consumer.
    """
    events: List[Event] = body if isinstance(body, list) else [body]
    if not events:
        raise HTTPException(status_code=400, detail="batch kosong")

    settings = app.state.settings
    await broker.publish_many(app.state.redis, settings.stream_key, events)
    await db.add_received(app.state.pool, len(events))
    return PublishResult(accepted=len(events), message="enqueued")


@app.get("/events", response_model=List[StoredEvent])
async def get_events(
    topic: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
):
    rows = await db.fetch_events(app.state.pool, topic, limit, offset)
    return rows


@app.get("/stats")
async def get_stats():
    received, unique, dup, topics = await db.fetch_stats(app.state.pool)
    pending = await broker.pending_count(
        app.state.redis,
        app.state.settings.stream_key,
        app.state.settings.consumer_group,
    )
    return {
        "received": received,
        "unique_processed": unique,
        "duplicate_dropped": dup,
        "topics": topics,
        "distinct_topics": len(topics),
        "uptime_seconds": round(time.time() - app.state.start_time, 2),
        "pending_in_stream": pending,
    }


@app.get("/health/live")
async def live():
    return {"status": "alive"}


@app.get("/health/ready")
async def ready():
    try:
        async with app.state.pool.acquire() as conn:
            await conn.execute("SELECT 1")
        await app.state.redis.ping()
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(
            status_code=503, content={"status": "not-ready", "detail": str(exc)}
        )
    return {"status": "ready"}
