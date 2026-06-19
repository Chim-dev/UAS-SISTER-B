"""Consumer internal: kumpulan worker async yang memproses event dari broker.

Beberapa worker berjalan paralel dalam satu proses (dikontrol CONSUMER_WORKERS).
Tujuannya membuktikan: walau banyak worker berebut event, dedup berbasis
constraint unik + transaksi memastikan TIDAK ada double-process.
"""
from __future__ import annotations

import asyncio
import logging
import time

import asyncpg
import redis.asyncio as aioredis

from . import broker, db
from .config import Settings

log = logging.getLogger("aggregator.consumer")


class ConsumerManager:
    def __init__(
        self, pool: asyncpg.Pool, redis: aioredis.Redis, settings: Settings
    ) -> None:
        self.pool = pool
        self.redis = redis
        self.settings = settings
        self._tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()

    async def start(self) -> None:
        await broker.ensure_group(
            self.redis, self.settings.stream_key, self.settings.consumer_group
        )
        for i in range(self.settings.consumer_workers):
            name = f"worker-{i}"
            self._tasks.append(asyncio.create_task(self._run_worker(name)))
        log.info("Started %d consumer worker(s)", self.settings.consumer_workers)

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        log.info("Consumer workers stopped")

    async def _handle(self, msg_id: str, fields: dict, worker: str) -> None:
        try:
            event = broker.decode_event(fields)
        except Exception:  # pesan rusak: ACK supaya tidak macet, catat log
            log.exception("Pesan tidak bisa di-decode, di-ACK & dilewati: %s", msg_id)
            await broker.ack(
                self.redis, self.settings.stream_key, self.settings.consumer_group, msg_id
            )
            return

        # Retry dengan exponential backoff untuk error transient DB.
        delay = 0.1
        for attempt in range(5):
            try:
                outcome = await db.process_event(self.pool, event, worker)
                if outcome == "duplicate":
                    log.info(
                        "DUPLICATE dropped topic=%s event_id=%s (worker=%s)",
                        event.topic,
                        event.event_id,
                        worker,
                    )
                else:
                    log.debug(
                        "processed topic=%s event_id=%s (worker=%s)",
                        event.topic,
                        event.event_id,
                        worker,
                    )
                # ACK hanya setelah commit DB sukses => at-least-once aman.
                await broker.ack(
                    self.redis,
                    self.settings.stream_key,
                    self.settings.consumer_group,
                    msg_id,
                )
                return
            except (asyncpg.PostgresError, OSError) as exc:
                log.warning(
                    "Gagal proses (attempt %d) event_id=%s: %s; retry in %.2fs",
                    attempt + 1,
                    event.event_id,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 2.0)
        # Bila tetap gagal: TIDAK di-ACK, biarkan pending untuk reclaim nanti.
        log.error("Menyerah memproses event_id=%s, tetap pending", event.event_id)

    async def _run_worker(self, name: str) -> None:
        last_autoclaim = 0.0
        while not self._stop.is_set():
            try:
                msgs = await broker.read_group(
                    self.redis,
                    self.settings.stream_key,
                    self.settings.consumer_group,
                    name,
                    self.settings.read_count,
                    self.settings.block_ms,
                )
                for msg_id, fields in msgs:
                    await self._handle(msg_id, fields, name)

                # Reclaim pesan pending milik worker mati secara periodik.
                now = time.monotonic()
                if now - last_autoclaim > 5.0:
                    last_autoclaim = now
                    claimed = await broker.autoclaim(
                        self.redis,
                        self.settings.stream_key,
                        self.settings.consumer_group,
                        name,
                        self.settings.autoclaim_idle_ms,
                    )
                    for msg_id, fields in claimed:
                        log.info("Reclaim pending msg %s oleh %s", msg_id, name)
                        await self._handle(msg_id, fields, name)
            except asyncio.CancelledError:
                break
            except Exception:  # noqa: BLE001 - worker tidak boleh mati diam-diam
                log.exception("Error tak terduga di %s, lanjut loop", name)
                await asyncio.sleep(0.5)
