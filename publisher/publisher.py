"""Publisher / simulator event.

Menghasilkan event log (termasuk DUPLIKAT secara sengaja) lalu mem-publish ke
aggregator via HTTP POST /publish secara batch. Mengukur throughput, latency,
dan duplicate rate, lalu menampilkan ringkasan metrik untuk laporan.

Konfigurasi via environment:
  TARGET_URL      URL endpoint publish (default http://aggregator:8080/publish)
  TOTAL_EVENTS    total event yang dikirim (default 20000)
  DUPLICATE_RATE  proporsi event yang merupakan duplikat (default 0.3)
  BATCH_SIZE      jumlah event per request (default 500)
  CONCURRENCY     jumlah request batch paralel (default 8)
  TOPICS          daftar topic dipisah koma (default svc-a,svc-b,svc-c)
  WAIT_READY      tunggu aggregator siap sebelum mulai (default true)
"""
from __future__ import annotations

import asyncio
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import httpx

TARGET_URL = os.environ.get("TARGET_URL", "http://aggregator:8080/publish")
STATS_URL = TARGET_URL.rsplit("/", 1)[0] + "/stats"
READY_URL = TARGET_URL.rsplit("/", 1)[0] + "/health/ready"

TOTAL_EVENTS = int(os.environ.get("TOTAL_EVENTS", "20000"))
DUPLICATE_RATE = float(os.environ.get("DUPLICATE_RATE", "0.3"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "8"))
TOPICS = [t.strip() for t in os.environ.get("TOPICS", "svc-a,svc-b,svc-c").split(",")]
WAIT_READY = os.environ.get("WAIT_READY", "true").lower() == "true"


def make_event(topic: str, event_id: str) -> Dict:
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "publisher",
        "payload": {
            "level": random.choice(["INFO", "WARN", "ERROR"]),
            "msg": "log line " + uuid.uuid4().hex[:8],
            "n": random.randint(0, 1_000_000),
        },
    }


def build_events() -> Tuple[List[Dict], int]:
    """Bangun daftar event dengan proporsi duplikat sesuai DUPLICATE_RATE.

    Sebagian event adalah pengiriman ulang (topic, event_id) yang sudah ada,
    mensimulasikan at-least-once delivery / retry dari sisi publisher.
    """
    events: List[Dict] = []
    unique_ids: List[tuple[str, str]] = []
    for _ in range(TOTAL_EVENTS):
        is_dup = unique_ids and random.random() < DUPLICATE_RATE
        if is_dup:
            topic, event_id = random.choice(unique_ids)
        else:
            topic = random.choice(TOPICS)
            event_id = f"{topic}-{uuid.uuid4().hex}"
            unique_ids.append((topic, event_id))
        events.append(make_event(topic, event_id))
    return events, len(unique_ids)


async def wait_ready(client: httpx.AsyncClient, timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = await client.get(READY_URL, timeout=5.0)
            if r.status_code == 200:
                print(f"[publisher] aggregator siap: {READY_URL}")
                return
        except Exception:
            pass
        await asyncio.sleep(1.0)
    raise RuntimeError("aggregator tidak siap dalam batas waktu")


async def send_batch(client: httpx.AsyncClient, batch: List[Dict]) -> float:
    t0 = time.perf_counter()
    r = await client.post(TARGET_URL, json=batch, timeout=60.0)
    r.raise_for_status()
    return time.perf_counter() - t0


async def main() -> None:
    print(
        f"[publisher] target={TARGET_URL} total={TOTAL_EVENTS} "
        f"dup_rate={DUPLICATE_RATE} batch={BATCH_SIZE} concurrency={CONCURRENCY}"
    )
    events, n_unique = build_events()
    n_dup = TOTAL_EVENTS - n_unique
    batches = [events[i : i + BATCH_SIZE] for i in range(0, len(events), BATCH_SIZE)]

    limits = httpx.Limits(max_connections=CONCURRENCY)
    async with httpx.AsyncClient(limits=limits) as client:
        if WAIT_READY:
            await wait_ready(client)

        latencies: List[float] = []
        sem = asyncio.Semaphore(CONCURRENCY)

        async def worker(batch: List[Dict]) -> None:
            async with sem:
                latencies.append(await send_batch(client, batch))

        t_start = time.perf_counter()
        await asyncio.gather(*(worker(b) for b in batches))
        elapsed = time.perf_counter() - t_start

        throughput = TOTAL_EVENTS / elapsed if elapsed > 0 else 0.0
        latencies.sort()
        p50 = latencies[len(latencies) // 2] if latencies else 0.0
        p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0.0

        print("\n========== METRIK PUBLISHER ==========")
        print(f"Total event dikirim : {TOTAL_EVENTS}")
        print(f"Event unik          : {n_unique}")
        print(f"Event duplikat      : {n_dup} ({n_dup / TOTAL_EVENTS:.1%})")
        print(f"Durasi total        : {elapsed:.2f} s")
        print(f"Throughput          : {throughput:,.0f} event/s")
        print(f"Latency batch p50   : {p50 * 1000:.1f} ms")
        print(f"Latency batch p95   : {p95 * 1000:.1f} ms")
        print("======================================\n")

        # Beri waktu consumer menyelesaikan antrian, lalu tampilkan /stats.
        await asyncio.sleep(2.0)
        try:
            r = await client.get(STATS_URL, timeout=10.0)
            print("[publisher] /stats setelah publish:")
            print(r.text)
        except Exception as exc:
            print(f"[publisher] gagal ambil /stats: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
