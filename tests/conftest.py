"""Fixtures & helper untuk test.

Test integrasi berjalan terhadap aggregator yang HIDUP (lihat README). Atur
alamatnya via env BASE_URL (default http://localhost:8080). Bila aggregator
tidak terjangkau, test integrasi otomatis di-skip dengan pesan jelas.

Test ditulis agar idempotent terhadap state: setiap test memakai event_id unik
(uuid) sehingga tidak bentrok antar-run, dan memeriksa DELTA pada /stats alih-
alih nilai absolut.
"""
from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

import httpx
import pytest

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8080")


def _aggregator_up() -> bool:
    try:
        r = httpx.get(f"{BASE_URL}/health/ready", timeout=3.0)
        return r.status_code == 200
    except Exception:
        return False


@pytest.fixture(scope="session")
def base_url() -> str:
    return BASE_URL


@pytest.fixture
def client() -> httpx.Client:
    with httpx.Client(base_url=BASE_URL, timeout=30.0) as c:
        yield c


@pytest.fixture
def require_aggregator():
    """Skip test integrasi bila aggregator tidak hidup."""
    if not _aggregator_up():
        pytest.skip(
            f"aggregator tidak terjangkau di {BASE_URL}. "
            "Jalankan `docker compose up --build -d` lebih dulu."
        )


# ----------------------------- helper umum ---------------------------------


def make_event(
    topic: Optional[str] = None,
    event_id: Optional[str] = None,
    source: str = "pytest",
    payload: Optional[Dict] = None,
) -> Dict:
    return {
        "topic": topic or f"t-{uuid.uuid4().hex[:8]}",
        "event_id": event_id or f"e-{uuid.uuid4().hex}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "payload": payload if payload is not None else {"k": "v"},
    }


def poll_until(fn: Callable[[], bool], timeout: float = 20.0, interval: float = 0.2) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return True
        time.sleep(interval)
    return False


def get_stats(client: httpx.Client) -> Dict:
    return client.get("/stats").json()


def events_for(client: httpx.Client, topic: str) -> List[Dict]:
    return client.get("/events", params={"topic": topic, "limit": 10000}).json()


def count_event_id(client: httpx.Client, topic: str, event_id: str) -> int:
    return sum(1 for e in events_for(client, topic) if e["event_id"] == event_id)
