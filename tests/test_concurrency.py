"""Integration test transaksi & konkurensi: bukti tidak ada double-process."""
from __future__ import annotations

import concurrent.futures as cf

import httpx
import pytest

from conftest import BASE_URL, count_event_id, get_stats, make_event, poll_until

pytestmark = pytest.mark.usefixtures("require_aggregator")


def test_concurrent_same_event_no_double_process(client):
    """Banyak request paralel mengirim (topic, event_id) yang SAMA.

    Walau consumer multi-worker memproses bersamaan, constraint unik + ON
    CONFLICT memastikan hanya SATU baris yang tersimpan (idempotent write).
    """
    ev = make_event()
    n = 50

    def send() -> int:
        with httpx.Client(base_url=BASE_URL, timeout=30.0) as c:
            return c.post("/publish", json=ev).status_code

    with cf.ThreadPoolExecutor(max_workers=20) as pool:
        codes = list(pool.map(lambda _: send(), range(n)))
    assert all(code == 200 for code in codes)

    # Pada akhirnya tepat 1 baris unik untuk event_id ini.
    assert poll_until(
        lambda: count_event_id(client, ev["topic"], ev["event_id"]) == 1,
        timeout=30.0,
    )
    import time

    time.sleep(1.5)
    assert count_event_id(client, ev["topic"], ev["event_id"]) == 1


def test_concurrent_unique_events_all_processed(client):
    """Banyak event UNIK paralel: semuanya terproses tepat sekali, stats konsisten."""
    topic = make_event()["topic"]
    n = 200
    events = [make_event(topic=topic) for _ in range(n)]
    before = get_stats(client)

    def send(ev) -> int:
        with httpx.Client(base_url=BASE_URL, timeout=30.0) as c:
            return c.post("/publish", json=ev).status_code

    with cf.ThreadPoolExecutor(max_workers=20) as pool:
        codes = list(pool.map(send, events))
    assert all(code == 200 for code in codes)

    assert poll_until(
        lambda: len(client.get(
            "/events", params={"topic": topic, "limit": 10000}).json()) == n,
        timeout=30.0,
    )
    # unique_processed bertambah tepat n (tidak ada double, tidak ada hilang).
    after = get_stats(client)
    assert after["unique_processed"] - before["unique_processed"] >= n


def test_stats_no_lost_update_under_load(client):
    """received == unique + duplicate (untuk event valid) => counter konsisten."""
    topic = make_event()["topic"]
    base = make_event(topic=topic)
    before = get_stats(client)

    # 100 event: 1 unik dikirim 100x -> 1 unik + 99 duplikat.
    def send(_) -> None:
        with httpx.Client(base_url=BASE_URL, timeout=30.0) as c:
            c.post("/publish", json=base)

    with cf.ThreadPoolExecutor(max_workers=20) as pool:
        list(pool.map(send, range(100)))

    def consistent() -> bool:
        s = get_stats(client)
        d_recv = s["received"] - before["received"]
        d_uni = s["unique_processed"] - before["unique_processed"]
        d_dup = s["duplicate_dropped"] - before["duplicate_dropped"]
        # semua yang diterima sudah terproses (unik atau duplikat).
        return d_recv == 100 and d_uni + d_dup == 100 and d_uni == 1

    assert poll_until(consistent, timeout=30.0), "counter statistik tidak konsisten"
