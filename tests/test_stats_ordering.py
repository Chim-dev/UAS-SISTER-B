"""Integration test /stats, ordering monotonik, dan stress kecil."""
from __future__ import annotations

import time

import pytest

from conftest import get_stats, make_event, poll_until

pytestmark = pytest.mark.usefixtures("require_aggregator")


def test_stats_shape(client):
    s = get_stats(client)
    for key in (
        "received",
        "unique_processed",
        "duplicate_dropped",
        "topics",
        "distinct_topics",
        "uptime_seconds",
        "pending_in_stream",
    ):
        assert key in s, f"field {key} hilang dari /stats"
    assert isinstance(s["topics"], list)
    assert s["uptime_seconds"] >= 0


def test_seq_is_monotonic_within_topic(client):
    topic = make_event()["topic"]
    n = 30
    client.post("/publish", json=[make_event(topic=topic) for _ in range(n)])
    assert poll_until(
        lambda: len(client.get(
            "/events", params={"topic": topic, "limit": 1000}).json()) == n
    )
    rows = client.get("/events", params={"topic": topic, "limit": 1000}).json()
    seqs = [r["seq"] for r in rows]
    # seq harus naik monoton sesuai urutan ingest (counter monotonik, Bab 5).
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == len(seqs)


def test_small_stress_batch_timing(client):
    """Kirim 2000 event (sebagian duplikat) dan ukur waktu eksekusi publish."""
    topic = make_event()["topic"]
    unique = [make_event(topic=topic) for _ in range(1400)]
    # 600 duplikat dari event unik yang sudah ada.
    dupes = [unique[i % len(unique)] for i in range(600)]
    payload = unique + dupes  # total 2000

    before = get_stats(client)
    t0 = time.perf_counter()
    r = client.post("/publish", json=payload)
    elapsed = time.perf_counter() - t0
    assert r.status_code == 200
    assert r.json()["accepted"] == 2000
    print(f"\n[stress] publish 2000 event dalam {elapsed:.3f}s")

    # Semua event unik akhirnya terproses tepat sekali.
    assert poll_until(
        lambda: len(client.get(
            "/events", params={"topic": topic, "limit": 5000}).json()) == 1400,
        timeout=60.0,
    )
    after = get_stats(client)
    assert after["received"] - before["received"] == 2000
    assert after["unique_processed"] - before["unique_processed"] == 1400
