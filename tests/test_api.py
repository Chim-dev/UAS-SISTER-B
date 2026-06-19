"""Integration test endpoint dasar: health, publish, events, validasi skema."""
from __future__ import annotations

import pytest

from conftest import count_event_id, make_event, poll_until

pytestmark = pytest.mark.usefixtures("require_aggregator")


def test_health_ready(client):
    r = client.get("/health/ready")
    assert r.status_code == 200
    assert r.json()["status"] == "ready"


def test_publish_single_event_appears(client):
    ev = make_event()
    r = client.post("/publish", json=ev)
    assert r.status_code == 200
    assert r.json()["accepted"] == 1

    appeared = poll_until(
        lambda: count_event_id(client, ev["topic"], ev["event_id"]) == 1
    )
    assert appeared, "event tunggal tidak muncul di /events setelah diproses"


def test_publish_batch_accepted(client):
    topic = make_event()["topic"]
    batch = [make_event(topic=topic) for _ in range(10)]
    r = client.post("/publish", json=batch)
    assert r.status_code == 200
    assert r.json()["accepted"] == 10

    ok = poll_until(lambda: len(client.get(
        "/events", params={"topic": topic, "limit": 100}).json()) == 10)
    assert ok, "tidak semua event batch terproses"


def test_invalid_event_rejected_422(client):
    # field 'source' & 'event_id' hilang => ditolak validasi.
    bad = {"topic": "t", "timestamp": "2026-06-18T10:00:00Z"}
    r = client.post("/publish", json=bad)
    assert r.status_code == 422


def test_invalid_batch_is_atomic_422(client):
    # Satu item rusak membuat SELURUH batch ditolak (batch atomic).
    good = make_event()
    bad = {"topic": "t", "event_id": "x"}  # tanpa timestamp & source
    r = client.post("/publish", json=[good, bad])
    assert r.status_code == 422
    # event 'good' tidak boleh ikut terproses karena batch ditolak utuh.
    assert not poll_until(
        lambda: count_event_id(client, good["topic"], good["event_id"]) > 0,
        timeout=3.0,
    )


def test_events_filtered_by_topic(client):
    topic_a = make_event()["topic"]
    topic_b = make_event()["topic"]
    client.post("/publish", json=make_event(topic=topic_a))
    client.post("/publish", json=make_event(topic=topic_b))

    poll_until(lambda: len(client.get(
        "/events", params={"topic": topic_a}).json()) == 1)
    rows = client.get("/events", params={"topic": topic_a}).json()
    assert all(e["topic"] == topic_a for e in rows)
