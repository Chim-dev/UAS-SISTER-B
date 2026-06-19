"""Integration test idempotency & deduplication."""
from __future__ import annotations

import pytest

from conftest import count_event_id, get_stats, make_event, poll_until

pytestmark = pytest.mark.usefixtures("require_aggregator")


def test_duplicate_event_processed_once(client):
    ev = make_event()
    # Kirim event yang sama 5x (simulasi at-least-once / retry).
    for _ in range(5):
        assert client.post("/publish", json=ev).status_code == 200

    # Tunggu sampai event muncul, lalu pastikan TETAP hanya 1 baris unik.
    assert poll_until(
        lambda: count_event_id(client, ev["topic"], ev["event_id"]) == 1
    )
    # Beri waktu duplikat lain (bila ada) terproses, lalu cek ulang.
    import time

    time.sleep(1.0)
    assert count_event_id(client, ev["topic"], ev["event_id"]) == 1


def test_duplicate_counter_increases(client):
    before = get_stats(client)
    ev = make_event()
    n = 4  # 1 unik + 3 duplikat
    for _ in range(n):
        client.post("/publish", json=ev)

    def dup_delta_ok() -> bool:
        s = get_stats(client)
        return (
            s["duplicate_dropped"] - before["duplicate_dropped"] >= n - 1
            and s["unique_processed"] - before["unique_processed"] >= 1
        )

    assert poll_until(dup_delta_ok), "counter duplikat/unik tidak konsisten"


def test_idempotent_when_republished_later(client):
    ev = make_event()
    client.post("/publish", json=ev)
    assert poll_until(
        lambda: count_event_id(client, ev["topic"], ev["event_id"]) == 1
    )

    # Publish lagi event yang sama setelah jeda -> tetap idempotent.
    import time

    time.sleep(0.5)
    client.post("/publish", json=ev)
    time.sleep(1.0)
    assert count_event_id(client, ev["topic"], ev["event_id"]) == 1
