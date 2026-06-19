"""Integration test persistensi (opt-in).

Test ini me-recreate container storage + aggregator (volume dipertahankan) lalu
membuktikan dedup store tetap mencegah reprocessing. Karena melibatkan
`docker compose`, test ini hanya berjalan bila env RUN_RESTART_TEST=1 di-set,
agar `pytest` biasa tidak mengganggu stack.

    RUN_RESTART_TEST=1 pytest tests/test_persistence.py
"""
from __future__ import annotations

import os
import subprocess
import time

import httpx
import pytest

from conftest import BASE_URL, count_event_id, make_event, poll_until

pytestmark = pytest.mark.usefixtures("require_aggregator")

RUN = os.environ.get("RUN_RESTART_TEST") == "1"


def _wait_ready(timeout: float = 90.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(f"{BASE_URL}/health/ready", timeout=3.0).status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(1.0)
    return False


@pytest.mark.skipif(not RUN, reason="set RUN_RESTART_TEST=1 untuk menjalankan")
def test_dedup_survives_container_recreate(client):
    ev = make_event()
    client.post("/publish", json=ev)
    assert poll_until(
        lambda: count_event_id(client, ev["topic"], ev["event_id"]) == 1
    )

    # Recreate container (volume named tetap => data persisten).
    subprocess.run(
        ["docker", "compose", "up", "-d", "--force-recreate", "storage", "aggregator"],
        check=True,
    )
    assert _wait_ready(), "aggregator tidak siap setelah recreate"

    # Event lama harus masih ada (bukti persistensi volume).
    assert count_event_id(client, ev["topic"], ev["event_id"]) == 1

    # Publish ulang event yang sama -> tetap tidak diproses ganda.
    client.post("/publish", json=ev)
    time.sleep(1.5)
    assert count_event_id(client, ev["topic"], ev["event_id"]) == 1
