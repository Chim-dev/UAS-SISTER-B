"""Unit test validasi skema event (tanpa perlu service hidup)."""
from __future__ import annotations

import os
import sys

import pytest
from pydantic import ValidationError

# Buat modul aggregator.app dapat di-import saat test dijalankan dari root repo.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "aggregator"))

from app.models import Event  # noqa: E402


def test_valid_event_parsed():
    ev = Event.model_validate(
        {
            "topic": "svc-a",
            "event_id": "abc-123",
            "timestamp": "2026-06-18T10:00:00+00:00",
            "source": "publisher",
            "payload": {"level": "INFO"},
        }
    )
    assert ev.topic == "svc-a"
    assert ev.payload["level"] == "INFO"


def test_missing_required_field_rejected():
    with pytest.raises(ValidationError):
        Event.model_validate(
            {"topic": "svc-a", "timestamp": "2026-06-18T10:00:00Z", "source": "p"}
        )


def test_blank_topic_rejected():
    with pytest.raises(ValidationError):
        Event.model_validate(
            {
                "topic": "   ",
                "event_id": "x",
                "timestamp": "2026-06-18T10:00:00Z",
                "source": "p",
            }
        )


def test_bad_timestamp_rejected():
    with pytest.raises(ValidationError):
        Event.model_validate(
            {
                "topic": "t",
                "event_id": "x",
                "timestamp": "bukan-tanggal",
                "source": "p",
            }
        )


def test_payload_optional_defaults_empty():
    ev = Event.model_validate(
        {
            "topic": "t",
            "event_id": "x",
            "timestamp": "2026-06-18T10:00:00Z",
            "source": "p",
        }
    )
    assert ev.payload == {}


def test_batch_of_events_parsed():
    raw = [
        {
            "topic": "t",
            "event_id": f"id-{i}",
            "timestamp": "2026-06-18T10:00:00Z",
            "source": "p",
        }
        for i in range(3)
    ]
    events = [Event.model_validate(r) for r in raw]
    assert len(events) == 3
    assert {e.event_id for e in events} == {"id-0", "id-1", "id-2"}
