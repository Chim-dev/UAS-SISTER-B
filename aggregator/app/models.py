"""Skema event dan model API (Pydantic v2).

Event JSON minimal sesuai spesifikasi:
    {
      "topic": "string",
      "event_id": "string-unik",
      "timestamp": "ISO8601",
      "source": "string",
      "payload": { ... }
    }
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Union

from pydantic import BaseModel, Field, field_validator


class Event(BaseModel):
    """Satu event log yang dipublish ke aggregator."""

    topic: str = Field(min_length=1, max_length=256)
    event_id: str = Field(min_length=1, max_length=256)
    timestamp: datetime
    source: str = Field(min_length=1, max_length=256)
    payload: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("topic", "event_id", "source")
    @classmethod
    def _not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("tidak boleh kosong / hanya spasi")
        return v


# Body /publish menerima single event ATAU batch (list of events).
PublishBody = Union[Event, List[Event]]


class PublishResult(BaseModel):
    accepted: int
    message: str


class TopicStat(BaseModel):
    topic: str
    count: int


class Stats(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: List[TopicStat]
    distinct_topics: int
    uptime_seconds: float
    pending_in_stream: int


class StoredEvent(BaseModel):
    seq: int
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: Dict[str, Any]
    processed_at: datetime
