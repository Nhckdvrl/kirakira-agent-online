"""Narrow event-publishing contracts for decoupled runtime services."""

from __future__ import annotations

from typing import Protocol

from bus.event_bus import EventBus, EventSubscription
from bus.events_lifecycle import TurnCommitted


class EventPublisher(Protocol):
    def enqueue(self, event: object): ...


__all__ = ["EventBus", "EventPublisher", "EventSubscription", "TurnCommitted"]
