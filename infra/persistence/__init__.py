"""Concrete persistence helpers shared by infrastructure adapters."""

from infra.persistence.json_store import atomic_write_text

__all__ = ["atomic_write_text"]
