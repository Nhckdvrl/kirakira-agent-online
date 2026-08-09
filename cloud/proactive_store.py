"""User-scoped PostgreSQL persistence for the original Proactive pipeline."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Dict, List, Sequence
from uuid import UUID

from sqlalchemy import Engine, delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from cloud.models import (
    ProactiveDecision,
    ProactiveDelivery,
    ProactiveEventRecord,
    ProactivePendingAcknowledgement,
    ProactivePushState,
    ProactiveSourceFeedback,
    ProactiveTick,
    ProactiveTickStep,
)


class UserScopedPostgresProactiveStore:
    def __init__(self, engine: Engine, user_id: UUID | str) -> None:
        self._engine = engine
        self._user_id = user_id if isinstance(user_id, UUID) else UUID(str(user_id))

    def close(self) -> None:
        return None

    def ingest(
        self, channel: str, events: Sequence[Dict[str, Any]], now: datetime
    ) -> List[str]:
        new_ids: List[str] = []
        with Session(self._engine) as db, db.begin():
            for event in events:
                item_id = str(event.get("item_id") or "").strip()
                if not item_id:
                    continue
                existing = db.get(ProactiveEventRecord, (self._user_id, item_id))
                if existing is not None:
                    continue
                row = ProactiveEventRecord(
                    user_id=self._user_id,
                    item_id=item_id,
                    channel=channel,
                    source_id=str(event.get("_source") or "").strip(),
                    source_event_id=str(
                        event.get("event_id") or event.get("id") or ""
                    ).strip(),
                    payload=dict(event),
                    first_seen_at=now,
                    status="unread",
                )
                try:
                    with db.begin_nested():
                        db.add(row)
                        db.flush()
                except IntegrityError:
                    continue
                new_ids.append(item_id)
        return new_ids

    def unread(self, channel: str) -> List[Dict[str, Any]]:
        with Session(self._engine) as db:
            rows = db.scalars(
                select(ProactiveEventRecord)
                .where(
                    ProactiveEventRecord.user_id == self._user_id,
                    ProactiveEventRecord.channel == channel,
                    ProactiveEventRecord.status == "unread",
                )
                .order_by(ProactiveEventRecord.first_seen_at)
            ).all()
        result: List[Dict[str, Any]] = []
        for row in rows:
            event = dict(row.payload or {})
            event["item_id"] = row.item_id
            event["_source"] = row.source_id
            event["first_seen_at"] = row.first_seen_at.isoformat()
            result.append(event)
        return result

    def consume(self, item_ids: Sequence[str], now: datetime) -> None:
        del now
        ids = self._ids(item_ids)
        if not ids:
            return
        with Session(self._engine) as db, db.begin():
            db.execute(
                update(ProactiveEventRecord)
                .where(
                    ProactiveEventRecord.user_id == self._user_id,
                    ProactiveEventRecord.item_id.in_(ids),
                )
                .values(status="consumed")
            )

    def consume_and_queue_ack(
        self,
        item_ids: Sequence[str],
        acknowledgements: Dict[str, Sequence[str]],
        now: datetime,
    ) -> None:
        ids = self._ids(item_ids)
        with Session(self._engine) as db, db.begin():
            if ids:
                db.execute(
                    update(ProactiveEventRecord)
                    .where(
                        ProactiveEventRecord.user_id == self._user_id,
                        ProactiveEventRecord.item_id.in_(ids),
                    )
                    .values(status="consumed")
                )
            self._queue_acks(db, acknowledgements, now)

    def queue_acknowledgements(
        self, acknowledgements: Dict[str, Sequence[str]], now: datetime
    ) -> None:
        with Session(self._engine) as db, db.begin():
            self._queue_acks(db, acknowledgements, now)

    def _queue_acks(
        self,
        db: Session,
        acknowledgements: Dict[str, Sequence[str]],
        now: datetime,
    ) -> None:
        for source_id, event_ids in acknowledgements.items():
            source = str(source_id).strip()
            for event_id in self._ids(event_ids):
                key = (self._user_id, source, event_id)
                if source and db.get(ProactivePendingAcknowledgement, key) is None:
                    db.add(
                        ProactivePendingAcknowledgement(
                            user_id=self._user_id,
                            source_id=source,
                            source_event_id=event_id,
                            queued_at=now,
                        )
                    )

    def pending_acknowledgements(self) -> Dict[str, List[str]]:
        with Session(self._engine) as db:
            rows = db.scalars(
                select(ProactivePendingAcknowledgement)
                .where(ProactivePendingAcknowledgement.user_id == self._user_id)
                .order_by(
                    ProactivePendingAcknowledgement.queued_at,
                    ProactivePendingAcknowledgement.source_id,
                    ProactivePendingAcknowledgement.source_event_id,
                )
            ).all()
        grouped: Dict[str, List[str]] = {}
        for row in rows:
            grouped.setdefault(row.source_id, []).append(row.source_event_id)
        return grouped

    def mark_acknowledged(
        self, source_id: str, source_event_ids: Sequence[str]
    ) -> None:
        ids = self._ids(source_event_ids)
        if not source_id or not ids:
            return
        with Session(self._engine) as db, db.begin():
            db.execute(
                delete(ProactivePendingAcknowledgement).where(
                    ProactivePendingAcknowledgement.user_id == self._user_id,
                    ProactivePendingAcknowledgement.source_id == source_id,
                    ProactivePendingAcknowledgement.source_event_id.in_(ids),
                )
            )

    def queue_feedback(
        self,
        source_id: str,
        source_event_ids: Sequence[str],
        feedback: str,
        now: datetime,
        reason: str = "",
    ) -> None:
        with Session(self._engine) as db, db.begin():
            for event_id in self._ids(source_event_ids):
                if not source_id or not feedback:
                    continue
                key = (self._user_id, source_id, event_id, feedback)
                row = db.get(ProactiveSourceFeedback, key)
                if row is None:
                    db.add(
                        ProactiveSourceFeedback(
                            user_id=self._user_id,
                            source_id=source_id,
                            source_event_id=event_id,
                            feedback=feedback,
                            reason=reason[:300],
                            queued_at=now,
                            status="pending",
                        )
                    )
                else:
                    row.reason = reason[:300]
                    row.queued_at = now
                    row.status = "pending"

    def pending_feedback(self) -> List[Dict[str, str]]:
        with Session(self._engine) as db:
            rows = db.scalars(
                select(ProactiveSourceFeedback)
                .where(
                    ProactiveSourceFeedback.user_id == self._user_id,
                    ProactiveSourceFeedback.status == "pending",
                )
                .order_by(
                    ProactiveSourceFeedback.queued_at,
                    ProactiveSourceFeedback.source_id,
                    ProactiveSourceFeedback.source_event_id,
                )
            ).all()
        return [
            {
                "source_id": row.source_id,
                "source_event_id": row.source_event_id,
                "feedback": row.feedback,
                "reason": row.reason,
            }
            for row in rows
        ]

    def mark_feedback_sent(
        self, source_id: str, source_event_id: str, feedback: str
    ) -> None:
        with Session(self._engine) as db, db.begin():
            row = db.get(
                ProactiveSourceFeedback,
                (self._user_id, source_id, source_event_id, feedback),
            )
            if row is not None:
                row.status = "sent"

    def record_tick_start(
        self, tick_id: str, session_key: str, started_at: datetime
    ) -> None:
        with Session(self._engine) as db, db.begin():
            row = db.get(ProactiveTick, (self._user_id, tick_id))
            if row is None:
                db.add(
                    ProactiveTick(
                        user_id=self._user_id,
                        tick_id=tick_id,
                        session_key=session_key,
                        started_at=started_at,
                        status="running",
                        step_count=0,
                        error="",
                    )
                )
            else:
                row.session_key = session_key
                row.started_at = started_at
                row.status = "running"

    def record_tick_step(
        self,
        tick_id: str,
        step_index: int,
        slot: str,
        status: str,
        duration_ms: int,
        *,
        terminal: str | None = None,
        error: str = "",
    ) -> None:
        with Session(self._engine) as db, db.begin():
            row = db.get(
                ProactiveTickStep, (self._user_id, tick_id, step_index)
            )
            values = {
                "slot": slot,
                "status": status,
                "duration_ms": duration_ms,
                "terminal": terminal,
                "error": error[:500],
            }
            if row is None:
                db.add(
                    ProactiveTickStep(
                        user_id=self._user_id,
                        tick_id=tick_id,
                        step_index=step_index,
                        **values,
                    )
                )
            else:
                for key, value in values.items():
                    setattr(row, key, value)

    def record_tick_finish(
        self,
        tick_id: str,
        finished_at: datetime,
        terminal: str | None,
        status: str,
        step_count: int,
        error: str = "",
    ) -> None:
        with Session(self._engine) as db, db.begin():
            row = db.get(ProactiveTick, (self._user_id, tick_id))
            if row is not None:
                row.finished_at = finished_at
                row.terminal = terminal
                row.status = status
                row.step_count = step_count
                row.error = error[:500]

    def recent_ticks(self, limit: int = 10) -> List[Dict[str, Any]]:
        with Session(self._engine) as db:
            ticks = db.scalars(
                select(ProactiveTick)
                .where(ProactiveTick.user_id == self._user_id)
                .order_by(ProactiveTick.started_at.desc())
                .limit(max(1, int(limit)))
            ).all()
            result: List[Dict[str, Any]] = []
            for tick in ticks:
                steps = db.scalars(
                    select(ProactiveTickStep)
                    .where(
                        ProactiveTickStep.user_id == self._user_id,
                        ProactiveTickStep.tick_id == tick.tick_id,
                    )
                    .order_by(ProactiveTickStep.step_index)
                ).all()
                result.append(
                    {
                        "tick_id": tick.tick_id,
                        "session_key": tick.session_key,
                        "started_at": tick.started_at.isoformat(),
                        "finished_at": tick.finished_at.isoformat()
                        if tick.finished_at
                        else None,
                        "terminal": tick.terminal,
                        "status": tick.status,
                        "step_count": tick.step_count,
                        "error": tick.error,
                        "steps": [
                            {
                                "step_index": step.step_index,
                                "slot": step.slot,
                                "status": step.status,
                                "terminal": step.terminal,
                                "duration_ms": step.duration_ms,
                                "error": step.error,
                            }
                            for step in steps
                        ],
                    }
                )
            return result

    def expire_old(self, channel: str, now: datetime, max_age_days: float) -> int:
        if max_age_days <= 0:
            return 0
        with Session(self._engine) as db, db.begin():
            result = db.execute(
                update(ProactiveEventRecord)
                .where(
                    ProactiveEventRecord.user_id == self._user_id,
                    ProactiveEventRecord.channel == channel,
                    ProactiveEventRecord.status == "unread",
                    ProactiveEventRecord.first_seen_at
                    < now - timedelta(days=max_age_days),
                )
                .values(status="expired")
            )
            return int(result.rowcount or 0)

    def last_push_at(self, session_key: str) -> datetime | None:
        with Session(self._engine) as db:
            row = db.get(ProactivePushState, (self._user_id, session_key))
            return self._aware(row.last_push_at) if row is not None else None

    def mark_push(self, session_key: str, now: datetime) -> None:
        with Session(self._engine) as db, db.begin():
            row = db.get(ProactivePushState, (self._user_id, session_key))
            if row is None:
                db.add(
                    ProactivePushState(
                        user_id=self._user_id,
                        session_key=session_key,
                        last_push_at=now,
                    )
                )
            else:
                row.last_push_at = now

    def is_delivery_duplicate(
        self,
        session_key: str,
        delivery_key: str,
        window_hours: int,
        now: datetime,
    ) -> bool:
        with Session(self._engine) as db:
            row = db.get(
                ProactiveDelivery, (self._user_id, session_key, delivery_key)
            )
            if row is None:
                return False
            sent_at = self._aware(row.sent_at)
            return bool(
                sent_at
                and sent_at >= now - timedelta(hours=max(int(window_hours), 1))
            )

    def mark_delivery(self, session_key: str, delivery_key: str, now: datetime) -> None:
        with Session(self._engine) as db, db.begin():
            row = db.get(
                ProactiveDelivery, (self._user_id, session_key, delivery_key)
            )
            if row is None:
                db.add(
                    ProactiveDelivery(
                        user_id=self._user_id,
                        session_key=session_key,
                        delivery_key=delivery_key,
                        sent_at=now,
                    )
                )
            else:
                row.sent_at = now

    def unmark_delivery(self, session_key: str, delivery_key: str) -> None:
        with Session(self._engine) as db, db.begin():
            row = db.get(
                ProactiveDelivery, (self._user_id, session_key, delivery_key)
            )
            if row is not None:
                db.delete(row)

    def record_decision(self, now: datetime, action: str, detail: str = "") -> None:
        with Session(self._engine) as db, db.begin():
            db.add(
                ProactiveDecision(
                    user_id=self._user_id,
                    decided_at=now,
                    action=action,
                    detail=detail[:300],
                )
            )

    def recent_decisions(self, limit: int = 10) -> List[Dict[str, Any]]:
        with Session(self._engine) as db:
            rows = db.scalars(
                select(ProactiveDecision)
                .where(ProactiveDecision.user_id == self._user_id)
                .order_by(ProactiveDecision.id.desc())
                .limit(max(1, int(limit)))
            ).all()
            return [
                {
                    "decided_at": row.decided_at.isoformat(),
                    "action": row.action,
                    "detail": row.detail,
                }
                for row in rows
            ]

    def unread_count(self, channel: str) -> int:
        with Session(self._engine) as db:
            return int(
                db.scalar(
                    select(func.count(ProactiveEventRecord.item_id)).where(
                        ProactiveEventRecord.user_id == self._user_id,
                        ProactiveEventRecord.channel == channel,
                        ProactiveEventRecord.status == "unread",
                    )
                )
                or 0
            )

    def in_cooldown(
        self, session_key: str, now: datetime, cooldown_hours: float
    ) -> bool:
        if cooldown_hours <= 0:
            return False
        last = self.last_push_at(session_key)
        return last is not None and (now - last).total_seconds() / 3600 < cooldown_hours

    @staticmethod
    def _ids(values: Sequence[object]) -> List[str]:
        return [str(value).strip() for value in values if str(value).strip()]

    @staticmethod
    def _aware(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
