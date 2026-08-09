"""User-scoped PostgreSQL persistence for the original Drift algorithms."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Dict, List, Optional
from uuid import UUID

from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

from cloud.models import DriftContinuum, DriftJournal, DriftRunRecord, DriftSchedule


class UserScopedPostgresDriftStore:
    def __init__(self, engine: Engine, user_id: UUID | str) -> None:
        self._engine = engine
        self._user_id = user_id if isinstance(user_id, UUID) else UUID(str(user_id))

    def close(self) -> None:
        return None

    def last_drift_at(self) -> Optional[datetime]:
        with Session(self._engine) as db:
            value = db.scalar(
                select(func.max(DriftRunRecord.run_at)).where(
                    DriftRunRecord.user_id == self._user_id
                )
            )
            return self._aware(value)

    def can_run(self, now: datetime, min_interval_hours: float) -> bool:
        if min_interval_hours <= 0:
            return True
        last = self.last_drift_at()
        return last is None or (now - last).total_seconds() / 3600 >= min_interval_hours

    def last_run_at_by_skill(self) -> Dict[str, datetime]:
        with Session(self._engine) as db:
            rows = db.execute(
                select(DriftRunRecord.skill, func.max(DriftRunRecord.run_at))
                .where(DriftRunRecord.user_id == self._user_id)
                .group_by(DriftRunRecord.skill)
            )
            return {
                str(skill): self._aware(run_at)
                for skill, run_at in rows
                if run_at is not None
            }

    def record_run(
        self,
        *,
        skill: str,
        now: datetime,
        status: str,
        briefing: str,
        message_result: str,
    ) -> None:
        with Session(self._engine) as db, db.begin():
            db.add(
                DriftRunRecord(
                    user_id=self._user_id,
                    skill=skill,
                    run_at=now,
                    status=status,
                    briefing=briefing,
                    message_result=message_result,
                )
            )

    def recent_runs(self, limit: int = 10) -> List[dict]:
        with Session(self._engine) as db:
            rows = db.scalars(
                select(DriftRunRecord)
                .where(DriftRunRecord.user_id == self._user_id)
                .order_by(DriftRunRecord.run_at.desc(), DriftRunRecord.id.desc())
                .limit(max(1, int(limit)))
            ).all()
            return [
                {
                    "skill": row.skill,
                    "run_at": row.run_at.isoformat(),
                    "status": row.status,
                    "briefing": row.briefing,
                    "message_result": row.message_result,
                }
                for row in rows
            ]

    def load_schedule(self, session_key: str) -> Optional[dict]:
        with Session(self._engine) as db:
            row = db.get(DriftSchedule, (self._user_id, session_key))
            if row is None:
                return None
            return {
                "timer_anchor": row.timer_anchor,
                "next_attempt_at": self._aware(row.next_attempt_at),
            }

    def save_schedule(
        self, session_key: str, timer_anchor: str, next_attempt_at: datetime, now: datetime
    ) -> None:
        with Session(self._engine) as db, db.begin():
            row = db.get(DriftSchedule, (self._user_id, session_key))
            if row is None:
                db.add(
                    DriftSchedule(
                        user_id=self._user_id,
                        session_key=session_key,
                        timer_anchor=timer_anchor,
                        next_attempt_at=next_attempt_at,
                        sampled_at=now,
                    )
                )
            else:
                row.timer_anchor = timer_anchor
                row.next_attempt_at = next_attempt_at
                row.sampled_at = now

    def clear_schedule(self, session_key: str) -> None:
        with Session(self._engine) as db, db.begin():
            row = db.get(DriftSchedule, (self._user_id, session_key))
            if row is not None:
                db.delete(row)

    def append_journal(
        self,
        skill: str,
        entry_type: str,
        payload: dict,
        now: datetime,
        *,
        key: str = "",
        run_id: Optional[int] = None,
    ) -> None:
        clean_skill = str(skill or "").strip()
        clean_type = str(entry_type or "").strip()
        if not clean_skill or not clean_type:
            return
        with Session(self._engine) as db, db.begin():
            db.add(
                DriftJournal(
                    user_id=self._user_id,
                    skill=clean_skill,
                    entry_type=clean_type,
                    entry_key=str(key or "").strip(),
                    payload=dict(payload or {}),
                    run_id=run_id,
                    created_at=now,
                )
            )

    def load_journal(
        self,
        skill: str,
        *,
        entry_type: str = "",
        key: str = "",
        limit: int = 20,
    ) -> List[dict]:
        query = select(DriftJournal).where(
            DriftJournal.user_id == self._user_id,
            DriftJournal.skill == str(skill or "").strip(),
        )
        if entry_type.strip():
            query = query.where(DriftJournal.entry_type == entry_type.strip())
        if key.strip():
            query = query.where(DriftJournal.entry_key == key.strip())
        with Session(self._engine) as db:
            rows = db.scalars(
                query.order_by(DriftJournal.id.desc()).limit(max(1, int(limit)))
            ).all()
        return [self._journal_dict(row) for row in reversed(rows)]

    def recent_self_observations(self, limit: int = 12) -> List[dict]:
        with Session(self._engine) as db:
            rows = db.scalars(
                select(DriftJournal)
                .where(
                    DriftJournal.user_id == self._user_id,
                    DriftJournal.entry_type == "self_observation",
                )
                .order_by(DriftJournal.id.desc())
                .limit(max(1, int(limit)))
            ).all()
        return [
            {**self._journal_dict(row), "skill": row.skill}
            for row in reversed(rows)
        ]

    @staticmethod
    def _journal_dict(row: DriftJournal) -> dict:
        return {
            "id": row.id,
            "entry_type": row.entry_type,
            "key": row.entry_key,
            "payload": dict(row.payload or {}),
            "run_id": row.run_id,
            "created_at": row.created_at.isoformat(),
        }

    def get_continuum(self, skill: str) -> dict:
        with Session(self._engine) as db:
            row = db.get(DriftContinuum, (self._user_id, skill))
            if row is None:
                return {"scratchpad": "", "next_tendency": ""}
            return {
                "scratchpad": row.scratchpad,
                "next_tendency": row.next_tendency,
            }

    def save_continuum(
        self,
        *,
        skill: str,
        now: datetime,
        scratchpad: str = "",
        next_tendency: str = "",
    ) -> None:
        with Session(self._engine) as db, db.begin():
            row = db.get(DriftContinuum, (self._user_id, skill))
            if row is None:
                db.add(
                    DriftContinuum(
                        user_id=self._user_id,
                        skill=skill,
                        scratchpad=scratchpad,
                        next_tendency=next_tendency,
                        updated_at=now,
                    )
                )
            else:
                row.scratchpad = scratchpad
                row.next_tendency = next_tendency
                row.updated_at = now

    @staticmethod
    def _aware(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
