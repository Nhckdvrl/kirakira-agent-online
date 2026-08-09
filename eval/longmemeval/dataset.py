"""LongMemEval loader, kept structurally aligned with the pinned Reference."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


SUPPORTED_QUESTION_TYPES = (
    "single-session-user",
    "single-session-preference",
    "knowledge-update",
)


@dataclass(frozen=True)
class LMETurn:
    role: str
    content: str
    has_answer: bool = False


@dataclass
class LMEInstance:
    question_id: str
    question_type: str
    question: str
    answer: str
    question_date: str
    haystack_session_ids: list[str]
    haystack_dates: list[str]
    haystack_sessions: list[list[LMETurn]]
    answer_session_ids: list[str] = field(default_factory=list)

    @property
    def session_key(self) -> str:
        return f"lme:{self.question_id}"


def load_longmemeval(path: Path | str) -> list[LMEInstance]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError(f"LongMemEval data must be a JSON array, got {type(raw)}")

    instances: list[LMEInstance] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("LongMemEval entries must be JSON objects")
        sessions = [
            [
                LMETurn(
                    role=str(turn.get("role", "user")),
                    content=str(turn.get("content", "")),
                    has_answer=bool(turn.get("has_answer", False)),
                )
                for turn in session
                if isinstance(turn, dict)
            ]
            for session in (item.get("haystack_sessions") or [])
            if isinstance(session, list)
        ]
        instances.append(
            LMEInstance(
                question_id=str(item["question_id"]),
                question_type=str(item.get("question_type", "")),
                question=str(item["question"]),
                answer=str(item["answer"]),
                question_date=str(item.get("question_date", "")),
                haystack_session_ids=[
                    str(value) for value in (item.get("haystack_session_ids") or [])
                ],
                haystack_dates=[
                    str(value) for value in (item.get("haystack_dates") or [])
                ],
                haystack_sessions=sessions,
                answer_session_ids=[
                    str(value) for value in (item.get("answer_session_ids") or [])
                ],
            )
        )
    return instances


def builtin_smoke_instances() -> list[LMEInstance]:
    """One deterministic, zero-cost fixture for each Reference-supported type."""

    return [
        _instance(
            "offline-user",
            "single-session-user",
            "Which city does the user live in?",
            "Nagoya",
            [
                ("user", "I live in Nagoya and commute by train.", True),
                ("assistant", "I will remember that you live in Nagoya.", False),
            ],
        ),
        _instance(
            "offline-preference",
            "single-session-preference",
            "What kind of cafe does the user prefer?",
            "quiet cafes",
            [
                ("user", "I prefer quiet cafes over crowded bars.", True),
                ("assistant", "Quiet cafes are your preference.", False),
            ],
        ),
        _instance(
            "offline-update",
            "knowledge-update",
            "Where does the user live now?",
            "Nagoya",
            [
                ("user", "I used to live in Tokyo.", False),
                ("assistant", "You used to live in Tokyo.", False),
                ("user", "I now live in Nagoya after moving from Tokyo.", True),
                ("assistant", "Your current home is Nagoya.", False),
            ],
        ),
    ]


def _instance(
    question_id: str,
    question_type: str,
    question: str,
    answer: str,
    turns: list[tuple[str, str, bool]],
) -> LMEInstance:
    return LMEInstance(
        question_id=question_id,
        question_type=question_type,
        question=question,
        answer=answer,
        question_date="2026-01-03T00:00:00+00:00",
        haystack_session_ids=[f"source:{question_id}"],
        haystack_dates=["2026-01-01T00:00:00+00:00"],
        haystack_sessions=[
            [LMETurn(role=role, content=content, has_answer=gold) for role, content, gold in turns]
        ],
        answer_session_ids=[f"source:{question_id}"],
    )
