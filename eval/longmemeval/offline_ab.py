"""Zero-cost Akasha vs Memory2 retrieval and persistence verifier.

This deliberately does not claim model-answer quality.  It uses the real engine,
store, event and session implementations while replacing only paid LLM and
embedding calls with deterministic local implementations.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from agent.config_models import (
    Config,
    MemoryConfig,
    MemoryEmbeddingConfig,
)
from infra.providers.model_client_adapter import LLMResponse
from plugins.akasha.config import AkashaConfig
from plugins.akasha.core import build_idf_table, set_idf_table
from plugins.akasha.engine import AkashaMemoryEngine
from plugins.default_memory.engine import DefaultMemoryEngine
from plugins.default_memory.config import (
    DefaultMemoryConfig,
    RetrievalConfig,
    RetrievalThresholdsConfig,
)
from core.memory.engine import MemoryQuery, MemoryScope
from core.memory.events import ConsolidationCommitted
from bus.event_bus import EventBus
from bus.events_lifecycle import TurnCommitted
from session.manager import SessionManager

from .dataset import (
    LMEInstance,
    LMETurn,
    SUPPORTED_QUESTION_TYPES,
    builtin_smoke_instances,
    load_longmemeval,
)
from .metrics import normalise, reciprocal_rank


_DIMENSION = 64
_TOKEN_RE = re.compile(r"[a-z0-9]+|[\u3400-\u9fff]", re.IGNORECASE)


class DeterministicEmbedder:
    """Stable hashing vectorizer; it never reads credentials or uses the network."""

    model_id = "offline-hash-v1"

    def __init__(self, dimension: int = _DIMENSION) -> None:
        self.dimension = dimension
        self.calls = 0
        self.texts = 0

    async def embed(self, text: str) -> list[float]:
        return (await self.embed_batch([text]))[0]

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        self.calls += 1
        self.texts += len(texts)
        return [_hash_vector(text, self.dimension) for text in texts]

    async def aclose(self) -> None:
        return None


class DeterministicProvider:
    """Minimal local substitute for Reference extraction and HyDE calls."""

    def __init__(self) -> None:
        self.calls = 0

    async def chat(
        self,
        *,
        messages: list[dict[str, Any]],
        tools: list[object],
        model: str,
        max_tokens: int,
        **kwargs: Any,
    ) -> LLMResponse:
        _ = (tools, model, max_tokens, kwargs)
        self.calls += 1
        prompt = "\n".join(str(item.get("content") or "") for item in messages)
        if "长期记忆提取专家" in prompt:
            return LLMResponse(
                content='{"profile": [], "preference": [], "procedure": []}'
            )
        # Empty hypotheses make answer retrieval use the literal benchmark query.
        return LLMResponse(content="")


class NetworkForbidden:
    async def post(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("offline evaluation attempted an HTTP request")

    async def get(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("offline evaluation attempted an HTTP request")


@dataclass(frozen=True)
class OfflineCaseResult:
    engine: str
    question_id: str
    question_type: str
    question: str
    gold_answer: str
    gold_message_refs: list[str]
    retrieved: list[dict[str, object]]
    evidence_rank: int | None
    answer_text_rank: int | None
    reciprocal_rank: float
    retrieval_hit: bool
    trace: dict[str, object]
    state: dict[str, object]
    offline_calls: dict[str, int]


def _hash_vector(text: str, dimension: int) -> list[float]:
    words = _TOKEN_RE.findall(normalise(text))
    features = list(words)
    features.extend(f"{left}_{right}" for left, right in zip(words, words[1:]))
    vector = [0.0] * dimension
    for feature in features:
        digest = hashlib.blake2b(feature.encode("utf-8"), digest_size=8).digest()
        index = int.from_bytes(digest[:4], "big") % dimension
        sign = 1.0 if digest[4] & 1 else -1.0
        vector[index] += sign
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0:
        return vector
    return [value / norm for value in vector]


def _parse_timestamp(raw: str, fallback_day: int) -> datetime:
    text = str(raw or "").strip()
    if text:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime(2026, 1, max(1, min(28, fallback_day)), tzinfo=timezone.utc)


def _turn_groups(turns: Iterable[LMETurn]) -> list[list[LMETurn]]:
    groups: list[list[LMETurn]] = []
    current: list[LMETurn] = []
    for turn in turns:
        if turn.role == "user" and current:
            groups.append(current)
            current = []
        if turn.role not in {"user", "assistant"}:
            continue
        current.append(turn)
        if turn.role == "assistant":
            groups.append(current)
            current = []
    if current:
        groups.append(current)
    return groups


def _replace_default_embedder(
    engine: DefaultMemoryEngine, embedder: DeterministicEmbedder
) -> None:
    engine._embedder = embedder  # type: ignore[assignment]
    if engine._memorizer is not None:
        engine._memorizer._embedder = embedder  # type: ignore[assignment]
    if engine._retriever is not None:
        engine._retriever._embedder = embedder  # type: ignore[assignment]


def _config() -> Config:
    return Config(
        model="offline-deterministic",
        memory=MemoryConfig(
            enabled=True,
            plugin="default",
            engine="default",
            embedding=MemoryEmbeddingConfig(
                model="offline-hash-v1",
                base_url="offline://forbidden",
                output_dimensionality=_DIMENSION,
            ),
        ),
    )


def _source_refs(record: object) -> set[str]:
    refs: set[str] = set()
    for evidence in getattr(record, "evidence", []) or []:
        candidates = [
            getattr(evidence, "source_ref", ""),
            *(getattr(evidence, "refs", []) or []),
        ]
        for candidate in candidates:
            raw = str(candidate or "").strip()
            if not raw:
                continue
            base = raw.split("#", 1)[0]
            if base.startswith("["):
                try:
                    loaded = json.loads(base)
                except json.JSONDecodeError:
                    loaded = []
                if isinstance(loaded, list):
                    refs.update(str(item) for item in loaded)
                    continue
            refs.add(base)
    return refs


async def _ingest_case(
    *,
    engine_name: str,
    engine: object,
    bus: EventBus,
    sessions: SessionManager,
    instance: LMEInstance,
) -> list[str]:
    session = sessions.get_or_create(instance.session_key)
    session.metadata.update({"channel": "benchmark", "chat_id": instance.question_id})
    gold_refs: list[str] = []

    for session_index, raw_turns in enumerate(instance.haystack_sessions):
        timestamp = _parse_timestamp(
            instance.haystack_dates[session_index]
            if session_index < len(instance.haystack_dates)
            else "",
            session_index + 1,
        )
        for group in _turn_groups(raw_turns):
            user = next((turn for turn in group if turn.role == "user"), None)
            assistant = next((turn for turn in group if turn.role == "assistant"), None)
            if engine_name == "akasha" and user is not None:
                await engine.query(  # type: ignore[attr-defined]
                    MemoryQuery(
                        text=user.content,
                        intent="context",
                        effect="stateful",
                        scope=MemoryScope(
                            session_key=instance.session_key,
                            channel="benchmark",
                            chat_id=instance.question_id,
                        ),
                        timestamp=timestamp,
                    )
                )

            group_refs: list[str] = []
            for turn in group:
                message = session.add_message(turn.role, turn.content)
                message["timestamp"] = timestamp.isoformat()
                source_ref = str(message["id"])
                group_refs.append(source_ref)
                if turn.has_answer:
                    gold_refs.append(source_ref)
            sessions.save(session)

            if engine_name == "akasha" and user is not None:
                await bus.fanout(
                    TurnCommitted(
                        session_key=instance.session_key,
                        channel="benchmark",
                        chat_id=instance.question_id,
                        input_message=user.content,
                        persisted_user_message=user.content,
                        assistant_response=assistant.content if assistant else "",
                        tools_used=(),
                        timestamp=timestamp,
                    )
                )
            elif engine_name == "default":
                text = " ".join(
                    f"{turn.role}: {turn.content}" for turn in group if turn.content.strip()
                )
                await bus.fanout(
                    ConsolidationCommitted(
                        history_entry_payloads=[(text, 0)] if text else [],
                        source_ref=json.dumps(group_refs, ensure_ascii=False),
                        scope_channel="benchmark",
                        scope_chat_id=instance.question_id,
                        conversation=text,
                    )
                )
    return gold_refs


def _refresh_akasha_idf(engine: AkashaMemoryEngine, workspace: Path) -> None:
    """Finish the same FTS/IDF projection that Reference rebuild requires."""

    idf = build_idf_table(str(workspace / "sessions.db"), engine._store.db)
    set_idf_table(idf)


def _integrity(path: Path) -> str:
    with sqlite3.connect(path) as db:
        row = db.execute("PRAGMA integrity_check").fetchone()
    return str(row[0] if row else "missing")


def _count(path: Path, table: str) -> int:
    with sqlite3.connect(path) as db:
        row = db.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()
    return int(row[0] if row else 0)


def _state_snapshot(engine_name: str, workspace: Path) -> dict[str, object]:
    sessions_db = workspace / "sessions.db"
    if engine_name == "default":
        memory_db = workspace / "memory" / "coremem.db"
        return {
            "sessions_integrity": _integrity(sessions_db),
            "memory_integrity": _integrity(memory_db),
            "messages": _count(sessions_db, "messages"),
            "memory_items": _count(memory_db, "memory_items"),
        }
    akasha_db = workspace / "memory" / "akasha.db"
    return {
        "sessions_integrity": _integrity(sessions_db),
        "memory_integrity": _integrity(akasha_db),
        "messages": _count(sessions_db, "messages"),
        "message_embeddings": _count(sessions_db, "message_embeddings"),
        "akasha_nodes": _count(akasha_db, "akasha_nodes"),
        "akasha_edges": _count(akasha_db, "akasha_edges"),
        "activation_events": _count(akasha_db, "akasha_activation_events"),
        "fts_idf_tokens": _count(akasha_db, "fts_token_idf"),
    }


async def _run_engine_case(
    engine_name: str,
    instance: LMEInstance,
    workspace: Path,
) -> OfflineCaseResult:
    workspace.mkdir(parents=True, exist_ok=False)
    sessions = SessionManager(workspace)
    bus = EventBus()
    provider = DeterministicProvider()
    embedder = DeterministicEmbedder()
    requester = NetworkForbidden()
    config = _config()

    if engine_name == "default":
        engine: object = DefaultMemoryEngine(
            config=config,
            default_config=DefaultMemoryConfig(
                retrieval=RetrievalConfig(
                    score_threshold=0.0,
                    relative_delta=1.0,
                    thresholds=RetrievalThresholdsConfig(
                        procedure=0.0,
                        preference=0.0,
                        event=0.0,
                        profile=0.0,
                    ),
                )
            ),
            workspace=workspace,
            provider=provider,
            light_provider=provider,
            http_resources=type("OfflineHttp", (), {"external_default": requester})(),
            event_publisher=bus,
        )
        _replace_default_embedder(engine, embedder)
    elif engine_name == "akasha":
        config.memory.plugin = "akasha"
        config.memory.engine = "akasha"
        engine = AkashaMemoryEngine(
            config=config,
            akasha_config=AkashaConfig(),
            workspace=workspace,
            http_resources=type("OfflineHttp", (), {"external_default": requester})(),
            event_publisher=bus,
        )
        engine._embedder = embedder  # type: ignore[assignment]
    else:
        raise ValueError(f"unsupported engine: {engine_name}")

    try:
        gold_refs = await _ingest_case(
            engine_name=engine_name,
            engine=engine,
            bus=bus,
            sessions=sessions,
            instance=instance,
        )
        if engine_name == "akasha":
            _refresh_akasha_idf(engine, workspace)  # type: ignore[arg-type]
        query_timestamp = _parse_timestamp(instance.question_date, 28)
        result = await engine.query(  # type: ignore[attr-defined]
            MemoryQuery(
                text=instance.question,
                intent="answer",
                effect="read_only",
                scope=MemoryScope(
                    session_key=instance.session_key,
                    channel="benchmark",
                    chat_id=instance.question_id,
                ),
                limit=10,
                timestamp=query_timestamp,
            )
        )
        evidence_rank: int | None = None
        answer_rank: int | None = None
        retrieved: list[dict[str, object]] = []
        for rank, record in enumerate(result.records, 1):
            refs = sorted(_source_refs(record))
            if evidence_rank is None and set(refs).intersection(gold_refs):
                evidence_rank = rank
            if answer_rank is None and normalise(instance.answer) in normalise(record.summary):
                answer_rank = rank
            retrieved.append(
                {
                    "rank": rank,
                    "id": record.id,
                    "kind": record.kind,
                    "summary": record.summary,
                    "score": round(record.score, 6),
                    "source_refs": refs,
                }
            )
        hit_rank = evidence_rank or answer_rank
        state = _state_snapshot(engine_name, workspace)
        return OfflineCaseResult(
            engine=engine_name,
            question_id=instance.question_id,
            question_type=instance.question_type,
            question=instance.question,
            gold_answer=instance.answer,
            gold_message_refs=gold_refs,
            retrieved=retrieved,
            evidence_rank=evidence_rank,
            answer_text_rank=answer_rank,
            reciprocal_rank=round(reciprocal_rank(hit_rank), 6),
            retrieval_hit=hit_rank is not None,
            trace=dict(result.trace),
            state=state,
            offline_calls={
                "llm": provider.calls,
                "embedding_batches": embedder.calls,
                "embedded_texts": embedder.texts,
                "http": 0,
            },
        )
    finally:
        await bus.shutdown()
        for closeable in reversed(getattr(engine, "closeables", [])):
            close = getattr(closeable, "aclose", None) or getattr(
                closeable, "close", None
            )
            if close is None:
                continue
            outcome = close()
            if asyncio.iscoroutine(outcome):
                await outcome
        sessions.close()


async def run_offline_ab(
    *,
    workspace: Path,
    instances: list[LMEInstance] | None = None,
) -> dict[str, object]:
    if workspace.exists() and any(workspace.iterdir()):
        raise FileExistsError(f"offline eval workspace must be empty: {workspace}")
    workspace.mkdir(parents=True, exist_ok=True)
    selected = builtin_smoke_instances() if instances is None else instances
    selected = [item for item in selected if item.question_type in SUPPORTED_QUESTION_TYPES]
    results: list[OfflineCaseResult] = []
    for instance in selected:
        for engine_name in ("default", "akasha"):
            results.append(
                await _run_engine_case(
                    engine_name,
                    instance,
                    workspace / engine_name / instance.question_id,
                )
            )

    by_engine: dict[str, dict[str, object]] = {}
    for engine_name in ("default", "akasha"):
        items = [item for item in results if item.engine == engine_name]
        hits = sum(1 for item in items if item.retrieval_hit)
        evidence_hits = sum(1 for item in items if item.evidence_rank is not None)
        answer_text_hits = sum(1 for item in items if item.answer_text_rank is not None)
        by_engine[engine_name] = {
            "cases": len(items),
            "retrieval_hits": hits,
            "evidence_hits": evidence_hits,
            "answer_text_hits": answer_text_hits,
            "recall_at_10": round(hits / len(items), 6) if items else 0.0,
            "evidence_recall_at_10": round(evidence_hits / len(items), 6)
            if items
            else 0.0,
            "mrr": round(sum(item.reciprocal_rank for item in items) / len(items), 6)
            if items
            else 0.0,
        }
    payload: dict[str, object] = {
        "mode": "offline-deterministic",
        "paid_api_calls": 0,
        "claims": {
            "verified": "engine ingestion, retrieval, evidence and SQLite persistence",
            "not_verified": "real-model final-answer quality",
        },
        "scores": by_engine,
        "results": [asdict(item) for item in results],
    }
    (workspace / "report.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a zero-cost Akasha vs Memory2 persistence/retrieval smoke."
    )
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--data", type=Path)
    parser.add_argument("--limit", type=int, default=3)
    return parser


def main() -> None:
    args = _parser().parse_args()
    instances = None
    if args.data:
        instances = [
            item
            for item in load_longmemeval(args.data)
            if item.question_type in SUPPORTED_QUESTION_TYPES
        ]
        if args.limit > 0:
            instances = instances[: args.limit]
    payload = asyncio.run(run_offline_ab(workspace=args.workspace, instances=instances))
    print(json.dumps(payload["scores"], ensure_ascii=False, indent=2))
    print(f"report: {args.workspace / 'report.json'}")


if __name__ == "__main__":
    main()
