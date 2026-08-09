"""Adapter that executes the original Agent pipeline against a Cloud transcript."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from contextlib import AbstractContextManager, ExitStack
from dataclasses import replace
from typing import Protocol

from agent.turns.models import TurnRequest, TurnResult
from cloud.transcript import RunScopedTranscriptStore


CLOUD_TRANSCRIPT_KEY = "_cloud_transcript"
CLOUD_TRANSCRIPT_COMMIT_KEY = "_cloud_transcript_commit"


class TurnPipeline(Protocol):
    async def execute(self, request: TurnRequest) -> TurnResult: ...


class CloudPipelineExecutor:
    """Hydrate durable history, delegate unchanged, and export the commit delta."""

    def __init__(
        self,
        pipeline: TurnPipeline,
        transcript_store: RunScopedTranscriptStore,
        *,
        scope_binders: Sequence[
            Callable[[str], AbstractContextManager[object]]
        ] = (),
        settle: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._pipeline = pipeline
        self._transcripts = transcript_store
        self._scope_binders = tuple(scope_binders)
        self._settle = settle

    async def execute(self, request: TurnRequest) -> TurnResult:
        transcript = request.metadata.get(CLOUD_TRANSCRIPT_KEY)
        if not isinstance(transcript, dict):
            raise ValueError("Cloud TurnRequest is missing its durable transcript")
        clean_metadata = dict(request.metadata)
        clean_metadata.pop(CLOUD_TRANSCRIPT_KEY, None)
        # The API transaction has already persisted this user message. The original
        # pipeline must reason over it as the current input but not append it twice.
        clean_metadata["omit_user_turn"] = True
        delegated = replace(request, metadata=clean_metadata)

        with ExitStack() as stack:
            binding = stack.enter_context(
                self._transcripts.bind(request.conversation_id, transcript)
            )
            for binder in self._scope_binders:
                stack.enter_context(binder(request.principal.subject_id))
            result = await self._pipeline.execute(delegated)
            if self._settle is not None:
                await self._settle(request.conversation_id)
            new_messages = binding.session.messages[binding.initial_message_count :]
            if not new_messages:
                # Core commands and plugin aborts legitimately return before the normal
                # transcript commit phase. Cloud still needs one durable assistant row.
                assistant = binding.session.add_message(
                    "assistant",
                    result.content,
                    media=list(result.media),
                    reasoning_content=result.thinking,
                )
            elif len(new_messages) == 1 and new_messages[0].get("role") == "assistant":
                assistant = new_messages[0]
            else:
                raise RuntimeError(
                    "the Agent pipeline must commit one assistant and no duplicate user message"
                )
            commit = {
                "assistant_message_id": str(assistant["id"]),
                "assistant_metadata": {
                    key: value
                    for key, value in assistant.items()
                    if key not in {"id", "seq", "role", "content", "timestamp"}
                },
                "conversation_metadata": dict(binding.session.metadata),
                "last_consolidated": int(binding.session.last_consolidated or 0),
            }

        return replace(
            result,
            metadata={**dict(result.metadata), CLOUD_TRANSCRIPT_COMMIT_KEY: commit},
        )
