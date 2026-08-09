#!/usr/bin/env python3
"""Run a secret-safe online smoke test against the configured providers.

The verifier uses an isolated temporary workspace. It never prints endpoint
credentials and never writes them to the generated config file.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from agent.config import config_value, load_dotenv, load_toml_config
from agent.model_runtime.usage import usage_from_mapping
from bootstrap.app import build_runtime
from core.memory.embeddings import EmbeddingClient
from core.memory.engine import MemoryQuery, MemoryScope
from core.schema import ToolSpec
from infra.providers.llm_provider import OpenAICompatibleClient


ROOT = Path(__file__).resolve().parents[1]


def _configured() -> tuple[dict[str, Any], dict[str, str]]:
    load_dotenv(ROOT / ".env")
    config = load_toml_config(ROOT / "config.toml")
    values = {
        "model": os.getenv("MODEL_ID")
        or str(config_value(config, "llm", "main", "model", default="")),
        "base_url": os.getenv("OPENAI_COMPATIBLE_BASE_URL")
        or str(config_value(config, "llm", "main", "base_url", default="")),
        "api_key": os.getenv("OPENAI_COMPATIBLE_API_KEY")
        or str(config_value(config, "llm", "main", "api_key", default="")),
        "embedding_model": os.getenv("EMBEDDING_MODEL_ID")
        or str(config_value(config, "memory", "embedding", "model", default="")),
        "embedding_base_url": os.getenv("EMBEDDING_BASE_URL")
        or str(
            config_value(config, "memory", "embedding", "base_url", default="")
        ),
        "embedding_api_key": os.getenv("EMBEDDING_API_KEY")
        or str(config_value(config, "memory", "embedding", "api_key", default="")),
    }
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise RuntimeError("missing online configuration: " + ", ".join(missing))
    return config, values


def _provider_checks(values: dict[str, str]) -> dict[str, Any]:
    client = OpenAICompatibleClient(
        base_url=values["base_url"], api_key=values["api_key"], timeout=90
    )
    response = client.complete(
        messages=[{"role": "user", "content": "只回复 KIRAKIRA_ONLINE_OK"}],
        tools=[],
        system="严格按要求回复。",
        model=values["model"],
        max_tokens=32,
    )
    if "KIRAKIRA_ONLINE_OK" not in response.text:
        raise AssertionError("model text probe returned an unexpected response")
    usage = usage_from_mapping(response.usage)
    if usage.input_tokens is None or usage.output_tokens is None:
        raise AssertionError("provider omitted input/output token usage")

    tool = ToolSpec(
        name="online_probe",
        description="验证工具调用协议，必须调用它。",
        input_schema={
            "type": "object",
            "properties": {"value": {"type": "string"}},
            "required": ["value"],
            "additionalProperties": False,
        },
    )
    called = client.complete(
        messages=[
            {
                "role": "user",
                "content": "调用 online_probe，value 必须是 probe-ok。",
            }
        ],
        tools=[tool],
        system="必须调用指定工具，不要直接回答。",
        model=values["model"],
        max_tokens=128,
        tool_choice={"type": "function", "function": {"name": "online_probe"}},
    )
    if len(called.tool_calls) != 1:
        raise AssertionError("provider did not return the forced tool call")
    call = called.tool_calls[0]
    if call.name != "online_probe" or call.arguments.get("value") != "probe-ok":
        raise AssertionError("provider returned malformed tool arguments")

    vector = EmbeddingClient(
        base_url=values["embedding_base_url"],
        api_key=values["embedding_api_key"],
        model=values["embedding_model"],
        timeout=90,
    ).embed("Kirakira 在线嵌入验证")
    if not vector:
        raise AssertionError("embedding provider returned an empty vector")
    return {
        "text": "pass",
        "tool_protocol": "pass",
        "usage": usage.to_dict(),
        "embedding": {"status": "pass", "dimensions": len(vector)},
    }


def _isolated_config(path: Path) -> None:
    path.write_text(
        """
[llm.main]
model = "${MODEL_ID}"
base_url = "${OPENAI_COMPATIBLE_BASE_URL}"
api_key = "${OPENAI_COMPATIBLE_API_KEY}"
context_window = 32768

[agent]
max_iterations = 5
max_tokens = 512

[agent.context]
memory_window = 20
effective_context_percent = 0.9

[memory]
enabled = true
plugin = "akasha"
engine = "legacy"

[memory.embedding]
model = "${EMBEDDING_MODEL_ID}"
base_url = "${EMBEDDING_BASE_URL}"
api_key = "${EMBEDDING_API_KEY}"

[proactive]
enabled = false
""".strip()
        + "\n",
        encoding="utf-8",
    )


async def _runtime_checks(values: dict[str, str]) -> dict[str, Any]:
    # The verifier needs deterministic usage telemetry from the non-streaming API.
    os.environ["AGENT_STREAM"] = "0"
    # The isolated config deliberately contains only variable references. Export
    # aliases from the already resolved source config so no credential reaches disk.
    os.environ["MODEL_ID"] = values["model"]
    os.environ["OPENAI_COMPATIBLE_BASE_URL"] = values["base_url"]
    os.environ["OPENAI_COMPATIBLE_API_KEY"] = values["api_key"]
    os.environ["EMBEDDING_MODEL_ID"] = values["embedding_model"]
    os.environ["EMBEDDING_BASE_URL"] = values["embedding_base_url"]
    os.environ["EMBEDDING_API_KEY"] = values["embedding_api_key"]
    with tempfile.TemporaryDirectory(prefix="kirakira-online-") as raw:
        workspace = Path(raw)
        config_path = workspace / "config.toml"
        _isolated_config(config_path)
        (workspace / "online_probe.txt").write_text(
            "KIRAKIRA_FILE_TOOL_OK\n", encoding="utf-8"
        )
        runtime = await build_runtime(workspace, config_path=config_path)
        try:
            tool_out = await runtime.process_direct(
                "必须调用 read_file 读取 online_probe.txt，然后只回复文件内容。",
                session_key="online:tool",
                skip_post_memory=True,
                skip_memory_retrieval=True,
            )
            tool_session = runtime.session_manager.get_or_create("online:tool")
            tool_messages = tool_session.messages
            used = {
                str(name)
                for message in tool_messages
                for name in (message.get("tools_used") or [])
            }
            if "read_file" not in used or "KIRAKIRA_FILE_TOOL_OK" not in tool_out.content:
                raise AssertionError("runtime did not execute and consume read_file")

            context_session = runtime.session_manager.get_or_create("online:context")
            for index in range(30):
                context_session.add_message("user", "历史问题 %02d" % index)
                context_session.add_message("assistant", "历史回答 %02d" % index)
            runtime.session_manager.save(context_session)
            old_ids = [str(item["id"]) for item in context_session.messages]
            context_out = await runtime.process_direct(
                "只回复 KIRAKIRA_CONTEXT_OK",
                session_key="online:context",
                skip_post_memory=True,
                skip_memory_retrieval=True,
            )
            if "KIRAKIRA_CONTEXT_OK" not in context_out.content:
                raise AssertionError("online context turn returned an unexpected response")
            context_session = runtime.session_manager.get_or_create("online:context")
            persisted_ids = [str(item["id"]) for item in context_session.messages]
            if persisted_ids[: len(old_ids)] != old_ids:
                raise AssertionError("context projection mutated durable history")
            context_trace = dict(context_session.messages[-1].get("context_trace") or {})
            attempts = list(context_trace.get("attempts") or [])
            if not attempts or int(attempts[0].get("history_messages") or 0) > 20:
                raise AssertionError("history projection did not honor memory_window")
            coverage = str(
                ((context_trace.get("react_stats") or {}).get("model_usage") or {}).get(
                    "coverage"
                )
                or ""
            )
            if coverage != "exact":
                raise AssertionError("runtime usage coverage is not exact")

            code = "KIRA-AKASHA-8404"
            await runtime.process_direct(
                "请记住：在线验证代号是 %s。只需简短确认。" % code,
                session_key="online:akasha",
                skip_memory_retrieval=True,
            )
            engine = runtime.memory_services.engine
            descriptor = engine.describe()
            if descriptor.name != "akasha":
                raise AssertionError("akasha was not the selected memory engine")
            recalled = await engine.query(
                MemoryQuery(
                    text="在线验证代号是什么",
                    intent="context",
                    scope=MemoryScope(
                        session_key="online:akasha",
                        channel="direct",
                        chat_id="local",
                    ),
                    timestamp=datetime.now().astimezone(),
                    limit=8,
                )
            )
            if code not in recalled.text_block:
                raise AssertionError("akasha did not retrieve the committed turn")
            recall_out = await runtime.process_direct(
                "在线验证代号是什么？只回复代号。",
                session_key="online:akasha",
                skip_post_memory=True,
            )
            if code not in recall_out.content:
                raise AssertionError("akasha retrieval was not consumed by the model")
            return {
                "runtime_tool_execution": "pass",
                "context_governance": {
                    "status": "pass",
                    "durable_history_preserved": True,
                    "projected_history_messages": int(
                        attempts[0].get("history_messages") or 0
                    ),
                    "usage_coverage": coverage,
                },
                "akasha_v1": {
                    "status": "pass",
                    "engine": descriptor.name,
                    "retrieved_records": len(recalled.records),
                    "model_consumed_recall": True,
                },
            }
        finally:
            await runtime.stop_background([])


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify configured LLM, tools, context governance, and Akasha online."
    )
    parser.parse_args()
    _config, values = _configured()
    report = {
        "provider": _provider_checks(values),
        "runtime": asyncio.run(_runtime_checks(values)),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
