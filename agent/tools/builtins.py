"""Kirakira Agent learning harness module."""

import asyncio
import base64
from datetime import datetime, timedelta, timezone
import gzip
import ipaddress
import json
import html
import os
import re
import socket
import threading
import urllib.parse
import urllib.request
import urllib.error
import zlib
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx

from bus.queue import MessageBus
from bus.events import (
    AttachmentKind,
    ChannelAttachment,
    ChannelMessage,
    DeliveryStatus,
    OutboundMessage,
)
from core.memory.legacy import MemoryRuntime
from core.memory.engine import (
    MemoryCapability,
    MemoryMutation,
    MemoryQuery,
    MemoryQueryFilters,
    MemoryScope,
)
from core.schema import ToolResult, ToolSpec
from agent.plugins.snapshot import SnapshotToolView, get_current_runtime_snapshot
from session.manager import SessionManager
from agent.skills import SkillLoader
from agent.tools.registry import ToolRegistry, object_schema
from agent.tools.shell_command import resolve_shell
from agent.tools.unified_exec import (
    DEFAULT_HARD_TIMEOUT_S,
    DEFAULT_INITIAL_YIELD_TIME_MS,
    DEFAULT_MAX_OUTPUT_TOKENS,
    MAX_HARD_TIMEOUT_S,
    format_execution_result,
)
from agent.tools.execution_backend import (
    ExecutionBackend,
    LocalExecutionBackend,
    WorkspaceBackend,
)
from utils.process_group import owned_process_env

OUTPUT_LIMIT = 50000
PRIVATE_FETCH_ENV = "KIRAKIRA_ALLOW_PRIVATE_WEB_FETCH"


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on", "enabled")


def safe_path(workdir: Path, path: str) -> Path:
    target = (workdir / path).resolve()
    root = workdir.resolve()
    try:
        target.relative_to(root)
    except ValueError:
        raise ValueError("Path escapes workspace: %s" % path)
    return target


def truncate(text: str, limit: int = OUTPUT_LIMIT) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "\n... (%d characters truncated)" % (len(text) - limit)


def _html_to_markdown(raw_html: str) -> str:
    """HTML → Markdown（参考 akashic-agent web_fetch，html2text 实现）。"""
    import html2text

    converter = html2text.HTML2Text()
    converter.ignore_links = False
    converter.ignore_images = False
    converter.body_width = 0  # 禁止自动折行
    converter.unicode_snob = True  # 保留 Unicode 字符
    converter.protect_links = True  # 防止链接被转义
    return converter.handle(raw_html).strip()


_VL_MAX_FILE_BYTES = 20 * 1024 * 1024  # 单张原始文件上限
_VL_MAX_DATA_URI_BYTES = 8 * 1024 * 1024  # base64 编码后 data URI 上限
_VL_MAX_EDGE = 4096  # 最长边像素上限，超限自动缩放


def _encode_image_data_uri(raw: bytes, mime: str) -> str:
    """读取图片并编码为 data URI，大图自动缩放压缩（参考 akashic-agent vision）。

    超限时抛 ValueError，带可操作的错误信息。
    """
    import io

    if len(raw) > _VL_MAX_FILE_BYTES:
        raise ValueError(
            "图片文件过大（%.1fMB），上限为 %.0fMB。请压缩或裁剪后重试。"
            % (len(raw) / 1024 / 1024, _VL_MAX_FILE_BYTES / 1024 / 1024)
        )
    try:
        from PIL import Image, ImageOps
    except ModuleNotFoundError as exc:
        raise ValueError("当前环境未安装 Pillow，无法处理图片。") from exc

    try:
        with Image.open(io.BytesIO(raw)) as probe:
            probe.verify()
    except Exception as exc:  # noqa: BLE001 - Pillow 各种解码异常统一归一
        raise ValueError("图片文件无法解码或已损坏。请确认这是有效图片。") from exc

    with Image.open(io.BytesIO(raw)) as img:
        img = ImageOps.exif_transpose(img)
        if img.mode not in ("RGB", "L"):
            canvas = Image.new("RGB", img.size, (255, 255, 255))
            alpha = img.getchannel("A") if "A" in img.getbands() else None
            canvas.paste(img.convert("RGB"), mask=alpha)
            img = canvas
        elif img.mode == "L":
            img = img.convert("RGB")

        raw_b64_len = len(base64.b64encode(raw).decode())
        if max(img.size) > _VL_MAX_EDGE or raw_b64_len > _VL_MAX_DATA_URI_BYTES:
            img.thumbnail((_VL_MAX_EDGE, _VL_MAX_EDGE))

        # 原图已在预算内：无损保留原格式（JPEG 高质量 / PNG）。
        if raw_b64_len <= _VL_MAX_DATA_URI_BYTES and max(img.size) <= _VL_MAX_EDGE:
            buf = io.BytesIO()
            if mime == "image/jpeg":
                img.save(buf, format="JPEG", quality=95, optimize=True)
                clean_mime = "image/jpeg"
            else:
                img.save(buf, format="PNG", optimize=True)
                clean_mime = "image/png"
            clean_b64 = base64.b64encode(buf.getvalue()).decode()
            if len(clean_b64) <= _VL_MAX_DATA_URI_BYTES:
                return "data:%s;base64,%s" % (clean_mime, clean_b64)

        best: bytes | None = None
        for quality in (85, 75, 65, 55, 45):
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=quality, optimize=True)
            candidate = buf.getvalue()
            best = candidate
            candidate_b64 = base64.b64encode(candidate).decode()
            if len(candidate_b64) <= _VL_MAX_DATA_URI_BYTES:
                return "data:image/jpeg;base64,%s" % candidate_b64

    if best is None:
        raise ValueError("图片压缩失败")
    raise ValueError(
        "图片压缩后仍然过大（%.1fMB base64），上限 %.0fMB。请继续压缩或裁剪。"
        % (
            len(base64.b64encode(best).decode()) / 1024 / 1024,
            _VL_MAX_DATA_URI_BYTES / 1024 / 1024,
        )
    )


def _html_to_plain_text(raw_html: str) -> str:
    """HTML → 纯文本（参考 akashic-agent web_fetch，lxml 抽正文并合并空白）。"""
    from lxml import html as lxml_html
    from lxml.etree import ParserError

    try:
        doc = lxml_html.fromstring(raw_html)
    except (ParserError, ValueError):
        return raw_html
    for tag in ("script", "style", "noscript", "iframe", "object", "embed"):
        for element in doc.xpath("//%s" % tag):
            parent = element.getparent()
            if parent is not None:
                parent.remove(element)
    return " ".join(doc.text_content().split())


def _toolsearch_normalize(query: str) -> set[str]:
    """把 query 归一化为搜索词集合（参考 akashic-agent search_backend）。

    lowercase 整串 + 空格切词 + CJK/非 CJK 边界切词 + CJK bigram/单字，
    让中文查询能命中工具 name/description，无需外部分词库。
    """
    query_lower = query.lower().strip()
    tokens: set[str] = {query_lower}
    for part in query_lower.split():
        tokens.add(part)
    for segment in re.split(r"([一-鿿]+)", query_lower):
        segment = segment.strip()
        if segment:
            tokens.add(segment)
    cjk = [c for c in query_lower if "一" <= c <= "鿿"]
    for i in range(len(cjk) - 1):
        tokens.add(cjk[i] + cjk[i + 1])
    tokens.update(cjk)
    tokens.discard("")
    return tokens


def _toolsearch_score(name: str, description: str, keywords: set[str]) -> int:
    """字段加权评分（参考 akashic-agent _score，仅用 name/description 字段）。

    名称 part 精确命中 10 / 部分命中 5 / 全名兜底 3；描述命中额外 +2。
    """
    name_parts = [p for p in name.lower().split("_") if p]
    name_lower = name.lower()
    desc_lower = description.lower()
    score = 0
    for kw in keywords:
        if kw in name_parts:
            score += 10
        elif any(kw in part or part in kw for part in name_parts):
            score += 5
        elif kw in name_lower:
            score += 3
        if kw in desc_lower:
            score += 2
    return score


# 引擎的注入选择器只接受这四类(retriever.py 的 `else: continue`),其余类型即使被
# 检索命中也**永远不会注入上下文**。Reference 靠工具 schema 的 enum 防住;kirakira 的
# 旧 schema 曾提供 identity/fact/requested_memory,写进去的行会静默失效。
# 这里在写入边界归一(与旧 MemoryRuntime._canonical_memory_type 同一张映射),
# 不改镜像的 default_engine.py。
_CANONICAL_MEMORY_KINDS = frozenset({"event", "profile", "preference", "procedure"})
_LEGACY_MEMORY_KIND_MAP = {
    "identity": "profile",
    "fact": "profile",
    "requested_memory": "profile",
}


def _canonical_memory_kind(kind: str) -> str:
    value = (kind or "").strip()
    if not value or value in _CANONICAL_MEMORY_KINDS:
        return value
    return _LEGACY_MEMORY_KIND_MAP.get(value, value)


# ── 记忆工具的渲染与过滤(照 Reference agent/tools/recall_memory.py / forget_memory.py) ──

_MEMORY_LOCAL_TZ = ZoneInfo("Asia/Shanghai")
_MEMORY_RECENT_PRESETS = {
    "recent_3d": 3,
    "recent_7d": 7,
    "recent_30d": 30,
}


def _normalize_recall_intent(value: str) -> str:
    intents = {"context", "answer", "timeline", "interest", "procedure"}
    return value if value in intents else "answer"


def _parse_memory_day(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=_MEMORY_LOCAL_TZ)
    except ValueError:
        return None


def _parse_time_filter(value: str) -> tuple[datetime, datetime] | None:
    text = (value or "").strip()
    if not text:
        return None
    now = datetime.now(_MEMORY_LOCAL_TZ)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if text == "today":
        return today, today + timedelta(days=1)
    if text == "yesterday":
        start = today - timedelta(days=1)
        return start, today
    if text in _MEMORY_RECENT_PRESETS:
        return now - timedelta(days=_MEMORY_RECENT_PRESETS[text]), now
    if "~" in text:
        left, right = [part.strip() for part in text.split("~", 1)]
        start = _parse_memory_day(left)
        end_day = _parse_memory_day(right)
        if start is None or end_day is None:
            return None
        return start, end_day + timedelta(days=1)
    day = _parse_memory_day(text)
    if day is None:
        return None
    return day, day + timedelta(days=1)


def _render_recall_records(records: list, *, trace: dict) -> str:
    """带 `§cited:` 引用协议的结构化返回;记忆问责依赖它,不是可选的花样。"""
    items: list[dict[str, object]] = []
    for record in records:
        evidence = [
            {
                "kind": ref.kind,
                "refs": ref.refs,
                "resolver": ref.resolver,
                "source_ref": ref.source_ref,
                "metadata": ref.metadata,
            }
            for ref in (record.evidence or [])
        ]
        source_ref = ""
        for entry in evidence:
            candidate = str(entry.get("source_ref") or "").strip()
            if candidate:
                source_ref = candidate
                break
            refs = entry.get("refs")
            if isinstance(refs, list):
                for ref in refs:
                    if isinstance(ref, str) and ref.strip():
                        source_ref = ref.strip()
                        break
            if source_ref:
                break
        item: dict[str, object] = {
            "id": record.id,
            "memory_type": record.kind,
            "summary": record.summary,
            "score": round(float(record.score), 4),
            "evidence": evidence,
            "signals": record.signals,
        }
        if source_ref:
            item["source_ref"] = source_ref
        items.append(item)
    cited_item_ids = [str(item["id"]) for item in items if str(item.get("id", "")).strip()]
    return json.dumps(
        {
            "count": len(items),
            "items": items,
            "trace": trace,
            "citation_required": True,
            "citation_format": "§cited:[id1,id2,...]§",
            "cited_item_ids": cited_item_ids,
            "citation_rule": (
                "若最终回复使用了本工具返回的任何记忆条目，"
                "必须在正文末尾输出 §cited:[实际使用的id列表]§"
            ),
        },
        ensure_ascii=False,
    )


def _render_forget_result(
    requested_ids: list[str],
    affected_ids: list,
    missing_ids: list,
    items: list,
) -> str:
    return json.dumps(
        {
            "requested_ids": requested_ids,
            "superseded_ids": list(affected_ids),
            "missing_ids": list(missing_ids),
            "count": len(affected_ids),
            "items": list(items),
        },
        ensure_ascii=False,
    )


class WorkspaceTools:
    # Exa 公开 MCP 搜索端点（无需 API key），见 web_search。
    _WEB_SEARCH_MCP_URL = "https://mcp.exa.ai/mcp"

    def __init__(
        self,
        workdir: Path,
        skill_loader: SkillLoader,
        memory: MemoryRuntime | None = None,
        session_manager: SessionManager | None = None,
        registry: ToolRegistry | None = None,
        bus: MessageBus | None = None,
        push_tool: Any = None,
        memory_services: Any = None,
        execution_backend: ExecutionBackend | None = None,
        workspace_backend: WorkspaceBackend | None = None,
    ) -> None:
        self.workdir = workdir.resolve()
        self.skill_loader = skill_loader
        self.memory = memory
        # Stage 5:显式记忆工具优先走引擎;引擎未承重(未配 embedding)时回退旧 MemoryRuntime。
        self.memory_services = memory_services
        self.session_manager = session_manager
        self.registry = registry
        self.bus = bus
        self.push_tool = push_tool
        self._mutation_locks: dict[str, threading.Lock] = {}
        self._execution_backend = execution_backend or LocalExecutionBackend()
        self._workspace_backend = workspace_backend
        # Transitional introspection surface used by local diagnostics/tests.
        self._shell_processes = getattr(self._execution_backend, "_manager", None)

    async def bash(
        self,
        command: str,
        timeout: Optional[int] = None,
        run_in_background: bool = False,
        auto_promote: bool = True,
        tty: bool = False,
        shell: str | None = None,
        login: bool = True,
        yield_time_ms: int = DEFAULT_INITIAL_YIELD_TIME_MS,
        max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
    ) -> str:
        if self._dangerous_shell_command(command):
            return "Error: Dangerous command blocked"
        hard_timeout = max(1, min(int(timeout or DEFAULT_HARD_TIMEOUT_S), MAX_HARD_TIMEOUT_S))
        if auto_promote:
            hard_timeout = min(hard_timeout, 600)
        try:
            resolved = resolve_shell(shell)
            result = await self._execution_backend.exec_command(
                command=command,
                argv=resolved.derive_argv(command, login=login),
                cwd=self.workdir,
                env=owned_process_env({}),
                tty=bool(tty),
                yield_time_ms=max(0, int(yield_time_ms)),
                max_output_tokens=max(0, int(max_output_tokens)),
                hard_timeout_s=hard_timeout,
                owner_session_key=self._shell_owner(),
                return_immediately=bool(run_in_background),
            )
            return self._compat_shell_payload(
                result,
                command=command,
                background_start=bool(run_in_background),
                auto_promoted=(not run_in_background and result.execution_id is not None),
            )
        except (OSError, RuntimeError, ValueError) as exc:
            return "Error: %s" % exc

    async def task_output(
        self,
        task_id: str,
        block: bool = False,
        timeout_ms: int = 30000,
        offset: int = 0,
    ) -> str:
        del offset  # unified executions return new output chunks, not cumulative logs.
        try:
            execution_id = self._execution_id(task_id)
            result = await self._execution_backend.write_stdin(
                execution_id=execution_id,
                chars="",
                yield_time_ms=max(0, min(int(timeout_ms), 300_000)),
                max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS,
                owner_session_key=self._shell_owner(),
                return_immediately=not bool(block),
            )
            return self._compat_shell_payload(result, task_id=execution_id)
        except (RuntimeError, ValueError) as exc:
            return "Error: %s" % exc

    async def write_stdin(
        self,
        execution_id: int,
        chars: str = "",
        yield_time_ms: int = 1000,
        max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
    ) -> str:
        try:
            result = await self._execution_backend.write_stdin(
                execution_id=int(execution_id),
                chars=chars,
                yield_time_ms=int(yield_time_ms),
                max_output_tokens=max(0, int(max_output_tokens)),
                owner_session_key=self._shell_owner(),
            )
            return self._compat_shell_payload(result, task_id=int(execution_id))
        except (RuntimeError, ValueError) as exc:
            return "Error: %s" % exc

    async def task_stop(self, task_id: str) -> str:
        try:
            execution_id = self._execution_id(task_id)
        except ValueError:
            return json.dumps({"task_id": task_id, "status": "not_found"})
        stopped = await self._execution_backend.terminate_execution(
            execution_id,
            owner_session_key=self._shell_owner(),
        )
        # Stopping an already completed task is idempotent for the legacy API.
        return json.dumps(
            {"task_id": execution_id, "execution_id": execution_id, "status": "stopped" if stopped else "stopped"}
        )

    async def shutdown(self) -> None:
        report = await self._execution_backend.shutdown()
        if report.failures:
            failed = ",".join(str(item) for item in report.failed_execution_ids)
            raise RuntimeError("shell cleanup failed for execution ids: %s" % failed)

    async def cleanup_shell_owner(self, owner: str) -> None:
        report = await self._execution_backend.terminate_owner(owner)
        if report.failures:
            failed = ",".join(str(item) for item in report.failed_execution_ids)
            raise RuntimeError(
                "shell cleanup failed for owner %s execution ids: %s" % (owner, failed)
            )

    def _shell_owner(self) -> str:
        if self.registry is not None:
            session_key = str(self.registry.context.get("session_key") or "").strip()
            if session_key:
                return session_key
        return "workspace:%s" % self.workdir

    @staticmethod
    def _execution_id(task_id: object) -> int:
        try:
            value = int(task_id)
        except (TypeError, ValueError) as exc:
            raise ValueError("Unknown background task '%s'" % task_id) from exc
        if value <= 0:
            raise ValueError("Unknown background task '%s'" % task_id)
        return value

    @staticmethod
    def _compat_shell_payload(
        result: Any,
        *,
        command: str | None = None,
        task_id: int | None = None,
        background_start: bool = False,
        auto_promoted: bool = False,
    ) -> str:
        payload = json.loads(format_execution_result(result, command=command))
        execution_id = result.execution_id if result.execution_id is not None else task_id
        if execution_id is not None:
            payload["task_id"] = execution_id
        if background_start and execution_id is not None:
            payload["background_task_id"] = execution_id
        running = payload["process_status"] == "running"
        payload["status"] = "running" if running else "completed"
        payload["done"] = not running
        payload["elapsed_ms"] = payload["wall_time_ms"]
        payload["auto_promoted"] = bool(auto_promoted)
        return json.dumps(payload, ensure_ascii=False)

    def read_file(
        self, path: str, limit: Optional[int] = None, offset: int = 0
    ) -> Any:
        if self._workspace_backend is not None:
            return self._workspace_backend.read_file(
                self._shell_owner(), path, limit, offset
            )
        target = safe_path(self.workdir, path)
        if not target.is_file():
            return "Error: File does not exist: %s" % path
        with target.open("rb") as handle:
            head = handle.read(4096)
        if b"\x00" in head:
            return "Error: Binary file cannot be read as text: %s" % path
        all_lines = target.read_text(encoding="utf-8", errors="replace").splitlines()
        offset = max(0, int(offset))
        lines = all_lines[offset:]
        if limit is not None and limit >= 0 and limit < len(lines):
            lines = lines[:limit] + ["... (%d more lines)" % (len(lines) - limit)]
        return truncate("\n".join(lines))

    def list_dir(self, path: str = ".") -> Any:
        if self._workspace_backend is not None:
            return self._workspace_backend.list_dir(self._shell_owner(), path)
        target = safe_path(self.workdir, path)
        if not target.exists():
            return "Error: Path does not exist: %s" % path
        if not target.is_dir():
            return "Error: Path is not a directory: %s" % path
        rows = []
        for item in sorted(target.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
            rel = item.relative_to(self.workdir)
            kind = "dir" if item.is_dir() else "file"
            size = "" if item.is_dir() else str(item.stat().st_size)
            rows.append("%s\t%s\t%s" % (kind, rel, size))
        return truncate("\n".join(rows) or "(empty)")

    def write_file(self, path: str, content: str) -> Any:
        if self._workspace_backend is not None:
            return self._workspace_backend.write_file(
                self._shell_owner(), path, content
            )
        target = safe_path(self.workdir, path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with self._mutation_lock(target):
            self._atomic_write_text(target, content)
        return "Wrote %d bytes to %s" % (len(content), path)

    def edit_file(
        self,
        path: str,
        old_text: str,
        new_text: str,
        replace_all: bool = False,
    ) -> Any:
        if self._workspace_backend is not None:
            return self._workspace_backend.edit_file(
                self._shell_owner(), path, old_text, new_text, replace_all
            )
        target = safe_path(self.workdir, path)
        if not old_text:
            return "Error: old_text cannot be empty"
        with self._mutation_lock(target):
            content = target.read_text(encoding="utf-8", errors="replace")
            count = content.count(old_text)
            if count == 0:
                return "Error: Text not found in %s" % path
            if count > 1 and not replace_all:
                return (
                    "Error: Text occurs %d times in %s; provide a unique old_text "
                    "or set replace_all=true" % (count, path)
                )
            replacements = count if replace_all else 1
            self._atomic_write_text(
                target, content.replace(old_text, new_text, replacements)
            )
        return "Edited %s (%d replacement%s)" % (
            path,
            replacements,
            "s" if replacements != 1 else "",
        )

    def load_skill(self, name: str) -> str:
        return self.skill_loader.load(name)

    def request_user_confirmation(self, prompt: str) -> ToolResult:
        """显式标记本轮需要用户确认后才能继续。

        照 Reference agent/tools/request_user_confirmation.py:这是一个**标记**,
        不是执行前闸门——它不阻止任何工具运行,只把 ``mobile_attention`` 抬到
        turn 级,让渠道能把这一轮渲染成"等待确认"。真正的拦截能力是
        ``tool_hooks`` 的 pre-hook ``deny``。
        """
        text = str(prompt).strip()
        if not text:
            return ToolResult("", "Error: prompt 不能为空", is_error=True)
        if len(text) > 500:
            return ToolResult("", "Error: prompt 不能超过 500 字符", is_error=True)
        return ToolResult(
            "",
            "已标记等待用户确认：%s" % text,
            mobile_attention="confirmation",
        )

    async def vision(self, image_paths, prompt: str = "请详细描述并分析图片。") -> str:
        from infra.providers.llm_provider import OpenAICompatibleClient

        if isinstance(image_paths, str):
            image_paths = [image_paths]
        if not isinstance(image_paths, list) or not image_paths:
            return "Error: image_paths must contain at least one image"
        model = os.getenv("VISION_MODEL_ID", "").strip()
        if not model:
            return "Error: VISION_MODEL_ID is not configured"
        content = [{"type": "text", "text": prompt}]
        total = 0
        for raw_path in image_paths[:8]:
            if self._workspace_backend is not None:
                try:
                    data = await self._workspace_backend.read_binary(
                        self._shell_owner(), str(raw_path)
                    )
                except (httpx.HTTPError, ValueError) as exc:
                    return "Error: Image does not exist: %s (%s)" % (raw_path, exc)
            else:
                path = safe_path(self.workdir, str(raw_path))
                if not path.is_file():
                    return "Error: Image does not exist: %s" % raw_path
                data = path.read_bytes()
            mime = self._image_mime(data)
            if not mime:
                return "Error: Unsupported or invalid image: %s" % raw_path
            # 参考 akashic-agent：大图用 Pillow 自动缩放/压缩到预算内，避免直接失败。
            try:
                data_uri = await asyncio.to_thread(_encode_image_data_uri, data, mime)
            except ValueError as exc:
                return "Error: %s: %s" % (raw_path, exc)
            total += len(data_uri)
            if total > 24 * 1024 * 1024:
                return "Error: Total image input exceeds the data budget"
            content.append(
                {"type": "image_url", "image_url": {"url": data_uri}}
            )
        client = OpenAICompatibleClient(
            base_url=os.getenv("VISION_BASE_URL")
            or os.getenv("OPENAI_COMPATIBLE_BASE_URL"),
            api_key=os.getenv("VISION_API_KEY")
            if os.getenv("VISION_API_KEY") is not None
            else os.getenv("OPENAI_COMPATIBLE_API_KEY", ""),
        )
        response = await asyncio.to_thread(
            client.complete,
            [{"role": "user", "content": content}],
            [],
            "你是视觉分析助手。只根据提供的图片和问题回答。",
            model,
            2048,
        )
        return response.text or "Error: Vision model returned an empty response"

    async def compact(self) -> str:
        """把当前会话的历史强制归档进长期记忆,推进 consolidation 游标。

        之前这里是一个只返回固定文案的空壳——模型调用后会以为上下文已压缩。
        现在接到真实的 MarkdownMemoryMaintenance:游标推进后,下一轮的历史窗口
        从归档点开始(runtime 以 last_consolidated 为 start_index 切历史)。
        """
        maintenance = getattr(
            getattr(self.memory_services, "markdown", None), "maintenance", None
        )
        if maintenance is None or self.session_manager is None or self.registry is None:
            return "Error: 当前 runtime 未配置记忆归档能力，无法压缩上下文"
        session_key = str(self.registry.context.get("session_key") or "")
        if not session_key:
            return "Error: 当前上下文没有会话，无法压缩"
        from core.memory.markdown import ConsolidateRequest

        # 与 pipeline 持有的是同一个缓存实例,游标推进不会被 turn 提交覆盖。
        session = self.session_manager.get_or_create(session_key)
        before = int(session.last_consolidated or 0)
        await maintenance.consolidate(
            ConsolidateRequest(
                session=session,
                force=True,
                scope_channel=str(session.metadata.get("channel") or ""),
                scope_chat_id=str(session.metadata.get("chat_id") or ""),
            )
        )
        after = int(session.last_consolidated or 0)
        if after > before:
            await self.session_manager.save_async(session)
            return "已归档 %d 条历史消息到长期记忆；下一轮起上下文从归档点开始。" % (
                after - before
            )
        return "没有需要归档的新历史。"

    async def tool_search(self, query: str = "", limit: int = 20) -> str:
        # 必须是 async：只有在 turn 自己的 task 里才能读到本轮锁定的快照，
        # 而 MCP 工具只存在于快照中，不在基础注册表里。
        if self.registry is None:
            return "[]"
        view = SnapshotToolView(self.registry, get_current_runtime_snapshot())
        query = query.strip()
        if not query:
            return json.dumps(
                {"matched": [], "unlocked": [], "tip": "query is required"},
                ensure_ascii=False,
            )
        selected = []
        if query.lower().startswith("select:"):
            requested = [item.strip() for item in query[7:].split(",") if item.strip()]
            selected = [name for name in requested if view.has(name)]
            missing = [name for name in requested if not view.has(name)]
            matched = [
                {
                    "name": name,
                    "description": view.get_tool(name).spec.description,
                    "input_schema": view.get_tool(name).spec.input_schema,
                    "risk": view.get_tool(name).meta.risk,
                    "source_type": view.get_tool(name).meta.source_type,
                    "source_name": view.get_tool(name).meta.source_name,
                }
                for name in selected
            ]
            return json.dumps(
                {"matched": matched, "unlocked": selected, "missing": missing},
                ensure_ascii=False,
                indent=2,
            )
        keywords = _toolsearch_normalize(query)
        matches = []
        for spec in view.specs():
            score = (
                _toolsearch_score(spec.name, spec.description, keywords)
                if keywords
                else 1
            )
            if score:
                matches.append(
                    {
                        "name": spec.name,
                        "description": spec.description,
                        "score": score,
                        "input_schema": spec.input_schema,
                        "risk": view.get_tool(spec.name).meta.risk,
                        "source_type": view.get_tool(spec.name).meta.source_type,
                        "source_name": view.get_tool(spec.name).meta.source_name,
                    }
                )
        matches.sort(key=lambda item: (item["score"], item["name"]), reverse=True)
        matched = matches[: max(1, min(20, int(limit)))]
        return json.dumps(
            {
                "matched": matched,
                "unlocked": [item["name"] for item in matched],
            },
            ensure_ascii=False,
            indent=2,
        )

    def web_fetch(self, url: str, max_chars: int = 12000, format: str = "markdown") -> str:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            return "Error: URL must start with http:// or https://"
        validation_error = self._validate_fetch_target(parsed)
        if validation_error:
            return validation_error
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "kirakira-agent/0.1 (+https://github.com/Nhckdvrl/kirakira-agent)",
                "Accept": "text/html,text/plain,application/json;q=0.9,*/*;q=0.5",
                "Accept-Encoding": "gzip, deflate",
            },
            method="GET",
        )
        owner = self

        class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, redirect_req, fp, code, msg, headers, newurl):
                redirect = urllib.parse.urlparse(newurl)
                if redirect.scheme not in ("http", "https") or not redirect.netloc:
                    raise urllib.error.URLError("unsafe redirect URL")
                error = owner._validate_fetch_target(redirect)
                if error:
                    raise urllib.error.URLError(error)
                return super().redirect_request(
                    redirect_req, fp, code, msg, headers, newurl
                )

        opener = urllib.request.build_opener(SafeRedirectHandler())
        try:
            with opener.open(req, timeout=30) as resp:
                content_type = resp.headers.get("content-type", "")
                content_encoding = resp.headers.get("content-encoding", "")
                final_url = resp.geturl()
                declared = int(resp.headers.get("content-length") or "0")
                if declared > 5 * 1024 * 1024:
                    return "Error: Response exceeds 5 MB"
                raw = resp.read(5 * 1024 * 1024 + 1)
        except urllib.error.HTTPError as exc:
            return "Error: HTTP %s while fetching %s" % (exc.code, url)
        except urllib.error.URLError as exc:
            return "Error: Fetch failed for %s: %s" % (url, exc.reason)
        if len(raw) > 5 * 1024 * 1024:
            return "Error: Response exceeds 5 MB"
        try:
            raw = self._decompress_web_body(raw, content_encoding)
        except (OSError, EOFError, zlib.error, ValueError) as exc:
            return "Error: Could not decode %s response from %s: %s" % (
                content_encoding or "compressed",
                url,
                exc,
            )
        if len(raw) > 5 * 1024 * 1024:
            return "Error: Decompressed response exceeds 5 MB"
        lowered_type = content_type.lower()
        if lowered_type and not any(
            item in lowered_type
            for item in ("text/", "json", "xml", "javascript", "x-www-form-urlencoded")
        ):
            return "Error: Unsupported response content type: %s" % content_type
        if self._looks_like_binary(raw):
            return "Error: Response appears to be binary data"
        text, charset = self._decode_web_body(raw, content_type)
        if self._looks_severely_garbled(text):
            return "Error: Response text is severely garbled or uses an unsupported encoding"
        title = self._extract_html_title(text)
        published_at = self._extract_published_at(text)
        # 参考 akashic-agent：format 决定输出形态。markdown（默认）用 html2text，
        # text 用 lxml 抽正文，html 原样返回解码后的 HTML。
        fmt = (format or "markdown").strip().lower()
        if fmt not in ("text", "markdown", "html"):
            fmt = "markdown"
        is_html = "html" in content_type.lower() or "<html" in text[:500].lower()
        if is_html and fmt == "markdown":
            text = _html_to_markdown(text)
        elif is_html and fmt == "text":
            text = _html_to_plain_text(text)
        # fmt == "html" 或非 HTML 内容：保留原始解码文本。
        source = {
            "url": final_url,
            "title": title or self._fallback_web_title(final_url),
            "format": fmt,
        }
        if published_at:
            source["published_at"] = published_at
        if content_type:
            source["content_type"] = content_type
        if charset:
            source["charset"] = charset
        return json.dumps(
            {
                "source": source,
                "content": truncate(text.strip(), max(1000, int(max_chars))),
            },
            ensure_ascii=False,
            indent=2,
        )

    def web_search(self, query: str, limit: int = 5) -> str:
        # 参考 akashic-agent：走 Exa 公开 MCP 端点（无需 API key），返回带标题/URL/摘要
        # 的结构化文本。旧的 DuckDuckGo HTML 抓取已被搜索引擎反爬全面封锁，永久失效。
        import httpx

        q = query.strip()
        if not q:
            return "Error: Web search query must not be empty"
        num_results = min(max(1, int(limit)), 20)
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "web_search_exa",
                "arguments": {
                    "query": q,
                    "numResults": num_results,
                    "livecrawl": "fallback",
                    "type": "auto",
                },
            },
        }
        try:
            with httpx.Client(timeout=25.0) as client:
                response = client.post(
                    self._WEB_SEARCH_MCP_URL,
                    json=payload,
                    headers={
                        "accept": "application/json, text/event-stream",
                        "content-type": "application/json",
                    },
                )
                response.raise_for_status()
        except httpx.HTTPError as exc:
            return "Error: Web search failed for %r: %s" % (q, exc)

        # 端点以 SSE 帧返回；逐行取第一帧带 content 的 result。
        for line in response.text.splitlines():
            if not line.startswith("data: "):
                continue
            try:
                data = json.loads(line[6:])
            except json.JSONDecodeError:
                continue
            content = (data.get("result") or {}).get("content") or []
            if content:
                text = content[0].get("text", "")
                if text.strip():
                    return json.dumps(
                        {"query": q, "result": text}, ensure_ascii=False
                    )
        return (
            "Error: Web search returned no results for %r. "
            "Try a different query or use web_fetch with a verified URL."
        ) % q

    @staticmethod
    def _decompress_web_body(raw: bytes, content_encoding: str) -> bytes:
        encodings = [
            value.strip().lower()
            for value in content_encoding.split(",")
            if value.strip() and value.strip().lower() != "identity"
        ]
        for encoding in reversed(encodings):
            if encoding in ("gzip", "x-gzip"):
                raw = gzip.decompress(raw)
            elif encoding == "deflate":
                try:
                    raw = zlib.decompress(raw)
                except zlib.error:
                    raw = zlib.decompress(raw, -zlib.MAX_WBITS)
            else:
                raise ValueError("unsupported content encoding %r" % encoding)
        return raw

    @staticmethod
    def _looks_like_binary(raw: bytes) -> bool:
        if not raw:
            return False
        sample = raw[:8192]
        if b"\x00" in sample:
            return True
        binary_controls = sum(
            1 for value in sample if value < 32 and value not in (9, 10, 12, 13)
        )
        return binary_controls / len(sample) > 0.08

    def _decode_web_body(self, raw: bytes, content_type: str) -> tuple[str, str]:
        if not raw:
            return "", "utf-8"
        declared_match = re.search(
            r"(?i)charset\s*=\s*['\"]?\s*([a-z0-9._:-]+)", content_type
        )
        html_head = raw[:4096].decode("ascii", errors="ignore")
        meta_match = re.search(
            r"(?i)(?:charset\s*=\s*['\"]?\s*|charset\s*['\"]?\s+content\s*=\s*['\"][^'\"]*charset=)([a-z0-9._:-]+)",
            html_head,
        )
        candidates = []
        if raw.startswith(b"\xef\xbb\xbf"):
            candidates.append("utf-8-sig")
        elif raw.startswith((b"\xff\xfe", b"\xfe\xff")):
            candidates.append("utf-16")
        for candidate in (
            declared_match.group(1) if declared_match else "",
            meta_match.group(1) if meta_match else "",
            "utf-8",
            "gb18030",
            "shift_jis",
        ):
            normalized = candidate.strip().lower()
            if normalized and normalized not in candidates:
                candidates.append(normalized)
        decoded = []
        for encoding in candidates:
            try:
                value = raw.decode(encoding)
            except (LookupError, UnicodeDecodeError):
                continue
            decoded.append((self._text_quality_score(value), value, encoding))
            if encoding in (
                declared_match.group(1).lower() if declared_match else "",
                meta_match.group(1).lower() if meta_match else "",
            ):
                return value, encoding
        if decoded:
            _score, value, encoding = max(decoded, key=lambda item: item[0])
            return value, encoding
        return raw.decode("utf-8", errors="replace"), "utf-8-replacement"

    @staticmethod
    def _text_quality_score(value: str) -> float:
        if not value:
            return 0.0
        replacements = value.count("\ufffd")
        controls = sum(
            1 for char in value if ord(char) < 32 and char not in "\t\n\r\f"
        )
        c1_controls = sum(1 for char in value if 0x80 <= ord(char) <= 0x9F)
        mojibake = sum(value.count(marker) for marker in ("Ã", "Â", "â€", "ï¿½"))
        return 1.0 - (
            replacements * 8 + controls * 8 + c1_controls * 4 + mojibake * 2
        ) / len(value)

    @staticmethod
    def _looks_severely_garbled(value: str) -> bool:
        if not value:
            return False
        replacements = value.count("\ufffd")
        controls = sum(
            1
            for char in value
            if (ord(char) < 32 and char not in "\t\n\r\f")
            or 0x80 <= ord(char) <= 0x9F
        )
        return replacements / len(value) > 0.02 or controls / len(value) > 0.03

    @staticmethod
    def _extract_html_title(value: str) -> str:
        for pattern in (
            r"(?is)<meta[^>]+(?:property|name)=['\"](?:og:title|twitter:title)['\"][^>]+content=['\"]([^'\"]+)",
            r"(?is)<meta[^>]+content=['\"]([^'\"]+)['\"][^>]+(?:property|name)=['\"](?:og:title|twitter:title)['\"]",
            r"(?is)<title[^>]*>(.*?)</title>",
        ):
            match = re.search(pattern, value)
            if match:
                title = html.unescape(re.sub(r"(?is)<[^>]+>", " ", match.group(1)))
                title = re.sub(r"\s+", " ", title).strip()
                if title:
                    return title[:500]
        return ""

    @staticmethod
    def _extract_published_at(value: str) -> str:
        patterns = (
            r"(?is)<meta[^>]+(?:property|name|itemprop)=['\"](?:article:published_time|datePublished|date|pubdate|publishdate|publish-date)['\"][^>]+content=['\"]([^'\"]+)",
            r"(?is)<meta[^>]+content=['\"]([^'\"]+)['\"][^>]+(?:property|name|itemprop)=['\"](?:article:published_time|datePublished|date|pubdate|publishdate|publish-date)['\"]",
            r'(?is)["\']datePublished["\']\s*:\s*["\']([^"\']+)',
        )
        for pattern in patterns:
            match = re.search(pattern, value)
            if match:
                published_at = html.unescape(match.group(1)).strip()
                if published_at:
                    return published_at[:100]
        return ""

    @staticmethod
    def _fallback_web_title(url: str) -> str:
        parsed = urllib.parse.urlparse(url)
        path = urllib.parse.unquote(parsed.path).rstrip("/")
        return (path.rsplit("/", 1)[-1] if path else parsed.netloc) or url

    async def message_push(
        self,
        channel: str,
        chat_id: str,
        message: str = "",
        file: str = "",
        image: str = "",
    ) -> str:
        channel = channel.strip()
        chat_id = chat_id.strip()
        if not channel or not chat_id:
            return "Error: channel and chat_id are required"
        if not message and not file and not image:
            return "Error: message, file, or image is required"
        if self.push_tool is not None:
            attachments = []
            if file:
                attachments.append(
                    ChannelAttachment(AttachmentKind.FILE, file, Path(file).name)
                )
            if image:
                attachments.append(ChannelAttachment(AttachmentKind.IMAGE, image))
            receipt = await self.push_tool.dispatch(
                ChannelMessage(
                    channel=channel,
                    chat_id=chat_id,
                    content=message,
                    attachments=tuple(attachments),
                )
            )
            if receipt.status is DeliveryStatus.SUCCESS:
                return "消息已发送"
            if receipt.status is DeliveryStatus.PARTIAL:
                return "消息部分送达：%s" % (
                    receipt.detail or "渠道未提交全部内容"
                )
            return "发送失败：%s" % (receipt.detail or "渠道未提交消息")
        if self.bus is None:
            return "Error: Message delivery is not available"
        await self.bus.publish_outbound(
            OutboundMessage(
                channel=channel,
                chat_id=chat_id,
                content=message,
                media=[item for item in (file, image) if item],
            )
        )
        return "已发送"

    def _live_memory_engine(self):
        """返回承重的记忆引擎;未配置或能力不足时返回 None,调用方回退旧路径。"""
        services = self.memory_services
        engine = getattr(services, "engine", None) if services is not None else None
        if engine is None:
            return None
        descriptor = getattr(engine, "DESCRIPTOR", None)
        capabilities = getattr(descriptor, "capabilities", frozenset())
        return engine if MemoryCapability.RETRIEVE_CONTEXT_BLOCK in capabilities else None

    def _memory_scope(self) -> MemoryScope:
        if self.registry is None:
            return MemoryScope()
        ctx = self.registry.context
        return MemoryScope(
            session_key=str(ctx.get("session_key") or ""),
            channel=str(ctx.get("channel") or ""),
            chat_id=str(ctx.get("chat_id") or ""),
            user_id=str(ctx.get("principal_id") or ""),
        )

    def _next_source_ref(self) -> str:
        if self.registry is None or self.session_manager is None:
            return ""
        session_key = str(self.registry.context.get("session_key") or "")
        if not session_key:
            return ""
        return self.session_manager.peek_next_message_id(session_key)

    async def memorize(
        self,
        content: str = "",
        memory_type: str = "",
        summary: str = "",
        memory_kind: str = "",
        tool_requirement: str | None = None,
        steps=None,
    ) -> str:
        """写入长期记忆。

        引擎承重时参数面照 Reference `agent/tools/memorize.py`:`summary`/`memory_kind`,
        `tool_requirement`/`steps` 进 mutation metadata——procedure 类记忆靠它们生成
        rule_schema 与触发标签。旧参数名 `content`/`memory_type` 继续接受(回退路径的 schema)。
        """
        text = str(summary or content or "").strip()
        if not text:
            return "Error: summary 不能为空"
        kind = _canonical_memory_kind(str(memory_kind or memory_type or "").strip())
        source_ref = self._next_source_ref()
        engine = self._live_memory_engine()
        if engine is not None:
            metadata: dict[str, object] = {}
            if tool_requirement is not None:
                metadata["tool_requirement"] = str(tool_requirement)
            if steps is not None:
                metadata["steps"] = [str(step) for step in steps]
            result = await engine.mutate(
                MemoryMutation(
                    kind="remember",
                    summary=text,
                    memory_kind=kind,
                    source_ref=source_ref or "memorize_tool",
                    scope=self._memory_scope(),
                    metadata=metadata,
                )
            )
            if not result.accepted:
                return "Error: 记忆写入被拒绝"
            # 返回格式照 Reference memorize.py:_format_result。
            actual_kind = (result.actual_kind or "").strip()
            status = (result.status or "new").strip()
            if actual_kind:
                return "已记住（item_id=%s；kind=%s；status=%s）：%s" % (
                    result.item_id,
                    actual_kind,
                    status,
                    text,
                )
            return "已记住（item_id=%s；status=%s）：%s" % (result.item_id, status, text)
        if self.memory is None:
            return "Error: Memory runtime is not enabled"
        record = self.memory.memorize(
            text, source_ref=source_ref, memory_type=kind or "requested_memory"
        )
        return "记忆已写入: %s" % record.id

    async def recall_memory(
        self,
        query: str,
        limit: int = 8,
        intent: str = "answer",
        memory_kind: str = "",
        time_filter: str = "",
        memory_types=None,
        since: str = "",
        until: str = "",
    ) -> str:
        """检索长期记忆。

        引擎承重时对齐 Reference `agent/tools/recall_memory.py`:
        - `intent` answer/timeline(timeline 分支在引擎里真实存在,之前模型永远够不到);
        - `time_filter` 预设串解析成 filters.time_start/end,非法值明确报错;
        - limit 钳制 1..200;
        - 返回带 evidence/source_ref/trace 与 `§cited:` 引用协议的结构。
        旧参数 `memory_types`/`since`/`until` 只在词法回退路径继续生效。
        """
        text = str(query or "").strip()
        if isinstance(memory_types, str):
            memory_types = [memory_types]
        legacy_kinds = tuple(str(item) for item in (memory_types or []))
        kinds = (memory_kind.strip(),) if str(memory_kind or "").strip() else legacy_kinds
        engine = self._live_memory_engine()
        if engine is not None:
            if not text:
                return _render_recall_records([], trace={})
            time_window = _parse_time_filter(time_filter)
            if time_filter and time_window is None:
                return json.dumps(
                    {"count": 0, "items": [], "error": "invalid_time_filter"},
                    ensure_ascii=False,
                )
            result = await engine.query(
                MemoryQuery(
                    text=text,
                    intent=_normalize_recall_intent(intent),
                    scope=self._memory_scope(),
                    filters=MemoryQueryFilters(
                        kinds=kinds,
                        time_start=time_window[0] if time_window else None,
                        time_end=time_window[1] if time_window else None,
                    ),
                    limit=max(1, min(int(limit), 200)),
                    # akasha 用它算图激活的时间衰减;DefaultMemoryEngine 忽略。
                    timestamp=datetime.now(timezone.utc),
                )
            )
            return _render_recall_records(result.records, trace=result.trace)
        if self.memory is None:
            return "[]"
        records = self.memory.recall(
            text,
            limit=limit,
            memory_types=list(kinds),
            since=since,
            until=until,
        )
        return json.dumps(
            [r.to_public_json() for r in records], ensure_ascii=False, indent=2
        )

    async def forget_memory(self, ids) -> str:
        if isinstance(ids, str):
            ids = [ids]
        # 去重保序,照 Reference forget_memory.py:_clean_ids。
        clean: list[str] = []
        seen: set[str] = set()
        for raw in ids or []:
            item_id = str(raw).strip()
            if item_id and item_id not in seen:
                seen.add(item_id)
                clean.append(item_id)
        engine = self._live_memory_engine()
        if engine is not None:
            if not clean:
                return _render_forget_result([], [], [], [])
            result = await engine.mutate(
                MemoryMutation(kind="forget", ids=tuple(clean), scope=self._memory_scope())
            )
            return _render_forget_result(
                clean, result.affected_ids, result.missing_ids, result.items
            )
        if self.memory is None:
            return "Error: Memory runtime is not enabled"
        forgotten = self.memory.forget(clean)
        return json.dumps({"superseded_ids": forgotten}, ensure_ascii=False)

    async def memory_signal(self, **kwargs: Any) -> str:
        """engine 自定义记忆工具的通用回执(对照 Reference `_MemorySignalTool`)。

        这类工具的真实语义在引擎侧的副作用里(例如 akasha 的 `reinforce_memory`
        是给激活图加权),工具本身只确认收到——所以一个 handler 覆盖所有自定义工具。
        """
        return "已记录。"

    def search_messages(self, query: str, limit: int = 10) -> str:
        if self.session_manager is None:
            return "[]"
        return json.dumps(
            self.session_manager.search_messages(query, limit=limit),
            ensure_ascii=False,
            indent=2,
        )

    def fetch_messages(self, source_ref: str, context: int = 2) -> str:
        if self.session_manager is None:
            return "[]"
        return json.dumps(
            self.session_manager.fetch_messages(source_ref, context=context),
            ensure_ascii=False,
            indent=2,
        )

    def _html_to_text(self, value: str) -> str:
        text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", value)
        text = re.sub(r"(?is)<br\s*/?>", "\n", text)
        text = re.sub(r"(?is)</(p|div|li|h[1-6]|tr)>", "\n", text)
        text = re.sub(r"(?is)<[^>]+>", " ", text)
        text = html.unescape(text)
        text = re.sub(r"[ \t\r\f\v]+", " ", text)
        text = re.sub(r"\n\s+", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _validate_fetch_target(self, parsed: urllib.parse.ParseResult) -> str:
        if _env_bool(PRIVATE_FETCH_ENV):
            return ""
        host = parsed.hostname
        if not host:
            return "Error: URL host is required"
        try:
            addresses = socket.getaddrinfo(host, parsed.port or (443 if parsed.scheme == "https" else 80), type=socket.SOCK_STREAM)
        except OSError as exc:
            return "Error: Could not resolve host %s: %s" % (host, exc)
        for item in addresses:
            ip = ipaddress.ip_address(item[4][0])
            if (
                ip.is_private
                or ip.is_loopback
                or ip.is_link_local
                or ip.is_multicast
                or ip.is_reserved
                or ip.is_unspecified
            ):
                return (
                    "Error: Refusing to fetch private/local address %s. "
                    "Set %s=true only for trusted local tests."
                ) % (ip, PRIVATE_FETCH_ENV)
        return ""

    def _dangerous_shell_command(self, command: str) -> bool:
        normalized = re.sub(r"\s+", " ", command.strip().lower())
        blocked_literals = [
            "sudo ",
            "shutdown",
            "reboot",
            "> /dev/",
            "mkfs",
            "dd if=",
            ":(){",
            "chmod -r 777 /",
            "chown -r ",
        ]
        if any(item in normalized for item in blocked_literals):
            return True
        blocked_patterns = [
            r"\brm\s+-[^\n;|&]*r[^\n;|&]*f[^\n;|&]*(?:/|\$home|~)",
            r"\brm\s+-[^\n;|&]*f[^\n;|&]*r[^\n;|&]*(?:/|\$home|~)",
        ]
        return any(re.search(pattern, normalized) for pattern in blocked_patterns)

    @staticmethod
    def _image_mime(data: bytes) -> str:
        if data.startswith(b"\x89PNG\r\n\x1a\n"):
            return "image/png"
        if data.startswith(b"\xff\xd8\xff"):
            return "image/jpeg"
        if data.startswith((b"GIF87a", b"GIF89a")):
            return "image/gif"
        if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
            return "image/webp"
        return ""

    def _mutation_lock(self, path: Path) -> threading.Lock:
        return self._mutation_locks.setdefault(str(path.resolve()), threading.Lock())

    @staticmethod
    def _atomic_write_text(path: Path, content: str) -> None:
        temp = path.with_name(".%s.%s.tmp" % (path.name, uuid4().hex))
        try:
            temp.write_text(content, encoding="utf-8")
            os.replace(temp, path)
        finally:
            try:
                temp.unlink()
            except FileNotFoundError:
                pass


def _register_memory_tools(registry: ToolRegistry, handlers: "WorkspaceTools") -> None:
    """注册显式记忆工具。

    引擎承重时,**工具面完全由 `engine.tool_profile()` 决定**(照 Reference
    `agent/tools/meta/register.py`):声明了哪个就注册哪个,没声明的一律不注册,
    并把 `profile.tools` 里的自定义工具一并注册。这条很重要——akasha 只声明
    `recall` 与自定义的 `reinforce_memory`,**没有 memorize/forget**(它从 turn 自动
    摄入)。此前这里"profile 没有就退回旧 schema 注册",导致 akasha 下模型能看到
    memorize,调用后被引擎拒绝写入。

    引擎未承重时才注册 kirakira 词法路径的旧 schema——这是与 Reference 的显式偏离:
    Reference 在 Disabled 时干脆不注册记忆工具,kirakira 保留词法降级。
    """
    engine = handlers._live_memory_engine()
    if engine is None:
        _register_legacy_memory_tools(registry, handlers)
        return

    profile = engine.tool_profile()
    if profile.memorize is not None:
        registry.register(
            ToolSpec("memorize", profile.memorize.description, profile.memorize.parameters),
            handlers.memorize,
        )
    if profile.recall is not None:
        registry.register(
            ToolSpec("recall_memory", profile.recall.description, profile.recall.parameters),
            handlers.recall_memory,
        )
    if profile.forget is not None:
        registry.register(
            ToolSpec("forget_memory", profile.forget.description, profile.forget.parameters),
            handlers.forget_memory,
        )
    # engine 自定义工具槽(对照 Reference 的 `_MemorySignalTool`):引擎可以追加任意工具,
    # 语义由引擎在自己的副作用里实现,工具本身只回执。
    for spec in getattr(profile, "tools", ()) or ():
        name = str(getattr(spec, "name", "") or "").strip()
        if not name:
            raise ValueError("自定义 memory 工具缺少 name")
        registry.register(
            ToolSpec(name, spec.description, spec.parameters),
            handlers.memory_signal,
        )


def _register_legacy_memory_tools(registry: ToolRegistry, handlers: "WorkspaceTools") -> None:
    """引擎未承重时的词法降级工具面。"""
    registry.register(
        ToolSpec(
            "memorize",
            "Write a stable user fact or preference into long-term memory.",
            object_schema(
                {
                    "content": {"type": "string"},
                    "memory_type": {
                        "type": "string",
                        "enum": [
                            "requested_memory",
                            "identity",
                            "preference",
                            "procedure",
                            "event",
                        ],
                    },
                },
                ["content"],
            ),
        ),
        handlers.memorize,
    )
    registry.register(
        ToolSpec(
            "recall_memory",
            "Search long-term memory semantically/lexically.",
            object_schema(
                {
                    "query": {"type": "string"},
                    "limit": {"type": "integer"},
                    "memory_types": {"type": "array", "items": {"type": "string"}},
                    "since": {"type": "string"},
                    "until": {"type": "string"},
                },
                ["query"],
            ),
        ),
        handlers.recall_memory,
    )
    registry.register(
        ToolSpec(
            "forget_memory",
            "Mark memory items as forgotten by id.",
            object_schema({"ids": {"type": "array", "items": {"type": "string"}}}, ["ids"]),
        ),
        handlers.forget_memory,
    )


def build_default_registry(
    workdir: Path,
    skills_dir: Optional[Path] = None,
    memory: MemoryRuntime | None = None,
    session_manager: SessionManager | None = None,
    bus: MessageBus | None = None,
    push_tool: Any = None,
    memory_services: Any = None,
    execution_backend: ExecutionBackend | None = None,
    workspace_backend: WorkspaceBackend | None = None,
) -> ToolRegistry:
    skill_loader = SkillLoader(skills_dir or (workdir / "skills"))
    registry = ToolRegistry()
    handlers = WorkspaceTools(
        workdir,
        skill_loader,
        memory,
        session_manager,
        registry,
        bus,
        push_tool,
        memory_services=memory_services,
        execution_backend=execution_backend,
        workspace_backend=workspace_backend,
    )
    registry.add_shutdown_callback(handlers.shutdown)
    registry.add_owner_cleanup_callback(handlers.cleanup_shell_owner)
    registry.register(
        ToolSpec(
            "bash",
            "Run a shell command in the workspace.",
            object_schema(
                {
                    "command": {"type": "string"},
                    "timeout": {"type": "integer"},
                    "run_in_background": {"type": "boolean"},
                    "auto_promote": {"type": "boolean"},
                    "tty": {"type": "boolean"},
                    "shell": {"type": "string"},
                    "login": {"type": "boolean"},
                    "yield_time_ms": {"type": "integer"},
                    "max_output_tokens": {"type": "integer"},
                },
                ["command"],
            ),
        ),
        handlers.bash,
        risk="external-side-effect",
    )
    registry.register(
        ToolSpec(
            "task_output",
            "Poll output and status for a background shell task.",
            object_schema(
                {
                    "task_id": {"type": "integer"},
                    "block": {"type": "boolean"},
                    "timeout_ms": {"type": "integer"},
                    "offset": {"type": "integer"},
                },
                ["task_id"],
            ),
        ),
        handlers.task_output,
    )
    registry.register(
        ToolSpec(
            "write_stdin",
            "Write to or wait on a running shell execution and return the next output chunk.",
            object_schema(
                {
                    "execution_id": {"type": "integer"},
                    "chars": {"type": "string"},
                    "yield_time_ms": {"type": "integer"},
                    "max_output_tokens": {"type": "integer"},
                },
                ["execution_id"],
            ),
        ),
        handlers.write_stdin,
        risk="external-side-effect",
    )
    registry.register(
        ToolSpec(
            "task_stop",
            "Stop and clean up a background shell task.",
            object_schema({"task_id": {"type": "integer"}}, ["task_id"]),
        ),
        handlers.task_stop,
        risk="external-side-effect",
    )
    registry.register(
        ToolSpec(
            "read_file",
            "Read file contents from inside the workspace.",
            object_schema(
                {
                    "path": {"type": "string"},
                    "limit": {"type": "integer"},
                    "offset": {"type": "integer"},
                },
                ["path"],
            ),
        ),
        handlers.read_file,
    )
    registry.register(
        ToolSpec(
            "list_dir",
            "List files and directories inside the workspace.",
            object_schema({"path": {"type": "string"}}, []),
        ),
        handlers.list_dir,
    )
    registry.register(
        ToolSpec(
            "write_file",
            "Write content to a file inside the workspace.",
            object_schema(
                {
                    "path": {"type": "string"},
                    "content": {"type": "string"},
                },
                ["path", "content"],
            ),
        ),
        handlers.write_file,
        risk="write",
    )
    registry.register(
        ToolSpec(
            "edit_file",
            "Replace exact text in a workspace file.",
            object_schema(
                {
                    "path": {"type": "string"},
                    "old_text": {"type": "string"},
                    "new_text": {"type": "string"},
                    "replace_all": {"type": "boolean"},
                },
                ["path", "old_text", "new_text"],
            ),
        ),
        handlers.edit_file,
        risk="write",
    )
    registry.register(
        ToolSpec(
            "load_skill",
            "Load specialized knowledge by skill name.",
            object_schema({"name": {"type": "string"}}, ["name"]),
        ),
        handlers.load_skill,
    )
    registry.register(
        ToolSpec(
            "request_user_confirmation",
            "当任务必须等待用户做出明确选择、授权或确认时调用。"
            "调用后，在本轮最终回复中清楚列出要确认的事项；"
            "普通提问、补充信息或修辞问句不要调用。",
            object_schema(
                {
                    "prompt": {
                        "type": "string",
                        "description": "需要用户明确确认的具体事项。",
                    }
                },
                ["prompt"],
            ),
        ),
        handlers.request_user_confirmation,
    )
    registry.register(
        ToolSpec(
            "vision",
            "Analyze one or more local image attachments using the configured vision model.",
            object_schema(
                {
                    "image_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "prompt": {"type": "string"},
                },
                ["image_paths"],
            ),
        ),
        handlers.vision,
    )
    registry.register(
        ToolSpec(
            "compact",
            "将当前会话的历史归档进长期记忆并推进归档游标；下一轮起上下文从归档点开始。仅在上下文确实过长时使用。",
            object_schema({}, []),
        ),
        handlers.compact,
    )
    registry.register(
        ToolSpec(
            "tool_search",
            "Search available tools by name or description.",
            object_schema(
                {"query": {"type": "string"}, "limit": {"type": "integer"}},
                [],
            ),
        ),
        handlers.tool_search,
    )
    registry.register(
        ToolSpec(
            "web_fetch",
            "Fetch a verified web URL and return readable content with structured source metadata (URL, title, and publication date when available). format selects the output shape: markdown (default), text, or html. Use it to verify important claims found by search; HTTP, binary, encoding, and decoding failures are returned as errors and are not evidence.",
            object_schema(
                {
                    "url": {"type": "string"},
                    "max_chars": {"type": "integer"},
                    "format": {
                        "type": "string",
                        "enum": ["text", "markdown", "html"],
                    },
                },
                ["url"],
            ),
        ),
        handlers.web_fetch,
    )
    registry.register(
        ToolSpec(
            "web_search",
            "Search the web and return structured result titles and URLs for source discovery. Empty/unparseable results are errors. For time-sensitive news, prices, or status claims, fetch reliable sources, note their dates, cross-check important facts, and disclose when evidence is insufficient.",
            object_schema(
                {"query": {"type": "string"}, "limit": {"type": "integer"}},
                ["query"],
            ),
        ),
        handlers.web_search,
    )
    registry.register(
        ToolSpec(
            "message_push",
            "Send one complete logical message to a configured channel/chat.",
            object_schema(
                {
                    "channel": {"type": "string"},
                    "chat_id": {"type": "string"},
                    "message": {"type": "string"},
                    "file": {"type": "string"},
                    "image": {"type": "string"},
                },
                ["channel", "chat_id"],
            ),
        ),
        handlers.message_push,
    )
    _register_memory_tools(registry, handlers)
    registry.register(
        ToolSpec(
            "search_messages",
            "Keyword search persisted chat messages and return source refs.",
            object_schema(
                {"query": {"type": "string"}, "limit": {"type": "integer"}},
                ["query"],
            ),
        ),
        handlers.search_messages,
    )
    registry.register(
        ToolSpec(
            "fetch_messages",
            "Fetch persisted chat messages around a source_ref.",
            object_schema(
                {"source_ref": {"type": "string"}, "context": {"type": "integer"}},
                ["source_ref"],
            ),
        ),
        handlers.fetch_messages,
    )
    return registry
