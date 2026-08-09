"""Dashboard 的只读数据面(对照 Reference `bootstrap/dashboard_api.py`)。

Reference 把 Dashboard 的数据装配放在 bootstrap 层,渠道只负责 HTTP;kirakira 沿用这条
边界:本模块把运行时各子系统的状态收敛成一组稳定投影,Web 渠道只做序列化与路由。

两条设计约束:

1. **记忆走引擎的 admin 协议,不绕过。** 此前 Web 渠道直接调 `memory.store2.*`,
   等于绕开 `MemoryAdminApi` 直接摸存储——换引擎时 Dashboard 会直接坏掉,而这正是
   admin 协议要防的事(Reference 为此单独构造一份 admin engine)。这里统一走
   `engine.*_for_dashboard`,引擎未承重才回退旧栈。
2. **只读优先。** 除记忆的编辑/删除与会话删除外,本模块不改变任何运行时状态;
   主动/Drift/插件面板全部是投影,不提供触发按钮——避免 Dashboard 变成第二个控制面
   (真正的驱动入口是 control.sock)。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PAGE_SIZE_MAX = 200
_SIMILAR_MAX = 50


def _as_bool(value: str) -> bool | None:
    """三态解析:未给返回 None(不过滤),给了才是真正的 True/False 过滤。"""
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes"}:
        return True
    if text in {"0", "false", "no"}:
        return False
    return None


@dataclass
class DashboardService:
    """运行时状态的只读投影。各依赖缺席时对应面板返回空,不抛异常。

    面板可以在最小构造(只有 workspace)下工作:这样单测与 `gateway` 调试模式都不必
    装配整个 runtime 才能打开页面。
    """

    workspace: Path
    session_manager: Any = None
    memory_services: Any = None
    memory: Any = None
    plugin_manager: Any = None
    proactive_loop: Any = None
    drift_runner: Any = None
    restart_coordinator: Any = None
    recall_inspector: Any = None
    # 运行时的事件循环。proactive.db / drift.db 的连接归它所在的线程独占,而 Web 渠道
    # 的 HTTP handler 跑在自己的线程里——直接读会撞 SQLite 的线程亲和检查。这里把读
    # marshal 回属主线程,而不是另开一条连接:后者会破坏"状态库单一 owner"这条不变量。
    loop: Any = None
    extra: dict[str, Any] = field(default_factory=dict)

    def _read(self, fn: Any) -> Any:
        """在状态库属主线程上执行一次只读调用。"""
        import asyncio

        loop = self.loop
        if loop is None or loop.is_closed():
            return fn()
        try:
            if asyncio.get_running_loop() is loop:
                return fn()  # 已在属主线程(如测试直接 await),不必再绕一圈
        except RuntimeError:
            pass  # 当前线程没有运行中的 loop —— 正是 HTTP handler 线程的情形

        async def call() -> Any:
            return fn()

        return asyncio.run_coroutine_threadsafe(call(), loop).result(timeout=10.0)

    # ── 记忆:统一走引擎的 admin 协议 ────────────────────────────────

    @property
    def _engine(self) -> Any:
        """承重引擎;未配 embedding 时是 DisabledMemoryEngine,不具备 admin 能力。"""
        engine = getattr(self.memory_services, "engine", None)
        return engine if hasattr(engine, "list_items_for_dashboard") else None

    @property
    def _legacy_store(self) -> Any:
        """引擎未承重时的回退:旧 MemoryRuntime 持有的同一个 store。"""
        memory = self.memory
        if memory is None or getattr(memory, "engine", "") != "coremem":
            return None
        return getattr(memory, "store2", None)

    def _memory_admin(self) -> Any:
        return self._engine or self._legacy_store

    def memories(self, params: dict[str, list[str]] | None = None) -> dict[str, Any]:
        params = params or {}

        def one(name: str, default: str = "") -> str:
            return str((params.get(name) or [default])[0])

        page = max(1, int(one("page", "1") or 1))
        page_size = max(1, min(_PAGE_SIZE_MAX, int(one("page_size", "50") or 50)))
        admin = self._memory_admin()
        if admin is not None:
            items, total = admin.list_items_for_dashboard(
                q=one("q"),
                memory_type=one("memory_type"),
                status=one("status"),
                source_ref=one("source_ref"),
                scope_channel=one("scope_channel"),
                scope_chat_id=one("scope_chat_id"),
                has_embedding=_as_bool(one("has_embedding")),
                page=page,
                page_size=page_size,
                sort_by=one("sort_by", "created_at"),
                sort_order=one("sort_order", "desc"),
            )
            return {
                "memories": items,
                "total": total,
                "page": page,
                "page_size": page_size,
                "engine": self._engine_name(),
            }
        if self.memory is None:
            return {"memories": [], "total": 0, "page": page, "page_size": page_size}
        items = self.memory.list_records(include_forgotten=True)
        return {
            "memories": items[(page - 1) * page_size : page * page_size],
            "total": len(items),
            "page": page,
            "page_size": page_size,
            "engine": "legacy",
        }

    def memory_item(self, memory_id: str) -> dict[str, Any] | None:
        """注意方法名不能叫 `memory`:那会覆盖同名 dataclass 字段的默认值。"""
        admin = self._memory_admin()
        if admin is not None:
            return admin.get_item_for_dashboard(memory_id)
        if self.memory is None:
            return None
        return next(
            (
                item
                for item in self.memory.list_records(include_forgotten=True)
                if item["id"] == memory_id
            ),
            None,
        )

    def memory_similar(self, memory_id: str, limit: int = 8) -> list[dict[str, Any]]:
        admin = self._memory_admin()
        if admin is None:
            return []
        return admin.find_similar_items_for_dashboard(
            memory_id, top_k=max(1, min(_SIMILAR_MAX, limit))
        )

    def update_memory(self, payload: dict[str, Any]) -> bool:
        """编辑仍走旧 runtime:引擎的写入口是 mutate(remember/forget),没有原地改写语义。"""
        if self.memory is None:
            return False
        return self.memory.update_record(
            str(payload.get("id") or ""),
            content=payload.get("content"),
            memory_type=payload.get("memory_type"),
        )

    def forget_memory(self, memory_id: str) -> bool:
        """逻辑退休(superseded),可恢复;与硬删除区分开。"""
        return bool(self.memory and self.memory.forget([memory_id]))

    def delete_memories(self, ids: list[str], *, confirm: str) -> int:
        """物理删除。需要显式 confirm——它绕过了记忆系统"逻辑退休"的默认保护。"""
        if confirm != "HARD_DELETE":
            raise ValueError("hard delete requires confirm=HARD_DELETE")
        admin = self._memory_admin()
        if admin is None:
            raise RuntimeError("记忆引擎未承重，无法执行物理删除")
        deleted = admin.delete_items_batch([item for item in ids if item])
        # 旧栈持有内存缓存,删除后要重读,否则页面还会显示已删条目。
        if self.memory is not None and hasattr(self.memory, "_load"):
            self.memory._load()
        return deleted

    def memory_health(self) -> dict[str, Any]:
        from bootstrap.memory_admin import doctor

        return doctor(self.workspace)

    def _engine_name(self) -> str:
        engine = getattr(self.memory_services, "engine", None)
        descriptor = getattr(engine, "DESCRIPTOR", None)
        return str(getattr(descriptor, "name", "") or type(engine).__name__ or "unknown")

    def engine_info(self) -> dict[str, Any]:
        """引擎能力面板(对照 Reference `/api/dashboard/memory/engine-info`)。"""
        engine = getattr(self.memory_services, "engine", None)
        descriptor = getattr(engine, "DESCRIPTOR", None)
        capabilities = sorted(
            str(getattr(item, "value", item))
            for item in (getattr(descriptor, "capabilities", None) or ())
        )
        profile = getattr(descriptor, "profile", None)
        return {
            "name": self._engine_name(),
            "profile": str(getattr(profile, "value", profile) or ""),
            "capabilities": capabilities,
            "load_bearing": self._engine is not None,
            "tools": self._engine_tool_names(engine),
        }

    @staticmethod
    def _engine_tool_names(engine: Any) -> list[str]:
        profile = getattr(engine, "tool_profile", None)
        if not callable(profile):
            return []
        try:
            spec = profile()
        except Exception:  # noqa: BLE001 - 面板不因引擎实现异常而整页失败
            return []
        names = []
        for attr, fallback in (
            ("recall", "recall_memory"),
            ("memorize", "memorize"),
            ("forget", "forget_memory"),
        ):
            item = getattr(spec, attr, None)
            if item is not None:
                names.append(str(getattr(item, "name", "") or fallback))
        names.extend(
            str(getattr(item, "name", "") or "?") for item in getattr(spec, "tools", ())
        )
        return names

    # ── 会话 ────────────────────────────────────────────────────────

    def sessions(self) -> list[dict[str, Any]]:
        if self.session_manager is None:
            return []
        return list(self.session_manager.list_sessions())

    def session(self, key: str, *, limit: int = 100) -> dict[str, Any] | None:
        """会话详情。只回最近 limit 条,长会话不把整页撑爆。"""
        if self.session_manager is None or not key:
            return None
        try:
            session = self.session_manager.get_or_create(key)
        except Exception:  # noqa: BLE001 - 未知 key 不是错误,面板显示空即可
            return None
        messages = list(getattr(session, "messages", []))
        return {
            "key": key,
            "metadata": dict(getattr(session, "metadata", {}) or {}),
            "total_messages": len(messages),
            "last_consolidated": int(getattr(session, "last_consolidated", 0) or 0),
            "messages": [
                {
                    "role": str(item.get("role") or ""),
                    "content": str(item.get("content") or "")[:4000],
                    "timestamp": str(item.get("timestamp") or ""),
                    "proactive": bool(item.get("proactive")),
                    "drift": bool(item.get("drift")),
                    "tools_used": list(item.get("tools_used") or []),
                }
                for item in messages[-max(1, limit) :]
            ],
        }

    def delete_session(self, key: str) -> bool:
        if self.session_manager is None or not key:
            return False
        return bool(self.session_manager.delete_session(key))

    def messages(self, params: dict[str, list[str]] | None = None) -> dict[str, Any]:
        """跨会话消息检索(只读)。

        消息现已使用稳定 id，SessionManager 也提供显式 ``delete_messages(ids)``。
        Dashboard 仍保持只读，是因为前端尚未实现破坏性操作的确认与 Akasha sidecar
        清理编排；不能仅因底层可删就在 GET 数据面暴露删除按钮。
        """
        params = params or {}
        query = str((params.get("q") or [""])[0]).strip()
        limit = max(1, min(200, int((params.get("limit") or ["50"])[0] or 50)))
        if self.session_manager is None or not query:
            return {"messages": [], "total": 0, "query": query, "deletable": False}
        try:
            rows = self.session_manager.search_messages(query, limit=limit)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[dashboard] 消息检索失败: %s", exc)
            return {"messages": [], "total": 0, "query": query, "error": str(exc)}
        return {
            "messages": list(rows),
            "total": len(rows),
            "query": query,
            # 前端据此不渲染删除按钮,而不是渲染了再报错
            "deletable": False,
            "deletable_reason": "消息存储支持稳定 ID 删除，但 Dashboard 尚未接入确认与索引清理流程",
        }

    # ── 检索回放 ────────────────────────────────────────────────────

    def recall_overview(self) -> dict[str, Any]:
        if self.recall_inspector is None:
            return {"available": False, "enabled": False, "total": 0}
        return self.recall_inspector.overview()

    def recall_turns(self, params: dict[str, list[str]] | None = None) -> dict[str, Any]:
        if self.recall_inspector is None:
            return {"turns": [], "total": 0, "available": False}
        params = params or {}

        def one(name: str, default: str = "") -> str:
            return str((params.get(name) or [default])[0])

        turns, total = self.recall_inspector.list_turns(
            session_key=one("session_key"),
            q=one("q"),
            page=max(1, int(one("page", "1") or 1)),
            page_size=max(1, min(200, int(one("page_size", "50") or 50))),
        )
        return {"turns": turns, "total": total, "available": True}

    def recall_turn(self, turn_id: str) -> dict[str, Any] | None:
        if self.recall_inspector is None:
            return None
        return self.recall_inspector.get_turn(turn_id)

    # ── 插件 ────────────────────────────────────────────────────────

    def plugins(self) -> dict[str, Any]:
        """插件面板:装载记录 + 当前代际 + 待排空代际。

        代际信息是 kirakira 热重载的可观测面——`lease_count` 非零说明还有在途 turn
        持着旧代际,这正是"换代不抽走在途能力"在运行时的证据。
        """
        manager = self.plugin_manager
        if manager is None:
            return {"active": [], "errors": {}, "generations": [], "retired": []}
        active = []
        for record in getattr(manager, "active", []) or []:
            active.append(
                {
                    "id": record.plugin_id,
                    "version": getattr(record, "version", ""),
                    "desc": getattr(record, "desc", ""),
                    "root": str(getattr(record, "root", "")),
                    "lifecycle": getattr(record, "instance", None) is not None,
                }
            )
        registry = getattr(manager, "generations", None)
        generations = [
            {
                "plugin_id": gen.plugin_id,
                "generation_id": gen.generation_id,
                "revision": gen.revision,
                "state": gen.state,
                "lease_count": gen.lease_count,
            }
            for gen in (getattr(registry, "active", ()) if registry else ())
        ]
        retired = [
            {
                "plugin_id": gen.plugin_id,
                "generation_id": gen.generation_id,
                "state": gen.state,
                "lease_count": gen.lease_count,
                "can_quiesce": gen.can_quiesce,
            }
            for gen in (getattr(registry, "retired", ()) if registry else ())
        ]
        return {
            "active": active,
            "errors": dict(getattr(manager, "errors", {}) or {}),
            "generations": generations,
            "retired": retired,
        }

    # ── 主动推送 ────────────────────────────────────────────────────

    def proactive(self) -> dict[str, Any]:
        loop = self.proactive_loop
        if loop is None:
            return {"enabled": False}
        try:
            status = dict(self._read(loop.status))
        except Exception as exc:  # noqa: BLE001 - 面板不因单个子系统失败而整页失败
            logger.warning("[dashboard] proactive status 失败: %s", exc)
            return {"enabled": True, "error": str(exc)}
        status["enabled"] = True
        status["modules"] = [
            getattr(module, "slot", type(module).__name__)
            for module in getattr(loop, "_modules", [])
        ]
        return status

    # ── Drift ───────────────────────────────────────────────────────

    def drift(self) -> dict[str, Any]:
        runner = self.drift_runner
        state = getattr(runner, "_state", None) if runner is not None else None
        if state is None:
            return {"enabled": False}
        payload: dict[str, Any] = {"enabled": True}
        try:
            # 整块 marshal 回属主线程:多次往返会放大 HTTP 线程的等待。
            payload.update(
                self._read(lambda: self._drift_snapshot(runner, state))
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[dashboard] drift 面板失败: %s", exc)
            payload["error"] = str(exc)
        return payload

    def _drift_snapshot(self, runner: Any, state: Any) -> dict[str, Any]:
        last = state.last_drift_at()
        return {
            "recent_runs": state.recent_runs(limit=20),
            "self_observations": state.recent_self_observations(limit=12),
            "last_drift_at": last.isoformat() if last else None,
            "skills": self._drift_skills(runner, state),
        }

    @staticmethod
    def _drift_skills(runner: Any, state: Any) -> list[dict[str, Any]]:
        """每个 skill 的连续性:上轮 scratchpad 与倾向,是 Drift 跨轮连续的证据。"""
        from plugins.drift_flow.skills import discover_skills

        names: list[str] = []
        workspace = getattr(runner, "_workspace", None)
        if workspace is not None:
            try:
                names = [getattr(skill, "name", "") for skill in discover_skills(workspace)]
            except Exception:  # noqa: BLE001
                names = []
        last_by_skill = {}
        try:
            last_by_skill = {
                key: value.isoformat() for key, value in state.last_run_at_by_skill().items()
            }
        except Exception:  # noqa: BLE001
            pass
        skills = []
        for name in [item for item in names if item]:
            continuum = {}
            try:
                continuum = state.get_continuum(name) or {}
            except Exception:  # noqa: BLE001
                continuum = {}
            skills.append(
                {
                    "name": name,
                    "last_run_at": last_by_skill.get(name),
                    "scratchpad": str(continuum.get("scratchpad") or ""),
                    "next_tendency": str(continuum.get("next_tendency") or ""),
                }
            )
        return skills

    # ── 总览 ────────────────────────────────────────────────────────

    def overview(self) -> dict[str, Any]:
        """一屏看清:引擎是否承重、三条链路是否在跑、插件与会话规模。"""
        memories = self.memories({"page_size": ["1"]})
        proactive = self.proactive()
        drift = self.drift()
        plugins = self.plugins()
        coordinator = self.restart_coordinator
        return {
            "workspace": str(self.workspace),
            "memory": {
                "engine": self._engine_name(),
                "load_bearing": self._engine is not None,
                "total": memories.get("total", 0),
            },
            "sessions": len(self.sessions()),
            "plugins": {
                "active": len(plugins["active"]),
                "errors": len(plugins["errors"]),
                "generations": len(plugins["generations"]),
            },
            "proactive": {
                "enabled": bool(proactive.get("enabled")),
                "target": proactive.get("target", ""),
                "unread_alert": proactive.get("unread_alert", 0),
                "unread_content": proactive.get("unread_content", 0),
                "in_cooldown": bool(proactive.get("in_cooldown")),
                "next_interval_s": proactive.get("estimated_next_interval_s"),
            },
            "drift": {
                "enabled": bool(drift.get("enabled")),
                "last_drift_at": drift.get("last_drift_at"),
                "runs": len(drift.get("recent_runs") or []),
            },
            "restart": {
                "supervised": bool(getattr(coordinator, "supervised", False)),
                "state": str(getattr(coordinator, "state", "") or "idle"),
            },
            "recall": self.recall_overview(),
        }
