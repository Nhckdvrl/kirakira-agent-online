"""M0/M1 Memory2 doctor, migration, verification, and rollback commands."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import os
import re
import shutil
import sqlite3
import time
import contextlib
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

from infra.persistence.json_store import atomic_write_text
from memory2.rule_schema import build_procedure_rule_schema
from memory2.store import MemoryStore2

REFERENCE_PIN = "012e37c8b51df045353972bb551d8e868ab52455"
CANONICAL_TYPES = {"procedure", "preference", "event", "profile"}
TYPE_MAP = {
    "identity": "profile",
    "fact": "profile",
    "requested_memory": "profile",
}


def _now_id() -> str:
    return datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")


def _content_hash(summary: str, memory_type: str) -> str:
    text = re.sub(r"\s+", " ", summary.lower().strip()) + memory_type
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _read_items(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("items.json 必须是 JSON array")
    items: list[dict[str, Any]] = []
    ids: set[str] = set()
    for index, raw in enumerate(payload):
        if not isinstance(raw, dict):
            raise ValueError(f"items.json[{index}] 必须是 object")
        item_id = str(raw.get("id") or "").strip()
        content = str(raw.get("content") or "").strip()
        if not item_id or not content:
            raise ValueError(f"items.json[{index}] 缺少 id/content")
        if item_id in ids:
            raise ValueError(f"items.json 存在重复 id: {item_id}")
        ids.add(item_id)
        items.append(dict(raw))
    return items


def _sqlite_report(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False, "integrity": "missing", "items": 0, "vectors": 0}
    uri = f"file:{path.resolve()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        tables = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "memory_items" not in tables:
            return {
                "exists": True,
                "integrity": conn.execute("PRAGMA integrity_check").fetchone()[0],
                "schema": "missing memory_items",
                "items": 0,
                "vectors": 0,
            }
        counts = {
            str(status): int(count)
            for status, count in conn.execute(
                "SELECT status, COUNT(*) FROM memory_items GROUP BY status"
            )
        }
        # 注入选择器只接受这四类,其余类型即使被检索命中也永远不会注入上下文
        # (retriever.py 的 `else: continue`)。旧 schema 写进来的 identity/fact 等
        # 会静默失效,所以在 doctor 里显式报出来。
        non_injectable = {
            str(memory_type): int(count)
            for memory_type, count in conn.execute(
                "SELECT memory_type, COUNT(*) FROM memory_items "
                "WHERE status = 'active' AND memory_type NOT IN (?, ?, ?, ?) "
                "GROUP BY memory_type",
                tuple(sorted(CANONICAL_TYPES)),
            )
        }
        return {
            "exists": True,
            "integrity": conn.execute("PRAGMA integrity_check").fetchone()[0],
            "schema": "ok",
            "items": sum(counts.values()),
            "statuses": counts,
            "non_injectable_types": non_injectable,
            "vectors": int(
                conn.execute(
                    "SELECT COUNT(*) FROM memory_items WHERE embedding IS NOT NULL"
                ).fetchone()[0]
            ),
            "replacements": int(
                conn.execute("SELECT COUNT(*) FROM memory_replacements").fetchone()[0]
            )
            if "memory_replacements" in tables
            else 0,
        }
    finally:
        conn.close()


def doctor(workspace: Path, *, project_root: Path | None = None) -> dict[str, Any]:
    """Read-only M0 audit. It never creates files or opens SQLite read-write."""
    del project_root  # 保留 CLI API；doctor 不允许读取外部 Reference checkout。
    memory_dir = workspace / "memory"
    items_path = memory_dir / "items.json"
    db_path = memory_dir / "coremem.db"
    owner_path = memory_dir / "structured-owner.json"
    dependencies = {
        name: importlib.util.find_spec(name) is not None
        for name in ("json_repair", "sqlite_vec", "numpy", "httpx")
    }
    modules: dict[str, str] = {}
    for name in (
        "memory2.store",
        "memory2.retriever",
        "memory2.memorizer",
        "memory2.post_response_worker",
        "core.memory.markdown",
    ):
        try:
            importlib.import_module(name)
        except Exception as exc:  # doctor reports; it does not hide the failure
            modules[name] = f"error: {type(exc).__name__}: {exc}"
        else:
            modules[name] = "ok"
    owner: dict[str, Any] = {}
    if owner_path.exists():
        try:
            owner = json.loads(owner_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            owner = {"error": str(exc)}
    markdown = {
        name: (memory_dir / name).exists()
        for name in ("MEMORY.md", "SELF.md", "PENDING.md", "RECENT_CONTEXT.md")
    }
    embedding_configured = False
    config_path = workspace / "config.toml"
    if config_path.exists():
        import tomllib

        config = tomllib.loads(config_path.read_text(encoding="utf-8"))
        embedding = ((config.get("memory") or {}).get("embedding") or {})
        # 与 build_memory_services 的门控同口径:base_url 决定是否承重,
        # model 未配时 build_config 会落到 Reference 同款默认 text-embedding-v3。
        embedding_configured = bool(embedding.get("base_url"))
    legacy_items = _read_items(items_path) if items_path.exists() else []
    report = {
        "ok": False,
        "workspace": str(workspace.resolve()),
        "dependencies": dependencies,
        "modules": modules,
        "owner": owner or {"owner": "legacy-unpublished"},
        "legacy": {
            "items_path": str(items_path),
            "items": len(legacy_items),
            "active": sum(str(item.get("status") or "active") == "active" for item in legacy_items),
        },
        "coremem": _sqlite_report(db_path),
        "embedding_configured": embedding_configured,
        "markdown": markdown,
    }
    report["ok"] = bool(
        all(dependencies.values())
        and all(value == "ok" for value in modules.values())
        and (report["coremem"]["integrity"] in {"ok", "missing"})
        and all(markdown.values())
    )
    return report


def backup(workspace: Path, *, label: str = "manual") -> dict[str, Any]:
    memory_dir = workspace / "memory"
    backup_id = f"{_now_id()}-{label}-{uuid4().hex[:8]}"
    target = memory_dir / "backups" / backup_id
    target.mkdir(parents=True, exist_ok=False)
    files: dict[str, bool] = {}
    for name in (
        "items.json",
        "coremem.db",
        "structured-owner.json",
        "MEMORY.md",
        "SELF.md",
        "PENDING.md",
        "RECENT_CONTEXT.md",
    ):
        source = memory_dir / name
        files[name] = source.exists()
        if source.exists():
            if name == "coremem.db":
                source_db = sqlite3.connect(str(source))
                target_db = sqlite3.connect(str(target / name))
                try:
                    source_db.backup(target_db)
                finally:
                    target_db.close()
                    source_db.close()
            else:
                shutil.copy2(source, target / name)
    manifest = {
        "backup_id": backup_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "label": label,
        "files": files,
    }
    atomic_write_text(
        target / "manifest.json",
        json.dumps(manifest, ensure_ascii=False, indent=2),
        domain="memory_migration_backup",
    )
    return manifest


@contextmanager
def _offline_lock(memory_dir: Path) -> Iterator[None]:
    path = memory_dir / ".coremem-migration.lock"
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise RuntimeError(f"存在迁移锁，请先确认没有服务或迁移在运行: {path}") from exc
    try:
        os.write(fd, f"pid={os.getpid()} started={time.time()}\n".encode())
        os.close(fd)
        yield
    finally:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def _assert_service_offline(workspace: Path) -> None:
    pid_path = workspace / ".supervisor.pid"
    if not pid_path.exists():
        return
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return
    if pid == os.getpid():
        return
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return
    except PermissionError as exc:
        raise RuntimeError(f"无法确认 Supervisor 是否停止: pid={pid}") from exc
    raise RuntimeError(
        f"Supervisor 仍在运行(pid={pid})；Memory2 发布必须离线，请先正常停止服务"
    )


def _canonical_item(raw: dict[str, Any]) -> dict[str, Any]:
    legacy_type = str(raw.get("memory_type") or "requested_memory").strip()
    memory_type = TYPE_MAP.get(legacy_type, legacy_type)
    if memory_type not in CANONICAL_TYPES:
        memory_type = "profile"
    extra: dict[str, Any] = {}
    if legacy_type != memory_type:
        extra["legacy_memory_type"] = legacy_type
    if memory_type == "procedure":
        extra["rule_schema"] = build_procedure_rule_schema(str(raw["content"]))
    embedding = raw.get("embedding")
    if embedding is not None and not isinstance(embedding, list):
        raise ValueError(f"{raw['id']} embedding 必须是 array 或 null")
    created_at = str(raw.get("created_at") or datetime.now(timezone.utc).isoformat())
    return {
        "id": str(raw["id"]),
        "memory_type": memory_type,
        "summary": str(raw["content"]).strip(),
        "content_hash": _content_hash(str(raw["content"]), memory_type),
        "embedding": embedding,
        "reinforcement": max(1, int(raw.get("reinforcement") or 1)),
        "emotional_weight": int(raw.get("emotional_weight") or 0),
        "extra_json": extra,
        "source_ref": str(raw.get("source_ref") or "") or None,
        "happened_at": raw.get("happened_at"),
        "status": "superseded"
        if str(raw.get("status") or "active") in {"forgotten", "superseded"}
        else "active",
        "created_at": created_at,
        "updated_at": str(raw.get("updated_at") or created_at),
    }


def _strip_legacy_managed_markdown(path: Path) -> None:
    if not path.exists():
        return
    text = path.read_text(encoding="utf-8")
    text = re.sub(
        r"\n*<!-- kirakira:managed-memory:start -->.*?<!-- kirakira:managed-memory:end -->\n*",
        "\n",
        text,
        flags=re.S,
    )
    text = re.sub(r"(?m)^- \[mem_\d+\](?: source=\S+)? .*\n?", "", text)
    atomic_write_text(path, text.rstrip() + "\n", domain="memory_migration_markdown")


def migrate(workspace: Path) -> dict[str, Any]:
    memory_dir = workspace / "memory"
    memory_dir.mkdir(parents=True, exist_ok=True)
    items_path = memory_dir / "items.json"
    db_path = memory_dir / "coremem.db"
    owner_path = memory_dir / "structured-owner.json"
    _assert_service_offline(workspace)
    with _offline_lock(memory_dir):
        if owner_path.exists():
            current_owner = json.loads(owner_path.read_text(encoding="utf-8"))
            if current_owner.get("owner") == "coremem":
                raise RuntimeError("workspace 已发布为 Memory2 owner；拒绝重复迁移")
        items = _read_items(items_path)
        snapshot = backup(workspace, label="pre-m1")
        backup_id = str(snapshot["backup_id"])
        canonical = [_canonical_item(item) for item in items]
        hashes: set[tuple[str, str]] = set()
        for item in canonical:
            key = (str(item["content_hash"]), str(item["memory_type"]))
            if key in hashes:
                raise ValueError(
                    "迁移后出现重复 content_hash/type；为保证逐条保留已停止发布"
                )
            hashes.add(key)
        dims = {
            len(item["embedding"])
            for item in canonical
            if isinstance(item.get("embedding"), list) and item["embedding"]
        }
        if len(dims) > 1:
            raise ValueError(f"历史 embedding 维度不一致: {sorted(dims)}")
        vec_dim = next(iter(dims), 1024)
        staging = memory_dir / f".coremem.db.staging-{uuid4().hex}"
        store = MemoryStore2(staging, vec_dim=vec_dim)
        try:
            with store._lock:
                store._db.execute("BEGIN IMMEDIATE")
                for item in canonical:
                    store._db.execute(
                        """INSERT INTO memory_items
                           (id, memory_type, summary, content_hash, embedding,
                            reinforcement, emotional_weight, extra_json, source_ref,
                            happened_at, status, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            item["id"],
                            item["memory_type"],
                            item["summary"],
                            item["content_hash"],
                            json.dumps(item["embedding"])
                            if item["embedding"] is not None
                            else None,
                            item["reinforcement"],
                            item["emotional_weight"],
                            json.dumps(item["extra_json"], ensure_ascii=False),
                            item["source_ref"],
                            item["happened_at"],
                            item["status"],
                            item["created_at"],
                            item["updated_at"],
                        ),
                    )
                store._db.execute("COMMIT")
                if dims:
                    store._migrate_existing_to_vec()
                integrity = str(store._db.execute("PRAGMA integrity_check").fetchone()[0])
                count = int(store._db.execute("SELECT COUNT(*) FROM memory_items").fetchone()[0])
            if integrity != "ok" or count != len(canonical):
                raise RuntimeError(
                    f"staging 校验失败: integrity={integrity} count={count}/{len(canonical)}"
                )
        except BaseException:
            store.close()
            try:
                staging.unlink()
            except FileNotFoundError:
                pass
            raise
        store.close()
        os.replace(staging, db_path)
        legacy_archive = ""
        if items_path.exists():
            archive = memory_dir / f"items.legacy.{backup_id}.json"
            os.replace(items_path, archive)
            archive.chmod(0o444)
            legacy_archive = archive.name
        _strip_legacy_managed_markdown(memory_dir / "MEMORY.md")
        owner = {
            "owner": "coremem",
            "reference_pin": REFERENCE_PIN,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "migration_backup": backup_id,
            "legacy_archive": legacy_archive,
            "items": len(canonical),
            "embedding_dim": vec_dim,
        }
        atomic_write_text(
            owner_path,
            json.dumps(owner, ensure_ascii=False, indent=2),
            domain="memory_owner_publish",
        )
    result = verify(workspace, backup_id=backup_id)
    if not result["ok"]:
        raise RuntimeError(f"迁移发布后校验失败，请 rollback {backup_id}: {result}")
    return {"backup_id": backup_id, "owner": owner, "verify": result}


def verify(workspace: Path, *, backup_id: str = "") -> dict[str, Any]:
    memory_dir = workspace / "memory"
    owner_path = memory_dir / "structured-owner.json"
    owner = (
        json.loads(owner_path.read_text(encoding="utf-8")) if owner_path.exists() else {}
    )
    selected_backup = backup_id or (
        "" if owner.get("cleared_at") else str(owner.get("migration_backup") or "")
    )
    expected: list[dict[str, Any]] = []
    if selected_backup:
        expected = _read_items(memory_dir / "backups" / selected_backup / "items.json")
    db_report = _sqlite_report(memory_dir / "coremem.db")
    mismatches: list[str] = []
    if expected:
        conn = sqlite3.connect(f"file:{(memory_dir / 'coremem.db').resolve()}?mode=ro", uri=True)
        try:
            rows = {
                str(row[0]): row
                for row in conn.execute(
                    "SELECT id, summary, memory_type, source_ref, status, reinforcement, created_at, updated_at, extra_json FROM memory_items"
                )
            }
            for raw in expected:
                item = _canonical_item(raw)
                row = rows.get(str(item["id"]))
                if row is None:
                    mismatches.append(f"missing:{item['id']}")
                    continue
                actual = (row[1], row[2], row[3], row[4], int(row[5]), row[6], row[7])
                wanted = (
                    item["summary"], item["memory_type"], item["source_ref"],
                    item["status"], item["reinforcement"], item["created_at"], item["updated_at"],
                )
                if actual != wanted:
                    mismatches.append(f"fields:{item['id']}")
                extra = json.loads(row[8] or "{}")
                if extra != item["extra_json"]:
                    mismatches.append(f"extra:{item['id']}")
        finally:
            conn.close()
    ok = bool(
        owner.get("owner") == "coremem"
        and db_report.get("integrity") == "ok"
        and int(db_report.get("items") or 0) >= len(expected)
        and not mismatches
    )
    return {
        "ok": ok,
        "owner": owner.get("owner"),
        "backup_id": selected_backup,
        "expected_items": len(expected),
        "database": db_report,
        "migration_snapshot_exact": db_report.get("items") == len(expected),
        "mismatches": mismatches,
    }


def repair_kinds(workspace: Path, *, dry_run: bool = False) -> dict[str, Any]:
    """把非规范 memory_type 归一,让它们重新可注入。

    背景:注入选择器只接受 `procedure/preference/event/profile`(retriever 的
    `else: continue`),旧工具 schema 写入的 `identity/fact/requested_memory`
    即使被检索命中也永远进不了上下文——**检索到了却用不上,且完全静默**。
    写入边界已归一(见 `tools/builtins._canonical_memory_kind`),本命令修存量。

    只改 `memory_type` 一列,不动 summary、向量与状态:同一条记忆换个标签,
    语义不变。改前自动备份;`dry_run` 只报告不落库。
    """
    db_path = workspace / "memory" / "coremem.db"
    if not db_path.exists():
        return {"ok": False, "error": "coremem.db 不存在", "path": str(db_path)}

    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        rows = [
            dict(row)
            for row in conn.execute(
                "SELECT id, memory_type, status, substr(summary, 1, 80) AS summary "
                "FROM memory_items WHERE memory_type NOT IN (?, ?, ?, ?)",
                tuple(sorted(CANONICAL_TYPES)),
            )
        ]

    planned = [
        {
            **row,
            "target_type": TYPE_MAP.get(row["memory_type"], "profile"),
        }
        for row in rows
    ]
    if not planned:
        return {"ok": True, "repaired": 0, "items": [], "note": "没有需要修复的条目"}
    if dry_run:
        return {"ok": True, "dry_run": True, "repaired": 0, "items": planned}

    # 改数据前先备份:这条命令改的是用户长期记忆,出错要能回去
    backup_info = backup(workspace, label="repair-kinds")
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.executemany(
            "UPDATE memory_items SET memory_type = ? WHERE id = ?",
            [(item["target_type"], item["id"]) for item in planned],
        )
        conn.commit()
    return {
        "ok": True,
        "repaired": len(planned),
        "items": planned,
        "backup_id": backup_info.get("backup_id"),
    }


def clear(
    workspace: Path,
    *,
    confirm: str,
    include_sessions: bool = False,
    clear_self: bool = False,
) -> dict[str, Any]:
    """Forget runtime memory, optionally including sessions and SELF.md."""
    if confirm != "CLEAR_ALL_MEMORY":
        raise ValueError("clear 必须提供 --confirm CLEAR_ALL_MEMORY")
    memory_dir = workspace / "memory"
    owner_path = memory_dir / "structured-owner.json"
    _assert_service_offline(workspace)
    with _offline_lock(memory_dir):
        snapshot = backup(workspace, label="pre-clear")
        backup_dir = memory_dir / "backups" / str(snapshot["backup_id"])
        sessions_dir = workspace / "sessions"
        if include_sessions and sessions_dir.exists():
            shutil.copytree(sessions_dir, backup_dir / "sessions")
        db_path = memory_dir / "coremem.db"
        deleted = 0
        if db_path.exists():
            owner = (
                json.loads(owner_path.read_text(encoding="utf-8"))
                if owner_path.exists()
                else {"owner": "coremem"}
            )
            vec_dim = int(owner.get("embedding_dim") or 1024)
            store = MemoryStore2(db_path, vec_dim=vec_dim)
            try:
                with store._lock:
                    ids = [
                        str(row[0])
                        for row in store._db.execute("SELECT id FROM memory_items").fetchall()
                    ]
                deleted = store.delete_items_batch(ids)
                with store._lock:
                    store._db.execute("BEGIN IMMEDIATE")
                    store._db.execute("DELETE FROM memory_replacements")
                    store._db.execute("DELETE FROM consolidation_events")
                    store._db.execute("COMMIT")
                    integrity = str(
                        store._db.execute("PRAGMA integrity_check").fetchone()[0]
                    )
                if integrity != "ok":
                    raise RuntimeError(f"清理后 SQLite integrity_check={integrity}")
            finally:
                store.close()
        headers = {
            "MEMORY.md": "# Long-Term Memory\n",
            "PENDING.md": "# Pending Memory\n",
            "RECENT_CONTEXT.md": "# Recent Context\n",
            "HISTORY.md": "# History\n",
        }
        if clear_self:
            headers["SELF.md"] = "# Self Model\n"
        for name, content in headers.items():
            atomic_write_text(
                memory_dir / name,
                content,
                domain="memory_clear",
            )
        consolidation_db = memory_dir / "consolidation_writes.db"
        if consolidation_db.exists():
            conn = sqlite3.connect(str(consolidation_db))
            try:
                tables = {
                    str(row[0])
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                if "consolidation_writes" in tables:
                    conn.execute("DELETE FROM consolidation_writes")
                    conn.commit()
            finally:
                conn.close()
        current_owner = (
            json.loads(owner_path.read_text(encoding="utf-8"))
            if owner_path.exists()
            else {"owner": "coremem", "reference_pin": REFERENCE_PIN}
        )
        current_owner.update(
            {
                "owner": "coremem",
                "items": 0,
                "cleared_at": datetime.now(timezone.utc).isoformat(),
                "clear_backup": snapshot["backup_id"],
            }
        )
        atomic_write_text(
            owner_path,
            json.dumps(current_owner, ensure_ascii=False, indent=2),
            domain="memory_owner_clear",
        )
        deleted_session_files = 0
        if include_sessions:
            if sessions_dir.exists():
                deleted_session_files = sum(
                    1 for path in sessions_dir.rglob("*") if path.is_file()
                )
                shutil.rmtree(sessions_dir)
            sessions_dir.mkdir(parents=True, exist_ok=True)
    return {
        "ok": True,
        "deleted_structured_items": deleted,
        "cleared_markdown": list(headers),
        "deleted_session_files": deleted_session_files,
        "preserved": [
            name
            for name, should_preserve in (
                ("SELF.md", not clear_self),
                ("sessions/", not include_sessions),
            )
            if should_preserve
        ],
        "backup_id": snapshot["backup_id"],
    }


def rollback(workspace: Path, *, backup_id: str) -> dict[str, Any]:
    if not backup_id.strip():
        raise ValueError("rollback 必须提供 --backup-id")
    memory_dir = workspace / "memory"
    source = memory_dir / "backups" / backup_id
    manifest_path = source / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"找不到备份: {backup_id}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    _assert_service_offline(workspace)
    with _offline_lock(memory_dir):
        safety = backup(workspace, label="pre-rollback")
        for name in ("items.json", "coremem.db", "MEMORY.md", "SELF.md", "PENDING.md", "RECENT_CONTEXT.md"):
            target = memory_dir / name
            backed = bool((manifest.get("files") or {}).get(name))
            if backed:
                shutil.copy2(source / name, target)
            elif name in {"items.json", "coremem.db"} and target.exists():
                target.unlink()
        atomic_write_text(
            memory_dir / "structured-owner.json",
            json.dumps(
                {
                    "owner": "legacy",
                    "rolled_back_at": datetime.now(timezone.utc).isoformat(),
                    "restored_backup": backup_id,
                    "safety_backup": safety["backup_id"],
                },
                ensure_ascii=False,
                indent=2,
            ),
            domain="memory_owner_rollback",
        )
    return {
        "ok": True,
        "owner": "legacy",
        "restored_backup": backup_id,
        "safety_backup": safety["backup_id"],
    }
