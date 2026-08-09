"""Drift skill 的发现与解析。

一个 drift skill 是一个目录 ``<workspace>/drift/skills/<name>/``，核心文件是
``SKILL.md``：带 YAML frontmatter（name / description）+ 正文（分步操作指南）。
正文会作为 drift agent run 的 system prompt。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DriftSkill:
    name: str
    description: str
    body: str
    path: Path


def _parse_frontmatter(text: str) -> tuple[dict, str]:
    """拆分 ``---`` frontmatter 与正文。无 frontmatter 时返回空 meta + 全文。"""
    if not text.startswith("---"):
        return {}, text
    parts = text.split("---", 2)
    if len(parts) < 3:
        return {}, text
    meta: dict = {}
    if yaml is not None:
        try:
            loaded = yaml.safe_load(parts[1]) or {}
            if isinstance(loaded, dict):
                meta = loaded
        except yaml.YAMLError:
            logger.warning("[drift] SKILL.md frontmatter 解析失败")
    return meta, parts[2].strip()


def discover_skills(
    workspace: Path,
    *,
    extra_roots: Iterable[Path] = (),
) -> List[DriftSkill]:
    """扫描 workspace 兼容目录与插件声明的 Drift skill roots。

    Reference 的正式 owner 是插件包；``<workspace>/drift/skills`` 仅保留
    给旧工作区。同名 skill 不允许静默覆盖，否则运行时会在不可见的
    条件下执行错的任务。
    """
    roots = [Path(workspace) / "drift" / "skills"]
    roots.extend(Path(root) for root in extra_roots)
    skills: List[DriftSkill] = []
    seen: dict[str, Path] = {}
    for skills_dir in roots:
        if not skills_dir.exists():
            continue
        for child in sorted(skills_dir.iterdir()):
            if not child.is_dir():
                continue
            skill_file = child / "SKILL.md"
            if not skill_file.exists():
                continue
            text = skill_file.read_text(encoding="utf-8")
            meta, body = _parse_frontmatter(text)
            name = str(meta.get("name") or child.name).strip()
            description = str(meta.get("description") or "").strip()
            previous = seen.get(name)
            if previous is not None:
                raise ValueError(
                    "duplicate Drift skill %r: %s and %s" % (name, previous, child)
                )
            seen[name] = child
            skills.append(
                DriftSkill(name=name, description=description, body=body, path=child)
            )
    return skills


def ensure_example_skill(workspace: Path) -> None:
    """首次运行时放两个可执行的示例 skill，方便直接演示 Drift 链路。

    - explore-curiosity：会推送（像朋友随口一问），演示 message_push 路径。
    - review-memory：纯后台（不推送），演示静默收尾与跨轮连续性。
    """
    base = workspace / "drift" / "skills"
    for name, body in (
        ("explore-curiosity", _EXAMPLE_SKILL),
        ("review-memory", _REVIEW_MEMORY_SKILL),
    ):
        skill_file = base / name / "SKILL.md"
        if skill_file.exists():
            continue
        skill_file.parent.mkdir(parents=True, exist_ok=True)
        skill_file.write_text(body, encoding="utf-8")


_EXAMPLE_SKILL = """---
name: explore-curiosity
description: 空闲时像朋友一样，随口问用户一个轻量、自然的生活化问题
---

## 目标
补足用户画像里的生活化空白，一次只问一个轻量、自然的问题。

## 工作流程
1. 读 Drift Briefing 里的长期记忆、近期上下文和本 skill 前情，避免短期重复。
2. 基于这些信息，现场想一个轻量、自然、像朋友随口一问的问题。
3. 如果此刻适合聊天，用 `message_push` 发送这个问题（最多一次）。
4. 如果不适合打扰，就不发，直接 `finish_drift(status="completed")` 静默收尾。

## 要求
- 问题要轻量、自然：优先问音乐偏好、开源项目、运动习惯、食物口味、日常消遣。
- 避开长期记忆里已经明确有答案的信息，不要重复最近问过的。
- 不要问太大、太虚、太像采访的问题。
- 结束前必须调用 `finish_drift`，填写 status 与 briefing。
"""


_REVIEW_MEMORY_SKILL = """---
name: review-memory
description: 空闲时抽查一条长期记忆是否仍然准确，纯后台记录，不打扰用户
---

## 目标
利用空闲时间做一次轻量的长期记忆自检：抽一条记忆，判断它是否仍然可信、是否过时或自相矛盾。
这是纯后台任务，不推送消息给用户。

## 工作流程
1. `recall_memory` 或 `read_file` 读取长期记忆里的若干条目。
2. 从 Drift Briefing 的本 skill 前情里看上次查到哪，避免重复抽同一条。
3. 挑一条，判断它是否仍然准确：有没有过时、有没有和其他记忆冲突。
4. 把判断结果写进本轮 `finish_drift` 的 briefing（例如"抽查了 X，看起来仍然可信"）。
5. 如果一轮没查完，用 `status="paused"` 在 `scratchpad_update` 写清下次从哪条继续。

## 要求
- 不调用 `message_push`：这是纯后台 skill，不打扰用户。
- 结束前必须调用 `finish_drift`，填写 status 与 briefing；`next_tendency` 可写下次想查的方向。
- 只读判断，不要在本轮直接删改长期记忆。
"""
