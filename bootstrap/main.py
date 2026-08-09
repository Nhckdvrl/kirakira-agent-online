"""Reference-compatible command dispatcher for ``python main.py``."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import click

from bootstrap.setup_wizard import DEFAULT_WORKSPACE, initialize_workspace, run_setup_wizard
from agent.config import config_value, load_dotenv, load_toml_config


HELP = """\
用法: uv run python main.py [命令] [选项]

命令:
  setup                         运行交互式初始化向导
  init                          非交互初始化配置和工作区
  gateway                       启动未托管 Agent 服务（调试）
  supervise                     显式进入 supervisor（兼容别名）
  memory doctor|backup|migrate|verify|rollback|clear|repair-kinds
                                Memory2 M0/M1 管理与恢复
  control <子命令>              连接已在跑的 Agent:看状态、跑一轮、中断、排空插件
                                (control -h 看完整子命令)

通用选项:
  --config PATH                 配置文件，默认 config.toml
  --workspace PATH              覆盖 config.toml 中的 runtime.workspace
  --force                       init 时覆盖已有初始化文件
  -h, --help                    显示帮助

无命令时启动完整 Agent 服务；首次无配置时自动进入 setup。
"""


def _flag_value(args: list[str], flag: str) -> str | None:
    if flag not in args:
        return None
    index = args.index(flag)
    if index + 1 >= len(args):
        raise SystemExit(f"参数 {flag} 缺少值")
    return args[index + 1]


def _validate_supervise_args(args: list[str]) -> None:
    """Match Reference: supervise only accepts fixed gateway path flags."""

    index = 0
    seen: set[str] = set()
    while index < len(args):
        flag = args[index]
        if flag not in {"--config", "--workspace"}:
            raise SystemExit(f"supervise 不支持参数: {flag}")
        if flag in seen or index + 1 >= len(args):
            raise SystemExit(f"supervise 参数无效: {flag}")
        seen.add(flag)
        index += 2


def _workspace(args: list[str], config_path: Path, *, allow_default: bool) -> Path:
    explicit = _flag_value(args, "--workspace") or os.getenv("KIRAKIRA_WORKSPACE", "")
    if explicit.strip():
        return Path(explicit).expanduser().resolve()
    if config_path.exists():
        load_dotenv(config_path.parent / ".env")
        configured = str(config_value(load_toml_config(config_path), "runtime", "workspace", default="") or "")
        if configured.strip():
            return Path(configured).expanduser().resolve()
    if allow_default:
        return Path(DEFAULT_WORKSPACE).expanduser().resolve()
    raise SystemExit(f"找不到配置文件 {config_path}，请先运行 uv run python main.py setup")


def _print_summary(summary) -> None:
    for title, paths in (("已创建：", summary.created), ("已覆盖：", summary.overwritten), ("已跳过：", summary.skipped)):
        if paths:
            click.echo(title)
            for path in paths:
                click.echo(f"  {path}")
    click.echo("\n下一步：")
    click.echo("  1. 编辑 config.toml / .env 填写凭据。")
    click.echo("  2. 运行 uv run python main.py 启动。")
    click.echo("  3. 打开 http://127.0.0.1:6322 使用 Web Chat。")


def _migrate_workspace(config_path: Path, workspace: Path) -> None:
    """Apply workspace migrations unless this is a supervised child generation."""
    if (
        os.environ.get("AKASHIC_SUPERVISED") == "1"
        or os.environ.get("KIRAKIRA_SUPERVISED") == "1"
    ):
        return
    from agent.migrations import migrate_installation

    try:
        outcome = migrate_installation(config_path, workspace)
    except RuntimeError as exc:
        raise SystemExit(f"启动迁移失败: {exc}") from exc
    if outcome.state == "migrated":
        click.echo(f"启动迁移完成: migrations={len(outcome.migrations)}")


def main(argv: list[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    command = args[0] if args and not args[0].startswith("-") else ""
    # 子命令自己的 -h 归子命令,不能被全局帮助吞掉。
    if ("-h" in args or "--help" in args) and command != "control":
        print(HELP)
        return
    if command not in {"", "setup", "init", "gateway", "supervise", "memory", "control"}:
        raise SystemExit(f"未知命令: {command}\n\n{HELP}")
    # 子命令拥有的参数必须先校验，不能让配置/工作区探测掩盖参数错误。
    if command == "supervise":
        _validate_supervise_args(args[1:])

    config_path = Path(_flag_value(args, "--config") or "config.toml").expanduser().resolve()
    bootstrap = command in {"setup", "init"} or (not command and not config_path.exists())
    workspace = _workspace(args, config_path, allow_default=bootstrap)

    if command == "setup":
        run_setup_wizard(config_path, workspace)
        if config_path.exists():
            _migrate_workspace(config_path, workspace)
        return
    if command == "init":
        _print_summary(initialize_workspace(config_path, workspace, force="--force" in args))
        _migrate_workspace(config_path, workspace)
        return
    if command == "memory":
        if len(args) < 2 or args[1] not in {"doctor", "backup", "migrate", "verify", "rollback", "clear", "repair-kinds"}:
            raise SystemExit("memory 需要 doctor/backup/migrate/verify/rollback/clear/repair-kinds")
        from bootstrap.app import main as runtime_main

        _migrate_workspace(config_path, workspace)

        forwarded = ["memory", args[1], "--config", str(config_path), "--workspace", str(workspace)]
        backup_id = _flag_value(args, "--backup-id")
        if backup_id:
            forwarded.extend(["--backup-id", backup_id])
        confirm = _flag_value(args, "--confirm")
        if confirm:
            forwarded.extend(["--confirm", confirm])
        if "--include-sessions" in args:
            forwarded.append("--include-sessions")
        if "--clear-self" in args:
            forwarded.append("--clear-self")
        if "--dry-run" in args:
            forwarded.append("--dry-run")
        runtime_main(forwarded)
        return
    if command == "control":
        # 只连已在跑的 agent,不加载 runtime,也不需要 config 存在。
        from bootstrap.control_cli import main as control_main

        raise SystemExit(control_main(args[1:], workspace))
    if not config_path.exists():
        if not sys.stdin.isatty():
            raise SystemExit(f"找不到配置文件 {config_path}；请先运行 uv run python main.py setup")
        run_setup_wizard(config_path, workspace)
        if not config_path.exists():
            return

    _migrate_workspace(config_path, workspace)

    if command != "gateway":
        from agent.supervisor import run_supervisor

        raise SystemExit(run_supervisor(config_path=config_path, workspace=workspace))

    from bootstrap.app import main as runtime_main

    runtime_main(["--serve", "--config", str(config_path), "--workspace", str(workspace)])
