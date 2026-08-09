"""Plugin data-directory ownership and path-safety rules."""

from __future__ import annotations

import re
from pathlib import Path


def workspace_plugin_data_dir(workspace: Path, plugin_name: str, marketplace: str) -> Path:
    """Resolve a plugin data directory without creating or migrating it."""
    for label, value in (("name", plugin_name), ("marketplace", marketplace)):
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", value) is None:
            raise ValueError("plugin %s is not a safe path segment: %r" % (label, value))
    return workspace.resolve(strict=False) / "plugin-data" / (
        "%s-%s" % (plugin_name, marketplace)
    )


def builtin_plugin_data_dir(plugin_name: str, workspace: Path) -> Path:
    return workspace_plugin_data_dir(workspace, plugin_name, "builtin")


def validate_workspace_plugin_data_path(path: Path, workspace: Path) -> None:
    """Require plugin data to stay in the workspace without symlink traversal."""
    root = workspace.resolve(strict=False)
    try:
        relative = path.relative_to(root)
    except ValueError as error:
        raise ValueError("plugin data directory escapes workspace: %s" % path) from error
    current = root
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise ValueError("plugin data directory crosses symlink: %s" % current)


def ensure_workspace_plugin_data_dir(path: Path, workspace: Path) -> None:
    """Safely create a plugin data directory below the workspace."""
    validate_workspace_plugin_data_path(path, workspace)
    path.mkdir(parents=True, exist_ok=True)
    validate_workspace_plugin_data_path(path, workspace)
