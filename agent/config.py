"""Kirakira Agent learning harness module."""

import os
import re
from pathlib import Path
from typing import Any, Dict

try:
    import tomllib
except ImportError:  # pragma: no cover - Python 3.10 fallback
    tomllib = None


def load_dotenv(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        values[key] = value
        os.environ.setdefault(key, value)
    return values


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError("Missing required environment variable: %s" % name)
    return value


def load_toml_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    if tomllib is None:
        raise RuntimeError("config.toml requires Python 3.11+")
    with path.open("rb") as handle:
        payload = tomllib.load(handle)
    return _expand_env(payload)


def config_value(config: Dict[str, Any], *path: str, default: Any = None) -> Any:
    current: Any = config
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _expand_env(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _expand_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    if not isinstance(value, str):
        return value
    pattern = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in os.environ:
            raise RuntimeError("Missing environment variable referenced by config: %s" % name)
        return os.environ[name]

    return pattern.sub(replace, value)
