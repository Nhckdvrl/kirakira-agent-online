"""Small stdio MCP server for curated RSS/Atom and webpage subscriptions.

The server owns refresh caching and exact event ACK state under
KIRAKIRA_PLUGIN_DATA_DIR.  The proactive runtime only sees a stable snapshot.
"""

from __future__ import annotations

import hashlib
import html
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Any
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import tomllib
except ImportError:  # pragma: no cover - runtime requires Python 3.11+
    tomllib = None


DATA_DIR = Path(os.environ.get("KIRAKIRA_PLUGIN_DATA_DIR") or ".").resolve()
LOCAL_CONFIG = DATA_DIR / "config.local.toml"
PACKAGED_CONFIG = Path(__file__).with_name("config.toml")
SNAPSHOT_FILE = DATA_DIR / "snapshot.json"
ACK_FILE = DATA_DIR / "acked.json"


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self.skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"script", "style", "svg", "noscript"}:
            self.skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style", "svg", "noscript"} and self.skip_depth:
            self.skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self.skip_depth:
            self.parts.append(data)


def _read_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    if tomllib is None:
        raise RuntimeError("curated-feeds requires Python 3.11+")
    with path.open("rb") as handle:
        value = tomllib.load(handle)
    if not isinstance(value, dict):
        raise RuntimeError("plugin config must be a TOML table")
    return value


def _config() -> dict[str, Any]:
    merged = _read_toml(PACKAGED_CONFIG)
    merged.update(_read_toml(LOCAL_CONFIG))
    return merged


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(".%s.%d.tmp" % (path.name, os.getpid()))
    temp.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    os.replace(temp, path)


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _child_text(element: ET.Element, name: str) -> str:
    for child in element:
        if _local_name(child.tag) == name:
            return "".join(child.itertext()).strip()
    return ""


def _clean_markup(value: str, limit: int = 1400) -> str:
    parser = _TextExtractor()
    parser.feed(html.unescape(value or ""))
    text = " ".join(parser.parts) if parser.parts else html.unescape(value or "")
    return re.sub(r"\s+", " ", text).strip()[:limit]


def _request(url: str, timeout: float) -> tuple[bytes, str]:
    if not url.startswith("https://"):
        raise RuntimeError("subscription URL must use https: %s" % url)
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "KirakiraAgent/0.1 (+local proactive feed reader)",
            "Accept": "application/json, application/atom+xml, application/rss+xml, application/xml, text/html;q=0.8",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read(4 * 1024 * 1024), str(response.headers.get_content_charset() or "utf-8")


def _request_feed(feed: dict[str, Any], timeout: float) -> tuple[bytes, str]:
    raw_urls = feed.get("urls")
    urls = raw_urls if isinstance(raw_urls, list) else [feed.get("url")]
    candidates = [str(url).strip() for url in urls if str(url or "").strip()]
    if not candidates:
        raise RuntimeError("subscription requires url or urls")
    failures: list[str] = []
    for url in candidates:
        try:
            return _request(url, timeout)
        except Exception as exc:
            failures.append("%s: %s" % (url, exc))
    raise RuntimeError("all feed URLs failed: %s" % "; ".join(failures))


def _event_id(feed_id: str, stable_value: str) -> str:
    digest = hashlib.sha256((feed_id + "\0" + stable_value).encode("utf-8")).hexdigest()
    return "%s:%s" % (feed_id, digest[:24])


def _parse_feed(feed: dict[str, Any], payload: bytes) -> list[dict[str, Any]]:
    root = ET.fromstring(payload)
    feed_id = str(feed["id"])
    source_name = str(feed.get("name") or feed_id)
    topic = str(feed.get("topic") or "")
    limit = max(1, min(20, int(feed.get("max_items", 5))))
    entries = [item for item in root.iter() if _local_name(item.tag) in {"item", "entry"}]
    events: list[dict[str, Any]] = []
    for entry in entries[:limit]:
        title = _clean_markup(_child_text(entry, "title"), 300) or "(untitled)"
        summary = (
            _child_text(entry, "description")
            or _child_text(entry, "summary")
            or _child_text(entry, "content")
        )
        link = _child_text(entry, "link")
        if not link:
            for child in entry:
                if _local_name(child.tag) == "link" and child.attrib.get("href"):
                    link = str(child.attrib["href"])
                    if child.attrib.get("rel") in (None, "alternate"):
                        break
        stable = _child_text(entry, "guid") or _child_text(entry, "id") or link or title
        published = (
            _child_text(entry, "published")
            or _child_text(entry, "updated")
            or _child_text(entry, "pubDate")
        )
        content = _clean_markup(summary)
        if topic:
            content = "[%s] %s" % (topic, content)
        events.append(
            {
                "event_id": _event_id(feed_id, stable),
                "kind": "content",
                "source_type": "feed",
                "source_name": source_name,
                "title": title,
                "content": content or title,
                "url": link,
                "published_at": published,
            }
        )
    return events


def _parse_webpage(
    feed: dict[str, Any], payload: bytes, charset: str, checked_at: str
) -> list[dict[str, Any]]:
    text = payload.decode(charset, errors="replace")
    title_match = re.search(r"<title[^>]*>(.*?)</title>", text, flags=re.I | re.S)
    title = _clean_markup(title_match.group(1), 300) if title_match else str(feed.get("name"))
    content = _clean_markup(text, 2200)
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    feed_id = str(feed["id"])
    topic = str(feed.get("topic") or "")
    if topic:
        content = "[%s] %s" % (topic, content)
    return [
        {
            "event_id": _event_id(feed_id, digest),
            "kind": "content",
            "source_type": "webpage",
            "source_name": str(feed.get("name") or feed_id),
            "title": title or str(feed.get("name") or feed_id),
            "content": content,
            "url": str(feed["url"]),
            "published_at": checked_at,
        }
    ]


def _parse_wordpress(feed: dict[str, Any], payload: bytes, charset: str) -> list[dict[str, Any]]:
    raw_items = json.loads(payload.decode(charset, errors="replace"))
    if not isinstance(raw_items, list):
        raise RuntimeError("WordPress endpoint must return an array")
    feed_id = str(feed["id"])
    source_name = str(feed.get("name") or feed_id)
    topic = str(feed.get("topic") or "")
    limit = max(1, min(20, int(feed.get("max_items", 5))))
    events: list[dict[str, Any]] = []
    for item in raw_items[:limit]:
        if not isinstance(item, dict):
            continue
        raw_title = item.get("title") or ""
        raw_excerpt = item.get("excerpt") or ""
        title = _clean_markup(
            str(raw_title.get("rendered") if isinstance(raw_title, dict) else raw_title),
            300,
        )
        content = _clean_markup(
            str(raw_excerpt.get("rendered") if isinstance(raw_excerpt, dict) else raw_excerpt)
        )
        if topic:
            content = "[%s] %s" % (topic, content)
        stable = str(item.get("id") or item.get("link") or title)
        events.append(
            {
                "event_id": _event_id(feed_id, stable),
                "kind": "content",
                "source_type": "wordpress",
                "source_name": source_name,
                "title": title or "(untitled)",
                "content": content or title,
                "url": str(item.get("link") or ""),
                "published_at": str(item.get("date_gmt") or item.get("date") or ""),
            }
        )
    return events


def _parse_yahoo_market(feed: dict[str, Any], payload: bytes) -> list[dict[str, Any]]:
    document = json.loads(payload.decode("utf-8", errors="replace"))
    results = ((document.get("chart") or {}).get("result") or []) if isinstance(document, dict) else []
    if not results or not isinstance(results[0], dict):
        raise RuntimeError("Yahoo chart response has no result")
    meta = results[0].get("meta") or {}
    price = meta.get("regularMarketPrice")
    previous = meta.get("chartPreviousClose") or meta.get("previousClose")
    market_time = int(meta.get("regularMarketTime") or 0)
    if not isinstance(price, (int, float)) or not isinstance(previous, (int, float)) or not previous:
        raise RuntimeError("Yahoo chart response has no usable price/previous close")
    change = (float(price) - float(previous)) / float(previous) * 100.0
    minimum = max(0.0, float(feed.get("min_abs_change_percent", 0.8)))
    if abs(change) < minimum:
        return []
    step = max(0.1, float(feed.get("alert_step_percent", 1.0)))
    band = max(1, int(abs(change) // step))
    happened = datetime.fromtimestamp(market_time, tz=timezone.utc) if market_time else datetime.now(timezone.utc)
    date_key = happened.strftime("%Y-%m-%d")
    direction = "up" if change >= 0 else "down"
    feed_id = str(feed["id"])
    name = str(feed.get("name") or meta.get("shortName") or meta.get("symbol") or feed_id)
    symbol = str(feed.get("symbol") or meta.get("symbol") or "")
    precision = max(0, min(4, int(feed.get("precision", 2))))
    signed = "%+.2f%%" % change
    return [
        {
            "event_id": _event_id(feed_id, "%s:%s:%d" % (date_key, direction, band)),
            "kind": "content",
            "source_type": "market",
            "source_name": name,
            "title": "%s %s" % (name, signed),
            "content": "%s（%s）现报 %.*f，前收 %.*f，日内变动 %s。" % (
                name,
                symbol,
                precision,
                float(price),
                precision,
                float(previous),
                signed,
            ),
            "url": str(feed.get("page_url") or ""),
            "published_at": happened.isoformat(),
            "symbol": symbol,
            "price": float(price),
            "previous_close": float(previous),
            "change_percent": round(change, 4),
        }
    ]


def _refresh(feed: dict[str, Any], timeout: float, checked_at: str) -> list[dict[str, Any]]:
    payload, charset = _request_feed(feed, timeout)
    kind = str(feed.get("kind") or "rss").strip().lower()
    if kind in {"rss", "atom", "feed"}:
        return _parse_feed(feed, payload)
    if kind == "webpage":
        return _parse_webpage(feed, payload, charset, checked_at)
    if kind == "wordpress":
        return _parse_wordpress(feed, payload, charset)
    if kind == "yahoo_market":
        return _parse_yahoo_market(feed, payload)
    raise RuntimeError("unsupported subscription kind: %s" % kind)


def get_proactive_events() -> list[dict[str, Any]]:
    config = _config()
    proactive = dict(config.get("proactive") or {})
    if not proactive.get("enabled", False):
        return []
    feeds = config.get("feeds") or []
    if not isinstance(feeds, list) or not feeds:
        raise RuntimeError("curated-feeds is enabled but no feeds are configured")
    timeout = max(1.0, min(60.0, float(proactive.get("request_timeout_seconds", 15))))
    refresh_after = max(60, int(proactive.get("refresh_interval_seconds", 1800)))
    now = time.time()
    checked_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))
    snapshot = _read_json(SNAPSHOT_FILE, {"feeds": {}})
    cached = snapshot.get("feeds") if isinstance(snapshot, dict) else {}
    if not isinstance(cached, dict):
        cached = {}
    configured_ids = {
        str(item.get("id") or "").strip()
        for item in feeds
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    }
    # 配置中撤掉的订阅不得继续潜伏在运行时快照里。
    cached = {key: value for key, value in cached.items() if key in configured_ids}
    errors: list[str] = []
    successful = 0
    refresh_jobs: dict[Any, tuple[str, dict[str, Any]]] = {}
    due: list[tuple[str, dict[str, Any]]] = []
    for raw in feeds:
        if not isinstance(raw, dict):
            errors.append("feed entry is not a table")
            continue
        feed_id = str(raw.get("id") or "").strip()
        if not feed_id or not (raw.get("url") or raw.get("urls")):
            errors.append("feed requires id and url/urls")
            continue
        prior = cached.get(feed_id) if isinstance(cached.get(feed_id), dict) else {}
        last_checked = float(prior.get("checked_epoch") or 0)
        if now - last_checked < refresh_after and isinstance(prior.get("items"), list):
            successful += 1
            continue
        due.append((feed_id, raw))
    # 一个插件 source 可以包含多个独立订阅。这些都是 I/O，不应让
    # 某个慢站点串行占满整个 MCP tool 超时窗口。并发有硬上限，
    # 且每个 feed 仍保留自己的 last-good snapshot 与错误语义。
    if due:
        with ThreadPoolExecutor(max_workers=min(8, len(due))) as executor:
            for feed_id, raw in due:
                refresh_jobs[executor.submit(_refresh, raw, timeout, checked_at)] = (
                    feed_id,
                    raw,
                )
            for future in as_completed(refresh_jobs):
                feed_id, _raw = refresh_jobs[future]
                try:
                    items = future.result()
                except Exception as exc:  # one broken feed keeps its last good snapshot
                    errors.append("%s: %s" % (feed_id, exc))
                    continue
                cached[feed_id] = {
                    "checked_epoch": now,
                    "checked_at": checked_at,
                    "items": items,
                }
                successful += 1
    if successful == 0 and errors:
        raise RuntimeError("all subscriptions failed: %s" % "; ".join(errors))
    _write_json(SNAPSHOT_FILE, {"feeds": cached, "errors": errors})
    acked = set(str(item) for item in _read_json(ACK_FILE, []))
    events: list[dict[str, Any]] = []
    for raw in feeds:
        if not isinstance(raw, dict):
            continue
        state = cached.get(str(raw.get("id") or ""), {})
        for item in state.get("items", []) if isinstance(state, dict) else []:
            if isinstance(item, dict) and str(item.get("event_id")) not in acked:
                events.append(item)
    events.sort(key=lambda item: (str(item.get("published_at") or ""), str(item.get("event_id"))), reverse=True)
    max_events = max(1, min(100, int(proactive.get("max_events", 30))))
    return events[:max_events]


def ack_proactive_events(event_ids: Any) -> dict[str, Any]:
    if not isinstance(event_ids, list):
        raise RuntimeError("event_ids must be an array")
    clean = [str(item).strip() for item in event_ids if str(item).strip()]
    acked = list(dict.fromkeys([*map(str, _read_json(ACK_FILE, [])), *clean]))[-5000:]
    _write_json(ACK_FILE, acked)
    return {"affected": len(clean), "event_ids": clean}


TOOLS = [
    {
        "name": "get_proactive_events",
        "description": "Return the current unacknowledged stable subscription snapshot.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    },
    {
        "name": "ack_proactive_events",
        "description": "Acknowledge exactly the event IDs that were delivered.",
        "inputSchema": {
            "type": "object",
            "properties": {"event_ids": {"type": "array", "items": {"type": "string"}}},
            "required": ["event_ids"],
            "additionalProperties": False,
        },
    },
]


def _send(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def _tool_result(request_id: Any, value: Any, *, error: bool = False) -> None:
    _send(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "content": [{"type": "text", "text": json.dumps(value, ensure_ascii=False)}],
                "isError": error,
            },
        }
    )


def main() -> None:
    for line in sys.stdin:
        try:
            request = json.loads(line)
        except ValueError:
            continue
        if "id" not in request:
            continue
        request_id = request["id"]
        method = request.get("method")
        if method == "initialize":
            _send(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "curated-feeds", "version": "0.1.0"},
                    },
                }
            )
        elif method == "tools/list":
            _send({"jsonrpc": "2.0", "id": request_id, "result": {"tools": TOOLS}})
        elif method == "tools/call":
            params = request.get("params") or {}
            arguments = params.get("arguments") or {}
            try:
                if params.get("name") == "get_proactive_events":
                    value = get_proactive_events()
                elif params.get("name") == "ack_proactive_events":
                    value = ack_proactive_events(arguments.get("event_ids"))
                else:
                    raise RuntimeError("unknown tool: %s" % params.get("name"))
            except Exception as exc:
                _tool_result(request_id, {"error": str(exc)}, error=True)
            else:
                _tool_result(request_id, value)
        else:
            _send(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {"code": -32601, "message": "method not found"},
                }
            )


if __name__ == "__main__":
    main()
