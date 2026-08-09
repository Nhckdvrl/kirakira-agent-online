"""Contract tests for the bundled curated-feeds proactive plugin."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest


SERVER = Path(__file__).parents[1] / "plugin_packages" / "curated_feeds" / "mcp_server.py"


def _load_server(data_dir: Path):
    previous = os.environ.get("KIRAKIRA_PLUGIN_DATA_DIR")
    os.environ["KIRAKIRA_PLUGIN_DATA_DIR"] = str(data_dir)
    try:
        spec = importlib.util.spec_from_file_location("curated_feeds_test_server", SERVER)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        if previous is None:
            os.environ.pop("KIRAKIRA_PLUGIN_DATA_DIR", None)
        else:
            os.environ["KIRAKIRA_PLUGIN_DATA_DIR"] = previous


class CuratedFeedsContractTests(unittest.TestCase):
    def test_rss_atom_and_wordpress_items_have_stable_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            server = _load_server(Path(tmp))
            rss = b"""<?xml version='1.0'?><rss><channel><item>
              <guid>entry-1</guid><title>Hello</title><link>https://example.com/1</link>
              <description><![CDATA[<b>Useful</b> summary]]></description>
              <pubDate>2026-08-01T00:00:00Z</pubDate>
            </item></channel></rss>"""
            feed = {"id": "demo", "name": "Demo", "topic": "AI", "max_items": 5}
            first = server._parse_feed(feed, rss)
            second = server._parse_feed(feed, rss)
            self.assertEqual(first, second)
            self.assertEqual(first[0]["kind"], "content")
            self.assertEqual(first[0]["event_id"], second[0]["event_id"])
            self.assertIn("Useful summary", first[0]["content"])

            wordpress = json.dumps(
                [
                    {
                        "id": 42,
                        "date": "2026-08-01T12:00:00",
                        "link": "https://example.com/42",
                        "title": {"rendered": "<b>Dharma</b>"},
                        "excerpt": {"rendered": "<p>Context</p>"},
                    }
                ]
            ).encode()
            item = server._parse_wordpress(feed, wordpress, "utf-8")[0]
            self.assertEqual(item["source_type"], "wordpress")
            self.assertEqual(item["title"], "Dharma")

    def test_ack_records_exact_ids_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            server = _load_server(Path(tmp))
            first = server.ack_proactive_events(["a", "b"])
            second = server.ack_proactive_events(["b"])
            self.assertEqual(first["affected"], 2)
            self.assertEqual(second["event_ids"], ["b"])
            self.assertEqual(json.loads(server.ACK_FILE.read_text()), ["a", "b"])

    def test_market_events_use_threshold_and_stable_change_bands(self):
        with tempfile.TemporaryDirectory() as tmp:
            server = _load_server(Path(tmp))
            feed = {
                "id": "nikkei-225",
                "name": "日经225",
                "symbol": "^N225",
                "min_abs_change_percent": 0.8,
                "alert_step_percent": 1.0,
            }

            def payload(price):
                return json.dumps(
                    {
                        "chart": {
                            "result": [
                                {
                                    "meta": {
                                        "regularMarketPrice": price,
                                        "chartPreviousClose": 100.0,
                                        "regularMarketTime": 1785480303,
                                    }
                                }
                            ]
                        }
                    }
                ).encode()

            self.assertEqual(server._parse_yahoo_market(feed, payload(100.5)), [])
            first = server._parse_yahoo_market(feed, payload(101.2))[0]
            same_band = server._parse_yahoo_market(feed, payload(101.8))[0]
            next_band = server._parse_yahoo_market(feed, payload(102.1))[0]
            self.assertEqual(first["event_id"], same_band["event_id"])
            self.assertNotEqual(first["event_id"], next_band["event_id"])
            self.assertEqual(first["source_type"], "market")
            self.assertAlmostEqual(first["change_percent"], 1.2)


if __name__ == "__main__":
    unittest.main()
