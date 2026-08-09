"""Multi-lane recall, RRF fusion, hotness decay and injection budget tests."""

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from core.memory.legacy import MemoryRuntime
from agent.retrieval.default_pipeline import (
    RRF_K,
    RetrievalRequest,
    hotness_boost,
    plan_injection,
    rrf_fuse,
)
from session.manager import SessionManager


class _Rec:
    def __init__(self, rid, content="c", memory_type="fact", reinforcement=0, source_ref=""):
        self.id = rid
        self.content = content
        self.memory_type = memory_type
        self.reinforcement = reinforcement
        self.source_ref = source_ref


class RrfFuseTests(unittest.TestCase):
    def test_fuses_by_rank_not_raw_score(self):
        # 两条 lane 的原始分数尺度完全不同，RRF 只看名次，所以不受影响。
        vector = [_Rec("a"), _Rec("b")]
        lexical = [_Rec("b"), _Rec("c")]
        fused = rrf_fuse([("vector", 1.0, vector), ("lexical", 0.5, lexical)])
        ids = [rid for rid, _ in fused]

        # b 同时出现在两条 lane，应当排第一。
        self.assertEqual(ids[0], "b")
        self.assertEqual(set(ids), {"a", "b", "c"})

    def test_score_matches_reciprocal_rank_formula(self):
        fused = dict(rrf_fuse([("vector", 1.0, [_Rec("a"), _Rec("b")])]))
        self.assertAlmostEqual(fused["a"], 1.0 / (RRF_K + 1))
        self.assertAlmostEqual(fused["b"], 1.0 / (RRF_K + 2))

    def test_weights_apply_per_lane(self):
        fused = dict(
            rrf_fuse([("vector", 1.0, [_Rec("a")]), ("lexical", 0.5, [_Rec("b")])])
        )
        self.assertAlmostEqual(fused["a"], 1.0 / (RRF_K + 1))
        self.assertAlmostEqual(fused["b"], 0.5 / (RRF_K + 1))

    def test_duplicate_ids_in_a_lane_keep_first_rank(self):
        fused = dict(rrf_fuse([("vector", 1.0, [_Rec("a"), _Rec("a")])]))
        self.assertAlmostEqual(fused["a"], 1.0 / (RRF_K + 1))

    def test_empty_lanes_are_safe(self):
        self.assertEqual(rrf_fuse([("vector", 1.0, []), ("lexical", 0.5, [])]), [])

    def test_ties_break_deterministically(self):
        first = rrf_fuse([("vector", 1.0, [_Rec("b")]), ("lexical", 1.0, [_Rec("a")])])
        second = rrf_fuse([("vector", 1.0, [_Rec("b")]), ("lexical", 1.0, [_Rec("a")])])
        self.assertEqual(first, second)
        # 同分时按 id 稳定排序，不受 set 遍历顺序影响。
        self.assertEqual([rid for rid, _ in first], ["a", "b"])


class HotnessTests(unittest.TestCase):
    def test_no_reinforcement_means_no_boost(self):
        now = datetime.now().astimezone()
        self.assertEqual(hotness_boost(0, now, now), 1.0)

    def test_fresh_reinforcement_boosts(self):
        now = datetime.now().astimezone()
        self.assertGreater(hotness_boost(4, now, now), 1.0)

    def test_boost_decays_with_age(self):
        now = datetime.now().astimezone()
        fresh = hotness_boost(4, now, now)
        old = hotness_boost(4, now - timedelta(days=14), now)
        older = hotness_boost(4, now - timedelta(days=140), now)

        # 半衰期 14 天：14 天前的加成应该约为新鲜时的一半。
        self.assertLess(old, fresh)
        self.assertAlmostEqual(old - 1.0, (fresh - 1.0) / 2, places=6)
        self.assertLess(older, old)
        self.assertAlmostEqual(older, 1.0, places=3)

    def test_missing_timestamp_is_neutral(self):
        now = datetime.now().astimezone()
        self.assertEqual(hotness_boost(5, None, now), 1.0)


class InjectionBudgetTests(unittest.TestCase):
    def test_empty_records_produce_empty_block(self):
        block, injected, truncated = plan_injection([])
        self.assertEqual((block, injected, truncated), ("", 0, False))

    def test_long_line_is_truncated(self):
        block, injected, truncated = plan_injection(
            [_Rec("1", content="x" * 500)], line_max=80
        )
        self.assertTrue(truncated)
        self.assertEqual(injected, 1)
        self.assertTrue(all(len(line) <= 80 for line in block.split("\n")[1:]))

    def test_char_budget_stops_injection(self):
        records = [_Rec(str(i), content="y" * 100) for i in range(50)]
        block, injected, truncated = plan_injection(records, max_chars=400)

        self.assertTrue(truncated)
        self.assertLess(injected, 50)
        self.assertLessEqual(len(block), 400 + 200)

    def test_at_least_one_record_survives_a_tiny_budget(self):
        # 预算再小也要给出一条，否则检索结果等于白拿。
        block, injected, _ = plan_injection([_Rec("1", content="z" * 300)], max_chars=10)
        self.assertEqual(injected, 1)
        self.assertIn("z", block)


class RecallIntegrationTests(unittest.TestCase):
    def _memory(self, tmp):
        sessions = SessionManager(Path(tmp))
        return MemoryRuntime(Path(tmp), session_manager=sessions)

    def test_lexical_lane_finds_exact_entity_without_embeddings(self):
        with tempfile.TemporaryDirectory() as tmp:
            memory = self._memory(tmp)
            memory.memorize("deploy script lives at scripts/rollout.sh", memory_type="fact")
            memory.memorize("user likes cats", memory_type="preference")

            hits = memory.recall("scripts/rollout.sh")
            self.assertTrue(hits)
            self.assertIn("rollout.sh", hits[0].content)

    def test_recall_degrades_to_lexical_when_no_embeddings(self):
        with tempfile.TemporaryDirectory() as tmp:
            memory = self._memory(tmp)
            memory.memorize("the error code is E4011", memory_type="fact")

            # 没有配置 embedding：vector lane 为空，纯词法仍然要能召回。
            self.assertEqual(memory.vector_lane("E4011", memory.candidates()), [])
            hits = memory.recall("E4011")
            self.assertTrue(hits)

    def test_retrieve_reports_trace(self):
        with tempfile.TemporaryDirectory() as tmp:
            memory = self._memory(tmp)
            memory.memorize("kirakira uses RRF fusion", memory_type="fact")

            result = memory.retrieve(RetrievalRequest(query="RRF fusion", limit=5))

            self.assertIn("RRF fusion", result.block)
            self.assertEqual(result.trace.injected, 1)
            self.assertFalse(result.trace.used_vector)
            self.assertGreaterEqual(result.trace.lanes["lexical"], 1)
            self.assertEqual(result.trace.lanes["vector"], 0)

    def test_empty_query_returns_most_recent(self):
        with tempfile.TemporaryDirectory() as tmp:
            memory = self._memory(tmp)
            memory.memorize("older", memory_type="fact")
            memory.memorize("newer", memory_type="fact")

            hits = memory.recall("", limit=1)
            self.assertEqual(len(hits), 1)

    def test_type_and_time_filters_still_apply(self):
        with tempfile.TemporaryDirectory() as tmp:
            memory = self._memory(tmp)
            memory.memorize("user prefers dark mode", memory_type="preference")
            memory.memorize("build uses uv", memory_type="fact")

            hits = memory.recall("user", memory_types=["preference"])
            self.assertTrue(all(h.memory_type == "preference" for h in hits))

    def test_non_matches_are_excluded_rather_than_ranked_last(self):
        """旧的 `semantic*0.75 + lexical*0.25` 给每条记录都算得出一个非零分，于是
        limit 永远会被无关记忆填满。每条 lane 各自的准入规则让不匹配的直接缺席。"""

        class FakeEmbed:
            def embed(self, text):
                t = text.lower()
                return [
                    1.0 if any(w in t for w in ("cat", "kitten", "feline")) else 0.0,
                    1.0 if any(w in t for w in ("deploy", "rollout")) else 0.0,
                    0.1,
                ]

        with tempfile.TemporaryDirectory() as tmp:
            memory = self._memory(tmp)
            memory.embedding_client = FakeEmbed()
            memory.memorize("cat food brand is Orijen", memory_type="preference")
            memory.memorize("kitten vaccination is due in May", memory_type="fact")

            # "Orijen" 是精确品牌名：向量空间没有这个维度，只有词法能命中。
            hits = memory.recall("Orijen", limit=5)

            self.assertEqual(len(hits), 1)
            self.assertIn("Orijen", hits[0].content)

    def test_memorize_primitive_dedupes_only_exact_repeats(self):
        """`memorize()` 是原语：只挡逐字重复，不做语义去重。

        改写后的同一事实在这一层会产生两条记录——这是**有意的**，因为词法比对分不清
        "改写"和"否定"。语义去重不在这一层:它由 coremem 的 dedup_decider 在
        consolidation / post-response 摄入时用 LLM 判断。
        """

        with tempfile.TemporaryDirectory() as tmp:
            memory = self._memory(tmp)
            memory.memorize("用户的部署脚本在 scripts/rollout.sh。", memory_type="procedure")

            # 逐字重复 → 强化旧记录，不新增
            memory.memorize("用户的部署脚本在 scripts/rollout.sh。", memory_type="procedure")
            self.assertEqual(len(memory.list_records()), 1)
            self.assertEqual(memory.list_records()[0]["reinforcement"], 2)

            # 改写 → 原语层留两条，交给 consolidation 去判
            memory.memorize(
                "部署脚本在 scripts/rollout.sh，每次发版都跑它", memory_type="procedure"
            )
            self.assertEqual(len(memory.list_records()), 2)



    def test_lexical_similarity_cannot_safely_dedupe_negation(self):
        """不要用词法相似度做去重：否定句的相似度比真重复还高。

        实测：
            0.833  "CI 跑在 GitHub Actions 上"  vs  "CI 不跑在 GitHub Actions 上"   ← 否定
            0.727  "错误码 E4011 表示配额超限"    vs  "用户的错误码 E4011 表示配额超限。" ← 真重复

        任何抓得住真重复的阈值都会把否定句合并掉，让 agent 说反话。
        """

        from core.memory.legacy import _tokenize

        def jaccard(a, b):
            ta, tb = _tokenize(a), _tokenize(b)
            return len(ta & tb) / max(1, len(ta | tb))

        negation = jaccard("CI 跑在 GitHub Actions 上", "CI 不跑在 GitHub Actions 上")
        true_dup = jaccard("错误码 E4011 表示配额超限", "用户的错误码 E4011 表示配额超限。")

        self.assertGreater(
            negation,
            true_dup,
            "若此断言失败，说明分词变了，可以重新评估词法去重是否安全",
        )

    def test_build_retrieval_block_respects_budget(self):
        with tempfile.TemporaryDirectory() as tmp:
            memory = self._memory(tmp)
            for i in range(40):
                memory.memorize("fact number %d about deployment" % i, memory_type="fact")

            block = memory.build_retrieval_block("deployment", limit=40)
            self.assertLessEqual(len(block), 1600)


if __name__ == "__main__":
    unittest.main()
