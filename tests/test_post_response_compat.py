"""post-response 解析的模型兼容边界。

真实对话验证时发现:Reference 的 `_parse_json_string_array` 要求裸 JSON 数组,而 deepseek
在同一段 prompt 下有时返回 `{"intent": []}`——单键对象包着数组。后果不是"少一点智能",
而是自动失效检测整条路抛异常。

单测全绿没抓到它,因为 mock 的 provider 不会这样返回。这组用例把真实观察到的形状固化下来。

容错只放宽"单键对象且值是数组"这一种;其余坏响应仍必须报错——放宽到猜模型意图就会把
真正的损坏响应吞掉。
"""

from __future__ import annotations

import unittest

from plugins.default_memory.compat_worker import (
    CompatPostResponseMemoryWorker,
    _unwrap_single_key_array,
)
from memory2.post_response_worker import PostResponseMemoryWorker


def _parse(text: str):
    return CompatPostResponseMemoryWorker._parse_json_string_array(text, "probe")


class UnwrapTests(unittest.TestCase):
    def test_single_key_object_is_unwrapped(self) -> None:
        # deepseek 实际返回过的形状
        self.assertEqual(_unwrap_single_key_array('{"intent": []}'), "[]")
        self.assertEqual(_unwrap_single_key_array('{"topics": ["a"]}'), '["a"]')

    def test_fenced_single_key_object_is_unwrapped(self) -> None:
        self.assertEqual(
            _unwrap_single_key_array('```json\n{"intent": []}\n```'), "[]"
        )

    def test_shapes_that_must_not_be_guessed(self) -> None:
        for text in (
            '{"a": 1, "b": 2}',   # 多键:不知道该取哪个
            '{"k": "notalist"}',  # 值不是数组
            "[]",                 # 本来就是数组,不该走这条路
            "not json at all",
            "",
        ):
            self.assertIsNone(_unwrap_single_key_array(text), text)


class CompatParserTests(unittest.TestCase):
    def test_bare_array_still_parses(self) -> None:
        self.assertEqual(_parse("[]"), [])
        self.assertEqual(_parse('["a", "b"]'), ["a", "b"])

    def test_fenced_array_still_parses(self) -> None:
        self.assertEqual(_parse('```json\n["a"]\n```'), ["a"])

    def test_object_wrapped_array_now_parses(self) -> None:
        # 修复前这里抛 ValueError,整条自动失效检测因此断掉
        self.assertEqual(_parse('{"intent": []}'), [])
        self.assertEqual(_parse('{"topics": ["steam查询流程"]}'), ["steam查询流程"])

    def test_base_class_still_rejects_the_wrapper(self) -> None:
        # 容错只属于 kirakira 边界;Reference 镜像文件的语义保持不变
        with self.assertRaises(ValueError):
            PostResponseMemoryWorker._parse_json_string_array('{"intent": []}', "probe")

    def test_genuinely_broken_responses_still_raise(self) -> None:
        for text in ('{"a": 1, "b": 2}', '{"k": "notalist"}', "", "nonsense"):
            with self.assertRaises(ValueError, msg=text):
                _parse(text)

    def test_array_with_non_string_items_still_raises(self) -> None:
        with self.assertRaises(ValueError):
            _parse('{"intent": [1, 2]}')

    def test_engine_wires_the_compat_worker(self) -> None:
        # 引擎必须用容错版,否则修复等于没接上
        import plugins.default_memory.engine as engine

        self.assertIs(engine.PostResponseMemoryWorker, CompatPostResponseMemoryWorker)


if __name__ == "__main__":
    unittest.main()
