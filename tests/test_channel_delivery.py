"""Complete logical-message delivery contract tests."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from agent.tools.message_push import MessagePushTool
from infra.channels.base import AttachmentStore
from infra.channels.delivery import deliver_message_parts
from bus.events import (
    AttachmentKind,
    ChannelAttachment,
    ChannelMessage,
    DeliveryReceipt,
    DeliveryStatus,
)


class ChannelDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_complete_message_success_reports_all_canonical_media(self) -> None:
        calls: list[tuple[object, ...]] = []

        async def send_text(chat_id: str, content: str) -> None:
            calls.append(("text", chat_id, content))

        async def send_file(chat_id: str, source: str, filename: str | None) -> None:
            calls.append(("file", chat_id, source, filename))

        async def send_image(chat_id: str, source: str) -> None:
            calls.append(("image", chat_id, source))

        message = ChannelMessage(
            channel="test",
            chat_id="chat-1",
            content="hello",
            attachments=(
                ChannelAttachment(AttachmentKind.FILE, "/tmp/report.txt", "report.txt"),
                ChannelAttachment(AttachmentKind.IMAGE, "/tmp/chart.png"),
            ),
        )
        receipt = await deliver_message_parts(
            message,
            send_text=send_text,
            send_file=send_file,
            send_image=send_image,
        )

        self.assertEqual(receipt.status, DeliveryStatus.SUCCESS)
        self.assertEqual(receipt.canonical_media, ("/tmp/report.txt", "/tmp/chart.png"))
        self.assertEqual(
            calls,
            [
                ("text", "chat-1", "hello"),
                ("file", "chat-1", "/tmp/report.txt", "report.txt"),
                ("image", "chat-1", "/tmp/chart.png"),
            ],
        )

    async def test_failure_after_text_is_reported_as_partial(self) -> None:
        calls: list[str] = []

        async def send_text(_chat_id: str, _content: str) -> None:
            calls.append("text")

        async def send_file(
            _chat_id: str, _source: str, _filename: str | None
        ) -> None:
            calls.append("file")
            raise RuntimeError("upload failed")

        async def send_image(_chat_id: str, _source: str) -> None:
            calls.append("image")

        receipt = await deliver_message_parts(
            ChannelMessage(
                channel="test",
                chat_id="chat-1",
                content="committed",
                attachments=(
                    ChannelAttachment(AttachmentKind.FILE, "/tmp/report.txt"),
                    ChannelAttachment(AttachmentKind.IMAGE, "/tmp/chart.png"),
                ),
            ),
            send_text=send_text,
            send_file=send_file,
            send_image=send_image,
        )

        self.assertEqual(receipt.status, DeliveryStatus.PARTIAL)
        self.assertEqual(receipt.detail, "upload failed")
        self.assertEqual(calls, ["text", "file"])

    async def test_failure_before_any_commit_is_failed(self) -> None:
        async def send_text(_chat_id: str, _content: str) -> None:
            raise AssertionError("there is no text")

        async def send_file(
            _chat_id: str, _source: str, _filename: str | None
        ) -> None:
            raise RuntimeError("upload failed")

        async def send_image(_chat_id: str, _source: str) -> None:
            raise AssertionError("must stop at first failure")

        receipt = await deliver_message_parts(
            ChannelMessage(
                channel="test",
                chat_id="chat-1",
                content="",
                attachments=(ChannelAttachment(AttachmentKind.FILE, "/tmp/report.txt"),),
            ),
            send_text=send_text,
            send_file=send_file,
            send_image=send_image,
        )

        self.assertEqual(receipt.status, DeliveryStatus.FAILED)

    async def test_message_push_uses_one_registered_adapter(self) -> None:
        delivered: list[ChannelMessage] = []
        tool = MessagePushTool()

        async def adapter(message: ChannelMessage) -> DeliveryReceipt:
            delivered.append(message)
            return DeliveryReceipt(DeliveryStatus.SUCCESS)

        registration = tool.register_channel("test", adapter)
        with self.assertRaisesRegex(RuntimeError, "渠道名称重复"):
            tool.register_channel("test", adapter)

        message = ChannelMessage("test", "chat-1", "hello")
        receipt = await tool.dispatch(message)
        self.assertTrue(receipt.succeeded)
        self.assertEqual(delivered, [message])

        registration.close()
        self.assertEqual(
            (await tool.dispatch(message)).status,
            DeliveryStatus.FAILED,
        )


class AttachmentStoreTests(unittest.TestCase):
    def test_write_bytes_atomically_publishes_private_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "attachments"
            path = AttachmentStore(root).write_bytes(
                b"payload",
                prefix="upload-",
                suffix=".bin",
            )

            self.assertEqual(path.read_bytes(), b"payload")
            self.assertEqual(path.parent.resolve(), root.resolve())
            self.assertFalse(any(root.glob("*.part")))
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)


if __name__ == "__main__":
    unittest.main()
