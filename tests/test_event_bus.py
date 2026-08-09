"""Lifecycle event bus ordering and isolation tests."""

import asyncio
from dataclasses import dataclass
import unittest

from bus.event_bus import EventBus


@dataclass
class Event:
    value: int


class EventBusTests(unittest.TestCase):
    def test_emit_is_ordered_and_enqueue_is_awaitable_on_shutdown(self):
        async def scenario():
            bus = EventBus()
            observed = []

            def first(event):
                event.value += 1
                return event

            async def second(event):
                await asyncio.sleep(0.01)
                event.value *= 2
                observed.append(event.value)
                return event

            bus.on(Event, first)
            bus.on(Event, second)
            emitted = await bus.emit(Event(2))
            self.assertEqual(emitted.value, 6)

            bus.enqueue(Event(3))
            await bus.shutdown()
            self.assertIn(8, observed)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
