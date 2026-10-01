import asyncio

import pytest

import emitix
from emitix import EventBus


@pytest.fixture
def bus():
    return EventBus()


def test_emit_calls_handler_with_payload(bus):
    calls = []
    bus.on("greet", lambda name, punct="!": calls.append(name + punct))

    bus.emit("greet", "Ada")
    bus.emit("greet", "Bob", punct="?")

    assert calls == ["Ada!", "Bob?"]


def test_emit_returns_handler_results(bus):
    bus.on("sum", lambda a, b: a + b)
    bus.on("sum", lambda a, b: a * b)

    assert bus.emit("sum", 2, 5) == [7, 10]


def test_emit_without_listeners_returns_empty_list(bus):
    assert bus.emit("nothing") == []


def test_on_as_decorator_returns_original_function(bus):
    @bus.on("ping")
    def handler():
        return "pong"

    assert handler() == "pong"
    assert bus.emit("ping") == ["pong"]


def test_priority_order_then_registration_order(bus):
    bus.on("e", lambda: "low", priority=-1)
    bus.on("e", lambda: "first")
    bus.on("e", lambda: "second")
    bus.on("e", lambda: "high", priority=10)

    assert bus.emit("e") == ["high", "first", "second", "low"]


def test_once_runs_a_single_time(bus):
    @bus.once("boot")
    def handler():
        return 1

    assert bus.emit("boot") == [1]
    assert bus.emit("boot") == []


def test_once_handler_is_not_retriggered_by_nested_emit(bus):
    calls = []

    @bus.once("loop")
    def handler():
        calls.append(1)
        bus.emit("loop")

    bus.emit("loop")

    assert calls == [1]


def test_wildcard_subscription(bus):
    seen = []
    bus.on("user.*", lambda event, uid: seen.append((event, uid)), pass_event=True)

    bus.emit("user.created", 1)
    bus.emit("user.deleted", 2)
    bus.emit("order.created", 3)

    assert seen == [("user.created", 1), ("user.deleted", 2)]


def test_wildcard_and_exact_handlers_share_priority_order(bus):
    bus.on("*", lambda: "any")
    bus.on("a", lambda: "exact", priority=1)

    assert bus.emit("a") == ["exact", "any"]


def test_off_removes_single_handler(bus):
    def keep():
        return "keep"

    def drop():
        return "drop"

    bus.on("e", keep)
    bus.on("e", drop)

    assert bus.off("e", drop) == 1
    assert bus.emit("e") == ["keep"]


def test_off_without_handler_removes_all(bus):
    bus.on("e", lambda: 1)
    bus.on("e", lambda: 2)

    assert bus.off("e") == 2
    assert bus.off("e") == 0
    assert bus.emit("e") == []


def test_off_wildcard_pattern(bus):
    bus.on("user.*", lambda: 1)

    assert bus.off("user.*") == 1
    assert bus.emit("user.created") == []


def test_off_bound_method(bus):
    class Listener:
        def handle(self):
            return "handled"

    listener = Listener()
    bus.on("e", listener.handle)

    assert bus.off("e", listener.handle) == 1
    assert bus.emit("e") == []


def test_listeners_does_not_consume_once_handlers(bus):
    def handler():
        return 1

    bus.once("e", handler)

    assert bus.listeners("e") == [handler]
    assert bus.emit("e") == [1]
    assert bus.listeners("e") == []


def test_clear(bus):
    bus.on("a", lambda: 1)
    bus.on("b.*", lambda: 2)

    bus.clear()

    assert bus.emit("a") == []
    assert bus.emit("b.c") == []


def test_handler_exception_propagates(bus):
    calls = []

    def boom():
        raise ValueError("boom")

    bus.on("e", boom)
    bus.on("e", lambda: calls.append("after"))

    with pytest.raises(ValueError, match="boom"):
        bus.emit("e")
    assert calls == []


def test_non_callable_handler_is_rejected(bus):
    with pytest.raises(TypeError):
        bus.on("e", "not a function")


def test_emit_rejects_async_handlers_before_calling_anything(bus):
    calls = []

    async def async_handler():
        calls.append("async")

    bus.once("e", lambda: calls.append("sync"), priority=1)
    bus.on("e", async_handler)

    with pytest.raises(TypeError, match="emit_async"):
        bus.emit("e")
    assert calls == []
    assert len(bus.listeners("e")) == 2


def test_emit_async_awaits_async_and_calls_sync_handlers(bus):
    async def async_handler(x):
        await asyncio.sleep(0)
        return x * 2

    bus.on("e", async_handler)
    bus.on("e", lambda x: x + 1)

    assert asyncio.run(bus.emit_async("e", 5)) == [10, 6]


def test_buses_are_independent():
    first, second = EventBus(), EventBus()
    first.on("e", lambda: 1)

    assert second.emit("e") == []


def test_module_level_default_bus():
    try:
        emitix.on("hello", lambda name: f"hi {name}")

        assert emitix.emit("hello", "Ada") == ["hi Ada"]
        assert emitix.default_bus.listeners("hello")
    finally:
        emitix.clear()
