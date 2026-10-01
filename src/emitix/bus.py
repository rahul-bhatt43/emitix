from __future__ import annotations

import inspect
import itertools
import threading
from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import Any, Callable, TypeVar, overload

Handler = Callable[..., Any]
H = TypeVar("H", bound=Handler)

_GLOB_CHARS = frozenset("*?[")


@dataclass(frozen=True)
class _Subscription:
    pattern: str
    handler: Handler
    priority: int
    once: bool
    pass_event: bool
    order: int

    def call(self, event: str, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        if self.pass_event:
            return self.handler(event, *args, **kwargs)
        return self.handler(*args, **kwargs)


class EventBus:
    """Publish/subscribe hub that routes named events to handlers.

    Event names are plain strings. A subscription may use shell-style
    wildcards (``*``, ``?``, ``[abc]``), so ``"user.*"`` receives both
    ``"user.created"`` and ``"user.deleted"``.

    All methods are safe to call from multiple threads.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._subs: dict[str, list[_Subscription]] = {}
        self._globs: set[str] = set()
        self._counter = itertools.count()

    @overload
    def on(
        self,
        event: str,
        handler: H,
        *,
        priority: int = ...,
        once: bool = ...,
        pass_event: bool = ...,
    ) -> H: ...

    @overload
    def on(
        self,
        event: str,
        handler: None = ...,
        *,
        priority: int = ...,
        once: bool = ...,
        pass_event: bool = ...,
    ) -> Callable[[H], H]: ...

    def on(
        self,
        event: str,
        handler: Handler | None = None,
        *,
        priority: int = 0,
        once: bool = False,
        pass_event: bool = False,
    ) -> Any:
        """Subscribe ``handler`` to ``event``.

        Can be called directly or used as a decorator::

            bus.on("user.created", send_welcome_email)

            @bus.on("user.created")
            def send_welcome_email(user): ...

        Handlers with a higher ``priority`` run first; equal priorities run
        in the order they were registered. With ``once=True`` the handler is
        removed after its first call. With ``pass_event=True`` the handler
        receives the event name as its first argument, which is useful for
        wildcard subscriptions.
        """

        def register(fn: H) -> H:
            if not callable(fn):
                raise TypeError(f"handler must be callable, got {type(fn).__name__}")
            with self._lock:
                sub = _Subscription(
                    event, fn, priority, once, pass_event, next(self._counter)
                )
                self._subs.setdefault(event, []).append(sub)
                if _GLOB_CHARS.intersection(event):
                    self._globs.add(event)
            return fn

        return register if handler is None else register(handler)

    @overload
    def once(
        self, event: str, handler: H, *, priority: int = ..., pass_event: bool = ...
    ) -> H: ...

    @overload
    def once(
        self,
        event: str,
        handler: None = ...,
        *,
        priority: int = ...,
        pass_event: bool = ...,
    ) -> Callable[[H], H]: ...

    def once(
        self,
        event: str,
        handler: Handler | None = None,
        *,
        priority: int = 0,
        pass_event: bool = False,
    ) -> Any:
        """Subscribe ``handler`` to ``event`` for a single call."""
        return self.on(
            event, handler, priority=priority, once=True, pass_event=pass_event
        )

    def off(self, event: str, handler: Handler | None = None) -> int:
        """Remove subscriptions registered under ``event``.

        ``event`` must be the exact name or pattern that was passed to
        :meth:`on`. Removes only ``handler`` if given, otherwise every
        handler for that name. Returns the number of subscriptions removed.
        """
        with self._lock:
            subs = self._subs.get(event, [])
            kept = (
                []
                if handler is None
                else [sub for sub in subs if sub.handler != handler]
            )
            removed = len(subs) - len(kept)
            if kept:
                self._subs[event] = kept
            else:
                self._subs.pop(event, None)
                self._globs.discard(event)
            return removed

    def emit(self, event: str, *args: Any, **kwargs: Any) -> list[Any]:
        """Call every handler subscribed to ``event`` and return their results.

        An exception raised by a handler propagates to the caller and stops
        the remaining handlers. Raises :class:`TypeError` if an ``async def``
        handler is subscribed; use :meth:`emit_async` for those.
        """
        subs = self._take(event, allow_async=False)
        return [sub.call(event, args, kwargs) for sub in subs]

    async def emit_async(self, event: str, *args: Any, **kwargs: Any) -> list[Any]:
        """Like :meth:`emit`, but also awaits ``async def`` handlers.

        Handlers run one after another in priority order, not concurrently.
        """
        results = []
        for sub in self._take(event, allow_async=True):
            result = sub.call(event, args, kwargs)
            if inspect.isawaitable(result):
                result = await result
            results.append(result)
        return results

    def listeners(self, event: str) -> list[Handler]:
        """Return the handlers that would run for ``event``, in call order."""
        with self._lock:
            return [sub.handler for sub in self._match(event)]

    def clear(self) -> None:
        """Remove every subscription."""
        with self._lock:
            self._subs.clear()
            self._globs.clear()

    def _match(self, event: str) -> list[_Subscription]:
        matched = list(self._subs.get(event, ()))
        for pattern in self._globs:
            if pattern != event and fnmatchcase(event, pattern):
                matched.extend(self._subs[pattern])
        matched.sort(key=lambda sub: (-sub.priority, sub.order))
        return matched

    def _take(self, event: str, *, allow_async: bool) -> list[_Subscription]:
        with self._lock:
            matched = self._match(event)
            if not allow_async:
                for sub in matched:
                    if inspect.iscoroutinefunction(sub.handler):
                        raise TypeError(
                            f"async handler {sub.handler!r} is subscribed to "
                            f"{event!r}; use emit_async() instead of emit()"
                        )
            # Drop one-shot handlers before calling them, so a handler that
            # emits the same event again cannot trigger them a second time.
            for sub in matched:
                if sub.once:
                    self._discard(sub)
            return matched

    def _discard(self, sub: _Subscription) -> None:
        subs = self._subs[sub.pattern]
        subs.remove(sub)
        if not subs:
            del self._subs[sub.pattern]
            self._globs.discard(sub.pattern)
