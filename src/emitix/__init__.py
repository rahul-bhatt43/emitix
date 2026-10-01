"""Lightweight event bus for Python applications."""

from .bus import EventBus, Handler

__version__ = "0.1.0"

__all__ = [
    "EventBus",
    "Handler",
    "default_bus",
    "on",
    "once",
    "off",
    "emit",
    "emit_async",
    "listeners",
    "clear",
]

# A shared bus for apps that only need one; the functions below are its methods.
default_bus = EventBus()

on = default_bus.on
once = default_bus.once
off = default_bus.off
emit = default_bus.emit
emit_async = default_bus.emit_async
listeners = default_bus.listeners
clear = default_bus.clear
