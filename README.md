# emitix

Lightweight event bus for Python applications. No dependencies, fully typed, works with both sync and async code.

```bash
pip install emitix
```

## Quick start

```python
import emitix

@emitix.on("user.created")
def send_welcome_email(user):
    print(f"Welcome, {user}!")

emitix.emit("user.created", "Ada")
# Welcome, Ada!
```

The module-level functions use one shared bus. Create your own when you want separate buses:

```python
from emitix import EventBus

bus = EventBus()
bus.on("order.paid", lambda order_id: print("paid", order_id))
bus.emit("order.paid", 42)
```

## Features

### Any arguments

Whatever you pass to `emit` is passed to the handlers, and `emit` returns their results.

```python
bus.on("add", lambda a, b: a + b)
bus.emit("add", 2, 3)  # [5]
```

### Wildcards

Subscriptions accept shell-style patterns (`*`, `?`, `[abc]`). Use `pass_event=True` to receive the event name as the first argument.

```python
@bus.on("user.*", pass_event=True)
def audit(event, user):
    print(event, user)

bus.emit("user.created", "Ada")  # user.created Ada
bus.emit("user.deleted", "Ada")  # user.deleted Ada
```

### One-shot handlers

```python
@bus.once("app.ready")
def warm_cache():
    ...
```

### Priorities

Higher priority runs first. Handlers with the same priority run in the order they were registered.

```python
bus.on("save", validate, priority=10)
bus.on("save", write_to_disk)
```

### Async handlers

`emit_async` awaits `async def` handlers and calls regular ones normally. Handlers run one after another, in priority order.

```python
@bus.on("job.done")
async def notify(job_id):
    await send_notification(job_id)

await bus.emit_async("job.done", 7)
```

Calling plain `emit` for an event that has an async handler raises `TypeError`.

### Unsubscribing

```python
bus.off("save", write_to_disk)  # remove one handler
bus.off("save")                 # remove every handler for "save"
bus.clear()                     # remove everything
```

## API

| Method | Description |
| --- | --- |
| `on(event, handler=None, *, priority=0, once=False, pass_event=False)` | Subscribe a handler. Works as a decorator when `handler` is omitted. |
| `once(event, handler=None, *, priority=0, pass_event=False)` | Subscribe a handler for a single call. |
| `off(event, handler=None)` | Remove one handler, or all handlers for `event`. Returns the number removed. |
| `emit(event, *args, **kwargs)` | Call the matching handlers and return a list of their results. |
| `emit_async(event, *args, **kwargs)` | Same as `emit`, but awaits async handlers. |
| `listeners(event)` | Handlers that would run for `event`, in call order. |
| `clear()` | Remove every subscription. |

An exception raised by a handler propagates to the caller of `emit` and stops the remaining handlers. All methods are thread-safe.

## Development

```bash
pip install -e ".[dev]"
pytest
```

## License

MIT
