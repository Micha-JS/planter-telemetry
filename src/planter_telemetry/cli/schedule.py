"""Replay pacing math. Pure: no clock, no sleeps.

Offsets are cumulative (seconds from replay start) rather than per-message
deltas: the edge sleeps until start + offset against a monotonic clock, so
sleep jitter never accumulates — the same pacing style as the simulator's
publisher. Streaming in, streaming out: replaying from stdin never buffers
the whole file.
"""

from collections.abc import Iterable, Iterator
from datetime import datetime


def replay_offsets(captured_at: Iterable[datetime], speed: float) -> Iterator[float]:
    """Wall-clock offset at which each message should be published.

    The first message publishes at 0.0; each subsequent one after the
    captured gap to its predecessor compressed by `speed`. Negative gaps
    (out-of-order captured_at, as injected by the simulator) clamp to zero:
    file order is preserved and the late message goes out immediately.
    """
    if speed <= 0:
        raise ValueError("speed must be positive")
    offset = 0.0
    previous: datetime | None = None
    for timestamp in captured_at:
        if previous is not None:
            offset += max(0.0, (timestamp - previous).total_seconds()) / speed
        previous = timestamp
        yield offset
