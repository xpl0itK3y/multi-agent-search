"""The in-process sliding-window limiter behind every throttle (AUD-027).

Framework-free, so the service layer can hold a budget of its own (the per-recipient
account email budget, src/services/account_recovery_mixin.py) without importing the API
throttles in login_rate_limit.py, which re-exports it for their importers.
"""
from __future__ import annotations

import threading
import time
from collections import OrderedDict, deque


# Keys one limiter tracks at most. Keys are caller-chosen (an email typed into the login
# form, a client address), so the table must not grow with every distinct value: keys
# whose hits all left the window are dropped as they age out, and past this cap the key
# idle longest goes first, unless it is locked out (see SlidingWindowLimiter). About
# 1 KB per key, so the cap bounds a limiter near 10 MB.
DEFAULT_MAX_KEYS = 10_000


class SlidingWindowLimiter:
    """Per-key sliding-window counter with a bounded key table.

    At the cap the key idle longest among those below their limit goes first, so a flood
    of fresh keys pushes out other fresh keys before a key that reached its limit (locked
    out). When every tracked key is locked out, protect_lockouts decides:

    - True (the brute-force limiters): a new key is refused like an over-limit one until
      the oldest lockout leaves the window. Otherwise a flood of keys each at its limit,
      such as typed emails, would push a brute-forced account out of the table and hand
      it a fresh budget; the price is refusing new keys during such a flood.
    - False (the default): the oldest lockout is forgotten and the new key admitted, as a
      plain LRU table would. A limit-1 gate, such as the activity touch gate, locks every
      key out on its first hit, so a full table is its normal state past max_keys keys per
      window, and refusing there would starve every newcomer instead of throttling anyone.

    Memory stays bounded either way. A key counts as locked out from the hit that reaches
    the limit of that call (each limiter passes one fixed limit).
    """

    def __init__(
        self,
        window_seconds: float = 60.0,
        max_keys: int = DEFAULT_MAX_KEYS,
        *,
        protect_lockouts: bool = False,
    ) -> None:
        self._window = window_seconds
        self._max_keys = max(1, max_keys)
        self._protect_lockouts = protect_lockouts
        # Keys below their limit, ordered by latest hit, oldest first (a key moves to the
        # end whenever it records a hit), so expired keys, and the one to evict at the cap,
        # sit in front.
        self._hits: OrderedDict[str, deque[float]] = OrderedDict()
        # Keys at their limit, in the order they reached it. A refusal records nothing,
        # so that is also the order of their latest hit: expired keys sit in front too.
        self._locked: OrderedDict[str, deque[float]] = OrderedDict()
        self._lock = threading.Lock()

    def allow(self, key: str, limit: int) -> bool:
        with self._lock:
            # Read the clock under the lock, so hits land in time order across threads.
            now = time.monotonic()
            cutoff = now - self._window
            self._drop_expired(self._hits, cutoff)
            self._drop_expired(self._locked, cutoff)
            table = self._hits if key in self._hits else self._locked
            hits = table.get(key)
            if hits is None:
                if limit <= 0:
                    return False
                if len(self._hits) + len(self._locked) >= self._max_keys:
                    if self._hits:
                        self._hits.popitem(last=False)
                    elif self._protect_lockouts:
                        return False  # every key is locked out: forget none of them
                    else:
                        self._locked.popitem(last=False)
                (self._hits if limit > 1 else self._locked)[key] = deque((now,))
                return True
            while hits and hits[0] < cutoff:
                hits.popleft()
            if len(hits) >= limit:
                return False
            hits.append(now)
            del table[key]
            (self._hits if len(hits) < limit else self._locked)[key] = hits
            return True

    @staticmethod
    def _drop_expired(table: OrderedDict[str, deque[float]], cutoff: float) -> None:
        """Forget every key whose latest hit left the window (they are all at the front)."""
        while table:
            _key, hits = next(iter(table.items()))
            if hits and hits[-1] >= cutoff:
                return
            table.popitem(last=False)

    def __len__(self) -> int:
        with self._lock:
            return len(self._hits) + len(self._locked)

    def reset(self) -> None:
        with self._lock:
            self._hits.clear()
            self._locked.clear()

