"""Slows down anyone guessing codes, without getting in an honest counselor's way.

A camp's address is public and a staff code is only four digits, so without a limit
someone could simply try all 10,000. This module counts WRONG codes across the whole
deployment (not per browser tab, which an attacker could just reopen) and, after too
many in a short window, pauses code entry for a minute. Repeated pauses get longer.

The numbers are chosen so normal use never notices: one or two typos in a busy minute
are far under the limit. Right codes never count and never reset the count, so a
guesser cannot hide behind a friend's correct entries.

Nothing here touches Streamlit. The two ready-made limiters at the bottom live in this
module on purpose: Streamlit re-runs the app script on every click, but a module is
loaded once, so the counts survive between clicks and between browser tabs.
"""
import threading
import time
from collections import deque


class FailureLimiter:
    def __init__(self, rules=((10, 120), (25, 3600)), lock_steps=(60, 120, 300, 900),
                 calm=3600, clock=time.time):
        # rules: (wrong tries, within this many seconds). A fast burst AND a slow, steady
        # trickle both count, so spreading guesses out does not get around the limit.
        self.rules = rules
        self.lock_steps = lock_steps        # pause lengths: 1st, 2nd, 3rd... (last one repeats)
        self.calm = calm                    # seconds without a pause before the steps start over
        self.clock = clock
        self._keep = max(w for _, w in rules)
        self._fails = deque()
        self._lock_until = 0.0
        self._level = 0
        self._last_lock = 0.0
        self._mu = threading.Lock()

    def remaining(self) -> int:
        """Whole seconds left in a pause (0 means entry is open)."""
        with self._mu:
            left = self._lock_until - self.clock()
            return int(left) + 1 if left > 0 else 0

    def fail(self) -> int:
        """Record one wrong try. Returns the pause length in seconds if THIS try
        started a pause, otherwise 0."""
        with self._mu:
            now = self.clock()
            if now < self._lock_until:
                return 0
            while self._fails and now - self._fails[0] > self._keep:
                self._fails.popleft()
            self._fails.append(now)
            tripped = any(
                sum(1 for t in self._fails if now - t <= window) >= limit for limit, window in self.rules
            )
            if not tripped:
                return 0
            if now - self._last_lock > self.calm:
                self._level = 0
            secs = self.lock_steps[min(self._level, len(self.lock_steps) - 1)]
            self._level += 1
            self._last_lock = now
            self._lock_until = now + secs
            self._fails.clear()
            return secs


# Staff and admin codes (every place a 4-digit code is typed).
CODES = FailureLimiter(rules=((10, 120), (25, 3600)))
# The admin-area password: far fewer honest mistakes, so a much lower bar.
ADMIN_PASSWORD = FailureLimiter(rules=((5, 300), (10, 3600)), lock_steps=(60, 300, 900, 3600))


def wait_message(seconds: int) -> str:
    if seconds >= 90:
        return f"Too many wrong codes. Entry is paused for about {round(seconds / 60)} minutes."
    return f"Too many wrong codes. Entry is paused for {seconds} seconds."
