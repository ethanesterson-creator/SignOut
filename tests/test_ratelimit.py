"""Tests for ratelimit. Run:  python tests/test_ratelimit.py  (or pytest)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import ratelimit as rl  # noqa: E402


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def lim(**kw):
    c = Clock()
    return rl.FailureLimiter(clock=c, **kw), c


def test_honest_mistakes_never_trip_it():
    l, c = lim()
    for _ in range(45):           # a typo every 4 minutes for 3 hours: far more than a real kiosk sees
        assert l.fail() == 0 and l.remaining() == 0
        c.t += 240
    c.t += 4000
    for _ in range(9):            # even a burst of 9 wrong in a busy minute is fine
        assert l.fail() == 0
        c.t += 1


def test_burst_of_wrong_codes_pauses_entry_then_reopens():
    l, c = lim()
    results = [l.fail() for _ in range(10)]
    assert results[:9] == [0] * 9 and results[9] == 60
    assert l.remaining() == 61 or l.remaining() == 60
    c.t += 30
    assert 0 < l.remaining() <= 31
    c.t += 31
    assert l.remaining() == 0


def test_repeat_offenders_wait_longer_and_calm_resets():
    l, c = lim()
    waits = []
    for _ in range(5):
        for _ in range(10):
            w = l.fail()
        waits.append(w)
        c.t += w + 1
    assert waits == [60, 120, 300, 900, 900]
    c.t += 3700                    # an hour of quiet
    for _ in range(10):
        w = l.fail()
    assert w == 60


def test_slow_steady_guessing_is_caught_too():
    l, c = lim()
    tripped_at = None
    for i in range(60):            # one guess every 15 seconds, under the burst rule forever
        w = l.fail()
        if w and tripped_at is None:
            tripped_at = i + 1
        c.t += 15
    assert tripped_at == 25        # the hourly rule stops it on the 25th wrong guess


def test_attempts_during_a_pause_do_not_extend_it():
    l, c = lim()
    for _ in range(10):
        l.fail()
    first = l.remaining()
    for _ in range(20):
        assert l.fail() == 0
    assert l.remaining() == first


def test_admin_password_limiter_is_stricter():
    l = rl.ADMIN_PASSWORD
    assert l.rules[0] == (5, 300) and l.lock_steps[0] == 60
    assert "paused" in rl.wait_message(45) and "minutes" in rl.wait_message(300)


if __name__ == "__main__":
    for k, v in sorted(globals().items()):
        if k.startswith("test_"):
            v()
            print("PASS", k)
    print("ALL PASS")
