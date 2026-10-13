"""Tests for opswatch. Run:  python tests/test_opswatch.py  (or pytest)."""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import opswatch as ow  # noqa: E402


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def make(**kw):
    sent = []
    clock = Clock(time.mktime((2026, 7, 14, 9, 0, 0, 0, 0, -1)))
    w = ow.Watchdog("Camp Example", lambda t, m, p="", g="": sent.append((t, m, p, g)), clock=clock, **kw)
    return w, sent, clock


def test_brief_blip_is_silent_then_sustained_failure_alerts_once():
    w, sent, clock = make()
    w.observe("sheets", False, "Google Sheets not responding", "read failed")
    clock.t += 60
    w.observe("sheets", True, "Google Sheets not responding")      # recovered inside 3 min
    assert sent == []
    for _ in range(8):                                               # fails for 4 minutes
        w.observe("sheets", False, "Google Sheets not responding", "read failed")
        clock.t += 30
    alerts = [s for s in sent if "not responding" in s[0]]
    assert len(alerts) == 1 and alerts[0][0].startswith("[Camp Example]") and alerts[0][2] == "high"


def test_reminder_every_half_hour_and_recovery_message():
    w, sent, clock = make()
    w.observe("w", False, "Saves are stuck", "3 waiting")
    clock.t += 200
    w.observe("w", False, "Saves are stuck", "3 waiting")           # first alert
    clock.t += 1700
    w.observe("w", False, "Saves are stuck", "3 waiting")           # not yet 30 min since alert
    assert len(sent) == 1
    clock.t += 200
    w.observe("w", False, "Saves are stuck", "3 waiting")           # reminder
    assert len(sent) == 2 and "Still failing" in sent[1][0]
    clock.t += 90
    w.observe("w", True, "Saves are stuck")
    assert len(sent) == 3 and "Recovered" in sent[2][0] and sent[2][3] == "white_check_mark"
    w.observe("w", True, "Saves are stuck")                          # no repeated recovery
    assert len(sent) == 3


def test_events_are_rate_limited_and_report_what_was_skipped():
    w, sent, clock = make()
    assert w.event("crash", "App error", "KeyError x") is True
    for _ in range(5):
        clock.t += 30
        assert w.event("crash", "App error", "KeyError x") is False
    clock.t += 600
    assert w.event("crash", "App error", "KeyError x") is True
    assert "5 more" in sent[-1][1] and len(sent) == 2
    assert w.event("other", "Different error", "y") is True           # separate key is separate


def test_daily_cap_then_quiet_then_resets_next_day():
    w, sent, clock = make(daily_cap=5, event_cooldown=0)
    for i in range(20):
        clock.t += 1
        w.event(f"k{i}", "boom", str(i))
    assert len(sent) == 6                                              # 5 + one "limit reached"
    assert "limit reached" in sent[-1][0]
    clock.t += 86400
    assert w.event("fresh", "boom again", "") is True and len(sent) == 7


def test_probes_and_a_broken_probe_both_work():
    w, sent, clock = make()
    w.add_probe("good", "Fine", lambda: (True, ""))
    w.add_probe("bad", "Broken thing", lambda: (False, "nope"))
    w.add_probe("boom", "Probe crash", lambda: 1 / 0)
    for _ in range(8):
        w.tick()
        clock.t += 30
    titles = " ".join(s[0] for s in sent)
    assert "Broken thing" in titles and "Probe crash" in titles and "Fine" not in titles


def test_digest_sent_once_per_day_after_the_hour():
    w, sent, clock = make()
    w.digest_fn = lambda: "12 sign-outs yesterday"
    w.digest_hour = 7
    clock.t = time.mktime((2026, 7, 14, 6, 0, 0, 0, 0, -1)); w.tick()
    assert sent == []                                                  # before the hour
    clock.t = time.mktime((2026, 7, 14, 7, 5, 0, 0, 0, -1)); w.tick(); w.tick()
    assert len(sent) == 1 and sent[0][1] == "12 sign-outs yesterday" and sent[0][2] == "low"
    clock.t = time.mktime((2026, 7, 15, 15, 0, 0, 0, 0, -1)); w.tick()
    assert len(sent) == 1                                              # a 3 PM restart: no check-in
    clock.t = time.mktime((2026, 7, 15, 7, 5, 0, 0, 0, -1)); w.tick()
    assert len(sent) == 2


def test_a_failing_sender_never_raises():
    def boom(*a, **k):
        raise RuntimeError("ntfy down")
    w = ow.Watchdog("X", boom, clock=Clock(time.time()))
    assert w.emit("t", "m") is False


if __name__ == "__main__":
    for k, v in sorted(globals().items()):
        if k.startswith("test_"):
            v()
            print("PASS", k)
    print("ALL PASS")
