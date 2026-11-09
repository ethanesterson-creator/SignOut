"""Tests for late_worker: who is late, who is alerted, and that nobody is alerted twice.
No network: the Sheet and the notifiers are faked.

Run:  python tests/test_late_worker.py   (or pytest)
"""
import os
import sys
from datetime import datetime, time as dtime

import pytz

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import camp_config as cc  # noqa: E402
import late_worker as lw  # noqa: E402

TZ = pytz.timezone("US/Eastern")
CFG = cc.load({"camp_name": "Camp Test", "camp_notify_prefix": "Test", "spreadsheet_id": "X",
               "alert_email_to": ["office@camp.org"], "alert_email_from": "A <a@b.co>", "server_late_check": True})
SCHEDULE = [("Period 3", dtime(11, 0), dtime(11, 50)), ("Rest", dtime(13, 0), dtime(14, 0))]


def at(h, m, day=9, s=0):
    return TZ.localize(datetime(2026, 11, day, h, m, s))


def row(name, reason, when, rid, status="OUT"):
    return {"name": name, "status": status, "reason": reason, "timestamp": when.isoformat(timespec="seconds"), "id": rid}


class FakeStore:
    def __init__(self, rows, dedupe="", schedule=None):
        self.rows, self.dedupe, self.schedule, self.writes = rows, dedupe, schedule, []

    def read_status(self):
        return self.rows

    def read_schedule(self):
        return self.schedule

    def read_dedupe(self):
        return self.dedupe

    def write_dedupe(self, v):
        self.dedupe = v
        self.writes.append(v)


def run(store, now):
    pushes, emails = [], []
    n = lw.run_once(store, CFG, lambda t, m: pushes.append((t, m)), lambda s, b: emails.append((s, b)), now, TZ)
    return n, pushes, emails


def test_period_off_is_alerted_a_full_minute_after_the_deadline_and_not_before():
    store = FakeStore([row("Ari", "Period Off", at(13, 21), "r1")], schedule=SCHEDULE)   # due 13:55
    assert run(store, at(13, 55, s=59))[0] == 0              # not yet a full minute late
    n, pushes, emails = run(store, at(13, 56, s=5))
    assert n == 1
    assert pushes == [("Test: LATE", "Ari is 1 min late (Period Off)")]
    assert emails[0][0] == "Camp Test: Ari is 1 min late" and "1:55 PM" in emails[0][1]


def test_nobody_is_alerted_twice_even_across_many_passes():
    store = FakeStore([row("Ari", "Period Off", at(13, 21), "r1")], schedule=SCHEDULE)
    assert run(store, at(14, 0))[0] == 1
    for minute in (1, 2, 5, 30):
        assert run(store, at(14, minute))[0] == 0
    assert store.dedupe == "r1"


def test_two_late_people_each_alerted_once_and_returned_people_never():
    store = FakeStore([row("Ari", "Period Off", at(13, 21), "r1"), row("Bo", "Period Off", at(13, 30), "r2"),
                       row("Cy", "Period Off", at(13, 10), "r3", status="IN")], schedule=SCHEDULE)
    n, pushes, _ = run(store, at(14, 5))
    assert n == 2 and {p[1].split(" is ")[0] for p in pushes} == {"Ari", "Bo"}
    assert set(store.dedupe.split(",")) == {"r1", "r2"}


def test_night_off_is_due_at_the_next_quarter_past_midnight():
    store = FakeStore([row("Dee", "Night Off", at(21, 0), "r9")], schedule=SCHEDULE)
    assert run(store, at(23, 59))[0] == 0
    assert run(store, at(0, 14, day=10))[0] == 0
    assert run(store, at(0, 17, day=10))[0] == 1             # 12:15 AM, 2 min late
    assert "12:15 AM" in run(FakeStore([row("Eve", "Night Off", at(21, 0), "r10")], schedule=SCHEDULE), at(0, 20, day=10))[2][0][1]
    assert lw.due_text(at(0, 15, day=10), at(0, 20, day=10)) == "12:15 AM"                 # same day: time only
    assert lw.due_text(at(0, 15, day=10), at(9, 0, day=11)) == "Tue 12:15 AM"              # other day: weekday too


def test_reasons_with_no_deadline_and_unreadable_rows_are_ignored():
    rows = [row("Fay", "Other (type reason)", at(9, 0), "a"), {"name": "Gus", "status": "OUT", "reason": "Period Off", "timestamp": "garbage", "id": "b"},
            {"name": "Hal", "status": "OUT", "reason": "Period Off", "timestamp": "", "id": "c"},
            {"name": "Ivy", "status": "OUT", "reason": "Period Off", "timestamp": at(9, 0).isoformat(), "id": ""}]
    assert run(FakeStore(rows, schedule=SCHEDULE), at(23, 0))[0] == 0


def test_falls_back_to_the_camp_default_schedule_when_the_tab_is_empty():
    store = FakeStore([row("Jo", "Period Off", at(9, 16), "z")], schedule=[])           # default Period 1 ends 10:05, due 10:00
    assert run(store, at(10, 5))[0] == 1


def test_dedupe_list_is_trimmed_to_the_newest_ids():
    old = ",".join(f"id{i}" for i in range(400))
    store = FakeStore([row("Kai", "Period Off", at(13, 21), "newest")], dedupe=old, schedule=SCHEDULE)
    run(store, at(14, 0))
    ids = store.dedupe.split(",")
    assert len(ids) == lw.KEEP_ALERTED and ids[-1] == "newest"


def test_parsers():
    assert lw.parse_time("9:15") == dtime(9, 15) and lw.parse_time("2:15 PM") == dtime(14, 15) and lw.parse_time("x") is None
    assert lw.parse_stamp("2026-11-09T13:21:00-05:00", TZ) == at(13, 21)
    assert lw.parse_stamp("2026-11-09T18:21:00+00:00", TZ) == at(13, 21)                 # converted to camp time
    assert lw.parse_stamp("2026-11-09T13:21:00", TZ) == at(13, 21) and lw.parse_stamp("", TZ) is None
    assert lw.schedule_from_rows([["period", "start", "end"], ["P1", "9:00", "9:50"], ["bad", "", ""]]) == [("P1", dtime(9, 0), dtime(9, 50))]


def test_worker_exits_with_the_disabled_code_when_the_camp_has_not_turned_it_on(tmp_path=None):
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        p = os.path.join(d, "s.toml")
        with open(p, "w", encoding="utf-8") as f:
            f.write('camp_name = "Camp Test"' + chr(10) + 'spreadsheet_id = "X"' + chr(10))
        assert lw.main(p) == lw.DISABLED_EXIT


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print("PASS", t.__name__)
    print("ALL PASS")
