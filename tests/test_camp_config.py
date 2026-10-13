"""Tests for camp_config: defaults must reproduce Bauercrest's behaviour exactly,
and bad settings must fail with a message that says what to fix.

Run:  python tests/test_camp_config.py   (or pytest)
"""
import os
import sys
from datetime import datetime, time as dtime, timedelta

import pytz

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import camp_config as cc  # noqa: E402

TZ = pytz.timezone("US/Eastern")
SCHEDULE = [
    (n, datetime.strptime(s, "%H:%M").time(), datetime.strptime(e, "%H:%M").time())
    for n, s, e in cc.DEFAULT_SCHEDULE
]


def _legacy_due_back(reason, now):
    """The original hardcoded Bauercrest rules, kept here as the reference."""
    def next_clock(now, hh, mm):
        c = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        return c + timedelta(days=1) if c <= now else c

    reason = str(reason or "").strip()
    if reason == "Night Off":
        return next_clock(now, 0, 15)
    if reason == "Day Off":
        d = now.date()
        if now.hour >= 12:
            d += timedelta(days=1)
        naive = datetime.combine(d + timedelta(days=1), dtime(7, 45))
        return TZ.localize(naive) if now.tzinfo else naive
    if reason == "Period Off":
        for _n, st, en in SCHEDULE:
            end_dt = TZ.localize(datetime.combine(now.date(), en)) if now.tzinfo else datetime.combine(now.date(), en)
            due = end_dt - timedelta(minutes=5)
            if due > now:
                return due
        return None
    return None


def test_defaults_match_legacy_rules_all_day_every_reason():
    cfg = cc.load({})
    base = TZ.localize(datetime(2026, 7, 14, 0, 0))
    checked = 0
    for reason in ["Period Off", "Day Off", "Night Off", cc.OTHER_REASON, "Van", "", "nonsense"]:
        for minutes in range(0, 24 * 60, 7):
            now = base + timedelta(minutes=minutes)
            got = cc.due_back(cfg, reason, now, TZ, lambda: SCHEDULE)
            want = _legacy_due_back(reason, now)
            assert got == want, (reason, now, got, want)
            checked += 1
    assert checked > 1000


def test_defaults_are_bauercrest():
    cfg = cc.load({})
    assert cfg.name == "Camp Bauercrest"
    assert cfg.reason_names == ["Period Off", "Day Off", "Night Off", "Other (type reason)"]
    assert cfg.forgot_threshold_minutes == 180 and cfg.fork_no_deadline_hours == 14
    assert cfg.nightly_summary_hour == 0 and cfg.auto_archive_hour == 3
    assert cfg.vans == ("Van 1", "Van 2", "Van 3")
    assert cfg.van_labels["Van 1"] == "Van 1 (White)"
    assert cfg.spreadsheet_id.startswith("1oS7")


def test_other_camp_must_name_its_own_sheet():
    try:
        cc.load({"camp_name": "Camp Example"})
    except cc.ConfigError as e:
        assert "spreadsheet_id" in str(e)
    else:
        raise AssertionError("a non-Bauercrest camp without spreadsheet_id must not load")
    cfg = cc.load({"camp_name": "Camp Example", "spreadsheet_id": "abc123"})
    assert cfg.spreadsheet_id == "abc123"


def test_new_camp_has_no_bauercrest_van_colors():
    cfg = cc.load({"camp_name": "X", "spreadsheet_id": "s", "vans": ["Bus A", "Bus B"]})
    assert cfg.vans == ("Bus A", "Bus B") and cfg.van_labels == {}


def test_custom_reasons_and_rules():
    cfg = cc.load({
        "camp_name": "X", "spreadsheet_id": "s",
        "reasons": [
            {"name": "Free Swim Break", "rule": "period_end", "grace_min": 10},
            {"name": "Overnight Trip", "rule": "day_after", "hour": 9, "minute": 0, "nextday_cutoff_hour": 18},
            {"name": "Late Night", "rule": "next_clock", "hour": 23, "minute": 30},
            "Doctor",
            {"name": cc.OTHER_REASON},
        ],
    })
    assert cfg.reason_names == ["Free Swim Break", "Overnight Trip", "Late Night", "Doctor", cc.OTHER_REASON]
    now = TZ.localize(datetime(2026, 7, 14, 9, 20))
    due = cc.due_back(cfg, "Free Swim Break", now, TZ, lambda: SCHEDULE)
    assert due == TZ.localize(datetime(2026, 7, 14, 9, 55))  # period 1 ends 10:05, minus 10
    assert cc.due_back(cfg, "Late Night", now, TZ, lambda: SCHEDULE) == TZ.localize(datetime(2026, 7, 14, 23, 30))
    # 9:20 AM is before the 18:00 cutoff, so the day off is today, back 9:00 tomorrow... the day AFTER
    assert cc.due_back(cfg, "Overnight Trip", now, TZ, lambda: SCHEDULE) == TZ.localize(datetime(2026, 7, 15, 9, 0))
    assert cc.due_back(cfg, "Doctor", now, TZ, lambda: SCHEDULE) is None


def test_bad_settings_give_clear_errors():
    base = {"camp_name": "X", "spreadsheet_id": "s"}
    bad = [
        ({"reasons": [{"name": "A", "rule": "sometimes"}]}, "rule must be one of"),
        ({"reasons": [{"rule": "none"}]}, "missing a 'name'"),
        ({"reasons": [{"name": "A", "rule": "next_clock", "hour": 25}]}, "hour must be 0-23"),
        ({"reasons": ["A", "A"]}, "unique name"),
        ({"reasons": ["Van"]}, "reserved"),
        ({"nightly_summary_hour": 24}, "between 0 and 23"),
        ({"forgot_threshold_minutes": "soon"}, "whole number"),
        ({"default_schedule": [["Only name"]]}, "default_schedule[0]"),
    ]
    for extra, needle in bad:
        try:
            cc.load({**base, **extra})
        except cc.ConfigError as e:
            assert needle in str(e), (extra, str(e))
        else:
            raise AssertionError(f"expected ConfigError for {extra}")


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print("PASS", t.__name__)
    print("ALL PASS")
