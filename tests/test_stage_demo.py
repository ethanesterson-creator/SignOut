"""Tests for provision/stage_demo.py. Run:  python tests/test_stage_demo.py  (or pytest)."""
import sys
import tempfile
from datetime import datetime, time
from pathlib import Path

import pytz

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "provision"))
import camp_config as cc  # noqa: E402
import stage_demo as sd  # noqa: E402

TZ = pytz.timezone("US/Eastern")
CFG = cc.load({"camp_name": "Camp Demo", "spreadsheet_id": "s", "vans": ["Van 1", "Van 2"]})
SCHED = [("Period 1", time(9, 0), time(9, 50)), ("Period 2", time(10, 0), time(10, 50)),
         ("Period 3", time(11, 0), time(11, 50)), ("Rest", time(13, 0), time(14, 0)),
         ("Period 4", time(14, 10), time(15, 0)), ("Period 5", time(15, 10), time(16, 0))]
STAFF = [f"Person {i}" for i in range(40)]
DRIVERS = ["Driver One", "Driver Two"]


def scene(hour, minute=0, cfg=CFG):
    return sd.build_scene(cfg, TZ.localize(datetime(2026, 7, 14, hour, minute)), STAFF, DRIVERS, SCHED)


def test_afternoon_scene_has_on_time_late_forgot_and_a_van():
    s = scene(16, 40)
    names = [r["name"] for r in s["status"]]
    assert len(set(names)) == len(names)                          # nobody is out twice
    assert len({r["id"] for r in s["logs"]}) == len(s["logs"])    # unique row ids
    reasons = [r["reason"] for r in s["status"]]
    assert "Night Off" in reasons and "Van" in reasons and "Other (type reason)" in reasons
    now = TZ.localize(datetime(2026, 7, 14, 16, 40))
    overdue = [r for r in s["status"] if r["due_back"] and datetime.fromisoformat(r["due_back"]) < now]
    assert len(overdue) >= 2
    assert s["vans"][0]["van"] == "Van 1" and s["vans"][0]["driver"] == "Driver One"


def test_before_the_day_starts_there_are_no_late_period_offs_but_the_scene_still_builds():
    s = scene(8, 0)
    assert s["status"] and s["vans"]
    assert [r for r in s["status"] if r["reason"] == "Period Off"] == []


def test_camp_without_vans_stages_no_van_trip():
    cfg = cc.load({"camp_name": "Camp Demo", "spreadsheet_id": "s", "vans": []})
    s = scene(16, 40, cfg)
    assert s["vans"] == [] and all(r["reason"] != "Van" for r in s["status"])


def test_refuses_any_camp_that_is_not_the_demo():
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "s.toml"
        p.write_text('camp_name = "Camp Bauercrest"\nspreadsheet_id = "x"\n[gcp_service_account]\ntype = "service_account"\n',
                     encoding="utf-8")
        try:
            sd.main([str(p)])
        except SystemExit as e:
            assert "Refusing" in str(e)
        else:
            raise AssertionError("must refuse a real camp")


if __name__ == "__main__":
    for k, v in sorted(globals().items()):
        if k.startswith("test_"):
            v()
            print("PASS", k)
    print("ALL PASS")
