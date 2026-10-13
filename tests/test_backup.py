"""Tests for provision/backup_camp.py. Run:  python tests/test_backup.py  (or pytest)."""
import csv
import os
import sys
import tempfile
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "provision"))
import backup_camp as bc  # noqa: E402


class FakeWS:
    def __init__(self, title, rows):
        self.title, self._rows = title, rows

    def get_all_values(self):
        return self._rows


class FakeSheet:
    def __init__(self, tabs):
        self._tabs = tabs

    def worksheets(self):
        return [FakeWS(t, r) for t, r in self._tabs.items()]


SECRETS = {"camp_name": "Camp Example"}
TABS = {
    "staff": [["name", "pin"], ["José Núñez", "0427"], ['Jo "JJ" Park', "9021"]],
    "logs": [["id", "timestamp"]],
    "weird/tab name": [["a"], ["b"]],
}


def test_every_tab_saved_with_header_and_special_characters():
    with tempfile.TemporaryDirectory() as d:
        res = bc.backup(SECRETS, d, 30, today=date(2026, 7, 14), spreadsheet=FakeSheet(TABS))
        day = Path(res["dir"])
        assert day.parts[-2:] == ("camp-example", "2026-07-14")
        assert res["tabs"] == {"staff": 2, "logs": 0, "weird/tab name": 1}
        rows = list(csv.reader((day / "staff.csv").open(encoding="utf-8-sig")))
        assert rows == TABS["staff"]                       # accents, quotes and leading zeros survive
        assert (day / "weird_tab_name.csv").exists()       # no path tricks from a tab name


def test_old_backups_pruned_but_other_folders_left_alone():
    with tempfile.TemporaryDirectory() as d:
        base = Path(d) / "camp-example"
        for name in ["2026-05-01", "2026-06-20", "2026-07-10", "notes", "2026-13-99"]:
            (base / name).mkdir(parents=True)
        res = bc.backup(SECRETS, d, 30, today=date(2026, 7, 14), spreadsheet=FakeSheet(TABS))
        assert res["removed"] == ["2026-05-01"]
        left = sorted(p.name for p in base.iterdir())
        assert left == ["2026-06-20", "2026-07-10", "2026-07-14", "2026-13-99", "notes"]


def test_rerun_same_day_overwrites_cleanly():
    with tempfile.TemporaryDirectory() as d:
        bc.backup(SECRETS, d, 30, today=date(2026, 7, 14), spreadsheet=FakeSheet(TABS))
        res = bc.backup(SECRETS, d, 30, today=date(2026, 7, 14), spreadsheet=FakeSheet({"staff": [["name"], ["A"]]}))
        assert res["tabs"] == {"staff": 1}


if __name__ == "__main__":
    for k, v in sorted(globals().items()):
        if k.startswith("test_"):
            v()
            print("PASS", k)
    print("ALL PASS")
