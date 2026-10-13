"""Tests for provision/seed_sheet.py. Run:  python tests/test_seed_sheet.py  (or pytest)."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "provision"))
import seed_sheet as ss  # noqa: E402


class FakeWS:
    def __init__(self, title, values):
        self.title, self.values, self.written = title, values, None

    def get_all_values(self):
        return self.values

    def update(self, range_name, values, value_input_option):
        self.written = (range_name, values, value_input_option)


def test_rows_land_under_the_header_by_column_name_as_raw_text():
    ws = FakeWS("staff", [["name", "pin", "active", "admin"]])
    n = ss.fill_tab(ws, [{"name": "Alex", "pin": "0427", "active": "TRUE", "admin": "", "extra": "x"}])
    assert n == 1
    assert ws.written == ("A2", [["Alex", "0427", "TRUE", ""]], "RAW")   # leading zero kept, extras ignored


def test_header_order_is_respected_even_if_columns_are_rearranged():
    ws = FakeWS("staff", [["active", "name", "admin", "pin"]])
    ss.fill_tab(ws, [{"name": "Alex", "pin": "0427", "active": "TRUE", "admin": "TRUE"}])
    assert ws.written[1] == [["TRUE", "Alex", "TRUE", "0427"]]


def test_refuses_to_duplicate_into_a_tab_with_data_and_to_write_without_a_header():
    for values, needle in [([["name"], ["Existing"]], "refusing"), ([], "no header")]:
        try:
            ss.fill_tab(FakeWS("staff", values), [{"name": "A"}])
        except SystemExit as e:
            assert needle in str(e)
        else:
            raise AssertionError("expected SystemExit")
    ws = FakeWS("staff", [["name"], ["Existing"]])
    assert ss.fill_tab(ws, [{"name": "A"}], force=True) == 1


def test_demo_days_off_cover_every_weekday():
    rows = ss.demo_days_off([f"P{i}" for i in range(30)])
    assert {r["weekday"] for r in rows} == set(ss.WEEKDAYS) and all(r["active"] == "TRUE" for r in rows)


if __name__ == "__main__":
    for k, v in sorted(globals().items()):
        if k.startswith("test_"):
            v()
            print("PASS", k)
    print("ALL PASS")
