"""Tests for provision/reset_demo.py. Run:  python tests/test_reset_demo.py  (or pytest)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "provision"))
import reset_demo as rd  # noqa: E402


class WS:
    def __init__(self, rows):
        self.rows, self.cleared, self.cells = rows, [], []

    def get_all_values(self):
        return self.rows

    def batch_clear(self, ranges):
        self.cleared += ranges

    def update_cell(self, r, c, v):
        self.cells.append((r, c, v))


class Sheet:
    def __init__(self):
        self.tabs = {
            "logs": WS([["id"], ["a"], ["b"]]), "vans": WS([["id"]]), "current_status": WS([["name"], ["x"]]),
            "settings": WS([["key", "value"], ["emergency", "TRUE"]]), "staff": WS([["name"], ["Keep"]]),
        }

    def worksheet(self, n):
        return self.tabs[n]


def test_clears_history_and_emergency_but_never_touches_staff():
    s = Sheet()
    res = rd.reset(s, "Camp Demo")
    assert res == {"logs": 2, "vans": 0, "current_status": 1, "emergency": 1}
    assert s.tabs["logs"].cleared == ["A2:Z7"] and s.tabs["vans"].cleared == []
    assert s.tabs["settings"].cells == [(2, 2, "FALSE")]
    assert s.tabs["staff"].cleared == []


def test_refuses_any_camp_that_is_not_the_demo():
    for name in ["Camp Bauercrest", "", "Camp Demo 2", "camp demo"]:
        s = Sheet()
        try:
            rd.reset(s, name)
        except SystemExit as e:
            assert "Refusing" in str(e)
        else:
            raise AssertionError(f"must refuse {name!r}")
        assert s.tabs["logs"].cleared == []


if __name__ == "__main__":
    for k, v in sorted(globals().items()):
        if k.startswith("test_"):
            v()
            print("PASS", k)
    print("ALL PASS")
