"""Tests for provision/new_camp.py. Run:  python tests/test_provision.py  (or pytest)."""
import csv
import json
import os
import sys
import tempfile
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "provision"))
import camp_config  # noqa: E402
import new_camp  # noqa: E402

SHEET_URL = "https://docs.google.com/spreadsheets/d/1AbCdEfGhIjKlMnOpQrStUvWxYz0123456789/edit#gid=0"
SA = {
    "type": "service_account", "project_id": "signout-fleet", "private_key_id": "k1",
    "private_key": "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n",
    "client_email": "example@signout-fleet.iam.gserviceaccount.com", "client_id": "1",
    "token_uri": "https://oauth2.googleapis.com/token",
}


def _setup(tmp: Path, with_sheet=True, with_key=True, with_staff=True):
    text = (ROOT / "provision" / "intake_template.toml").read_text(encoding="utf-8")
    if with_key:
        (tmp / "key.json").write_text(json.dumps(SA), encoding="utf-8")
        text = text.replace('service_account_json = ""', f'service_account_json = "{(tmp / "key.json").as_posix()}"')
    if with_sheet:
        text = text.replace('spreadsheet_url = ""', f'spreadsheet_url = "{SHEET_URL}"')
    if with_staff:
        (tmp / "staff.csv").write_text(
            "name,admin,driver\nAlex Rivera,TRUE,TRUE\nSam Lee,,\nJo \"JJ\" Park,,TRUE\n", encoding="utf-8")
        text = text.replace('staff_csv = ""', f'staff_csv = "{(tmp / "staff.csv").as_posix()}"')
    p = tmp / "intake.toml"
    p.write_text(text, encoding="utf-8")
    return str(p)


def test_full_run_produces_valid_secrets():
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        res = new_camp.provision(_setup(tmp), str(tmp / "camps"))
        assert res["missing"] == []
        out = Path(res["dir"])
        assert {"secrets.toml", "staff_import.csv", "drivers_import.csv", "director_email.txt",
                "CHECKLIST.md", "credentials.txt"} <= set(res["written"])
        sec = tomllib.loads((out / "secrets.toml").read_text(encoding="utf-8"))
        cfg = camp_config.load(sec)
        assert cfg.name == "Camp Example"
        assert cfg.spreadsheet_id == "1AbCdEfGhIjKlMnOpQrStUvWxYz0123456789"
        assert cfg.reason_names[-1] == camp_config.OTHER_REASON and len(cfg.reasons) == 4
        assert cfg.vans == ("Van 1", "Van 2") and cfg.van_labels["Van 2"] == "Van 2 (Green)"
        assert cfg.default_schedule[0] == ("Period 1", "09:00", "09:50")
        assert sec["gcp_service_account"]["client_email"] == SA["client_email"]
        assert "\n" in sec["gcp_service_account"]["private_key"]  # key newlines survive
        assert len(sec["admin_password"]) >= 10 and len(sec["pin_salt"]) == 32
        assert sec["ntfy_topic"] != sec["ntfy_topic_vans"]
        assert "Camp Bauercrest" not in json.dumps(sec) and "1oS7" not in json.dumps(sec)
        email = (out / "director_email.txt").read_text(encoding="utf-8")
        assert SA["client_email"] in email and "Camp Example" in email


def test_staff_codes_unique_and_flags_carried():
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        res = new_camp.provision(_setup(tmp), str(tmp / "camps"))
        rows = list(csv.DictReader((Path(res["dir"]) / "staff_import.csv").open(encoding="utf-8")))
        assert [r["name"] for r in rows] == ["Alex Rivera", "Sam Lee", 'Jo "JJ" Park']
        pins = [r["pin"] for r in rows]
        assert len(set(pins)) == 3 and all(len(p) == 4 and p.isdigit() for p in pins)
        assert rows[0]["admin"] == "TRUE" and rows[1]["admin"] == ""
        drivers = list(csv.DictReader((Path(res["dir"]) / "drivers_import.csv").open(encoding="utf-8")))
        assert [r["name"] for r in drivers] == ["Alex Rivera", 'Jo "JJ" Park']


def test_pins_skip_guessable_and_scale():
    pins = new_camp.make_pins(300)
    assert len(set(pins)) == 300 and not (set(pins) & new_camp._BAD_PINS)


def test_rerun_keeps_credentials_stable():
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        path = _setup(tmp)
        a = new_camp.provision(path, str(tmp / "camps"))
        c1 = (Path(a["dir"]) / "credentials.txt").read_text(encoding="utf-8")
        new_camp.provision(path, str(tmp / "camps"))
        c2 = (Path(a["dir"]) / "credentials.txt").read_text(encoding="utf-8")
        assert c1 == c2  # re-running must not change the admin password or salt of a live camp


def test_first_stage_before_camp_replies():
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        res = new_camp.provision(_setup(tmp, with_sheet=False, with_staff=False), str(tmp / "camps"))
        assert "secrets.toml" not in res["written"]
        assert any("spreadsheet_url" in m for m in res["missing"])
        assert SA["client_email"] in (Path(res["dir"]) / "director_email.txt").read_text(encoding="utf-8")
        assert not (Path(res["dir"]) / "secrets.toml").exists()


def test_errors_are_plain_and_nothing_secret_is_written():
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        base = _setup(tmp)
        text = Path(base).read_text(encoding="utf-8")
        cases = [
            (text.replace('spreadsheet_url = "' + SHEET_URL + '"', 'spreadsheet_url = "https://example.com"'), "Google Sheets link"),
            (text.replace('timezone = "US/Eastern"', 'timezone = "Mars/Base"'), "timezone"),
            (text.replace('rule = "period_end"', 'rule = "whenever"'), "rule must be one of"),
            (text.replace('camp_name = "Camp Example"', 'camp_name = ""'), "camp_name"),
        ]
        for i, (t, needle) in enumerate(cases):
            p = tmp / f"bad{i}.toml"
            p.write_text(t, encoding="utf-8")
            try:
                new_camp.provision(str(p), str(tmp / f"out{i}"))
            except new_camp.IntakeError as e:
                assert needle in str(e), (needle, str(e))
            else:
                raise AssertionError(f"expected IntakeError containing {needle!r}")
        # duplicate staff names would collide on codes
        (tmp / "staff.csv").write_text("name\nAlex\nalex\n", encoding="utf-8")
        try:
            new_camp.provision(base, str(tmp / "dup"))
        except new_camp.IntakeError as e:
            assert "more than once" in str(e)
        else:
            raise AssertionError("duplicate names must be rejected")


if __name__ == "__main__":
    for k, v in sorted(globals().items()):
        if k.startswith("test_"):
            v()
            print("PASS", k)
    print("ALL PASS")
