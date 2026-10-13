"""Put a realistic, busy moment on the demo camp's board.

    python provision/stage_demo.py camps/camp-demo/secrets.toml [--emergency]

Clears the demo's history first, then writes people who are out right now: some
on time, two late, one who never signed back in, plus a van on a trip. Everything is
relative to the current time, so the board looks alive whenever you run it. Use it
before a showing or a screenshot, and reset_demo.py afterward. Like reset_demo it
REFUSES to touch any camp that is not "Camp Demo".
"""

import argparse
import sys
import tomllib
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pytz

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import camp_config  # noqa: E402
import reset_demo  # noqa: E402

LOG_HEADERS = ["id", "timestamp", "name", "reason", "other_reason", "action", "status", "due_back", "late"]
STATUS_HEADERS = ["name", "status", "reason", "other_reason", "timestamp", "due_back", "id"]
VAN_HEADERS = ["id", "timestamp", "van", "driver", "purpose", "passengers", "other_purpose", "action", "status", "gas_left"]


def _iso(dt):
    return dt.isoformat(timespec="seconds")


def build_scene(cfg, now, staff, drivers, schedule):
    """Return {'status': [...], 'logs': [...], 'vans': [...]} as lists of dicts.

    `schedule` is [(name, start_time, end_time)]. `now` is timezone-aware.
    """
    tz = now.tzinfo
    pool = iter(list(staff))
    out = []  # (name, reason, other, signed_out_at)

    def add(reason, other, minutes_ago=None, at=None):
        name = next(pool)
        out.append((name, reason, other, at or now - timedelta(minutes=minutes_ago)))
        return name

    period_reason = next((r.name for r in cfg.reasons if r.rule == camp_config.RULE_PERIOD_END), None)
    night_reason = next((r.name for r in cfg.reasons if r.rule == camp_config.RULE_NEXT_CLOCK), None)
    day_reason = next((r.name for r in cfg.reasons if r.rule == camp_config.RULE_DAY_AFTER), None)

    # On time
    if period_reason:
        for _p, start, end in schedule:
            s = tz.localize(datetime.combine(now.date(), start))
            e = tz.localize(datetime.combine(now.date(), end))
            if s <= now - timedelta(minutes=4) and now < e - timedelta(minutes=8):
                add(period_reason, "", at=now - timedelta(minutes=4))
                break
    if night_reason:
        add(night_reason, "", 22)
    if day_reason:
        add(day_reason, "", 140)
    if cfg.has_free_text_reason:
        add(camp_config.OTHER_REASON, "Dentist appointment", 75)

    # Late: a period-off from an earlier period today whose deadline has passed
    late_made = 0
    if period_reason:
        for _p, start, end in reversed(schedule):
            due = tz.localize(datetime.combine(now.date(), end)) - timedelta(minutes=5)
            mins_late = (now - due).total_seconds() / 60
            if 12 <= mins_late <= 170 and late_made < 2:
                add(period_reason, "", at=tz.localize(datetime.combine(now.date(), start)) + timedelta(minutes=6))
                late_made += 1

    # Never signed back in: out last night, deadline long gone
    if night_reason:
        yday = (now - timedelta(days=1)).replace(hour=21, minute=40, second=0, microsecond=0)
        add(night_reason, "", at=yday)

    status, logs = [], []
    for name, reason, other, when in out:
        rid = str(uuid.uuid4())
        due = camp_config.due_back(cfg, reason, when, tz, lambda: schedule)
        row = {"id": rid, "timestamp": _iso(when), "name": name, "reason": reason, "other_reason": other,
               "action": "OUT", "status": "OUT", "due_back": _iso(due) if due else "", "late": ""}
        logs.append(row)
        status.append({"name": name, "status": "OUT", "reason": reason, "other_reason": other,
                       "timestamp": _iso(when), "due_back": row["due_back"], "id": rid})

    vans = []
    if cfg.vans and drivers:
        driver = drivers[0]
        riders = [next(pool), next(pool)]
        when = now - timedelta(minutes=26)
        vid = str(uuid.uuid4())
        vans.append({"id": vid, "timestamp": _iso(when), "van": cfg.vans[0], "driver": driver,
                     "purpose": cfg.van_purposes[0] if cfg.van_purposes else "Other",
                     "passengers": ", ".join(riders), "other_purpose": "", "action": "OUT",
                     "status": "OUT", "gas_left": "3/4"})
        for who in [driver] + riders:
            rid = str(uuid.uuid4())
            logs.append({"id": rid, "timestamp": _iso(when), "name": who, "reason": camp_config.VAN_REASON,
                         "other_reason": "", "action": "OUT", "status": "OUT", "due_back": "", "late": ""})
            status.append({"name": who, "status": "OUT", "reason": camp_config.VAN_REASON, "other_reason": "",
                           "timestamp": _iso(when), "due_back": "", "id": rid})
    return {"status": status, "logs": logs, "vans": vans}


def _write(ws, headers_expected, rows):
    values = ws.get_all_values()
    header = [h.strip() for h in values[0]] if values else headers_expected
    if rows:
        ws.update(range_name="A2", values=[[str(r.get(h, "")) for h in header] for r in rows],
                  value_input_option="RAW")


def _parse_hhmm(s):
    return datetime.strptime(str(s).strip(), "%H:%M").time()


def main(argv=None):
    import gspread
    from google.oauth2.service_account import Credentials

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("secrets")
    ap.add_argument("--emergency", action="store_true", help="also declare a sample campwide emergency")
    args = ap.parse_args(argv)
    with open(args.secrets, "rb") as f:
        secrets = tomllib.load(f)
    cfg = camp_config.load(secrets)
    if cfg.name != reset_demo.DEMO_NAME:
        raise SystemExit(f"Refusing to stage '{cfg.name}': this tool only works on '{reset_demo.DEMO_NAME}'.")
    creds = Credentials.from_service_account_info(
        dict(secrets["gcp_service_account"]), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    sheet = gspread.authorize(creds).open_by_key(secrets["spreadsheet_id"])
    reset_demo.reset(sheet, cfg.name)

    tz = pytz.timezone(cfg.timezone)
    now = datetime.now(tz)
    staff_rows = sheet.worksheet("staff").get_all_records()
    staff = [r["name"] for r in staff_rows if str(r.get("admin", "")).upper() != "TRUE" and r.get("name")]
    drv_rows = sheet.worksheet("drivers").get_all_records()
    drivers = [r["name"] for r in drv_rows if str(r.get("passed_test", "")).upper() == "TRUE"]
    sched_rows = sheet.worksheet("schedule").get_all_records()
    schedule = [(r["period"], _parse_hhmm(r["start"]), _parse_hhmm(r["end"])) for r in sched_rows]

    scene = build_scene(cfg, now, staff[8:], drivers, schedule)  # the first few stay free for the demo
    _write(sheet.worksheet("current_status"), STATUS_HEADERS, scene["status"])
    _write(sheet.worksheet("logs"), LOG_HEADERS, scene["logs"])
    _write(sheet.worksheet("vans"), VAN_HEADERS, scene["vans"])
    if args.emergency:
        ws = sheet.worksheet("settings")
        vals = ws.get_all_values()
        for i, row in enumerate(vals, start=1):
            if row and row[0] == "emergency":
                ws.update_cell(i, 2, "TRUE")
        keys = [r[0] for r in vals if r]
        msg = "Severe thunderstorm. Everyone report to your cabin counselors now."
        if "emergency_message" in keys:
            ws.update_cell(keys.index("emergency_message") + 1, 2, msg)
        else:
            ws.append_row(["emergency_message", msg])
    print(f"Staged: {len(scene['status'])} people out, {len(scene['vans'])} van trip."
          + (" Emergency on." if args.emergency else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
