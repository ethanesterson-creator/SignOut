"""Fill a new camp's Sheet with its staff, drivers and (for a demo) days off.

    python provision/seed_sheet.py camps/<camp>/secrets.toml [--demo-days-off]

Run it AFTER the camp's tabs exist (Admin > New Camp Setup > Create Any Missing Tabs).
It reads camps/<camp>/staff_import.csv and drivers_import.csv, which new_camp.py wrote,
and writes the rows under each tab's header. It refuses to write into a tab that already
has data, so running it twice cannot create duplicates.
"""

import argparse
import csv
import random
import sys
import tomllib
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def read_csv_rows(path: Path) -> list:
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def fill_tab(ws, rows: list, force: bool = False) -> int:
    """Write dict rows under the tab's header, matching columns by header name."""
    values = ws.get_all_values()
    if not values:
        raise SystemExit(f"Tab '{ws.title}' has no header row. Run Create Any Missing Tabs first.")
    if len(values) > 1 and not force:
        raise SystemExit(f"Tab '{ws.title}' already has {len(values) - 1} rows; refusing to add duplicates.")
    header = [h.strip() for h in values[0]]
    out = [[str(r.get(h, "")) for h in header] for r in rows]
    if out:
        ws.update(range_name="A2", values=out, value_input_option="RAW")
    return len(out)


def demo_days_off(staff_names: list, seed: int = 11) -> list:
    """A few fake weekly days off so the Day Off board has something to show."""
    rng = random.Random(seed)
    rows = []
    for i, name in enumerate(staff_names[: len(WEEKDAYS) * 3]):
        rows.append({"name": name, "weekday": WEEKDAYS[i % 7], "active": "TRUE"})
    rng.shuffle(rows)
    return rows


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("secrets")
    ap.add_argument("--demo-days-off", action="store_true")
    ap.add_argument("--force", action="store_true", help="write even if a tab already has rows")
    args = ap.parse_args(argv)

    sp = Path(args.secrets)
    with sp.open("rb") as f:
        secrets = tomllib.load(f)
    creds = Credentials.from_service_account_info(dict(secrets["gcp_service_account"]), scopes=SCOPES)
    sheet = gspread.authorize(creds).open_by_key(secrets["spreadsheet_id"])

    staff = read_csv_rows(sp.parent / "staff_import.csv")
    n = fill_tab(sheet.worksheet("staff"), staff, args.force)
    print(f"staff: wrote {n} rows")
    drivers_path = sp.parent / "drivers_import.csv"
    if drivers_path.exists():
        n = fill_tab(sheet.worksheet("drivers"), read_csv_rows(drivers_path), args.force)
        print(f"drivers: wrote {n} rows")
    if args.demo_days_off:
        n = fill_tab(sheet.worksheet("days_off"), demo_days_off([r["name"] for r in staff]), args.force)
        print(f"days_off: wrote {n} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
