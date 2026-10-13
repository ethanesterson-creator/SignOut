"""Save a read-only copy of every tab of a camp's Google Sheet to files on this computer.

    python provision/backup_camp.py camps/<camp>/secrets.toml [--out backups] [--keep 30]

Writes backups/<camp>/<YYYY-MM-DD>/<tab>.csv, one file per tab, and removes copies
older than --keep days. It only ever READS the Sheet. Run it nightly (Windows Task
Scheduler, or a scheduled job later) for every camp you operate.

Why this exists: the camp owns its Sheet and Google keeps a version history, but a
copy you hold yourself means a mistaken bulk delete or a bad admin action can be
undone even if nobody noticed for a while.
"""

import argparse
import csv
import shutil
import sys
import tomllib
from datetime import date, datetime, timedelta
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def load_secrets(path: str) -> dict:
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        raise SystemExit(f"Secrets file not found: {path}")
    except tomllib.TOMLDecodeError as e:
        raise SystemExit(f"Secrets file isn't valid TOML: {e}")


def camp_slug(secrets: dict) -> str:
    import re
    name = str(secrets.get("camp_name", "camp-bauercrest"))
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "camp"


def safe_filename(title: str) -> str:
    import re
    return re.sub(r"[^A-Za-z0-9._-]+", "_", title).strip("_") or "tab"


def write_tab(path: Path, rows: list):
    """CSV with a UTF-8 BOM so Excel opens accented names correctly."""
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerows(rows)


def prune(camp_dir: Path, keep_days: int, today: date) -> list:
    """Delete dated folders older than keep_days. Only folders named YYYY-MM-DD are touched."""
    removed = []
    cutoff = today - timedelta(days=keep_days)
    for d in camp_dir.iterdir() if camp_dir.exists() else []:
        if not d.is_dir():
            continue
        try:
            when = datetime.strptime(d.name, "%Y-%m-%d").date()
        except ValueError:
            continue
        if when < cutoff:
            shutil.rmtree(d)
            removed.append(d.name)
    return removed


def backup(secrets: dict, out_root: str = "backups", keep_days: int = 30, today: date = None, spreadsheet=None) -> dict:
    """Back up every tab. `spreadsheet` can be injected for tests."""
    today = today or date.today()
    if spreadsheet is None:
        sheet_id = str(secrets.get("spreadsheet_id", "")).strip()
        if not sheet_id:
            raise SystemExit("These secrets have no spreadsheet_id.")
        creds = Credentials.from_service_account_info(dict(secrets["gcp_service_account"]), scopes=SCOPES)
        spreadsheet = gspread.authorize(creds).open_by_key(sheet_id)

    day_dir = Path(out_root) / camp_slug(secrets) / today.isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)
    saved = {}
    for ws in spreadsheet.worksheets():
        rows = ws.get_all_values()
        write_tab(day_dir / f"{safe_filename(ws.title)}.csv", rows)
        saved[ws.title] = max(0, len(rows) - 1)  # data rows, not counting the header
    removed = prune(day_dir.parent, keep_days, today)
    return {"dir": str(day_dir), "tabs": saved, "removed": removed}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("secrets")
    ap.add_argument("--out", default="backups")
    ap.add_argument("--keep", type=int, default=30, help="days of backups to keep (default 30)")
    args = ap.parse_args(argv)
    res = backup(load_secrets(args.secrets), args.out, args.keep)
    print(f"Saved {len(res['tabs'])} tabs to {res['dir']}")
    for name, n in res["tabs"].items():
        print(f"  {name}: {n} rows")
    if res["removed"]:
        print("Removed old backups: " + ", ".join(res["removed"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
