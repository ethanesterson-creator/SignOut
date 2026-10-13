"""Wipe the demo camp's sign-in/out history so the next showing starts clean.

    python provision/reset_demo.py camps/camp-demo/secrets.toml

Clears the logs, vans and current_status tabs and any campwide emergency. Staff, drivers,
days off and the schedule are left alone. It REFUSES to run unless the secrets are for the
camp named "Camp Demo", so it can never wipe a real camp's records.
"""

import sys
import tomllib

import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
DEMO_NAME = "Camp Demo"
CLEAR_TABS = ("logs", "vans", "current_status")


def reset(spreadsheet, camp_name: str) -> dict:
    if camp_name != DEMO_NAME:
        raise SystemExit(f"Refusing to reset '{camp_name}': this tool only works on '{DEMO_NAME}'.")
    cleared = {}
    for tab in CLEAR_TABS:
        ws = spreadsheet.worksheet(tab)
        n = max(0, len(ws.get_all_values()) - 1)
        if n:
            ws.batch_clear([f"A2:Z{n + 5}"])
        cleared[tab] = n
    settings = spreadsheet.worksheet("settings")
    for i, row in enumerate(settings.get_all_values(), start=1):
        if row and row[0] == "emergency" and len(row) > 1 and row[1].strip().upper() != "FALSE":
            settings.update_cell(i, 2, "FALSE")
            cleared["emergency"] = 1
    return cleared


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    if len(argv) != 1:
        raise SystemExit("usage: python provision/reset_demo.py camps/camp-demo/secrets.toml")
    with open(argv[0], "rb") as f:
        secrets = tomllib.load(f)
    name = str(secrets.get("camp_name", ""))
    creds = Credentials.from_service_account_info(dict(secrets["gcp_service_account"]), scopes=SCOPES)
    sheet = gspread.authorize(creds).open_by_key(secrets["spreadsheet_id"])
    res = reset(sheet, name)
    print("Demo reset. Rows cleared: " + ", ".join(f"{k}={v}" for k, v in res.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
