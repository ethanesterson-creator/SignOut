"""Late-alert worker: a small process that runs next to the app and needs no browser.

The app only notices a late person while some page with a board is open. A kiosk parked
on the Sign In page does no checking, so this worker does it instead: every 30 seconds it
reads who is out straight from the camp's Google Sheet, works out who has just gone past
their deadline, and sends one push and one email for each. It shares the app's own record
of who has been alerted (the late_alerted setting), so nobody is alerted twice.

It also writes a heartbeat into the settings tab (late_worker_ok, late_worker_error) so
anyone can see it is alive.

    python late_worker.py            # reads .streamlit/secrets.toml, the same file the app uses

Exit code 3 means "this camp has not turned the worker on" (server_late_check = false);
start.sh then stops trying.
"""

import sys
import time
import tomllib
from datetime import datetime, time as dtime
from pathlib import Path

import pytz

import camp_config
import emailalert

LATE_KEY = "late_alerted"
TICK_SECONDS = 30
HEARTBEAT_SECONDS = 120
KEEP_ALERTED = 300
SCHEDULE_CACHE_SECONDS = 300
DISABLED_EXIT = 3


# ---------------------------------------------------------------------------
# Pure logic (no network, so it can be tested)
# ---------------------------------------------------------------------------

def parse_time(text):
    """'9:15', '09:15', '14:15', '2:15 PM' -> a time. None if unusable."""
    text = str(text or "").strip()
    for fmt in ("%H:%M", "%I:%M %p", "%I:%M%p", "%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return None


def parse_stamp(text, tz):
    """A timestamp cell (ISO with offset, or naive) as an aware datetime in camp time."""
    text = str(text or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return tz.localize(dt)
    return dt.astimezone(tz)


def schedule_from_rows(rows):
    """[(name, start, end)] from the schedule tab's rows (a header row first)."""
    out = []
    for r in rows[1:]:
        if len(r) < 3:
            continue
        start, end = parse_time(r[1]), parse_time(r[2])
        if start and end:
            out.append((str(r[0]).strip(), start, end))
    return sorted(out, key=lambda p: p[1])


def default_schedule(cfg):
    out = []
    for name, s, e in cfg.default_schedule:
        start, end = parse_time(s), parse_time(e)
        if start and end:
            out.append((name, start, end))
    return sorted(out, key=lambda p: p[1])


def due_text(due, now):
    """'1:28 PM' for today, 'Tue 12:15 AM' otherwise. Matches the app's board."""
    if due.date() == now.date():
        return due.strftime("%I:%M %p").lstrip("0")
    return due.strftime("%a %I:%M %p").replace(" 0", " ")


def newly_late(status_rows, already, now, cfg, tz, schedule):
    """People who are out, past their deadline by a full minute or more, and not yet
    alerted. status_rows are dicts with name, status, reason, timestamp, id."""
    found = []
    for r in status_rows:
        if str(r.get("status", "")).strip().upper() != "OUT":
            continue
        rid = str(r.get("id", "")).strip()
        if not rid or rid in already:
            continue
        signed_out = parse_stamp(r.get("timestamp"), tz)
        if signed_out is None:
            continue
        reason = str(r.get("reason", "")).strip()
        due = camp_config.due_back(cfg, reason, signed_out, tz, lambda: schedule)
        if due is None:
            continue
        mins = int((now - due).total_seconds() // 60)
        if mins > 0:
            found.append({
                "id": rid, "name": str(r.get("name", "")).strip(), "reason": reason,
                "mins": mins, "due": due,
            })
    return found


def run_once(store, cfg, notify, send_email, now, tz):
    """One pass: alert everyone newly late. Returns how many were alerted.

    Google limits how often a camp's Sheet can be read, so a quiet pass costs one read
    (who is out). The alerted-already record is only read when someone is late and this
    process has not already dealt with them."""
    schedule = store.read_schedule() or default_schedule(cfg)
    known = getattr(store, "known", None)
    if known is None:
        known = store.known = set()
    candidates = newly_late(store.read_status(), known, now, cfg, tz, schedule)
    if not candidates:
        return 0
    already_list = [x for x in store.read_dedupe().split(",") if x]
    already = set(already_list)
    known.update(i for i in already)
    late = [p for p in candidates if p["id"] not in already]
    known.update(p["id"] for p in candidates)
    for p in late:
        notify(f"{cfg.notify_prefix}: LATE", f"{p['name']} is {p['mins']} min late ({p['reason']})")
        subject, body = emailalert.late_message(cfg.name, p["name"], p["mins"], p["reason"], due_text(p["due"], now))
        send_email(subject, body)
        # Record each one as soon as it is sent, so a crash can never re-alert it.
        already_list.append(p["id"])
        store.write_dedupe(",".join(already_list[-KEEP_ALERTED:]))
    return len(late)


# ---------------------------------------------------------------------------
# Google Sheets and the outside world
# ---------------------------------------------------------------------------

class SheetStore:
    def __init__(self, secrets):
        import gspread
        from google.oauth2.service_account import Credentials

        creds = Credentials.from_service_account_info(
            dict(secrets["gcp_service_account"]),
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        self.sheet = gspread.authorize(creds).open_by_key(secrets["spreadsheet_id"])
        self._settings = None
        self._schedule, self._schedule_at = None, 0.0
        self.known = set()

    def read_status(self):
        return self.sheet.worksheet("current_status").get_all_records()

    def read_schedule(self):
        # The schedule rarely changes, so read it at most every five minutes.
        if self._schedule is None or time.time() - self._schedule_at > SCHEDULE_CACHE_SECONDS:
            self._schedule = schedule_from_rows(self.sheet.worksheet("schedule").get_all_values())
            self._schedule_at = time.time()
        return self._schedule

    def _settings_ws(self):
        if self._settings is None:
            self._settings = self.sheet.worksheet("settings")
        return self._settings

    def get(self, key):
        for row in self._settings_ws().get_all_values()[1:]:
            if row and row[0].strip().lower() == key:
                return row[1] if len(row) > 1 else ""
        return ""

    def put(self, key, value):
        ws = self._settings_ws()
        cell = ws.find(key, in_column=1)
        if cell:
            ws.update_cell(cell.row, 2, value)
        else:
            ws.append_row([key, value])

    def read_dedupe(self):
        return self.get(LATE_KEY)

    def write_dedupe(self, value):
        self.put(LATE_KEY, value)


def make_notifier(secrets):
    import requests

    server = str(secrets.get("ntfy_server", "https://ntfy.sh")).rstrip("/")
    topic = secrets.get("ntfy_topic", "")

    def notify(title, message):
        if not topic:
            return
        try:
            requests.post(f"{server}/{topic}", data=message.encode("utf-8"), headers={"Title": title}, timeout=8)
        except Exception:
            pass

    return notify


def make_emailer(secrets, cfg):
    key = str(secrets.get("resend_api_key", "") or "")

    def send_email(subject, body):
        if cfg.alert_email_to and key and cfg.alert_email_from:
            emailalert.send(key, cfg.alert_email_from, list(cfg.alert_email_to), subject, body, background=False)

    return send_email


def main(secrets_path=".streamlit/secrets.toml"):
    with open(secrets_path, "rb") as f:
        secrets = tomllib.load(f)
    cfg = camp_config.load(secrets)
    if not cfg.server_late_check:
        print("late_worker: server_late_check is off for this camp; not starting.", flush=True)
        return DISABLED_EXIT
    tz = pytz.timezone(cfg.timezone)
    store = SheetStore(secrets)
    notify, send_email = make_notifier(secrets), make_emailer(secrets, cfg)
    print(f"late_worker: watching {cfg.name} every {TICK_SECONDS}s", flush=True)
    last_beat, last_error = 0.0, ""
    while True:
        started = time.time()
        error = ""
        try:
            n = run_once(store, cfg, notify, send_email, datetime.now(tz), tz)
            if n:
                print(f"late_worker: alerted {n} at {datetime.now(tz):%H:%M:%S}", flush=True)
        except Exception as e:  # keep going; a Sheets hiccup must not stop the watch
            error = f"{type(e).__name__}: {e}"[:200]
            print(f"late_worker: error {error}", flush=True)
        try:
            if error != last_error or time.time() - last_beat > HEARTBEAT_SECONDS:
                if not error:
                    store.put("late_worker_ok", datetime.now(tz).isoformat(timespec="seconds"))
                if error != last_error:
                    store.put("late_worker_error", error)
                last_beat, last_error = time.time(), error
        except Exception:
            pass
        time.sleep(max(1.0, TICK_SECONDS - (time.time() - started)))


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:2]))
