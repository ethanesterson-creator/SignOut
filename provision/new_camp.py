"""Turn a filled-in camp intake into everything needed to stand that camp up.

    python provision/new_camp.py provision/intake_<camp>.toml [--out camps]

Writes to camps/<slug>/ (git-ignored: it holds secrets):
    secrets.toml        the deployment's settings, validated before writing
    staff_import.csv    staff with unique random 4-digit codes, ready to paste
    drivers_import.csv  anyone cleared to drive (only if the intake marks any)
    director_email.txt  what to send the camp director
    CHECKLIST.md        the remaining steps, in order, with the camp's own values
    credentials.txt     the admin password and anything you must hand over (keep private)

It can be run more than once. Run it before the camp has shared its Sheet to get
the director email, then again once the Sheet URL and service-account key are in
the intake to get the finished secrets.toml. It never talks to Google.
"""

import argparse
import csv
import json
import re
import secrets as _secrets
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import camp_config  # noqa: E402


class IntakeError(ValueError):
    """The intake file is incomplete or wrong; the message says what to fix."""


# ---------------------------------------------------------------------------
# small pieces
# ---------------------------------------------------------------------------

def slugify(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    if not s:
        raise IntakeError("camp_name must contain letters or numbers.")
    return s


def sheet_id_from_url(url: str) -> str:
    m = re.search(r"/spreadsheets/d/([A-Za-z0-9_-]{20,})", url or "")
    if not m:
        raise IntakeError(
            "spreadsheet_url doesn't look like a Google Sheets link "
            "(expected https://docs.google.com/spreadsheets/d/<ID>/edit)."
        )
    return m.group(1)


_BAD_PINS = {"0000", "1111", "2222", "3333", "4444", "5555", "6666", "7777", "8888", "9999",
             "1234", "4321", "2580", "0852", "1212", "2020", "2021", "2022", "2023", "2024",
             "2025", "2026", "2027"}


def make_pins(n: int, rng=None) -> list:
    """n unique 4-digit codes, skipping the obviously guessable ones."""
    rng = rng or _secrets.SystemRandom()
    pool = [f"{i:04d}" for i in range(10000) if f"{i:04d}" not in _BAD_PINS]
    if n > len(pool):
        raise IntakeError(f"Too many staff ({n}) for unique 4-digit codes.")
    return rng.sample(pool, n)


def read_staff(path: str) -> list:
    """[(name, admin, driver)] from the intake's staff CSV."""
    p = Path(path)
    if not p.exists():
        raise IntakeError(f"staff_csv not found: {path}")
    truthy = {"true", "yes", "y", "1", "x"}
    rows = []
    with p.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "name" not in [h.strip().lower() for h in reader.fieldnames]:
            raise IntakeError("staff_csv needs a header row with a 'name' column.")
        for r in reader:
            r = {k.strip().lower(): (v or "").strip() for k, v in r.items() if k}
            if not r.get("name"):
                continue
            rows.append((r["name"], r.get("admin", "").lower() in truthy, r.get("driver", "").lower() in truthy))
    names = [n.lower() for n, _, _ in rows]
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise IntakeError(f"These names appear more than once in staff_csv (codes would collide): {', '.join(dupes)}")
    if not rows:
        raise IntakeError("staff_csv has no staff in it.")
    return rows


# ---------------------------------------------------------------------------
# TOML writing (the standard library only reads TOML)
# ---------------------------------------------------------------------------

def _q(s) -> str:
    return json.dumps(str(s), ensure_ascii=False)  # JSON string syntax is valid TOML basic-string syntax


def _val(v) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(_val(x) for x in v) + "]"
    return _q(v)


def build_settings(intake: dict, sheet_id: str) -> dict:
    """The camp-level settings, shaped exactly like camp_config reads them."""
    s = {
        "camp_name": intake["camp_name"],
        "camp_tagline": intake.get("tagline", ""),
        "camp_logo_path": intake.get("logo_path", "logo-header-2.png"),
        "camp_notify_prefix": intake.get("notify_prefix", intake["camp_name"].replace("Camp ", "", 1)),
        "camp_csv_prefix": slugify(intake["camp_name"]).replace("camp-", "", 1),
        "camp_board_label": intake.get("board_label", f"The {intake['camp_name']} board"),
        "camp_timezone": intake.get("timezone", "US/Eastern"),
        "spreadsheet_id": sheet_id,
    }
    if intake.get("vans"):
        s["vans"] = list(intake["vans"])
        s["van_labels"] = dict(intake.get("van_labels", {}))
    else:
        s["vans"] = []
        s["van_labels"] = {}
    periods = (intake.get("schedule") or {}).get("periods")
    if periods:
        s["default_schedule"] = [list(p) for p in periods]
    if intake.get("reasons"):
        s["reasons"] = [dict(r) for r in intake["reasons"]]
    for k in ("forgot_threshold_minutes", "fork_no_deadline_hours", "nightly_summary_hour",
              "auto_archive_hour", "archive_keep_days"):
        if k in intake:
            s[k] = intake[k]
    return s


def render_secrets(settings: dict, extras: dict, service_account: dict) -> str:
    out = ["# Generated by provision/new_camp.py. Contains secrets: never commit this file.", ""]
    flat = {**extras, **{k: v for k, v in settings.items() if k not in ("van_labels", "reasons")}}
    for k, v in flat.items():
        out.append(f"{k} = {_val(v)}")
    out.append("")
    if settings.get("van_labels"):
        out.append("[van_labels]")
        for k, v in settings["van_labels"].items():
            out.append(f"{_q(k)} = {_q(v)}")
        out.append("")
    for r in settings.get("reasons", []):
        out.append("[[reasons]]")
        for k, v in r.items():
            out.append(f"{k} = {_val(v)}")
        out.append("")
    out.append("[gcp_service_account]")
    for k, v in service_account.items():
        out.append(f"{k} = {_q(v)}")
    out.append("")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# the documents handed to people
# ---------------------------------------------------------------------------

def director_email(intake: dict, sa_email: str) -> str:
    sa = sa_email or "<<< service account email: create the key first, then re-run this script >>>"
    return f"""Subject: Setting up SignOut for {intake['camp_name']}: one 5-minute step

Hi {intake.get('director_name', 'there')},

To get {intake['camp_name']}'s staff sign-out board running, I need one thing
from you. Your staff data will live in a Google Sheet that you own, so it
stays yours at all times.

1. While signed in to the Google account the camp will use, create a new, blank
   Google Sheet. Name it anything, for example "{intake['camp_name']} SignOut".
2. Click Share, paste this address, set it to Editor, and uncheck "Notify people":
       {sa}
3. Copy the Sheet's web address from your browser and reply with it.

If Google says it can't share outside your organization, tell me and we'll use
another way.

Please also send, if you have them:
  - your staff list (names; mark anyone in the office/admin, and anyone cleared
    to drive a camp van),
  - your daily schedule (period names and times),
  - your logo as a PNG,
  - the phone number(s) that should get alerts.

Once I have these, your board is usually ready the same day.

Thanks,
Ethan
"""


def checklist(intake: dict, slug: str, have_sheet: bool, have_key: bool, n_staff: int, sa_email: str) -> str:
    def box(done): return "[x]" if done else "[ ]"
    return f"""# Onboarding checklist: {intake['camp_name']}  ({slug})

Director: {intake.get('director_name', '')} <{intake.get('director_email', '')}>

## Before the camp does anything
- {box(have_key)} Create this camp's own Google service account (Google Cloud Console >
      IAM > Service Accounts, Sheets API enabled), download its JSON key, put the path
      in the intake as `service_account_json`. One service account per camp, never shared.
- [ ] Send `director_email.txt` (contains the service account address{(': ' + sa_email) if sa_email else ''}).

## When the camp replies
- {box(have_sheet)} Paste the Sheet link into the intake as `spreadsheet_url`.
- [ ] Put their staff list in a CSV, point the intake's `staff_csv` at it.
- [ ] Re-run `new_camp.py`. `secrets.toml`, `staff_import.csv`, `credentials.txt` appear.

## Stand it up
- [ ] Deploy a new instance of the app with this camp's `secrets.toml` as its secrets.
      (Host not chosen yet. Record the chosen host here.)
- [ ] Open the app > Admin / History, log in with the admin password in `credentials.txt`.
- [ ] Open "New Camp Setup" > "Create Any Missing Tabs".
- [ ] Paste `staff_import.csv` ({n_staff} staff) into the Sheet's `staff` tab (A1 down).
      Paste `drivers_import.csv` into `drivers` if it exists.
- [ ] Admin > Staff PIN Security > "Hash All Plaintext PINs Now" (the codes in the Sheet
      become unreadable; the camp keeps `staff_import.csv` to hand out codes, then deletes it).
- [ ] Edit the `schedule` tab if the real times differ.

## Test day (before opening day)
- [ ] Sign out and in on every reason; confirm the deadlines match what the camp expects.
- [ ] Take out and return a van; sign out a group.
- [ ] Confirm a phone notification arrives (ntfy topics are in `secrets.toml`).
- [ ] Confirm the kiosk device's browser is set to open the camp's address on start.

## Hand-off
- [ ] Give the director: the app address, the admin password, `staff_import.csv`.
- [ ] Add an outside uptime check for `<the camp's address>/_stcore/health` (a free service such as
      UptimeRobot or Better Stack), alerting you by email and push. It catches the one case the app
      cannot report itself: the whole app being down.
- [ ] Confirm you got the app's own "Started" alert on your ops channel (needs provision/ops.local.toml).
- [ ] Put the season renewal date in your calendar.
"""


# ---------------------------------------------------------------------------
# the run
# ---------------------------------------------------------------------------

def load_ops(path=None) -> dict:
    """The operator's private alert channel, shared by every camp you run. Lives in
    provision/ops.local.toml (git-ignored); see ops.example.toml."""
    p = Path(path) if path else ROOT / "provision" / "ops.local.toml"
    if not p.exists():
        return {}
    with p.open("rb") as f:
        return tomllib.load(f)


def provision(intake_path: str, out_root: str = "camps", rng=None, ops_path=None) -> dict:
    """Do the whole job. Returns what was written, for the CLI and for tests."""
    try:
        with open(intake_path, "rb") as f:
            intake = tomllib.load(f)
    except FileNotFoundError:
        raise IntakeError(f"Intake file not found: {intake_path}")
    except tomllib.TOMLDecodeError as e:
        raise IntakeError(f"The intake file isn't valid TOML: {e}")

    for req in ("camp_name", "director_email"):
        if not str(intake.get(req, "")).strip():
            raise IntakeError(f"'{req}' is required in the intake.")
    tz = intake.get("timezone", "US/Eastern")
    try:
        import pytz
        pytz.timezone(tz)
    except Exception:
        raise IntakeError(f"timezone '{tz}' isn't a real timezone name (try US/Eastern, US/Central, US/Pacific).")

    slug = slugify(intake["camp_name"])
    out = Path(out_root) / slug
    out.mkdir(parents=True, exist_ok=True)
    written = []

    key_path = str(intake.get("service_account_json", "")).strip()
    sa = None
    if key_path:
        try:
            sa = json.loads(Path(key_path).read_text(encoding="utf-8"))
        except FileNotFoundError:
            raise IntakeError(f"service_account_json not found: {key_path}")
        except json.JSONDecodeError:
            raise IntakeError("service_account_json isn't a valid Google key file.")
        for k in ("client_email", "private_key", "project_id"):
            if k not in sa:
                raise IntakeError(f"service_account_json is missing '{k}'. Is it the right key file?")
    sa_email = sa["client_email"] if sa else ""

    sheet_url = str(intake.get("spreadsheet_url", "")).strip()
    sheet_id = sheet_id_from_url(sheet_url) if sheet_url else ""

    staff_csv = str(intake.get("staff_csv", "")).strip()
    staff = read_staff(staff_csv) if staff_csv else []

    (out / "director_email.txt").write_text(director_email(intake, sa_email), encoding="utf-8")
    written.append("director_email.txt")

    if staff:
        pins = make_pins(len(staff), rng)
        with (out / "staff_import.csv").open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["name", "pin", "active", "admin"])
            for (name, admin, _drv), pin in zip(staff, pins):
                w.writerow([name, pin, "TRUE", "TRUE" if admin else ""])
        written.append("staff_import.csv")
        drivers = [n for n, _, d in staff if d]
        if drivers:
            with (out / "drivers_import.csv").open("w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["name", "passed_test"])
                for n in drivers:
                    w.writerow([n, "TRUE"])
            written.append("drivers_import.csv")

    if sa and sheet_id:
        existing = {}
        if (out / "credentials.txt").exists():
            for line in (out / "credentials.txt").read_text(encoding="utf-8").splitlines():
                if ": " in line:
                    k, v = line.split(": ", 1)
                    existing[k.strip()] = v.strip()
        extras = {
            "admin_password": existing.get("admin_password") or _secrets.token_urlsafe(9),
            "pin_salt": existing.get("pin_salt") or _secrets.token_hex(16),
            "ntfy_topic": existing.get("ntfy_topic") or f"signout-{slug}-{_secrets.token_hex(5)}",
            "ntfy_topic_vans": existing.get("ntfy_topic_vans") or f"signout-{slug}-vans-{_secrets.token_hex(5)}",
        }
        ops = load_ops(ops_path)
        if ops.get("ops_ntfy_topic"):
            extras["ops_ntfy_topic"] = ops["ops_ntfy_topic"]
            if ops.get("ops_ntfy_server"):
                extras["ops_ntfy_server"] = ops["ops_ntfy_server"]
        settings = build_settings(intake, sheet_id)
        # Prove the camp's settings are valid BEFORE writing anything the app reads.
        try:
            camp_config.load({**settings, **extras})
        except camp_config.ConfigError as e:
            raise IntakeError(f"The camp's settings are invalid: {e}")
        (out / "secrets.toml").write_text(render_secrets(settings, extras, sa), encoding="utf-8")
        (out / "credentials.txt").write_text(
            "PRIVATE. Do not email or commit this file.\n"
            f"camp: {intake['camp_name']}\n"
            f"admin_password: {extras['admin_password']}\n"
            f"pin_salt: {extras['pin_salt']}\n"
            f"ntfy_topic: {extras['ntfy_topic']}\n"
            f"ntfy_topic_vans: {extras['ntfy_topic_vans']}\n",
            encoding="utf-8",
        )
        written += ["secrets.toml", "credentials.txt"]

    (out / "CHECKLIST.md").write_text(
        checklist(intake, slug, bool(sheet_id), bool(sa), len(staff), sa_email), encoding="utf-8"
    )
    written.append("CHECKLIST.md")

    missing = []
    if not sa:
        missing.append("service_account_json (create the camp's service account first)")
    if not sheet_id:
        missing.append("spreadsheet_url (the director's reply)")
    if not staff:
        missing.append("staff_csv")
    return {"dir": str(out), "slug": slug, "written": written, "missing": missing}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("intake")
    ap.add_argument("--out", default="camps")
    args = ap.parse_args(argv)
    try:
        res = provision(args.intake, args.out)
    except IntakeError as e:
        print(f"Can't continue: {e}", file=sys.stderr)
        return 1
    print(f"Wrote to {res['dir']}/: " + ", ".join(res["written"]))
    if res["missing"]:
        print("Still needed for the finished settings: " + "; ".join(res["missing"]))
    else:
        print("Everything needed is in place. Follow CHECKLIST.md next.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
