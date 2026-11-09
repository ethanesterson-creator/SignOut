"""Per-camp configuration, kept free of Streamlit so it can be unit tested.

Everything that differs from one camp to the next lives here, read from the
deployment's secrets. Every default is Camp Bauercrest's current behaviour, so a
deployment that sets nothing runs exactly as it always has, and a new camp only
overrides what is actually different.

Two kinds of setting, on purpose:
  * Structural settings (this module, set in secrets.toml by whoever deploys the
    camp): name, timezone, which reasons exist and how their deadlines work,
    vans, alert thresholds.
  * Day-to-day data (the camp's own Google Sheet, edited by the camp): staff,
    drivers, days off, the period schedule.
"""

from dataclasses import dataclass, field
from datetime import time as dtime

# The free-text reason. Picking it makes the app ask "type a reason". It is the
# only reason the code treats specially besides the deadline rules below.
OTHER_REASON = "Other (type reason)"

# The reason the app writes itself for van trips. Never offered in the picker.
VAN_REASON = "Van"

# Deadline rules a reason can use.
RULE_NONE = "none"                  # no deadline
RULE_PERIOD_END = "period_end"      # back N minutes before the current period ends
RULE_NEXT_CLOCK = "next_clock"      # back at HH:MM, the next time the clock reads that
RULE_DAY_AFTER = "day_after"        # a full-day absence; back HH:MM the morning after
VALID_RULES = (RULE_NONE, RULE_PERIOD_END, RULE_NEXT_CLOCK, RULE_DAY_AFTER)


class ConfigError(ValueError):
    """The deployment's settings are wrong. The message says exactly what to fix."""


@dataclass(frozen=True)
class Reason:
    name: str
    rule: str = RULE_NONE
    grace_min: int = 5                       # period_end: minutes before the period ends
    hour: int = 0                            # next_clock / day_after: the due time
    minute: int = 0
    nextday_cutoff_hour: int = 12            # day_after: sign out at/after this hour -> tomorrow is the day off


# Camp Bauercrest's rules, unchanged.
DEFAULT_REASONS = (
    Reason("Period Off", RULE_PERIOD_END, grace_min=5),
    Reason("Day Off", RULE_DAY_AFTER, hour=7, minute=45, nextday_cutoff_hour=12),
    Reason("Night Off", RULE_NEXT_CLOCK, hour=0, minute=15),
    Reason(OTHER_REASON, RULE_NONE),
)

DEFAULT_VAN_PURPOSES = ("Period Off", "Night Off", "Day Off", "Field Trip", "Tournament", "Other")

DEFAULT_SCHEDULE = (
    ("Period 1", "09:15", "10:05"),
    ("Period 2", "10:15", "11:05"),
    ("Period 3", "11:15", "12:05"),
    ("Rest Period", "13:00", "14:00"),
    ("Period 4", "14:15", "15:05"),
    ("Snack", "15:15", "15:30"),
    ("Period 5", "15:40", "16:30"),
    ("G-Swim", "16:30", "17:30"),
    ("Free Play", "18:35", "19:15"),
)

# Used only while the live app is Camp Bauercrest. Any other camp has to say
# which Sheet is theirs, so a forgotten setting can never point a new camp's
# kiosk at Bauercrest's data.
LEGACY_DEFAULT_CAMP = "Camp Bauercrest"
LEGACY_DEFAULT_SPREADSHEET_ID = "1oS7KMged-KMGkeT9BHq1He8_K1oXMNuCvWQig21S5Xg"


@dataclass(frozen=True)
class CampConfig:
    # identity
    name: str = LEGACY_DEFAULT_CAMP
    tagline: str = "Amesbury, MA &middot; Est. 1931"
    logo_path: str = "logo-header-2.png"
    notify_prefix: str = "Bauercrest"
    csv_prefix: str = "bauercrest"
    board_label: str = "The Big House board"   # small label over the Who's Out page
    timezone: str = "US/Eastern"
    pin_salt: str = ""
    spreadsheet_id: str = LEGACY_DEFAULT_SPREADSHEET_ID

    # sign-out reasons and how each one sets a deadline
    reasons: tuple = DEFAULT_REASONS
    van_purposes: tuple = DEFAULT_VAN_PURPOSES
    default_schedule: tuple = DEFAULT_SCHEDULE

    # vans
    vans: tuple = ("Van 1", "Van 2", "Van 3")
    van_labels: dict = field(default_factory=lambda: {
        "Van 1": "Van 1 (White)", "Van 2": "Van 2 (Black)", "Van 3": "Van 3 (Red)",
    })

    # thresholds and timings
    forgot_threshold_minutes: int = 180   # this overdue = "forgot to sign in", not "late"
    fork_no_deadline_hours: int = 14      # out this long on a no-deadline reason = ask first
    nightly_summary_hour: int = 0         # hour (0-23) the "still out tonight" push goes out
    auto_archive_hour: int = 3            # hour (0-23) old logs are archived
    archive_keep_days: int = 3

    # board extras, all OFF by default so existing camps look exactly as before
    due_soon_minutes: int = 0             # >0: a person turns yellow this many minutes before they are late
    board_reason_filter: bool = False     # True: Who's Out gets a filter by reason (for example Night Off)
    staff_directory: bool = False         # True: Admin gets a staff search with phone, email and other info

    # interface tidying, both off by default so existing camps look exactly as before
    compact_admin: bool = False           # True: the Admin page is four tabs of collapsible sections instead of one long page
    quiet_code_boxes: bool = False        # True: code boxes are masked text, so the browser never offers to save or generate a password

    # look: the accent colour that replaces the default sky blue (a #RRGGBB hex). Empty = the default look.
    brand_accent: str = ""

    # drivers and downtime
    one_van_per_driver: bool = False      # True: a driver who already has a van out cannot take a second one
    driver_cert_tracking: bool = False    # True: drivers carry certified_on / expires_on; expired drivers cannot take a van
    driver_cert_warn_days: int = 30       # flag a certification this many days before it expires
    downtime_sheet: bool = False          # True: Admin can download a printable paper sheet for when the internet is down

    # alert emails (sent through Resend; the API key lives in secrets as resend_api_key, never here)
    alert_email_to: tuple = ()            # who gets the late and "late person is back" emails; empty = no email
    alert_email_from: str = ""            # e.g. "Headcount <alerts@headcountcamp.com>"
    server_late_check: bool = False       # True: a separate worker (late_worker.py) sends late alerts every 30 seconds, with no screen needed
    late_alerts_only: bool = False        # True: skip routine sign-out/in, van and nightly pushes. Late, back-in and emergency stay.

    # operator alerts (see opswatch.py): a private ntfy topic that only the person
    # running the app subscribes to. Empty = no operator alerts.
    ops_topic: str = ""
    ops_server: str = "https://ntfy.sh"

    @property
    def reason_names(self) -> list:
        return [r.name for r in self.reasons]

    def reason(self, name: str):
        name = str(name or "").strip()
        for r in self.reasons:
            if r.name == name:
                return r
        return None

    @property
    def has_free_text_reason(self) -> bool:
        return any(r.name == OTHER_REASON for r in self.reasons)


def _get(secrets, key, default):
    try:
        return secrets.get(key, default)
    except Exception:
        return default


def _as_int(secrets, key, default, lo, hi):
    raw = _get(secrets, key, default)
    try:
        val = int(raw)
    except (TypeError, ValueError):
        raise ConfigError(f"'{key}' must be a whole number, got {raw!r}.")
    if not lo <= val <= hi:
        raise ConfigError(f"'{key}' must be between {lo} and {hi}, got {val}.")
    return val


def _as_bool(secrets, key, default):
    raw = _get(secrets, key, default)
    if isinstance(raw, bool):
        return raw
    word = str(raw).strip().lower()
    if word in ("true", "yes", "1", "on"):
        return True
    if word in ("false", "no", "0", "off", ""):
        return False
    raise ConfigError(f"'{key}' must be true or false, got {raw!r}.")


def _as_hex(secrets, key, default) -> str:
    raw = str(_get(secrets, key, default) or "").strip()
    if not raw:
        return ""
    body = raw[1:] if raw.startswith("#") else raw
    if len(body) != 6 or any(c not in "0123456789abcdefABCDEF" for c in body):
        raise ConfigError(f"'{key}' must be a colour like #3EA5D8, got {raw!r}.")
    return "#" + body.upper()


def _as_email_list(secrets, key) -> tuple:
    """A list (or comma-separated string) of email addresses. Each must look like one."""
    raw = _get(secrets, key, ())
    if isinstance(raw, str):
        raw = raw.split(",")
    out = []
    for item in raw or ():
        addr = str(item).strip()
        if not addr:
            continue
        if addr.count("@") != 1 or " " in addr or "." not in addr.split("@")[1] or addr.startswith("@"):
            raise ConfigError(f"'{key}' has an invalid email address: {addr!r}.")
        out.append(addr)
    return tuple(out)


def _parse_reason(item, index: int) -> Reason:
    """One [[reasons]] table (or a bare string) from secrets.toml."""
    if isinstance(item, str):
        return Reason(item.strip(), RULE_NONE)
    try:
        d = dict(item)
    except Exception:
        raise ConfigError(f"reasons[{index}] must be a table with at least a 'name'.")
    name = str(d.get("name", "")).strip()
    if not name:
        raise ConfigError(f"reasons[{index}] is missing a 'name'.")
    rule = str(d.get("rule", RULE_NONE)).strip()
    if rule not in VALID_RULES:
        raise ConfigError(
            f"reason '{name}': rule must be one of {', '.join(VALID_RULES)}, got '{rule}'."
        )
    hour = d.get("hour", 0)
    minute = d.get("minute", 0)
    grace = d.get("grace_min", 5)
    cutoff = d.get("nextday_cutoff_hour", 12)
    try:
        hour, minute, grace, cutoff = int(hour), int(minute), int(grace), int(cutoff)
    except (TypeError, ValueError):
        raise ConfigError(f"reason '{name}': hour, minute, grace_min and nextday_cutoff_hour must be whole numbers.")
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ConfigError(f"reason '{name}': hour must be 0-23 and minute 0-59.")
    if not 0 <= cutoff <= 23:
        raise ConfigError(f"reason '{name}': nextday_cutoff_hour must be 0-23.")
    return Reason(name, rule, grace_min=grace, hour=hour, minute=minute, nextday_cutoff_hour=cutoff)


def _parse_schedule(items) -> tuple:
    out = []
    for i, it in enumerate(items):
        try:
            nm, s, e = it
        except Exception:
            raise ConfigError(f"default_schedule[{i}] must be [name, start, end], e.g. [\"Period 1\", \"09:00\", \"09:50\"].")
        out.append((str(nm), str(s), str(e)))
    return tuple(out)


def load(secrets) -> CampConfig:
    """Build the configuration from a secrets mapping (st.secrets or a dict).

    Raises ConfigError, with a message that names the setting and what is wrong,
    rather than letting a typo surface later as a confusing failure on the
    kiosk.
    """
    d = CampConfig()

    name = str(_get(secrets, "camp_name", d.name)).strip() or d.name

    sheet_id = str(_get(secrets, "spreadsheet_id", "") or "").strip()
    if not sheet_id:
        if name == LEGACY_DEFAULT_CAMP:
            sheet_id = LEGACY_DEFAULT_SPREADSHEET_ID
        else:
            raise ConfigError(
                f"This deployment is named '{name}' but has no 'spreadsheet_id' in secrets. "
                "Add the ID of this camp's own Google Sheet."
            )

    raw_reasons = _get(secrets, "reasons", None)
    if raw_reasons:
        reasons = tuple(_parse_reason(r, i) for i, r in enumerate(raw_reasons))
        names = [r.name for r in reasons]
        if len(set(names)) != len(names):
            raise ConfigError("Two reasons share the same name. Each reason needs a unique name.")
        if VAN_REASON in names:
            raise ConfigError(f"'{VAN_REASON}' is reserved for van trips; choose a different reason name.")
    else:
        reasons = d.reasons

    raw_sched = _get(secrets, "default_schedule", None)
    schedule = _parse_schedule(raw_sched) if raw_sched else d.default_schedule

    vans = tuple(_get(secrets, "vans", d.vans))
    van_labels = dict(_get(secrets, "van_labels", d.van_labels if vans == d.vans else {}))

    return CampConfig(
        name=name,
        tagline=str(_get(secrets, "camp_tagline", d.tagline)),
        logo_path=str(_get(secrets, "camp_logo_path", d.logo_path)),
        notify_prefix=str(_get(secrets, "camp_notify_prefix", d.notify_prefix)),
        csv_prefix=str(_get(secrets, "camp_csv_prefix", d.csv_prefix)),
        board_label=str(_get(secrets, "camp_board_label", d.board_label)),
        timezone=str(_get(secrets, "camp_timezone", d.timezone)),
        pin_salt=str(_get(secrets, "pin_salt", d.pin_salt)),
        spreadsheet_id=sheet_id,
        reasons=reasons,
        van_purposes=tuple(_get(secrets, "van_purposes", d.van_purposes)),
        default_schedule=schedule,
        vans=vans,
        van_labels=van_labels,
        forgot_threshold_minutes=_as_int(secrets, "forgot_threshold_minutes", d.forgot_threshold_minutes, 30, 24 * 60),
        fork_no_deadline_hours=_as_int(secrets, "fork_no_deadline_hours", d.fork_no_deadline_hours, 1, 72),
        nightly_summary_hour=_as_int(secrets, "nightly_summary_hour", d.nightly_summary_hour, 0, 23),
        auto_archive_hour=_as_int(secrets, "auto_archive_hour", d.auto_archive_hour, 0, 23),
        archive_keep_days=_as_int(secrets, "archive_keep_days", d.archive_keep_days, 1, 60),
        due_soon_minutes=_as_int(secrets, "due_soon_minutes", d.due_soon_minutes, 0, 120),
        board_reason_filter=_as_bool(secrets, "board_reason_filter", d.board_reason_filter),
        staff_directory=_as_bool(secrets, "staff_directory", d.staff_directory),
        compact_admin=_as_bool(secrets, "compact_admin", d.compact_admin),
        quiet_code_boxes=_as_bool(secrets, "quiet_code_boxes", d.quiet_code_boxes),
        brand_accent=_as_hex(secrets, "brand_accent", d.brand_accent),
        one_van_per_driver=_as_bool(secrets, "one_van_per_driver", d.one_van_per_driver),
        driver_cert_tracking=_as_bool(secrets, "driver_cert_tracking", d.driver_cert_tracking),
        driver_cert_warn_days=_as_int(secrets, "driver_cert_warn_days", d.driver_cert_warn_days, 1, 365),
        downtime_sheet=_as_bool(secrets, "downtime_sheet", d.downtime_sheet),
        alert_email_to=_as_email_list(secrets, "alert_email_to"),
        alert_email_from=str(_get(secrets, "alert_email_from", d.alert_email_from)).strip(),
        server_late_check=_as_bool(secrets, "server_late_check", d.server_late_check),
        late_alerts_only=_as_bool(secrets, "late_alerts_only", d.late_alerts_only),
        ops_topic=str(_get(secrets, "ops_ntfy_topic", d.ops_topic)).strip(),
        ops_server=str(_get(secrets, "ops_ntfy_server", d.ops_server)).strip() or d.ops_server,
    )


# ---------------------------------------------------------------------------
# Deadline math (pure, so it can be tested without Streamlit or a Sheet)
# ---------------------------------------------------------------------------

def _next_clock_time(now, hh: int, mm: int):
    """The next moment the clock reads hh:mm, strictly after now."""
    from datetime import timedelta

    candidate = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if candidate <= now:
        candidate = candidate + timedelta(days=1)
    return candidate


def due_back(cfg: CampConfig, reason: str, now, tz, schedule):
    """When this person must be signed back in. None means no deadline.

    `schedule` is a callable returning [(period name, start time, end time)],
    only called for reasons that need it. `tz` localizes naive datetimes.
    """
    from datetime import datetime, timedelta

    r = cfg.reason(reason)
    if r is None or r.rule == RULE_NONE:
        return None

    if r.rule == RULE_NEXT_CLOCK:
        return _next_clock_time(now, r.hour, r.minute)

    if r.rule == RULE_DAY_AFTER:
        # Which calendar day is the day off?
        day_off_date = now.date()
        if now.hour >= r.nextday_cutoff_hour:
            day_off_date = day_off_date + timedelta(days=1)
        # Back the morning AFTER the day off.
        due_date = day_off_date + timedelta(days=1)
        naive = datetime.combine(due_date, dtime(r.hour, r.minute))
        return tz.localize(naive) if now.tzinfo else naive

    if r.rule == RULE_PERIOD_END:
        today = now.date()
        for _name, start, end in schedule():
            if not start or not end:
                continue
            end_dt = tz.localize(datetime.combine(today, end)) if now.tzinfo else datetime.combine(today, end)
            due = end_dt - timedelta(minutes=r.grace_min)
            # The first period whose deadline is still ahead of them is theirs.
            # That covers being mid-period and being in a gap between periods.
            if due > now:
                return due
        return None  # signed out after the last period; no deadline to enforce

    return None


def minutes_until_due(due, now, window: int):
    """Whole minutes left (rounded up) if this person is inside the warning window,
    otherwise None. 'Inside' means not late yet and no more than `window` minutes
    from their deadline. A window of 0 turns the warning off. No deadline, no warning.
    """
    if not due or window <= 0:
        return None
    try:
        remaining = (due - now).total_seconds()
    except TypeError:
        return None  # naive and aware mixed: say nothing rather than guess
    if remaining <= 0:
        return None
    mins = int(-(-remaining // 60))  # ceiling
    return mins if mins <= window else None


CERT_VALID, CERT_EXPIRING, CERT_EXPIRED, CERT_MISSING = "valid", "expiring", "expired", "missing"


def cert_status(expires, today, warn_days: int = 30) -> str:
    """Where a driver's certification stands on `today` (both are dates).

    expired:  the expiry date is before today (it is good through the expiry date itself)
    expiring: good now, but ends within warn_days
    missing:  no expiry date on file
    """
    from datetime import timedelta

    if expires is None:
        return CERT_MISSING
    if expires < today:
        return CERT_EXPIRED
    if expires <= today + timedelta(days=warn_days):
        return CERT_EXPIRING
    return CERT_VALID


def other_van_out(status_map: dict, driver: str, this_van: str):
    """The name of another van this driver already has out, or None.

    status_map is the app's van status: {van: {"status": "IN"/"OUT", "driver": name, ...}}.
    A van is never in conflict with itself, and a returned van does not count.
    """
    who = str(driver or "").strip()
    if not who:
        return None
    for van, info in status_map.items():
        if van == this_van:
            continue
        if str(info.get("status", "")).upper() == "OUT" and str(info.get("driver", "")).strip() == who:
            return van
    return None
