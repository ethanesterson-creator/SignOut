import html as html_lib
import uuid
from datetime import datetime
from pathlib import Path

import requests
import pandas as pd
import pytz
import streamlit as st

import gspread
from gspread.exceptions import APIError, GSpreadException, WorksheetNotFound
from google.oauth2.service_account import Credentials

# =================================================
# TIMEZONE
# =================================================
TZ = pytz.timezone("US/Eastern")

# =================================================
# CONFIG
# =================================================
SPREADSHEET_ID = "1oS7KMged-KMGkeT9BHq1He8_K1oXMNuCvWQig21S5Xg"

SHEET_LOGS = "logs"
SHEET_VANS = "vans"
SHEET_STAFF = "staff"
SHEET_DRIVERS = "drivers"
SHEET_DAYS_OFF = "days_off"  # optional tab; used for the Day Off board (display only)
SHEET_SETTINGS = "settings"  # auto-created; holds the campwide emergency flag

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

REASONS = ["Period Off", "Day Off", "Night Off", "Other (type reason)"]

VANS = ["Van 1", "Van 2", "Van 3"]
VAN_LABELS = {"Van 1": "Van 1 (White)", "Van 2": "Van 2 (Black)", "Van 3": "Van 3 (Red)"}


def van_label(v: str) -> str:
    return VAN_LABELS.get(v, v)


VAN_PURPOSES = ["Period Off", "Night Off", "Day Off", "Field Trip", "Tournament", "Other"]

# Legacy tag from the old auto day-off feature. Kept only so old rows
# display cleanly on the board. The app no longer writes these rows.
LEGACY_AUTO_TAG_PREFIX = "AUTO_DAY_OFF"

# Vans sheet required headers
VANS_HEADERS_REQUIRED = [
    "id", "timestamp", "van", "driver", "purpose", "passengers",
    "other_purpose", "action", "status", "gas_left"
]

# Logs sheet required headers
LOGS_HEADERS_REQUIRED = ["id", "timestamp", "name", "reason", "other_reason", "action", "status"]

# Pages that auto-refresh for the Big House kiosk. The sign-out form is
# excluded on purpose so a refresh never wipes a PIN mid-entry.
# Pages that show the live self-refreshing boards. The Sign In / Out and Admin
# pages never auto-refresh, so typing is never interrupted.
KIOSK_PAGES = {"Who's Out", "Vans"}

# =================================================
# THEME / CSS
# =================================================
NAVY = "#13294B"
NAVY_DEEP = "#0B1B33"
NAVY_SOFT = "#1E3A66"
CLOUD = "#F5F7FA"
LINE = "#D8DFE9"
MIST = "#5C6B82"
WHITE = "#FFFFFF"

APP_CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Archivo:wght@500;600;700;800&family=Public+Sans:wght@400;500;600;700&display=swap');

:root {{
    --navy: {NAVY};
    --navy-deep: {NAVY_DEEP};
    --navy-soft: {NAVY_SOFT};
    --cloud: {CLOUD};
    --line: {LINE};
    --mist: {MIST};
}}

/* ---------- base ---------- */
.stApp {{
    background: var(--cloud);
    font-family: 'Public Sans', sans-serif;
    color: var(--navy-deep);
}}

#MainMenu, footer {{ visibility: hidden; }}
header[data-testid="stHeader"] {{ background: transparent; }}

h1, h2, h3, .stApp h1, .stApp h2, .stApp h3 {{
    font-family: 'Archivo', sans-serif;
    color: var(--navy);
    letter-spacing: -0.01em;
}}

/* ---------- sidebar ---------- */
section[data-testid="stSidebar"] {{
    background: var(--navy);
    border-right: 4px solid var(--navy-deep);
}}
section[data-testid="stSidebar"] * {{
    color: {WHITE} !important;
}}
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] label,
section[data-testid="stSidebar"] span:not([data-testid="stIconMaterial"]),
section[data-testid="stSidebar"] div[role="radiogroup"] {{
    font-family: 'Public Sans', sans-serif;
}}
/* Streamlit icons are ligatures in the Material Symbols font.
   Restore the icon font so names like keyboard_double_arrow_left
   render as glyphs, not text. */
section[data-testid="stSidebar"] [data-testid="stIconMaterial"],
[data-testid="stIconMaterial"],
.material-symbols-rounded,
.material-symbols-outlined {{
    font-family: 'Material Symbols Rounded' !important;
}}
section[data-testid="stSidebar"] .stRadio label p {{
    font-size: 1.02rem;
    font-weight: 600;
}}
section[data-testid="stSidebar"] hr {{
    border-color: var(--navy-soft);
}}
section[data-testid="stSidebar"] [data-testid="stExpander"] {{
    background: var(--navy-soft);
    border-radius: 10px;
    border: none;
}}

/* ---------- buttons ---------- */
.stButton > button, .stFormSubmitButton > button {{
    background: var(--navy);
    color: {WHITE};
    border: none;
    border-radius: 8px;
    font-family: 'Archivo', sans-serif;
    font-weight: 700;
    letter-spacing: 0.02em;
    padding: 0.55rem 1.4rem;
    transition: background 0.15s ease;
}}
.stButton > button:hover, .stFormSubmitButton > button:hover {{
    background: var(--navy-soft);
    color: {WHITE};
}}
.stDownloadButton > button {{
    background: {WHITE};
    color: var(--navy);
    border: 1.5px solid var(--navy);
    border-radius: 8px;
    font-weight: 700;
}}

/* ---------- inputs ---------- */
.stTextInput input, .stSelectbox [data-baseweb="select"] > div,
.stMultiSelect [data-baseweb="select"] > div {{
    background: {WHITE};
    border-radius: 8px;
    border-color: var(--line);
}}

/* ---------- forms ---------- */
[data-testid="stForm"] {{
    background: {WHITE};
    border: 1px solid var(--line);
    border-radius: 14px;
    padding: 1.4rem 1.4rem 1.1rem 1.4rem;
}}

/* ---------- custom components ---------- */
.bc-eyebrow {{
    font-family: 'Archivo', sans-serif;
    font-size: 0.72rem;
    font-weight: 800;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: var(--mist);
    margin-bottom: 0.15rem;
}}
.bc-pagetitle {{
    font-family: 'Archivo', sans-serif;
    font-size: 2rem;
    font-weight: 800;
    color: var(--navy);
    margin: 0 0 1.1rem 0;
    line-height: 1.1;
}}
.bc-sectiontitle {{
    font-family: 'Archivo', sans-serif;
    font-size: 1.15rem;
    font-weight: 800;
    color: var(--navy);
    margin: 0 0 0.7rem 0;
}}

.bc-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
    gap: 0.85rem;
    margin-bottom: 0.5rem;
}}

.bc-card {{
    background: {WHITE};
    border: 1px solid var(--line);
    border-top: 4px solid var(--navy);
    border-radius: 12px;
    padding: 0.95rem 1.05rem;
    box-shadow: 0 1px 3px rgba(11, 27, 51, 0.06);
}}
.bc-card .bc-name {{
    font-family: 'Archivo', sans-serif;
    font-size: 1.18rem;
    font-weight: 800;
    color: var(--navy-deep);
    margin-bottom: 0.35rem;
}}
.bc-card .bc-meta {{
    font-size: 0.92rem;
    color: var(--mist);
    line-height: 1.45;
}}
.bc-card .bc-time {{
    font-variant-numeric: tabular-nums;
    font-weight: 600;
    color: var(--navy);
}}

.bc-chip {{
    display: inline-block;
    background: var(--navy);
    color: {WHITE};
    font-family: 'Archivo', sans-serif;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    border-radius: 999px;
    padding: 0.18rem 0.65rem;
    margin-bottom: 0.45rem;
}}
.bc-chip.bc-chip-light {{
    background: var(--cloud);
    color: var(--navy);
    border: 1px solid var(--line);
}}

.bc-dayoff-row {{
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
}}
.bc-dayoff {{
    background: {WHITE};
    border: 1.5px solid var(--navy);
    color: var(--navy);
    font-family: 'Archivo', sans-serif;
    font-weight: 700;
    font-size: 0.95rem;
    border-radius: 999px;
    padding: 0.35rem 1rem;
}}

.bc-van-card {{
    background: {WHITE};
    border: 1px solid var(--line);
    border-radius: 12px;
    padding: 1rem 1.1rem;
    box-shadow: 0 1px 3px rgba(11, 27, 51, 0.06);
}}
.bc-van-card.bc-van-out {{
    background: var(--navy);
    border-color: var(--navy-deep);
}}
.bc-van-card .bc-van-title {{
    font-family: 'Archivo', sans-serif;
    font-size: 1.25rem;
    font-weight: 800;
    color: var(--navy);
    margin-bottom: 0.3rem;
}}
.bc-van-card.bc-van-out .bc-van-title,
.bc-van-card.bc-van-out .bc-meta {{
    color: {WHITE};
}}
.bc-van-card.bc-van-out .bc-meta strong {{
    color: {WHITE};
}}
.bc-van-card .bc-meta {{
    font-size: 0.93rem;
    color: var(--mist);
    line-height: 1.5;
}}
.bc-van-status {{
    display: inline-block;
    font-family: 'Archivo', sans-serif;
    font-size: 0.7rem;
    font-weight: 800;
    letter-spacing: 0.1em;
    border-radius: 999px;
    padding: 0.16rem 0.6rem;
    margin-bottom: 0.4rem;
}}
.bc-van-status.in {{
    background: var(--cloud);
    color: var(--navy);
    border: 1px solid var(--line);
}}
.bc-van-status.out {{
    background: {WHITE};
    color: var(--navy);
}}

.bc-empty {{
    background: {WHITE};
    border: 1px dashed var(--line);
    border-radius: 12px;
    padding: 1.1rem 1.2rem;
    color: var(--mist);
    font-size: 0.97rem;
}}

@keyframes bcFlashFade {{
    0%   {{ opacity: 0; transform: translateY(-4px); }}
    10%  {{ opacity: 1; transform: translateY(0); }}
    75%  {{ opacity: 1; }}
    100% {{ opacity: 0; transform: translateY(-4px); }}
}}
.bc-flash {{
    background: #E7F4EA;
    border: 1px solid #2E7D32;
    color: #1B5E20;
    border-radius: 10px;
    padding: 0.7rem 1rem;
    font-family: 'Public Sans', sans-serif;
    font-weight: 600;
    margin-bottom: 0.6rem;
    animation: bcFlashFade 2.4s ease forwards;
}}

.bc-footer {{
    margin-top: 2.5rem;
    padding-top: 0.8rem;
    border-top: 1px solid var(--line);
    font-family: 'Archivo', sans-serif;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--mist);
    text-align: center;
}}
</style>
"""


def inject_css():
    st.markdown(APP_CSS, unsafe_allow_html=True)


def esc(s) -> str:
    return html_lib.escape(str(s or "").strip())


def page_title(eyebrow: str, title: str):
    st.markdown(
        f"<div class='bc-eyebrow'>{esc(eyebrow)}</div>"
        f"<div class='bc-pagetitle'>{esc(title)}</div>",
        unsafe_allow_html=True,
    )


def section_title(title: str):
    st.markdown(f"<div class='bc-sectiontitle'>{esc(title)}</div>", unsafe_allow_html=True)


def empty_note(text: str):
    st.markdown(f"<div class='bc-empty'>{esc(text)}</div>", unsafe_allow_html=True)


def flash_banner(msg: str):
    """Inline green confirmation that fades out on its own after ~2 seconds."""
    st.markdown(f"<div class='bc-flash'>{esc(msg)}</div>", unsafe_allow_html=True)


def crest_footer():
    st.markdown(
        "<div class='bc-footer'>Camp Bauercrest &middot; Amesbury, MA &middot; Est. 1931</div>",
        unsafe_allow_html=True,
    )

# =================================================
# SMALL UTILS
# =================================================
def normalize_pin(pin: str) -> str:
    s = str(pin).strip().replace(" ", "")
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(4)


def format_time(dt):
    """Full timestamp for admin tables."""
    if pd.isna(dt):
        return ""
    try:
        return dt.strftime("%Y-%m-%d %I:%M %p")
    except Exception:
        return str(dt)


def format_board_time(dt):
    """Short timestamp for the public board: time only if today, else day + time."""
    if pd.isna(dt):
        return ""
    try:
        now = datetime.now(TZ)
        if dt.date() == now.date():
            return dt.strftime("%I:%M %p").lstrip("0")
        return dt.strftime("%a %I:%M %p").replace(" 0", " ")
    except Exception:
        return str(dt)


BOARD_REFRESH_SECONDS = 60
SCREEN_REFRESH_SECONDS = 20


def escalate_if_emergency_changed(current_screen: str = "normal"):
    """From inside a live fragment, jump to a full app rerun when the campwide
    emergency flag no longer matches what this screen is showing.

    Normal boards flip TO the red screen when an emergency is declared
    elsewhere. The red screen flips back to normal when it is cleared
    elsewhere. This is what makes the emergency truly campwide without a
    jarring full-page reload on every tick.
    """
    flag = get_emergency_flag()
    if current_screen == "emergency":
        if not flag:
            st.rerun(scope="app")
    else:
        if flag:
            st.rerun(scope="app")


def normalize_weekday(s: str) -> str:
    """Normalize weekday strings like 'Mon', 'monday', 'MONDAY' -> 'monday'."""
    s = (s or "").strip().lower()
    mapping = {
        "mon": "monday", "monday": "monday",
        "tue": "tuesday", "tues": "tuesday", "tuesday": "tuesday",
        "wed": "wednesday", "weds": "wednesday", "wednesday": "wednesday",
        "thu": "thursday", "thur": "thursday", "thurs": "thursday", "thursday": "thursday",
        "fri": "friday", "friday": "friday",
        "sat": "saturday", "saturday": "saturday",
        "sun": "sunday", "sunday": "sunday",
    }
    return mapping.get(s, s)

# =================================================
# GOOGLE SHEETS HELPERS
# =================================================
@st.cache_resource
def get_gspread_client():
    creds_info = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_resource
def get_spreadsheet():
    client = get_gspread_client()
    return client.open_by_key(SPREADSHEET_ID)


def get_worksheet(name: str):
    ss = get_spreadsheet()
    return ss.worksheet(name)


def get_settings_sheet():
    """Get the settings tab, creating it if missing. Holds key/value rows."""
    ss = get_spreadsheet()
    try:
        return ss.worksheet(SHEET_SETTINGS)
    except WorksheetNotFound:
        sheet = ss.add_worksheet(title=SHEET_SETTINGS, rows=20, cols=2)
        sheet.update("A1:B2", [["key", "value"], ["emergency", "FALSE"]])
        return sheet


def set_emergency_flag(on: bool):
    """Write the campwide emergency flag. Every kiosk reads this on refresh."""
    try:
        sheet = get_settings_sheet()
        cell = sheet.find("emergency")
        value = "TRUE" if on else "FALSE"
        if cell:
            sheet.update_cell(cell.row, 2, value)
        else:
            sheet.append_row(["emergency", value])
        get_emergency_flag.clear()
    except Exception:
        # Never let a flag write crash the app. The triggering screen still
        # shows locally; other screens pick it up once the write lands.
        pass


@st.cache_data(ttl=8)
def get_emergency_flag() -> bool:
    """Read the campwide emergency flag. Short cache so it spreads fast.

    Fails safe to False if the settings tab cannot be read, so a Sheets hiccup
    never traps every kiosk on the red screen.
    """
    try:
        sheet = get_settings_sheet()
        df = read_sheet_df(sheet)
        if df.empty or "key" not in df.columns or "value" not in df.columns:
            return False
        row = df[df["key"].astype(str).str.strip().str.lower() == "emergency"]
        if row.empty:
            return False
        return str(row.iloc[0]["value"]).strip().upper() in ("TRUE", "1", "YES", "ON")
    except Exception:
        return False


def read_sheet_df(sheet) -> pd.DataFrame:
    """Read a worksheet into a DataFrame without crashing on bad headers.

    gspread's get_all_records throws when row 1 has a blank or duplicate
    header. That happens if a column gets added twice or a header cell is
    cleared. This reader uses raw values, makes every header unique and
    non-blank, and never throws, so the app stays up no matter the sheet's
    state. Extra de-duplicated columns are harmless and ignored downstream.
    """
    values = sheet.get_all_values()
    if not values:
        return pd.DataFrame()

    raw_headers = values[0]
    headers = []
    seen = {}
    for i, h in enumerate(raw_headers):
        name = str(h).strip()
        if not name:
            name = f"col_{i}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        headers.append(name)

    body = values[1:]
    width = len(headers)
    norm = []
    for r in body:
        r = list(r)
        if len(r) < width:
            r = r + [""] * (width - len(r))
        elif len(r) > width:
            r = r[:width]
        norm.append(r)

    return pd.DataFrame(norm, columns=headers)

# =================================================
# STAFF + DRIVERS (FROM SHEETS)
# =================================================
@st.cache_data(ttl=30)
def load_staff_df_cached():
    sheet = get_worksheet(SHEET_STAFF)
    df = read_sheet_df(sheet)
    for c in ["name", "pin", "active", "admin"]:
        if c not in df.columns:
            df[c] = ""

    df["name"] = df["name"].astype(str).str.strip()
    df["pin"] = df["pin"].astype(str).str.strip().apply(normalize_pin)

    # active: treat blank as TRUE
    a = df["active"].astype(str).str.upper().str.strip()
    df["active"] = a.isin(["TRUE", "1", "YES", "Y", ""])

    # admin: only explicit TRUE counts. Blank means not an admin.
    adm = df["admin"].astype(str).str.upper().str.strip()
    df["admin"] = adm.isin(["TRUE", "1", "YES", "Y"])

    df = df[df["name"] != ""].copy()
    return df


@st.cache_data(ttl=30)
def load_drivers_df_cached():
    sheet = get_worksheet(SHEET_DRIVERS)
    df = read_sheet_df(sheet)
    for c in ["name", "passed_test"]:
        if c not in df.columns:
            df[c] = ""

    df["name"] = df["name"].astype(str).str.strip()
    p = df["passed_test"].astype(str).str.upper().str.strip()
    df["passed_test"] = p.isin(["TRUE", "1", "YES", "Y"])
    df = df[df["name"] != ""].copy()
    return df


def get_staff_pins_and_lists():
    staff_df = load_staff_df_cached()
    drivers_df = load_drivers_df_cached()

    active_staff_df = staff_df[staff_df["active"]].copy()
    staff_pins = dict(zip(active_staff_df["name"], active_staff_df["pin"]))
    staff_names = sorted(list(staff_pins.keys()))

    eligible_driver_names = set(
        drivers_df.loc[drivers_df["passed_test"], "name"].tolist()
    )
    driver_names = sorted([n for n in staff_names if n in eligible_driver_names])

    return staff_pins, staff_names, driver_names


def build_pin_lookup(staff_pins: dict) -> dict:
    """Map each code to the staff who use it. A list catches shared codes."""
    lookup = {}
    for name, pin in staff_pins.items():
        p = normalize_pin(pin)
        lookup.setdefault(p, []).append(name)
    return lookup


def resolve_code(code: str, pin_lookup: dict):
    """Turn a typed code into a single staff name.

    Returns (name, error). Exactly one match returns the name. No match or a
    shared code returns an error message and no name.
    """
    p = normalize_pin(code)
    if not str(code).strip():
        return None, "Enter your code."
    names = pin_lookup.get(p, [])
    if len(names) == 1:
        return names[0], None
    if len(names) == 0:
        return None, "Code not recognized."
    return None, "This code is shared by more than one person. Ask the office for a unique code."


def get_admin_names() -> set:
    """Names of active staff flagged admin=TRUE in the staff sheet."""
    staff_df = load_staff_df_cached()
    active_admins = staff_df[(staff_df["active"]) & (staff_df["admin"])]
    return set(active_admins["name"].tolist())


def resolve_admin_code(code: str, staff_pins: dict):
    """Turn a typed code into an admin name, or an error.

    The code must belong to exactly one active staff member AND that person
    must be flagged admin=TRUE. Anyone else, even with a valid code, is
    rejected. Returns (admin_name, error).
    """
    lookup = build_pin_lookup(staff_pins)
    name, err = resolve_code(code, lookup)
    if err:
        return None, err
    if name not in get_admin_names():
        return None, "This code is not an admin code."
    return name, None

# =================================================
# LOGS SHEET HELPERS
# =================================================
def ensure_logs_header(sheet):
    """Ensure logs header exists; append missing columns to end."""
    try:
        headers = sheet.row_values(1)
        if not headers:
            sheet.insert_row(LOGS_HEADERS_REQUIRED, 1)
            return
        missing = [h for h in LOGS_HEADERS_REQUIRED if h not in headers]
        if missing:
            new_headers = headers + missing
            sheet.delete_rows(1)
            sheet.insert_row(new_headers, 1)
    except Exception as e:
        st.error(f"Could not ensure logs header: {e}")
        st.stop()


@st.cache_data(ttl=10)
def load_logs_df_cached():
    try:
        sheet = get_worksheet(SHEET_LOGS)
        ensure_logs_header(sheet)
        df = read_sheet_df(sheet)
    except Exception:
        return pd.DataFrame(columns=LOGS_HEADERS_REQUIRED)

    for c in LOGS_HEADERS_REQUIRED:
        if c not in df.columns:
            df[c] = ""

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["name"] = df["name"].astype(str).str.strip()
    df["status"] = df["status"].astype(str).str.strip().str.upper()
    df["action"] = df["action"].astype(str).str.strip().str.upper()
    df["reason"] = df["reason"].astype(str).str.strip()
    df["other_reason"] = df["other_reason"].astype(str)

    return df


def clear_logs_cache():
    load_logs_df_cached.clear()


def notify_phone(title: str, message: str):
    """Push a notification to the main phone through ntfy.

    Reads the topic from secrets, so nothing leaks in the code. A push failure
    never blocks a sign-out, because the whole call is wrapped and ignored on
    error. Set ntfy_topic in Streamlit secrets to turn this on.
    """
    _ntfy_send(st.secrets.get("ntfy_topic", ""), title, message)


def notify_vans(title: str, message: str):
    """Push van events to the vans phone.

    Posts to ntfy_topic_vans if set, so van pushes land on a different phone
    than staff sign-outs. Falls back to the main topic if the vans topic is
    not configured, so van alerts never silently vanish.
    """
    topic = st.secrets.get("ntfy_topic_vans", "") or st.secrets.get("ntfy_topic", "")
    _ntfy_send(topic, title, message)


def notify_emergency(title: str, message: str):
    """High-priority push to the main phone for the emergency code."""
    _ntfy_send(st.secrets.get("ntfy_topic", ""), title, message, priority="urgent", tags="rotating_light")


def _ntfy_send(topic: str, title: str, message: str, priority: str = "", tags: str = ""):
    try:
        if not topic:
            return
        server = str(st.secrets.get("ntfy_server", "https://ntfy.sh")).rstrip("/")
        headers = {"Title": title}
        if priority:
            headers["Priority"] = priority
        if tags:
            headers["Tags"] = tags
        requests.post(
            f"{server}/{topic}",
            data=message.encode("utf-8"),
            headers=headers,
            timeout=4,
        )
    except Exception:
        pass


def append_log_row(name: str, reason: str, other_reason: str, action: str, status: str, notify: bool = True):
    """Write a log row mapped to the sheet's actual header order.

    Mapping by header (instead of fixed position) keeps rows aligned even if
    someone reorders columns in Google Sheets. Set notify=False for van-driven
    auto sign-outs, so they do not buzz the staff phone (the vans phone covers
    those).
    """
    try:
        sheet = get_worksheet(SHEET_LOGS)
        ensure_logs_header(sheet)

        row_dict = {
            "id": str(uuid.uuid4())[:8],
            "timestamp": datetime.now(TZ).isoformat(timespec="seconds"),
            "name": name,
            "reason": reason,
            "other_reason": other_reason or "",
            "action": action,
            "status": status,
        }
        headers = [h.strip() for h in sheet.row_values(1) if str(h).strip()]
        row = [row_dict.get(h, "") for h in headers]
        sheet.append_row(row)
        clear_logs_cache()

        # Phone push after a clean write. Sign-out shows the reason; the typed
        # detail wins when the reason is Other.
        if notify:
            if action == "OUT":
                detail = other_reason.strip() if (reason.startswith("Other") and other_reason.strip()) else reason
                notify_phone("Bauercrest: Signed OUT", f"{name}: {detail}")
            else:
                notify_phone("Bauercrest: Signed IN", name)
    except (APIError, GSpreadException):
        st.error("Could not record this sign-in/sign-out due to a problem talking to Google Sheets.")
        st.stop()


def clear_all_logs():
    try:
        sheet = get_worksheet(SHEET_LOGS)
        sheet.clear()
        sheet.insert_row(LOGS_HEADERS_REQUIRED, 1)
        clear_logs_cache()
    except (APIError, GSpreadException):
        st.error("Could not clear logs in Google Sheets. Please try again later.")
        st.stop()


def delete_logs_by_ids(ids_to_delete):
    try:
        sheet = get_worksheet(SHEET_LOGS)
        df = load_logs_df_cached()
    except (APIError, GSpreadException):
        st.error("Could not update logs in Google Sheets. Please try again later.")
        st.stop()

    if df.empty:
        return

    df_keep = df[~df["id"].isin(ids_to_delete)].copy()

    try:
        sheet.clear()
        sheet.insert_row(LOGS_HEADERS_REQUIRED, 1)
        if not df_keep.empty:
            df_out = df_keep.copy()
            df_out["timestamp"] = df_out["timestamp"].astype(str)
            rows = df_out[LOGS_HEADERS_REQUIRED].values.tolist()
            if rows:
                sheet.append_rows(rows)
        clear_logs_cache()
    except (APIError, GSpreadException):
        st.error("Could not finish deleting selected log entries. Please try again later.")
        st.stop()

# =================================================
# LOGIC HELPERS
# =================================================
def get_currently_out(df: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe of people whose latest status is OUT."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["name", "reason", "other_reason", "timestamp"])

    tmp = df.copy()
    tmp["timestamp"] = pd.to_datetime(tmp["timestamp"], errors="coerce")
    tmp = tmp.sort_values("timestamp", na_position="last")

    last_actions = tmp.groupby("name", dropna=False).tail(1)
    out_rows = last_actions[last_actions["status"] == "OUT"].copy()

    if out_rows.empty:
        return pd.DataFrame(columns=["name", "reason", "other_reason", "timestamp"])

    return out_rows[["name", "reason", "other_reason", "timestamp"]]


# Marker stored in a van-driven sign-out's other_reason. Lets the van return
# sign back in only the people the van itself signed out, and never touch
# someone who was already out for their own reason.
VAN_SIGNOUT_TAG = "VAN_TRIP"


def get_latest_status_map(df: pd.DataFrame) -> dict:
    """Map each name to their most recent log row: status, reason, other_reason.

    Stable sort keeps sheet order for same-second timestamps, so the last row
    written for a person wins.
    """
    if df is None or df.empty:
        return {}
    tmp = df.copy()
    tmp["timestamp"] = pd.to_datetime(tmp["timestamp"], errors="coerce")
    tmp = tmp.sort_values("timestamp", na_position="last", kind="stable")
    last = tmp.groupby("name", dropna=False).tail(1)
    out = {}
    for _, r in last.iterrows():
        out[str(r.get("name", "")).strip()] = {
            "status": str(r.get("status", "")).strip().upper(),
            "reason": str(r.get("reason", "")).strip(),
            "other_reason": str(r.get("other_reason", "")).strip(),
        }
    return out


def append_log_rows_batch(rows: list) -> bool:
    """Write several log rows in ONE API call.

    One call instead of one per person avoids tripping Google's per-minute
    write limit on a full van. Never halts the app: returns True on success,
    False on failure, so the caller stays in control.
    """
    if not rows:
        return True
    try:
        sheet = get_worksheet(SHEET_LOGS)
        ensure_logs_header(sheet)
        headers = [h.strip() for h in sheet.row_values(1) if str(h).strip()]
        matrix = [[rd.get(h, "") for h in headers] for rd in rows]
        sheet.append_rows(matrix)
        clear_logs_cache()
        return True
    except Exception:
        return False


def auto_signout_for_van(party: list, van_name: str):
    """Sign out everyone on a van who is currently IN. Reason: Van.

    Anyone already OUT (a Period Off, an earlier trip) is left untouched, so
    no doubles and no overwriting a real reason. Tagged so the van return can
    find exactly these people. Written in one batched call and never halts the
    van flow. Returns the list of names signed out.
    """
    status_map = get_latest_status_map(load_logs_df_cached())
    rows = []
    signed = []
    for name in party:
        name = (name or "").strip()
        if not name:
            continue
        info = status_map.get(name)
        if info and info["status"] == "OUT":
            continue  # already out, leave their own sign-out alone
        rows.append({
            "id": str(uuid.uuid4())[:8],
            "timestamp": datetime.now(TZ).isoformat(timespec="seconds"),
            "name": name,
            "reason": "Van",
            "other_reason": f"{VAN_SIGNOUT_TAG}|{van_name}",
            "action": "OUT",
            "status": "OUT",
        })
        signed.append(name)
    ok = append_log_rows_batch(rows)
    return signed if ok else []


def auto_signin_for_van(party: list):
    """Sign back in only the people whose latest row is a van sign-out.

    Someone who signed themselves in already, or who was out for their own
    reason, is skipped. No doubles, no overriding. One batched call, never
    halts the van flow. Returns the list of names signed in.
    """
    status_map = get_latest_status_map(load_logs_df_cached())
    rows = []
    signed = []
    for name in party:
        name = (name or "").strip()
        if not name:
            continue
        info = status_map.get(name)
        if not info:
            continue
        is_van_out = (
            info["status"] == "OUT"
            and info["reason"] == "Van"
            and info["other_reason"].startswith(VAN_SIGNOUT_TAG)
        )
        if is_van_out:
            rows.append({
                "id": str(uuid.uuid4())[:8],
                "timestamp": datetime.now(TZ).isoformat(timespec="seconds"),
                "name": name,
                "reason": "Van",
                "other_reason": info["other_reason"],
                "action": "IN",
                "status": "IN",
            })
            signed.append(name)
    ok = append_log_rows_batch(rows)
    return signed if ok else []


# =================================================
# SPECIAL OPERATOR CODES (only you know these)
# =================================================
# Stored in Streamlit secrets so they never sit in the repo. Set any subset:
#   code_field_trip_out, code_field_trip_in, code_emergency,
#   code_headcount, code_clear
FIELD_TRIP_TAG = "FIELD_TRIP"

# Marker written into a log row when an admin signs someone in from the Admin
# page. Lets the history show a human corrected the board.
ADMIN_SIGNIN_TAG = "ADMIN_SIGNIN"


def get_special_code(name: str) -> str:
    """Read one special code from secrets. Blank if unset."""
    return str(st.secrets.get(name, "")).strip()


def match_special_code(code: str):
    """Return which special action a typed code triggers, or None.

    Only non-blank configured codes can match, so an empty box never fires
    anything. Codes are compared as typed (not zero-padded like staff PINs),
    so make them 5-6 digits to stay clear of 4-digit staff codes.
    """
    code = str(code or "").strip()
    if not code:
        return None
    table = {
        "field_trip_out": get_special_code("code_field_trip_out"),
        "field_trip_in": get_special_code("code_field_trip_in"),
        "emergency": get_special_code("code_emergency"),
        "headcount": get_special_code("code_headcount"),
        "clear": get_special_code("code_clear"),
    }
    for action, secret in table.items():
        if secret and code == secret:
            return action
    return None


def field_trip_signout_all(staff_names: list) -> int:
    """Sign out every active staff member who is currently IN. Reason: Field Trip.

    Anyone already out (a Period Off, a van) is skipped, so no doubles and no
    overwriting a real reason. Tagged so the return code signs back in exactly
    these people. One batched write. Returns how many were signed out.
    """
    status_map = get_latest_status_map(load_logs_df_cached())
    rows = []
    stamp = datetime.now(TZ).isoformat(timespec="seconds")
    for name in staff_names:
        name = (name or "").strip()
        if not name:
            continue
        info = status_map.get(name)
        if info and info["status"] == "OUT":
            continue
        rows.append({
            "id": str(uuid.uuid4())[:8],
            "timestamp": stamp,
            "name": name,
            "reason": "Field Trip",
            "other_reason": FIELD_TRIP_TAG,
            "action": "OUT",
            "status": "OUT",
        })
    append_log_rows_batch(rows)
    return len(rows)


def field_trip_signin_all() -> int:
    """Sign back in only the people the field trip signed out.

    Anyone who signed themselves in already, or who is out for another reason,
    is left alone. One batched write. Returns how many were signed in.
    """
    status_map = get_latest_status_map(load_logs_df_cached())
    rows = []
    stamp = datetime.now(TZ).isoformat(timespec="seconds")
    for name, info in status_map.items():
        name = (name or "").strip()
        if not name:
            continue
        is_trip_out = (
            info["status"] == "OUT"
            and info["reason"] == "Field Trip"
            and info["other_reason"].startswith(FIELD_TRIP_TAG)
        )
        if is_trip_out:
            rows.append({
                "id": str(uuid.uuid4())[:8],
                "timestamp": stamp,
                "name": name,
                "reason": "Field Trip",
                "other_reason": info["other_reason"],
                "action": "IN",
                "status": "IN",
            })
    append_log_rows_batch(rows)
    return len(rows)


def handle_special_code(action: str, staff_names: list) -> bool:
    """Run a special action. Returns True if it set a flash and needs a rerun.

    Screen actions (emergency, headcount, clear) are handled by the caller via
    query params; this handles the data actions and alerts.
    """
    if action == "field_trip_out":
        count = field_trip_signout_all(staff_names)
        notify_phone("Bauercrest: FIELD TRIP", f"All staff signed out for a field trip ({count}).")
        st.session_state["log_flash"] = f"Field trip: {count} staff signed out of camp."
        return True
    if action == "field_trip_in":
        count = field_trip_signin_all()
        notify_phone("Bauercrest: Field trip back", f"Field trip returned, {count} signed back in.")
        st.session_state["log_flash"] = f"Field trip: {count} staff signed back in."
        return True
    return False

# =================================================
# DAYS OFF (DISPLAY ONLY)
# =================================================
@st.cache_data(ttl=15)
def load_days_off_df_cached():
    """Reads days_off sheet if present. If missing, returns empty DF (feature disabled)."""
    try:
        sheet = get_worksheet(SHEET_DAYS_OFF)
        df = read_sheet_df(sheet)
    except Exception:
        return pd.DataFrame(columns=["name", "weekday", "active"])

    for c in ["name", "weekday", "active"]:
        if c not in df.columns:
            df[c] = ""

    df["name"] = df["name"].astype(str).str.strip()
    df["weekday"] = df["weekday"].astype(str).apply(normalize_weekday)

    a = df["active"].astype(str).str.upper().str.strip()
    df["active"] = a.isin(["TRUE", "1", "YES", "Y", ""])

    df = df[df["name"] != ""].copy()
    return df


def get_day_off_names_today() -> list:
    """Names scheduled for a day off today, from the days_off sheet.

    Display only. The app never signs anyone out automatically; counselors
    still sign out at the Big House like everyone else.
    """
    now = datetime.now(TZ)
    today_weekday = normalize_weekday(now.strftime("%A"))

    df_days = load_days_off_df_cached()
    if df_days.empty:
        return []

    names = df_days[
        (df_days["active"]) &
        (df_days["weekday"] == today_weekday)
    ]["name"].tolist()

    return sorted(set(n for n in names if n))

# =================================================
# VANS HELPERS
# =================================================
def get_vans_sheet():
    return get_worksheet(SHEET_VANS)


def ensure_vans_header(sheet):
    """Keep the vans header row clean: every required column present exactly once.

    Compares case-insensitively and trims spaces, so it never adds a second
    gas_left because of casing or a stray space. If the header is blank or has
    duplicates, it rewrites a clean header without touching the data rows.
    """
    try:
        headers = sheet.row_values(1)
        if not headers:
            sheet.insert_row(VANS_HEADERS_REQUIRED, 1)
            return

        norm = [str(h).strip().lower() for h in headers]
        has_dupes = len(norm) != len(set(n for n in norm if n))
        has_blanks = any(not n for n in norm)
        missing = [h for h in VANS_HEADERS_REQUIRED if h.lower() not in norm]

        if has_dupes or has_blanks:
            # Rebuild a clean header: keep the required columns in order, then
            # any extra real columns that already hold data, de-duplicated.
            extras = []
            seen = set(h.lower() for h in VANS_HEADERS_REQUIRED)
            for h in headers:
                hl = str(h).strip().lower()
                if hl and hl not in seen:
                    extras.append(str(h).strip())
                    seen.add(hl)
            clean = VANS_HEADERS_REQUIRED + extras
            sheet.delete_rows(1)
            sheet.insert_row(clean, 1)
        elif missing:
            new_headers = [str(h).strip() for h in headers] + missing
            sheet.delete_rows(1)
            sheet.insert_row(new_headers, 1)
    except Exception as e:
        st.error(f"Could not ensure vans header: {e}")
        st.stop()


@st.cache_data(ttl=10)
def load_vans_df_cached():
    sheet = get_vans_sheet()
    ensure_vans_header(sheet)
    return read_sheet_df(sheet)


def clear_vans_cache():
    load_vans_df_cached.clear()


def append_vans_row(row_dict: dict):
    sheet = get_vans_sheet()
    ensure_vans_header(sheet)

    headers = [h.strip() for h in sheet.row_values(1) if str(h).strip()]
    row = [row_dict.get(h, "") for h in headers]
    sheet.append_row(row)
    clear_vans_cache()


def compute_van_status(vans_df: pd.DataFrame) -> dict:
    status_map = {v: {"status": "IN"} for v in VANS}
    if vans_df is None or vans_df.empty:
        return status_map

    for col in ["timestamp", "van", "status", "driver", "purpose", "passengers", "other_purpose", "action"]:
        if col not in vans_df.columns:
            vans_df[col] = ""

    tmp = vans_df.copy()
    tmp["timestamp"] = pd.to_datetime(tmp["timestamp"], errors="coerce")
    tmp = tmp.sort_values("timestamp", na_position="last")

    for v in VANS:
        rows = tmp[tmp["van"] == v]
        if rows.empty:
            continue
        last = rows.iloc[-1]
        st_val = str(last.get("status", "")).strip().upper()
        if st_val not in ("IN", "OUT"):
            st_val = "IN"
        status_map[v] = {
            "status": st_val,
            "driver": str(last.get("driver", "")).strip(),
            "purpose": str(last.get("purpose", "")).strip(),
            "other_purpose": str(last.get("other_purpose", "")).strip(),
            "passengers": str(last.get("passengers", "")).strip(),
        }
    return status_map


def next_available_van(status_map: dict):
    for v in VANS:
        if status_map.get(v, {}).get("status") != "OUT":
            return v
    return None

# =================================================
# BOARD RENDERING
# =================================================
def clean_other_reason(other_reason: str) -> str:
    """Hide legacy auto day-off tags from public display."""
    s = str(other_reason or "").strip()
    if s.startswith(f"{LEGACY_AUTO_TAG_PREFIX}|"):
        return ""
    return s


def render_out_cards(df_out: pd.DataFrame):
    cards = []
    df = df_out.sort_values("timestamp")
    for _, row in df.iterrows():
        name = esc(row.get("name", ""))
        reason = esc(row.get("reason", ""))
        details = esc(clean_other_reason(row.get("other_reason", "")))
        when = esc(format_board_time(row.get("timestamp")))

        details_html = f"<div class='bc-meta'>{details}</div>" if details else ""
        cards.append(
            f"<div class='bc-card'>"
            f"<div class='bc-chip'>{reason}</div>"
            f"<div class='bc-name'>{name}</div>"
            f"{details_html}"
            f"<div class='bc-meta'>Signed out at <span class='bc-time'>{when}</span></div>"
            f"</div>"
        )
    st.markdown(f"<div class='bc-grid'>{''.join(cards)}</div>", unsafe_allow_html=True)


def render_day_off_chips(names: list):
    chips = "".join(f"<div class='bc-dayoff'>{esc(n)}</div>" for n in names)
    st.markdown(f"<div class='bc-dayoff-row'>{chips}</div>", unsafe_allow_html=True)


def render_van_cards(status_map: dict):
    cards = []
    for v in VANS:
        info = status_map.get(v, {"status": "IN"})
        out = info.get("status") == "OUT"

        if out:
            purpose = info.get("purpose", "")
            if purpose == "Other" and info.get("other_purpose"):
                purpose = f"Other: {info.get('other_purpose')}"
            passengers = info.get("passengers", "")
            passengers_html = (
                f"<div class='bc-meta'>Passengers: {esc(passengers)}</div>" if passengers else ""
            )
            cards.append(
                f"<div class='bc-van-card bc-van-out'>"
                f"<div class='bc-van-status out'>OUT</div>"
                f"<div class='bc-van-title'>{esc(van_label(v))}</div>"
                f"<div class='bc-meta'>Driver: <strong>{esc(info.get('driver', ''))}</strong></div>"
                f"<div class='bc-meta'>Purpose: {esc(purpose)}</div>"
                f"{passengers_html}"
                f"</div>"
            )
        else:
            cards.append(
                f"<div class='bc-van-card'>"
                f"<div class='bc-van-status in'>IN</div>"
                f"<div class='bc-van-title'>{esc(van_label(v))}</div>"
                f"<div class='bc-meta'>Parked at camp</div>"
                f"</div>"
            )
    st.markdown(f"<div class='bc-grid'>{''.join(cards)}</div>", unsafe_allow_html=True)

# =================================================
# PAGES
# =================================================
def page_sign_in_out(staff_pins: dict, staff_names: list):
    page_title("Camp Bauercrest Staff", "Sign In / Out")

    pin_lookup = build_pin_lookup(staff_pins)

    # Bumping this nonce changes the code field key, so the box comes back
    # empty after each use. The next counselor starts fresh.
    if "signio_nonce" not in st.session_state:
        st.session_state["signio_nonce"] = 0
    n = st.session_state["signio_nonce"]

    flash = st.session_state.pop("log_flash", "")
    if flash:
        flash_banner(flash)

    df_logs = load_logs_df_cached()
    df_out = get_currently_out(df_logs)

    st.caption("Type your code and press Enter. If you are in, you go out. If you are out, you come back in.")

    # Reason only matters when the code turns out to be a sign-out. It sits
    # above the box and is read only if the person is currently in.
    reason = st.selectbox("Reason (only used if you are signing out)", REASONS, key="signout_reason")
    other_reason = ""
    if reason == "Other (type reason)":
        other_reason = st.text_input("Type your reason", key="signout_other_reason")

    with st.form("signio_form", clear_on_submit=False):
        code = st.text_input("Your code", type="password", max_chars=6, key=f"signio_code_{n}")
        submitted = st.form_submit_button("Enter", use_container_width=True)

    if submitted:
        special = match_special_code(code)
        if special:
            # Operator codes take over before any normal sign logic.
            st.session_state["signio_nonce"] += 1
            if special == "emergency":
                set_emergency_flag(True)
                notify_emergency("BAUERCREST EMERGENCY", "Emergency declared. All screens showing head count.")
                st.rerun()
            elif special == "headcount":
                st.query_params["screen"] = "headcount"
                st.rerun()
            elif special == "clear":
                set_emergency_flag(False)
                if "screen" in st.query_params:
                    del st.query_params["screen"]
                st.rerun()
            else:
                handle_special_code(special, staff_names)
                st.rerun()
        else:
            name, err = resolve_code(code, pin_lookup)
            if err:
                st.error(err)
            else:
                is_out = (not df_out.empty) and name in df_out["name"].values
                if is_out:
                    # Currently out -> sign them back in, carrying their reason.
                    row = df_out[df_out["name"] == name].iloc[0]
                    append_log_row(name, row["reason"], row["other_reason"], action="IN", status="IN")
                    st.session_state["log_flash"] = f"{name} signed IN. Welcome back."
                    st.session_state["signio_nonce"] += 1
                    st.rerun()
                elif reason == "Other (type reason)" and not other_reason.strip():
                    st.error("Please type a reason for 'Other'.")
                else:
                    append_log_row(name, reason, other_reason, action="OUT", status="OUT")
                    st.session_state["log_flash"] = f"{name} signed OUT. Reason: {reason if reason != 'Other (type reason)' else other_reason}."
                    st.session_state["signio_nonce"] += 1
                    st.rerun()

    crest_footer()


def page_whos_out():
    page_title("The Big House Board", "Who's Out Right Now")

    @st.fragment(run_every=BOARD_REFRESH_SECONDS)
    def live_board():
        # Flip to the red screen if an emergency is declared on another machine.
        escalate_if_emergency_changed("normal")

        df_logs = load_logs_df_cached()
        df_out = get_currently_out(df_logs)

        if df_out.empty:
            empty_note("No staff are currently signed out.")
        else:
            render_out_cards(df_out)

        # Day Off board (display only). Reads the days_off sheet. The app never
        # signs anyone out automatically; this is a reminder of who is scheduled.
        day_off_names = get_day_off_names_today()
        if day_off_names:
            st.markdown("")
            section_title(f"Day Off Today ({datetime.now(TZ).strftime('%A')})")
            render_day_off_chips(day_off_names)
            st.caption("Scheduled days off from the days_off sheet. Everyone still signs out and in at the Big House.")

    live_board()
    crest_footer()


def page_vans(staff_pins: dict, staff_names: list, driver_names: list):
    page_title("Camp Vehicles", "Vans")

    if "van_form_nonce" not in st.session_state:
        st.session_state["van_form_nonce"] = 0
    van_nonce = st.session_state["van_form_nonce"]

    flash = st.session_state.pop("van_flash", "")
    if flash:
        flash_banner(flash)

    vans_df = load_vans_df_cached()
    status_map = compute_van_status(vans_df)
    out_vans = [v for v in VANS if status_map.get(v, {}).get("status") == "OUT"]
    free_vans = [v for v in VANS if status_map.get(v, {}).get("status") != "OUT"]

    @st.fragment(run_every=BOARD_REFRESH_SECONDS)
    def live_van_status():
        # Flip to the red screen if an emergency is declared elsewhere.
        escalate_if_emergency_changed("normal")
        fresh = compute_van_status(load_vans_df_cached())
        section_title("Van Status")
        render_van_cards(fresh)

    live_van_status()

    st.divider()
    section_title("Sign Out a Van")

    if not free_vans:
        st.warning("No vans available. All vans are currently out.")
    else:
        # IMPORTANT: check eligibility OUTSIDE the form to avoid "missing submit button" warning
        if not driver_names:
            st.warning("No eligible drivers found. Set drivers.passed_test=TRUE for cleared drivers.")
        else:
            pin_lookup = build_pin_lookup(staff_pins)
            with st.form("van_signout_form", clear_on_submit=False):
                st.caption("Pick the van and type your driver code. Passengers sign themselves out on the main page.")
                chosen_van = st.selectbox(
                    "Which van are you taking?",
                    free_vans,
                    format_func=van_label,
                    key=f"van_choice_{van_nonce}",
                )
                driver_code = st.text_input(
                    "Driver code",
                    type="password",
                    max_chars=4,
                    key=f"van_driver_code_{van_nonce}",
                )
                purpose = st.selectbox("Purpose", VAN_PURPOSES, key=f"van_purpose_{van_nonce}")

                other_purpose = ""
                if purpose == "Other":
                    other_purpose = st.text_input("Other purpose (required)", key=f"van_other_purpose_{van_nonce}")

                submitted = st.form_submit_button("Sign Out Van", use_container_width=True)

            if submitted:
                driver, err = resolve_code(driver_code, pin_lookup)
                if err:
                    st.error(err)
                    return

                if driver not in driver_names:
                    st.error("This code is not cleared to drive a van.")
                    return

                # Guard against two people grabbing the same van at once.
                fresh_status = compute_van_status(load_vans_df_cached())
                if fresh_status.get(chosen_van, {}).get("status") == "OUT":
                    st.error(f"{van_label(chosen_van)} was taken a moment ago. Pick another van.")
                    return

                if purpose == "Other" and not other_purpose.strip():
                    st.error("Please enter the other purpose.")
                    return

                row = {
                    "id": str(uuid.uuid4())[:8],
                    "timestamp": datetime.now(TZ).isoformat(timespec="seconds"),
                    "van": chosen_van,
                    "driver": driver,
                    "purpose": purpose,
                    "passengers": "",
                    "other_purpose": other_purpose.strip(),
                    "action": "CHECKOUT",
                    "status": "OUT",
                }
                try:
                    append_vans_row(row)
                except Exception:
                    st.error("Could not save the van checkout. Please try again.")
                    return

                # Van is saved. Push to the vans phone right away, before the
                # camp-board link, so nothing downstream can block the alert.
                purpose_text = other_purpose.strip() if (purpose == "Other" and other_purpose.strip()) else purpose
                notify_vans(
                    "Bauercrest: Van OUT",
                    f"{van_label(chosen_van)} - {driver} ({purpose_text})",
                )

                # Sign the driver out of camp. A hiccup here never undoes the
                # van checkout or the alert above.
                try:
                    auto_signout_for_van([driver], chosen_van)
                except Exception:
                    st.warning("Van saved and alert sent, but linking the driver to the Who's Out board hit a snag.")

                st.session_state["van_form_nonce"] += 1
                st.session_state["van_flash"] = f"{van_label(chosen_van)} signed out under {driver}. Driver signed out of camp."
                st.rerun()

    # Sign IN section
    if out_vans:
        st.divider()
        section_title("Sign In a Van")

        pin_lookup_in = build_pin_lookup(staff_pins)
        with st.form("van_signin_form", clear_on_submit=True):
            st.caption("Pick the van you are returning, type your code, set the gas left, and submit.")
            van_to_in = st.selectbox(
                "Which van are you signing back in?",
                out_vans,
                format_func=van_label,
            )
            return_driver_code = st.text_input("Your code", type="password", max_chars=4)
            gas_left = st.selectbox("Gas left", ["Full", "3/4", "Half", "1/4", "Low / Empty"])
            submitted_in = st.form_submit_button("Sign In Van", use_container_width=True)

        if submitted_in:
            # Signing a van back in records its return. The driving is already
            # done, so any recognized active staff code works here. Taking a
            # van OUT still requires a driving-tested driver.
            return_driver, err = resolve_code(return_driver_code, pin_lookup_in)
            if err:
                st.error(err)
                return

            last_purpose = ""
            last_other_purpose = ""
            original_driver = ""
            try:
                tmp = vans_df.copy()
                tmp["timestamp"] = pd.to_datetime(tmp["timestamp"], errors="coerce")
                tmp = tmp.sort_values("timestamp", na_position="last")
                van_rows = tmp[tmp["van"] == van_to_in]
                if not van_rows.empty:
                    out_rows = van_rows[van_rows["status"].astype(str).str.upper() == "OUT"]
                    src = out_rows.iloc[-1] if not out_rows.empty else van_rows.iloc[-1]
                    last_purpose = str(src.get("purpose", "")).strip()
                    last_other_purpose = str(src.get("other_purpose", "")).strip()
                    original_driver = str(src.get("driver", "")).strip()
            except Exception:
                pass

            row = {
                "id": str(uuid.uuid4())[:8],
                "timestamp": datetime.now(TZ).isoformat(timespec="seconds"),
                "van": van_to_in,
                "driver": return_driver,
                "purpose": last_purpose,
                "passengers": "",
                "other_purpose": last_other_purpose,
                "action": "CHECKIN",
                "status": "IN",
                "gas_left": gas_left,
            }
            try:
                append_vans_row(row)
            except Exception:
                st.error("Could not save the van sign-in. Please try again.")
                return

            # Van is saved. Push to the vans phone right away, with the gas
            # level, before the camp-board link.
            notify_vans(
                "Bauercrest: Van IN",
                f"{van_label(van_to_in)} returned by {return_driver}, gas: {gas_left}",
            )

            # Sign the driver back into camp. Only the driver the van signed
            # out is touched. A hiccup here never undoes the sign-in or alert.
            try:
                auto_signin_for_van([original_driver])
            except Exception:
                st.warning("Van signed in and alert sent, but linking the driver back to the Who's Out board hit a snag.")

            st.session_state["van_flash"] = f"{van_label(van_to_in)} signed back in under {return_driver}. Gas: {gas_left}. Driver signed back in."
            st.rerun()

    crest_footer()


def page_admin_history(staff_pins: dict):
    page_title("Office Use Only", "Admin / History")
    ADMIN_PASSWORD = st.secrets.get("admin_password", "")

    if "admin_authenticated" not in st.session_state:
        st.session_state.admin_authenticated = False

    if not st.session_state.admin_authenticated:
        st.info("Admin access is password protected.")
        pw = st.text_input("Enter admin password", type="password", key="admin_pw_input")
        col_pw_btn, _ = st.columns([1, 3])
        with col_pw_btn:
            if st.button("Unlock Admin", key="admin_pw_btn"):
                if pw == ADMIN_PASSWORD and ADMIN_PASSWORD != "":
                    st.session_state.admin_authenticated = True
                    st.success("Access granted.")
                    st.rerun()
                else:
                    st.error("Incorrect password (or admin_password not set in secrets).")
        st.stop()

    with st.expander("Admin Session", expanded=False):
        st.caption("You are logged in to the admin area.")
        if st.button("Lock Admin Area", key="admin_logout_btn"):
            st.session_state.admin_authenticated = False
            st.success("Admin area locked again.")
            st.rerun()

    # -------------------------------------------------
    # ADMIN SIGN-IN: fix the board when someone forgot
    # -------------------------------------------------
    admin_flash = st.session_state.pop("admin_flash", "")
    if admin_flash:
        st.success(admin_flash)

    section_title("Sign Someone Back In")
    st.caption("For when a staff member forgot to sign in. Pick the person, type your admin code, and sign them in.")

    df_out_now = get_currently_out(load_logs_df_cached())
    out_names_now = sorted(df_out_now["name"].tolist())

    if not out_names_now:
        empty_note("No staff are currently signed out.")
    else:
        with st.form("admin_signin_form", clear_on_submit=True):
            who = st.selectbox("Who is signed out?", out_names_now)
            admin_code = st.text_input("Your admin code", type="password", max_chars=6)
            do_signin = st.form_submit_button("Sign This Person In")

        if do_signin:
            admin_name, err = resolve_admin_code(admin_code, staff_pins)
            if err:
                st.error(err)
            else:
                row = df_out_now[df_out_now["name"] == who].iloc[0]
                append_log_row(
                    who,
                    row["reason"],
                    f"{ADMIN_SIGNIN_TAG}|{admin_name}",
                    action="IN",
                    status="IN",
                )
                st.session_state["admin_flash"] = f"{who} signed in by {admin_name}."
                st.rerun()

    st.markdown("---")

    # -------------------------------------------------
    # ADMIN VAN SIGN-IN: fix a van left showing OUT
    # -------------------------------------------------
    section_title("Sign a Van Back In")
    st.caption("For when a van was left showing out. Pick the van, type your admin code, and sign it in.")

    vans_now = load_vans_df_cached()
    status_now = compute_van_status(vans_now)
    out_vans_now = [v for v in VANS if status_now.get(v, {}).get("status") == "OUT"]

    if not out_vans_now:
        empty_note("No vans are currently signed out.")
    else:
        with st.form("admin_van_signin_form", clear_on_submit=True):
            which_van = st.selectbox("Which van is out?", out_vans_now, format_func=van_label)
            van_admin_code = st.text_input("Your admin code", type="password", max_chars=6, key="admin_van_code")
            do_van_signin = st.form_submit_button("Sign This Van In")

        if do_van_signin:
            admin_name, err = resolve_admin_code(van_admin_code, staff_pins)
            if err:
                st.error(err)
            else:
                # Pull the van's original checkout so we can free its driver too.
                orig_driver = ""
                last_purpose = ""
                last_other_purpose = ""
                try:
                    tmp = vans_now.copy()
                    tmp["timestamp"] = pd.to_datetime(tmp["timestamp"], errors="coerce")
                    tmp = tmp.sort_values("timestamp", na_position="last")
                    vr = tmp[tmp["van"] == which_van]
                    if not vr.empty:
                        outr = vr[vr["status"].astype(str).str.upper() == "OUT"]
                        src = outr.iloc[-1] if not outr.empty else vr.iloc[-1]
                        orig_driver = str(src.get("driver", "")).strip()
                        last_purpose = str(src.get("purpose", "")).strip()
                        last_other_purpose = str(src.get("other_purpose", "")).strip()
                except Exception:
                    pass

                van_row = {
                    "id": str(uuid.uuid4())[:8],
                    "timestamp": datetime.now(TZ).isoformat(timespec="seconds"),
                    "van": which_van,
                    "driver": orig_driver or admin_name,
                    "purpose": last_purpose,
                    "passengers": "",
                    "other_purpose": (last_other_purpose + f" [{ADMIN_SIGNIN_TAG}|{admin_name}]").strip(),
                    "action": "CHECKIN",
                    "status": "IN",
                    "gas_left": "",
                }
                try:
                    append_vans_row(van_row)
                except Exception:
                    st.error("Could not sign the van in. Please try again.")
                    st.stop()

                # Free the original driver on the camp board too, if the van
                # had signed them out.
                if orig_driver:
                    try:
                        auto_signin_for_van([orig_driver])
                    except Exception:
                        pass

                notify_vans("Bauercrest: Van IN", f"{van_label(which_van)} signed in by admin {admin_name}")
                st.session_state["admin_flash"] = f"{van_label(which_van)} signed in by {admin_name}."
                st.rerun()

    st.markdown("---")

    df_logs = load_logs_df_cached()

    section_title("Full Log History")
    if df_logs.empty:
        st.info("No logs recorded yet.")
    else:
        df_display = df_logs.copy()
        df_display["timestamp_str"] = df_display["timestamp"].apply(format_time)
        df_display = df_display.rename(columns={
            "id": "ID",
            "timestamp_str": "Time",
            "name": "Name",
            "reason": "Reason",
            "other_reason": "Other Details",
            "action": "Action",
            "status": "Status",
        })
        df_display = df_display[["ID", "Time", "Name", "Reason", "Other Details", "Action", "Status"]]
        st.dataframe(df_display, use_container_width=True)

        st.download_button(
            "Download Full Log as CSV",
            data=df_display.to_csv(index=False),
            file_name="signout_log.csv",
            mime="text/csv",
        )

    st.markdown("---")
    df_vans = load_vans_df_cached()
    section_title("Van Log History")
    if df_vans is None or df_vans.empty:
        st.info("No van logs recorded yet.")
    else:
        dfv = df_vans.copy()
        if "timestamp" in dfv.columns:
            dfv["timestamp"] = pd.to_datetime(dfv["timestamp"], errors="coerce")
            dfv["timestamp_str"] = dfv["timestamp"].apply(format_time)
        else:
            dfv["timestamp_str"] = ""

        dfv = dfv.rename(columns={
            "id": "ID",
            "timestamp_str": "Time",
            "van": "Van",
            "driver": "Driver",
            "purpose": "Purpose",
            "passengers": "Passengers",
            "other_purpose": "Other Purpose",
            "action": "Action",
            "status": "Status",
            "gas_left": "Gas Left",
        })
        cols = [c for c in ["ID", "Time", "Van", "Driver", "Purpose", "Passengers", "Other Purpose", "Action", "Status", "Gas Left"] if c in dfv.columns]
        st.dataframe(dfv[cols], use_container_width=True)

        st.download_button(
            "Download Van Log as CSV",
            data=dfv[cols].to_csv(index=False),
            file_name="van_log.csv",
            mime="text/csv",
        )

    st.markdown("---")
    section_title("Delete Specific Log Entries (for testing / pre-season only)")

    if df_logs.empty:
        st.info("No deletable entries.")
    else:
        id_to_label = {}
        for _, row in df_logs.iterrows():
            rid = str(row.get("id", "")).strip()
            if not rid:
                continue
            label = f"{rid} – {row.get('name','')} – {format_time(row.get('timestamp'))} – {row.get('action','')}"
            id_to_label[rid] = label

        selected_labels = st.multiselect(
            "Select entries to delete",
            list(id_to_label.values()),
            key="admin_delete_specific_multiselect",
        )

        selected_ids = [log_id for log_id, label in id_to_label.items() if label in selected_labels]

        if selected_ids and st.button("Delete Selected Entries", key="admin_delete_specific_button"):
            delete_logs_by_ids(selected_ids)
            st.success(f"Deleted {len(selected_ids)} log(s).")
            st.rerun()

    st.markdown("---")
    section_title("Delete ALL Logs (for testing / pre-season only)")
    st.error("WARNING: This will delete ALL sign-in/out records from Google Sheets.")

    confirm_all = st.checkbox("I understand this will permanently delete all logs.", key="admin_confirm_delete_all_logs")
    if confirm_all and st.button("Delete ALL Logs", key="admin_delete_all_logs_button"):
        clear_all_logs()
        st.success("All logs cleared.")
        st.rerun()

    crest_footer()

# =================================================
# MAIN
# =================================================
def get_screen_mode() -> str:
    """Read the special screen from the URL, set by an operator code."""
    try:
        v = st.query_params.get("screen", "")
    except Exception:
        v = ""
    if isinstance(v, (list, tuple)):
        v = v[0] if v else ""
    return str(v).strip().lower()


def render_special_screen(screen: str, staff_names: list):
    """Full-screen head count. Red for emergency, calm navy for head count.

    The count refreshes quietly in a fragment, no full-page reload. Stays up
    until the clear code is typed. An emergency cleared on another machine
    drops this screen back to normal within a tick.
    """
    is_emerg = (screen == "emergency")

    @st.fragment(run_every=SCREEN_REFRESH_SECONDS)
    def live_count():
        # If the campwide state changed elsewhere, re-evaluate the whole app.
        escalate_if_emergency_changed("emergency" if is_emerg else "normal")

        df_logs = load_logs_df_cached()
        df_out = get_currently_out(df_logs)
        out_names = sorted(df_out["name"].tolist())
        out_set = set(out_names)
        in_names = sorted([s for s in staff_names if s not in out_set])

        reason_by_name = {}
        for _, r in df_out.iterrows():
            re = str(r.get("reason", "")).strip()
            det = clean_other_reason(r.get("other_reason", ""))
            reason_by_name[str(r.get("name", "")).strip()] = f"{re}{(' - ' + det) if det else ''}"

        bg = "#7A1620" if is_emerg else "#13294B"
        accent = "#FFFFFF"
        eyebrow = "EMERGENCY HEAD COUNT" if is_emerg else "HEAD COUNT"
        title = "ACCOUNT FOR EVERYONE" if is_emerg else "Who Is In and Out"

        in_items = "".join(f"<li>{esc(s)}</li>" for s in in_names) or "<li class='none'>None</li>"
        out_items = "".join(
            f"<li>{esc(s)} <span class='hc-reason'>{esc(reason_by_name.get(s, ''))}</span></li>"
            for s in out_names
        ) or "<li class='none'>None</li>"

        st.markdown(
            f"""
            <style>
            .hc-wrap {{ background:{bg}; color:{accent}; border-radius:16px; padding:1.6rem 1.8rem; }}
            .hc-eyebrow {{ font-family:'Archivo',sans-serif; font-weight:800; letter-spacing:0.16em;
                text-transform:uppercase; font-size:0.8rem; opacity:0.85; }}
            .hc-title {{ font-family:'Archivo',sans-serif; font-weight:800; font-size:2.2rem; margin:0.1rem 0 1rem 0; }}
            .hc-counts {{ display:flex; gap:1rem; margin-bottom:1.2rem; }}
            .hc-count {{ background:rgba(255,255,255,0.12); border-radius:12px; padding:0.8rem 1.4rem; flex:1; }}
            .hc-count .num {{ font-family:'Archivo',sans-serif; font-weight:800; font-size:2.6rem; line-height:1; }}
            .hc-count .lbl {{ font-weight:700; letter-spacing:0.08em; text-transform:uppercase; font-size:0.8rem; opacity:0.85; }}
            .hc-cols {{ display:grid; grid-template-columns:1fr 1fr; gap:1.2rem; }}
            .hc-col h3 {{ font-family:'Archivo',sans-serif; font-weight:800; font-size:1.1rem;
                border-bottom:2px solid rgba(255,255,255,0.4); padding-bottom:0.3rem; color:{accent}; }}
            .hc-col ul {{ list-style:none; padding:0; margin:0.4rem 0 0 0; columns:2; }}
            .hc-col li {{ font-size:1.02rem; font-weight:600; padding:0.15rem 0; break-inside:avoid; }}
            .hc-col li.none {{ opacity:0.6; font-weight:500; }}
            .hc-reason {{ font-weight:500; opacity:0.8; font-size:0.85rem; }}
            </style>
            <div class="hc-wrap">
                <div class="hc-eyebrow">{esc(eyebrow)}</div>
                <div class="hc-title">{esc(title)}</div>
                <div class="hc-counts">
                    <div class="hc-count"><div class="num">{len(in_names)}</div><div class="lbl">In Camp</div></div>
                    <div class="hc-count"><div class="num">{len(out_names)}</div><div class="lbl">Out of Camp</div></div>
                    <div class="hc-count"><div class="num">{len(in_names) + len(out_names)}</div><div class="lbl">Total Active</div></div>
                </div>
                <div class="hc-cols">
                    <div class="hc-col"><h3>In Camp ({len(in_names)})</h3><ul>{in_items}</ul></div>
                    <div class="hc-col"><h3>Out of Camp ({len(out_names)})</h3><ul>{out_items}</ul></div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    live_count()

    st.markdown("")
    with st.form("screen_clear_form", clear_on_submit=True):
        clear_code = st.text_input("Exit code", type="password", max_chars=6)
        exit_clicked = st.form_submit_button("Exit Screen")
    if exit_clicked:
        if match_special_code(clear_code) == "clear":
            # Lift the campwide emergency and any local head-count screen.
            set_emergency_flag(False)
            if "screen" in st.query_params:
                del st.query_params["screen"]
            st.rerun()
        else:
            st.error("Wrong exit code.")


def main():
    st.set_page_config(
        page_title="Bauercrest Staff Sign-Out",
        page_icon="🏕️",
        layout="wide",
    )
    inject_css()

    # Emergency is campwide: read the shared flag so every screen flips. Head
    # count stays local to the machine that opened it, via the URL.
    if get_emergency_flag():
        _, staff_names, _ = get_staff_pins_and_lists()
        render_special_screen("emergency", staff_names)
        return

    screen = get_screen_mode()
    if screen == "headcount":
        _, staff_names, _ = get_staff_pins_and_lists()
        render_special_screen("headcount", staff_names)
        return

    logo_path = Path("logo-header-2.png")
    if logo_path.exists():
        st.sidebar.image(str(logo_path), use_container_width=True)

    st.sidebar.title("Staff Sign-Out")
    st.sidebar.caption("Sign in and out with your 4-digit code.")

    page = st.sidebar.radio(
        "Go to",
        ["Sign In / Out", "Who's Out", "Vans", "Admin / History"],
        key="main_page_radio",
    )

    st.sidebar.markdown("---")
    st.sidebar.caption("The Who's Out and Vans boards refresh on their own every minute.")

    staff_pins, staff_names, driver_names = get_staff_pins_and_lists()

    if page == "Sign In / Out":
        page_sign_in_out(staff_pins, staff_names)
    elif page == "Who's Out":
        page_whos_out()
    elif page == "Vans":
        page_vans(staff_pins, staff_names, driver_names)
    elif page == "Admin / History":
        page_admin_history(staff_pins)


if __name__ == "__main__":
    main()
