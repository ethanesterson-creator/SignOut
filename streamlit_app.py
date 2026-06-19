import html as html_lib
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
import streamlit as st

import gspread
from gspread.exceptions import APIError, GSpreadException
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

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

REASONS = ["Period Off", "Day Off", "Night Off", "Other (type reason)"]

VANS = ["Van 1", "Van 2", "Van 3"]
VAN_PURPOSES = ["Period Off", "Night Off", "Day Off", "Field Trip", "Tournament", "Other"]

# Legacy tag from the old auto day-off feature. Kept only so old rows
# display cleanly on the board. The app no longer writes these rows.
LEGACY_AUTO_TAG_PREFIX = "AUTO_DAY_OFF"

# Vans sheet required headers
VANS_HEADERS_REQUIRED = [
    "id", "timestamp", "van", "driver", "purpose", "passengers",
    "other_purpose", "action", "status"
]

# Logs sheet required headers
LOGS_HEADERS_REQUIRED = ["id", "timestamp", "name", "reason", "other_reason", "action", "status"]

# Pages that auto-refresh for the Big House kiosk. The sign-out form is
# excluded on purpose so a refresh never wipes a PIN mid-entry.
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


def kiosk_autorefresh(seconds: int):
    """Reload the whole page on a timer.

    The old version used a meta-refresh tag inside a Streamlit component.
    Components render in a sandboxed iframe, so a meta refresh there reloaded
    only the hidden iframe, not the app. Calling parent.location.reload()
    from the component reloads the real page, so the board actually updates.
    """
    if seconds and seconds > 0:
        st.components.v1.html(
            f"""
            <script>
            setTimeout(function() {{
                if (window.parent) {{ window.parent.location.reload(); }}
                else {{ window.location.reload(); }}
            }}, {int(seconds) * 1000});
            </script>
            """,
            height=0,
        )


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

# =================================================
# STAFF + DRIVERS (FROM SHEETS)
# =================================================
@st.cache_data(ttl=30)
def load_staff_df_cached():
    sheet = get_worksheet(SHEET_STAFF)
    df = pd.DataFrame(sheet.get_all_records())
    for c in ["name", "pin", "active"]:
        if c not in df.columns:
            df[c] = ""

    df["name"] = df["name"].astype(str).str.strip()
    df["pin"] = df["pin"].astype(str).str.strip().apply(normalize_pin)

    # active: treat blank as TRUE
    a = df["active"].astype(str).str.upper().str.strip()
    df["active"] = a.isin(["TRUE", "1", "YES", "Y", ""])

    df = df[df["name"] != ""].copy()
    return df


@st.cache_data(ttl=30)
def load_drivers_df_cached():
    sheet = get_worksheet(SHEET_DRIVERS)
    df = pd.DataFrame(sheet.get_all_records())
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
        df = pd.DataFrame(sheet.get_all_records())
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


def append_log_row(name: str, reason: str, other_reason: str, action: str, status: str):
    """Write a log row mapped to the sheet's actual header order.

    Mapping by header (instead of fixed position) keeps rows aligned even if
    someone reorders columns in Google Sheets.
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

# =================================================
# DAYS OFF (DISPLAY ONLY)
# =================================================
@st.cache_data(ttl=15)
def load_days_off_df_cached():
    """Reads days_off sheet if present. If missing, returns empty DF (feature disabled)."""
    try:
        sheet = get_worksheet(SHEET_DAYS_OFF)
        df = pd.DataFrame(sheet.get_all_records())
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
    """Ensure the vans sheet has the expected header row (adds missing columns to the end)."""
    try:
        headers = sheet.row_values(1)
        if not headers:
            sheet.insert_row(VANS_HEADERS_REQUIRED, 1)
            return
        missing = [h for h in VANS_HEADERS_REQUIRED if h not in headers]
        if missing:
            new_headers = headers + missing
            sheet.delete_rows(1)
            sheet.insert_row(new_headers, 1)
    except Exception as e:
        st.error(f"Could not ensure vans header: {e}")
        st.stop()


@st.cache_data(ttl=10)
def load_vans_df_cached():
    sheet = get_vans_sheet()
    ensure_vans_header(sheet)
    return pd.DataFrame(sheet.get_all_records())


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
                f"<div class='bc-van-title'>{esc(v)}</div>"
                f"<div class='bc-meta'>Driver: <strong>{esc(info.get('driver', ''))}</strong></div>"
                f"<div class='bc-meta'>Purpose: {esc(purpose)}</div>"
                f"{passengers_html}"
                f"</div>"
            )
        else:
            cards.append(
                f"<div class='bc-van-card'>"
                f"<div class='bc-van-status in'>IN</div>"
                f"<div class='bc-van-title'>{esc(v)}</div>"
                f"<div class='bc-meta'>Parked at camp</div>"
                f"</div>"
            )
    st.markdown(f"<div class='bc-grid'>{''.join(cards)}</div>", unsafe_allow_html=True)

# =================================================
# PAGES
# =================================================
def page_sign_in_out(staff_pins: dict, staff_names: list):
    page_title("Camp Bauercrest Staff", "Sign Out / Sign In")

    pin_lookup = build_pin_lookup(staff_pins)

    # Bumping this nonce changes the code field keys, so the boxes come back
    # empty after a sign out or sign in. The next counselor starts fresh.
    if "signio_nonce" not in st.session_state:
        st.session_state["signio_nonce"] = 0
    n = st.session_state["signio_nonce"]

    flash = st.session_state.pop("log_flash", "")
    if flash:
        st.success(flash)

    df_logs = load_logs_df_cached()
    df_out = get_currently_out(df_logs)

    section_title("Sign Out")
    st.caption("Type your code. The app knows who you are.")

    col1, col2 = st.columns(2)
    with col1:
        code = st.text_input("Your code", type="password", max_chars=4, key=f"signout_code_{n}")
    with col2:
        reason = st.selectbox("Reason for going out", REASONS, key="signout_reason")

    other_reason = ""
    if reason == "Other (type reason)":
        other_reason = st.text_input("Type your reason", key="signout_other_reason")

    if st.button("Sign Out", key="signout_button"):
        name, err = resolve_code(code, pin_lookup)
        if err:
            st.error(err)
        elif reason == "Other (type reason)" and not other_reason.strip():
            st.error("Please type a reason for 'Other'.")
        elif (not df_out.empty) and name in df_out["name"].values:
            st.error(f"{name} is already signed out.")
        else:
            append_log_row(name, reason, other_reason, action="OUT", status="OUT")
            st.session_state["log_flash"] = f"{name} signed OUT successfully."
            st.session_state["signio_nonce"] += 1
            st.rerun()

    st.markdown("---")

    section_title("Sign In")
    st.caption("Type your code to sign back in.")

    df_logs = load_logs_df_cached()
    df_out = get_currently_out(df_logs)

    if df_out.empty:
        empty_note("No one is currently signed out.")
        crest_footer()
        return

    code_in = st.text_input("Your code", type="password", max_chars=4, key=f"signin_code_{n}")

    if st.button("Sign In", key="signin_button"):
        name_in, err = resolve_code(code_in, pin_lookup)
        if err:
            st.error(err)
        elif name_in not in df_out["name"].values:
            st.error(f"{name_in} is not currently signed out.")
        else:
            row = df_out[df_out["name"] == name_in].iloc[0]
            append_log_row(name_in, row["reason"], row["other_reason"], action="IN", status="IN")
            st.session_state["log_flash"] = f"{name_in} signed IN successfully."
            st.session_state["signio_nonce"] += 1
            st.rerun()

    crest_footer()


def page_whos_out():
    page_title("The Big House Board", "Who's Out Right Now")

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

    crest_footer()


def page_vans(staff_pins: dict, staff_names: list, driver_names: list):
    page_title("Camp Vehicles", "Vans")

    if "van_form_nonce" not in st.session_state:
        st.session_state["van_form_nonce"] = 0
    van_nonce = st.session_state["van_form_nonce"]

    flash = st.session_state.pop("van_flash", "")
    if flash:
        st.success(flash)

    vans_df = load_vans_df_cached()
    status_map = compute_van_status(vans_df)
    out_vans = [v for v in VANS if status_map.get(v, {}).get("status") == "OUT"]
    available = next_available_van(status_map)

    section_title("Van Status")
    render_van_cards(status_map)

    st.divider()
    section_title("Sign Out a Van")

    if available is None:
        st.warning("No vans available. All vans are currently out.")
    else:
        st.info(f"Next available: **{available}**")

        # IMPORTANT: check eligibility OUTSIDE the form to avoid "missing submit button" warning
        if not driver_names:
            st.warning("No eligible drivers found. Set drivers.passed_test=TRUE for cleared drivers.")
        else:
            pin_lookup = build_pin_lookup(staff_pins)
            with st.form("van_signout_form", clear_on_submit=False):
                st.caption("The driver types their code. Only driving-tested staff are accepted.")
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

                passengers = st.multiselect(
                    "Passengers (select everyone riding with the driver)",
                    options=staff_names,
                    key=f"van_passengers_{van_nonce}",
                )

                submitted = st.form_submit_button("Sign Out Van", use_container_width=True)

            if submitted:
                driver, err = resolve_code(driver_code, pin_lookup)
                if err:
                    st.error(err)
                    return

                if driver not in driver_names:
                    st.error("This code is not cleared to drive a van.")
                    return

                if purpose == "Other" and not other_purpose.strip():
                    st.error("Please enter the other purpose.")
                    return

                passengers_selected = st.session_state.get(f"van_passengers_{van_nonce}", passengers) or []
                passengers_selected = [p for p in passengers_selected if p != driver]

                row = {
                    "id": str(uuid.uuid4())[:8],
                    "timestamp": datetime.now(TZ).isoformat(timespec="seconds"),
                    "van": available,
                    "driver": driver,
                    "purpose": purpose,
                    "passengers": ", ".join(passengers_selected),
                    "other_purpose": other_purpose.strip(),
                    "action": "CHECKOUT",
                    "status": "OUT",
                }
                append_vans_row(row)

                st.session_state["van_form_nonce"] += 1
                st.session_state["van_flash"] = f"{available} signed out under {driver}."
                st.rerun()

    # Sign IN section
    if out_vans:
        st.divider()
        section_title("Sign In a Van")

        van_to_in = out_vans[0] if len(out_vans) == 1 else st.selectbox("Which van is returning?", out_vans)

        pin_lookup_in = build_pin_lookup(staff_pins)
        with st.form("van_signin_form", clear_on_submit=True):
            st.caption("The driver returning the van types their code.")
            return_driver_code = st.text_input("Driver code", type="password", max_chars=4)
            submitted_in = st.form_submit_button("Sign In Van", use_container_width=True)

        if submitted_in:
            return_driver, err = resolve_code(return_driver_code, pin_lookup_in)
            if err:
                st.error(err)
                return
            if return_driver not in driver_names:
                st.error("This code is not cleared to drive a van.")
                return

            last_passengers = ""
            last_purpose = ""
            last_other_purpose = ""
            try:
                tmp = vans_df.copy()
                tmp["timestamp"] = pd.to_datetime(tmp["timestamp"], errors="coerce")
                tmp = tmp.sort_values("timestamp", na_position="last")
                van_rows = tmp[tmp["van"] == van_to_in]
                if not van_rows.empty:
                    out_rows = van_rows[van_rows["status"].astype(str).str.upper() == "OUT"]
                    src = out_rows.iloc[-1] if not out_rows.empty else van_rows.iloc[-1]
                    last_passengers = str(src.get("passengers", "")).strip()
                    last_purpose = str(src.get("purpose", "")).strip()
                    last_other_purpose = str(src.get("other_purpose", "")).strip()
            except Exception:
                pass

            row = {
                "id": str(uuid.uuid4())[:8],
                "timestamp": datetime.now(TZ).isoformat(timespec="seconds"),
                "van": van_to_in,
                "driver": return_driver,
                "purpose": last_purpose,
                "passengers": last_passengers,
                "other_purpose": last_other_purpose,
                "action": "CHECKIN",
                "status": "IN",
            }
            append_vans_row(row)
            st.session_state["van_flash"] = f"{van_to_in} signed back in under {return_driver}."
            st.rerun()

    crest_footer()


def page_admin_history():
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
        })
        cols = [c for c in ["ID", "Time", "Van", "Driver", "Purpose", "Passengers", "Other Purpose", "Action", "Status"] if c in dfv.columns]
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
def main():
    st.set_page_config(
        page_title="Bauercrest Staff Sign-Out",
        page_icon="🏕️",
        layout="wide",
    )
    inject_css()

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
    with st.sidebar.expander("Kiosk Settings", expanded=False):
        auto_refresh_on = st.checkbox("Auto-refresh kiosk", value=True)
        refresh_seconds = st.slider("Refresh every (seconds)", 10, 120, 30, step=5)

    # Refresh only the display pages. The Sign In / Out and Admin pages
    # never auto-refresh, so typing is never interrupted.
    if auto_refresh_on and page in KIOSK_PAGES:
        kiosk_autorefresh(refresh_seconds)

    staff_pins, staff_names, driver_names = get_staff_pins_and_lists()

    if page == "Sign In / Out":
        page_sign_in_out(staff_pins, staff_names)
    elif page == "Who's Out":
        page_whos_out()
    elif page == "Vans":
        page_vans(staff_pins, staff_names, driver_names)
    elif page == "Admin / History":
        page_admin_history()


if __name__ == "__main__":
    main()
