import html as html_lib
import threading
import uuid
from datetime import datetime, timedelta, time as dtime
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
SHEET_LOGS_ARCHIVE = "logs_archive"  # auto-created; permanent history moved out of the live tab
SHEET_VANS_ARCHIVE = "vans_archive"  # auto-created; same idea, for the vans tab

# When archiving, always keep rows from at least this many days back in the
# live tab, on top of keeping every row for anyone currently out. A day-off
# cycle can span more than a day, so this buffer keeps recent completed
# activity and any in-flight deadline visible in the app.
ARCHIVE_KEEP_DAYS = 3
SHEET_SCHEDULE = "schedule"  # auto-created; period start/end times for Period Off deadlines

# Auto-created; one row per person holding their LATEST status only. The logs
# tab stays the permanent history forever; this tab exists purely so a status
# check (every sign-in/out toggle, the board, the van flows) reads a handful
# of rows instead of the entire history. It is a cache, not a second source of
# truth: it is written every time logs is written, and can be fully rebuilt
# from logs at any time (see rebuild_current_status_from_logs).
SHEET_CURRENT_STATUS = "current_status"
CURRENT_STATUS_HEADERS = ["name", "status", "reason", "other_reason", "timestamp", "due_back", "id"]

# How often the live board silently re-archives old logs on its own, so the
# live tab never grows unbounded just because nobody remembered to click
# "Archive Old Logs Now". Deduped on the calendar date, same pattern as the
# nightly summary, so it runs once a day no matter how many times the board
# ticks after the hour.
AUTO_ARCHIVE_HOUR = 3
AUTO_ARCHIVE_KEY = "auto_archive_date"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Hard timeout on every Google Sheets call, so a slow Google response can never
# freeze the kiosk on a blank screen. A read taking longer than this is broken
# anyway, so failing fast and recovering on the next tick beats a long hang.
SHEETS_TIMEOUT_SECONDS = 10

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
LOGS_HEADERS_REQUIRED = ["id", "timestamp", "name", "reason", "other_reason", "action", "status", "due_back", "late"]

# -------------------------------------------------
# LATENESS RULES
# -------------------------------------------------
# Period Off: back 5 minutes before the period ends.
PERIOD_OFF_GRACE_MIN = 5
# Night Off: back by 12:15 AM the next morning.
NIGHT_OFF_DUE = (0, 15)
# Day Off: back by 7:45 AM the morning AFTER the day off.
DAY_OFF_DUE = (7, 45)
# A day off is a full calendar day. Sign out before noon and the day off is
# that same day (you left in the morning to start it). Sign out at noon or
# later and the day off is the NEXT day (you left during or after today for
# tomorrow's day off). Either way you are due back the morning after the day
# off. Example: out Tuesday 9:50 PM -> day off Wednesday -> back Thursday 7:45.
DAY_OFF_NEXTDAY_CUTOFF_HOUR = 12

# Default camp schedule, used if the schedule tab is missing or empty. The tab
# wins when present, so the office can change times without touching the code.
DEFAULT_SCHEDULE = [
    ("Period 1", "09:15", "10:05"),
    ("Period 2", "10:15", "11:05"),
    ("Period 3", "11:15", "12:05"),
    ("Rest Period", "13:00", "14:00"),
    ("Period 4", "14:15", "15:05"),
    ("Snack", "15:15", "15:30"),
    ("Period 5", "15:40", "16:30"),
    ("G-Swim", "16:30", "17:30"),
    ("Free Play", "18:35", "19:15"),
]
SCHEDULE_HEADERS = ["period", "start", "end"]

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

/* Streamlit's default block container reserves ~96px on top and ~160px on
   the bottom - on a kiosk screen that is the difference between everything
   fitting and needing to scroll for no real reason. */
.block-container {{
    padding-top: 2rem;
    padding-bottom: 2rem;
}}

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
/* Nav rows get real padding and a highlighted background on the active page,
   instead of a bare radio dot next to plain text. */
section[data-testid="stSidebar"] .stRadio label[data-testid="stRadioOption"] {{
    padding: 0.5rem 0.7rem;
    border-radius: 8px;
    margin-bottom: 0.1rem;
    transition: background 0.12s ease;
}}
section[data-testid="stSidebar"] .stRadio label[data-testid="stRadioOption"]:has(input:checked) {{
    background: var(--navy-soft);
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
/* Sized for a touchscreen kiosk: 44px is the standard minimum comfortable tap
   target, and this is tapped by hand as often as it's clicked with a mouse. */
.stButton > button, .stFormSubmitButton > button {{
    background: var(--navy);
    color: {WHITE};
    border: none;
    border-radius: 8px;
    font-family: 'Archivo', sans-serif;
    font-weight: 700;
    font-size: 1.02rem;
    letter-spacing: 0.02em;
    padding: 0.7rem 1.5rem;
    min-height: 44px;
    transition: background 0.12s ease, transform 0.08s ease;
}}
.stButton > button:hover, .stFormSubmitButton > button:hover {{
    background: var(--navy-soft);
    color: {WHITE};
}}
/* Touchscreens have no hover state, so a tap needs its own visible feedback -
   otherwise a counselor can't tell whether their tap actually registered. */
.stButton > button:active, .stFormSubmitButton > button:active {{
    background: var(--navy-deep);
    transform: scale(0.97);
}}
.stDownloadButton > button {{
    background: {WHITE};
    color: var(--navy);
    border: 1.5px solid var(--navy);
    border-radius: 8px;
    font-weight: 700;
    min-height: 44px;
}}

/* ---------- inputs ---------- */
/* The code box is the single most-tapped element on the kiosk, so it gets the
   same touch-friendly sizing as buttons. */
.stTextInput input {{
    min-height: 44px;
    font-size: 1.05rem;
}}
.stSelectbox [data-baseweb="select"] > div,
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

/* Live-board freshness indicator: a small pulsing dot plus a timestamp, so a
   counselor glancing at the kiosk can trust the board without wondering when
   it last checked in. */
@keyframes bcLiveDotPulse {{
    0%, 100% {{ opacity: 1; }}
    50% {{ opacity: 0.35; }}
}}
.bc-liveclock {{
    display: flex;
    align-items: center;
    gap: 0.4rem;
    margin: -0.6rem 0 1rem 0;
    font-family: 'Public Sans', sans-serif;
    font-size: 0.85rem;
    font-weight: 600;
    color: var(--mist);
}}
.bc-liveclock-dot {{
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #2E7D32;
    animation: bcLiveDotPulse 2s ease-in-out infinite;
}}

/* Big, unmistakable section banners. Counselors read these from a step away,
   so nobody signs a van out when they meant to sign one in. The headline word
   is always short and fixed ("SIGNING OUT"); any variable-length detail (a
   reason, a typed note) lives in its own smaller pill line below, so it can
   never break the headline into an awkward multi-line wrap. */
.bc-banner {{
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 0.4rem;
    border-radius: 12px;
    padding: 1.1rem 1.3rem;
    margin: 0.2rem 0 0.9rem 0;
}}
.bc-banner .bc-banner-word {{
    font-family: 'Archivo', sans-serif;
    font-size: 1.55rem;
    font-weight: 800;
    letter-spacing: 0.02em;
    line-height: 1.15;
}}
.bc-banner .bc-banner-reason {{
    font-family: 'Public Sans', sans-serif;
    font-size: 0.9rem;
    font-weight: 700;
    letter-spacing: 0.01em;
    padding: 0.22rem 0.7rem;
    border-radius: 999px;
    max-width: 100%;
}}
.bc-banner .bc-banner-sub {{
    font-family: 'Public Sans', sans-serif;
    font-size: 0.85rem;
    font-weight: 500;
    opacity: 0.8;
}}
.bc-banner-out {{
    background: var(--navy);
    color: {WHITE};
}}
.bc-banner-out .bc-banner-word,
.bc-banner-out .bc-banner-sub {{ color: {WHITE}; }}
.bc-banner-out .bc-banner-reason {{
    background: {WHITE};
    color: var(--navy);
}}
.bc-banner-in {{
    background: {WHITE};
    border: 1px solid var(--line);
    border-left: 3px solid #2E7D32;
    color: #1B5E20;
}}
.bc-banner-in .bc-banner-word {{ color: var(--navy-deep); }}
.bc-banner-in .bc-banner-sub {{ color: var(--mist); opacity: 1; }}
.bc-banner-in .bc-banner-reason {{
    background: #E7F4EA;
    color: #1B5E20;
}}

/* Van picker tiles. These are the whole interface: pick the van, the app
   decides whether you are taking it or returning it. */
.bc-vangrid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
    gap: 0.7rem;
    margin-bottom: 0.5rem;
}}
.bc-vantile {{
    border-radius: 14px;
    padding: 1rem 1.1rem;
    border: 2px solid var(--line);
    background: {WHITE};
}}
.bc-vantile-state {{
    font-family: 'Archivo', sans-serif;
    font-size: 0.7rem;
    font-weight: 800;
    letter-spacing: 0.12em;
    opacity: 0.85;
}}
.bc-vantile-name {{
    font-family: 'Archivo', sans-serif;
    font-size: 1.35rem;
    font-weight: 800;
    margin: 0.15rem 0 0.1rem 0;
}}
.bc-vantile-who {{ font-size: 0.92rem; font-weight: 600; opacity: 0.9; }}
.bc-vantile-action {{ font-size: 0.85rem; margin-top: 0.35rem; opacity: 0.8; }}
.bc-gas {{
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-top: 0.6rem;
    padding-top: 0.5rem;
    border-top: 1px solid rgba(120,130,150,0.3);
}}
.bc-gas-word {{
    font-family: 'Archivo', sans-serif;
    font-weight: 800;
    font-size: 0.95rem;
    letter-spacing: 0.01em;
}}
.bc-gas-unknown {{ color: #8A93A3; font-weight: 600; }}
.bc-vantile-out .bc-gas {{ border-top-color: rgba(255,255,255,0.25); }}
.bc-vantile-in {{ border-color: #2E7D32; background: #E7F4EA; color: #1B5E20; }}
.bc-vantile-out {{ background: var(--navy); border-color: var(--navy-deep); color: {WHITE}; }}
.bc-vantile-sel {{ box-shadow: 0 0 0 4px rgba(19,41,75,0.25); }}

/* Big confirmation banner. Deliberately loud: a counselor who typed their
   code must SEE which direction they just went. Just a quick pop-in here -
   actual removal after FLASH_DISPLAY_SECONDS is handled server-side by a
   fragment (see flash_ticker in page_sign_in_out), which really removes the
   element instead of leaving an invisible box holding its space open. */
@keyframes bcBigFlash {{
    0%   {{ opacity: 0; transform: translateY(-6px); }}
    100% {{ opacity: 1; transform: translateY(0); }}
}}
.bc-bigflash {{
    border-radius: 14px;
    padding: 1.1rem 1.3rem;
    margin-bottom: 0.9rem;
    animation: bcBigFlash 0.25s ease forwards;
}}
.bc-bigflash-word {{
    font-family: 'Archivo', sans-serif;
    font-size: 2rem;
    font-weight: 800;
    line-height: 1.1;
}}
.bc-bigflash-sub {{
    font-family: 'Public Sans', sans-serif;
    font-size: 1.02rem;
    font-weight: 600;
    margin-top: 0.25rem;
}}
.bc-bigflash-in {{ background: #E7F4EA; border: 3px solid #2E7D32; color: #14521A; }}
.bc-bigflash-out {{ background: var(--navy); border: 3px solid var(--navy-deep); color: {WHITE}; }}
.bc-bigflash-ask {{
    margin-top: 0.6rem;
    padding-top: 0.5rem;
    border-top: 2px solid rgba(0,0,0,0.15);
    font-family: 'Archivo', sans-serif;
    font-size: 1.05rem;
    font-weight: 800;
}}

/* Stale sign-in fork: shown only when someone out for hours enters a code. */
.bc-fork {{
    background: #FBF3E4;
    border: 3px solid #B07A1E;
    border-radius: 14px;
    padding: 1rem 1.2rem;
    margin: 0.4rem 0 0.7rem 0;
}}
.bc-fork-head {{
    font-family: 'Archivo', sans-serif;
    font-size: 1.35rem;
    font-weight: 800;
    color: #6B4A0F;
}}
.bc-fork-sub {{
    font-family: 'Public Sans', sans-serif;
    font-size: 1rem;
    font-weight: 600;
    color: #7C5A1C;
    margin-top: 0.25rem;
}}

/* Who's-out strip, right under the sign box. */
.bc-strip-title {{
    font-family: 'Archivo', sans-serif;
    font-weight: 800;
    font-size: 0.95rem;
    color: var(--navy);
    margin: 1.1rem 0 0.4rem 0;
    letter-spacing: 0.02em;
}}
.bc-strip-empty {{
    font-family: 'Public Sans', sans-serif;
    color: #5A6472;
    font-size: 0.95rem;
}}
.bc-strip {{
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
    margin-bottom: 0.3rem;
}}
.bc-chip-strip {{
    display: inline-block;
    background: #EAF0F7;
    border: 1px solid #C6D2E1;
    color: #1B2A45;
    border-radius: 999px;
    padding: 0.28rem 0.7rem;
    font-family: 'Public Sans', sans-serif;
    font-size: 0.88rem;
    font-weight: 600;
}}
.bc-chip-strip-forgot {{
    background: #FBF3E4;
    border-color: #B07A1E;
    color: #6B4A0F;
}}
.bc-strip-forgot-label {{
    font-family: 'Archivo', sans-serif;
    font-weight: 800;
    font-size: 0.8rem;
    color: #7C5A1C;
    margin: 0.35rem 0 0.3rem 0;
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

/* Late: past due-back and still not signed in. */
.bc-card.bc-card-late {{
    background: #FDECEC;
    border: 1px solid #B3261E;
    border-top: 4px solid #B3261E;
}}
.bc-card.bc-card-late .bc-name {{ color: #7A1620; }}
.bc-card.bc-card-late .bc-meta,
.bc-card.bc-card-late .bc-time {{ color: #8C3A33; }}
.bc-chip.bc-chip-late {{
    background: #B3261E;
    color: {WHITE};
    margin-left: 0.3rem;
}}

/* Probably-forgot zone: muted amber, not alarming red. These need cleanup,
   not urgency, and they must not compete with someone genuinely late now. */
.bc-card.bc-card-forgot {{
    background: #FBF3E4;
    border: 1px solid #B07A1E;
    border-top: 4px solid #B07A1E;
    opacity: 0.95;
}}
.bc-card.bc-card-forgot .bc-name {{ color: #6B4A0F; }}
.bc-card.bc-card-forgot .bc-meta,
.bc-card.bc-card-forgot .bc-time {{ color: #7C5A1C; }}
.bc-chip.bc-chip-forgot {{
    background: #B07A1E;
    color: {WHITE};
    margin-left: 0.3rem;
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

/* Campwide emergency banner. Shown on every page, top of screen, until an
   admin clears it. Deliberately the loudest thing in the whole app: this is
   the one case where being impossible to miss matters more than being calm. */
@keyframes bcEmergencyPulse {{
    0%, 100% {{ box-shadow: 0 0 0 0 rgba(179,38,30,0.55); }}
    50% {{ box-shadow: 0 0 0 8px rgba(179,38,30,0); }}
}}
.bc-emergency-banner {{
    background: #B3261E;
    color: {WHITE};
    border-radius: 12px;
    padding: 1rem 1.3rem;
    margin: 0 0 1.2rem 0;
    animation: bcEmergencyPulse 2s infinite;
}}
.bc-emergency-word {{
    font-family: 'Archivo', sans-serif;
    font-size: 1.3rem;
    font-weight: 800;
    letter-spacing: 0.04em;
    color: {WHITE};
}}
.bc-emergency-msg {{
    font-family: 'Public Sans', sans-serif;
    font-size: 1rem;
    font-weight: 600;
    color: {WHITE};
    margin-top: 0.25rem;
    opacity: 0.96;
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


def live_clock_note():
    """Small pulsing 'Live · updated at H:MM:SS' line for a self-refreshing
    board, so a counselor can trust what they're looking at is current
    without wondering when it last checked in."""
    now_str = datetime.now(TZ).strftime("%I:%M:%S %p").lstrip("0")
    st.markdown(
        "<div class='bc-liveclock'>"
        "<div class='bc-liveclock-dot'></div>"
        f"<div>Live &middot; updated {esc(now_str)}</div>"
        "</div>",
        unsafe_allow_html=True,
    )


def big_banner(word: str, sub: str, kind: str = "out"):
    """Large color-coded section banner.

    Navy for leaving, green for returning. The point is that a counselor
    glancing at the screen knows which half of the page they are in without
    reading small text.
    """
    cls = "bc-banner-in" if kind == "in" else "bc-banner-out"
    st.markdown(
        f"<div class='bc-banner {cls}'>"
        f"<div class='bc-banner-word'>{esc(word)}</div>"
        f"<div class='bc-banner-sub'>{esc(sub)}</div>"
        f"</div>",
        unsafe_allow_html=True,
    )


def empty_note(text: str):
    st.markdown(f"<div class='bc-empty'>{esc(text)}</div>", unsafe_allow_html=True)


def flash_banner(msg: str):
    """Inline green confirmation that fades out on its own after ~2 seconds."""
    st.markdown(f"<div class='bc-flash'>{esc(msg)}</div>", unsafe_allow_html=True)


def big_flash(msg: str, kind: str = "in", word: str = "", ask: str = ""):
    """Loud confirmation banner. Does NOT fade.

    A counselor who typed their code has to see which direction they went. The
    dangerous case is someone who forgot to sign in days ago, walks up meaning
    to sign OUT, and the toggle signs them IN instead. A small green line is
    easy to miss. This is not.

    ask is an extra line for exactly that case, telling them to type again if
    they actually meant to leave.
    """
    cls = "bc-bigflash-in" if kind == "in" else "bc-bigflash-out"
    headline = word or ("SIGNED IN" if kind == "in" else "SIGNED OUT")
    ask_html = f"<div class='bc-bigflash-ask'>{esc(ask)}</div>" if ask else ""
    st.markdown(
        f"<div class='bc-bigflash {cls}'>"
        f"<div class='bc-bigflash-word'>{esc(headline)}</div>"
        f"<div class='bc-bigflash-sub'>{esc(msg)}</div>"
        f"{ask_html}"
        f"</div>",
        unsafe_allow_html=True,
    )


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

# If the kiosk is left on a display page (Who's Out or Vans) and untouched for
# this long, it returns itself to the Sign In / Out screen. The whole system
# depends on one central kiosk that is always ready to sign people out, so it
# should never sit parked on a board where the next counselor does not realize
# they have to navigate back. Admin is left alone, since that is a deliberate
# staff session, not a place someone gets stranded.
IDLE_RETURN_SECONDS = 120


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
    client = gspread.authorize(creds)
    # Give every Google Sheets call a hard timeout. Without this, a slow Google
    # response hangs the whole kiosk on a blank "running..." screen with no way
    # out but a reboot. With it, a slow call fails fast and the app recovers on
    # the next tick instead of freezing.
    try:
        client.set_timeout(SHEETS_TIMEOUT_SECONDS)
    except Exception:
        try:
            client.http_client.timeout = SHEETS_TIMEOUT_SECONDS
        except Exception:
            pass
    return client


@st.cache_resource
def get_spreadsheet():
    client = get_gspread_client()
    # secrets can override which sheet to open, so a local/test run can point
    # at a copy of the spreadsheet without ever touching the production ID
    # baked into the code above.
    sheet_id = st.secrets.get("spreadsheet_id", SPREADSHEET_ID)
    return client.open_by_key(sheet_id)


@st.cache_resource
def get_worksheet(name: str):
    """Return a cached handle to a worksheet.

    gspread's worksheet(name) hits the network to look up the tab every call.
    Caching the handle removes that round-trip from every read and write. The
    handle stays valid for the whole session; actual reads/writes still hit
    the live sheet.
    """
    ss = get_spreadsheet()
    return ss.worksheet(name)


def get_settings_sheet():
    """Get the settings tab, creating it if missing. Holds key/value rows."""
    try:
        return get_worksheet(SHEET_SETTINGS)
    except WorksheetNotFound:
        ss = get_spreadsheet()
        sheet = ss.add_worksheet(title=SHEET_SETTINGS, rows=20, cols=2)
        sheet.update("A1:B2", [["key", "value"], ["emergency", "FALSE"]])
        # Drop the cached lookup so the next call finds the new tab.
        try:
            get_worksheet.clear()
        except Exception:
            pass
        return sheet


def set_setting(key: str, value: str):
    """Write any key/value into the settings tab."""
    try:
        sheet = get_settings_sheet()
        # Match the KEY column only. A whole-sheet find could land on the same
        # text sitting in a value cell and overwrite the wrong row.
        try:
            cell = sheet.find(key, in_column=1)
        except TypeError:
            cell = sheet.find(key)
            if cell and cell.col != 1:
                cell = None
        if cell and cell.col == 1:
            sheet.update_cell(cell.row, 2, value)
        else:
            sheet.append_row([key, value])
        get_setting.clear()
    except Exception:
        pass


@st.cache_data(ttl=30)
def get_setting(key: str) -> str:
    """Read any key from the settings tab. Blank if missing."""
    try:
        df = read_sheet_df(get_settings_sheet())
        if df.empty or "key" not in df.columns or "value" not in df.columns:
            return ""
        row = df[df["key"].astype(str).str.strip().str.lower() == key.strip().lower()]
        if row.empty:
            return ""
        return str(row.iloc[0]["value"]).strip()
    except Exception:
        return ""


EMERGENCY_KEY = "emergency"
EMERGENCY_MESSAGE_KEY = "emergency_message"


def is_emergency_active() -> bool:
    """True if an admin has declared a campwide emergency.

    The settings tab has carried this flag since the app's first version,
    but nothing ever actually read it. Board pages pick this up within the
    normal settings cache window (30s) plus their own refresh tick, so the
    push notification sent when an emergency is declared is the fast channel
    - this on-screen banner is the persistent one that follows shortly after.
    """
    return get_setting(EMERGENCY_KEY).strip().upper() == "TRUE"


def get_emergency_message() -> str:
    return get_setting(EMERGENCY_MESSAGE_KEY).strip()


def set_emergency(active: bool, message: str = ""):
    set_setting(EMERGENCY_KEY, "TRUE" if active else "FALSE")
    set_setting(EMERGENCY_MESSAGE_KEY, message if active else "")


def render_emergency_banner():
    """Loud, non-dismissible banner shown at the top of every page while an
    emergency is active. Cheap to call every render: get_setting is cached."""
    if not is_emergency_active():
        return
    msg = get_emergency_message()
    msg_html = f"<div class='bc-emergency-msg'>{esc(msg)}</div>" if msg else ""
    st.markdown(
        "<div class='bc-emergency-banner'>"
        "<div class='bc-emergency-word'>&#9888; CAMPWIDE EMERGENCY IN EFFECT</div>"
        f"{msg_html}"
        "</div>",
        unsafe_allow_html=True,
    )


# =================================================
# SCHEDULE + LATENESS
# =================================================
def get_schedule_sheet():
    """Get the schedule tab, creating it with the camp default if missing."""
    ss = get_spreadsheet()
    try:
        return get_worksheet(SHEET_SCHEDULE)
    except WorksheetNotFound:
        sheet = ss.add_worksheet(title=SHEET_SCHEDULE, rows=30, cols=3)
        rows = [SCHEDULE_HEADERS] + [list(p) for p in DEFAULT_SCHEDULE]
        sheet.update(f"A1:C{len(rows)}", rows)
        try:
            get_worksheet.clear()
        except Exception:
            pass
        return sheet


def _parse_hhmm(s: str):
    """Parse '9:15', '09:15', or '14:15' into a time. None if unusable."""
    s = str(s or "").strip()
    if not s:
        return None
    for fmt in ("%H:%M", "%I:%M %p", "%I:%M%p", "%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    return None


@st.cache_data(ttl=120)
def load_schedule():
    """Return the day's periods as [(name, start_time, end_time)], sorted.

    Read from the schedule tab so the office can edit times without a code
    change. Falls back to the built-in camp default if the tab is missing,
    empty, or unreadable, so lateness never silently stops working.
    """
    try:
        df = read_sheet_df(get_schedule_sheet())
        periods = []
        if not df.empty and "start" in df.columns and "end" in df.columns:
            for _, r in df.iterrows():
                start = _parse_hhmm(r.get("start"))
                end = _parse_hhmm(r.get("end"))
                nm = str(r.get("period", "")).strip()
                if start and end:
                    periods.append((nm, start, end))
        if periods:
            return sorted(periods, key=lambda p: p[1])
    except Exception:
        pass
    return sorted(
        [(n, _parse_hhmm(s), _parse_hhmm(e)) for n, s, e in DEFAULT_SCHEDULE],
        key=lambda p: p[1],
    )


def _next_clock_time(now: datetime, hh: int, mm: int) -> datetime:
    """The next moment the clock reads hh:mm, strictly after now."""
    candidate = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if candidate <= now:
        candidate = candidate + timedelta(days=1)
    return candidate


def compute_due_back(reason: str, now: datetime):
    """When this person must be signed back in. None means no deadline.

    Period Off  -> 5 minutes before the period they are in ends. If they sign
                   out between periods, the next period's end is used.
    Night Off   -> 12:15 AM the next morning.
    Day Off     -> 7:45 AM the morning after.
    Anything else (Other, Van) -> no deadline.
    """
    reason = str(reason or "").strip()

    if reason == "Night Off":
        return _next_clock_time(now, *NIGHT_OFF_DUE)

    if reason == "Day Off":
        # Which calendar day is the day off?
        day_off_date = now.date()
        if now.hour >= DAY_OFF_NEXTDAY_CUTOFF_HOUR:
            day_off_date = day_off_date + timedelta(days=1)
        # Back the morning AFTER the day off.
        due_date = day_off_date + timedelta(days=1)
        hh, mm = DAY_OFF_DUE
        naive = datetime.combine(due_date, dtime(hh, mm))
        return TZ.localize(naive) if now.tzinfo else naive

    if reason == "Period Off":
        today = now.date()
        for _name, start, end in load_schedule():
            if not start or not end:
                continue
            end_dt = TZ.localize(datetime.combine(today, end)) if now.tzinfo else datetime.combine(today, end)
            due = end_dt - timedelta(minutes=PERIOD_OFF_GRACE_MIN)
            # The first period whose deadline is still ahead of them is theirs.
            # That covers being mid-period and being in a gap between periods.
            if due > now:
                return due
        return None  # signed out after the last period; no deadline to enforce

    return None


def parse_due(s):
    """Parse a stored due_back string back into an aware datetime, or None."""
    s = str(s or "").strip()
    if not s:
        return None
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        dt = dt.to_pydatetime()
        if dt.tzinfo is None:
            dt = TZ.localize(dt)
        return dt
    except Exception:
        return None


def effective_due_back(reason, signed_out_at):
    """The deadline computed LIVE from the current reason and sign-out time.

    The deadline used to be frozen into the due_back column at sign-out. So if
    someone picked the wrong reason, fixing the reason in the sheet changed
    nothing, because lateness read the stored deadline. Recomputing from the
    reason (the label everyone sees) and the original sign-out timestamp makes
    the reason the single source of truth: edit it in the sheet and lateness
    corrects itself. The sign-out time is fixed, so the deadline never drifts.
    """
    ts = signed_out_at if isinstance(signed_out_at, datetime) else parse_due(signed_out_at)
    if ts is None:
        return None
    return compute_due_back(str(reason or "").strip(), ts)


def row_minutes_late(row, when=None) -> int:
    """Minutes late for a log row, recomputing the deadline from its reason."""
    due = effective_due_back(row.get("reason", ""), row.get("timestamp", ""))
    return minutes_late(due, when)


def minutes_late(due, when=None) -> int:
    """How many whole minutes past the deadline. 0 if not late or no deadline."""
    if not due:
        return 0
    when = when or datetime.now(TZ)
    # Never let a naive/aware mix throw. A raw TypeError here would be swallowed
    # by the board's guard and quietly show a late person as on time. Localize
    # anything naive to camp time so the comparison is always valid.
    try:
        if due.tzinfo is None:
            due = TZ.localize(due)
        if when.tzinfo is None:
            when = TZ.localize(when)
    except Exception:
        return 0
    delta = (when - due).total_seconds()
    return int(delta // 60) if delta > 0 else 0


LATE_ALERT_KEY = "late_alerted"

# Past this many minutes overdue, a person almost certainly forgot to sign in
# rather than being genuinely late. Their card drops to a separate zone at the
# bottom of the board and stops showing a meaningless minute count. Nothing is
# deleted. Change this one number to move the line.
FORGOT_THRESHOLD_MINUTES = 180

# For reasons that carry no deadline (Other, Van), lateness is always 0, so the
# forgot logic above can never flag them. If someone is out on one of those for
# this many hours, it is almost certainly a forgotten sign-in, so the sign
# fork still steps in. Long enough that an all-day errand does not trip it.
FORK_NO_DEADLINE_HOURS = 14


def is_surprising_signin(info: dict, when=None) -> bool:
    """True if signing this person IN would be surprising enough to ask first.

    Surprising means their sign-out is stale enough that they probably forgot
    to sign in and may be standing at the kiosk now to LEAVE again. Two ways to
    qualify: far past their own deadline, or out for many hours on a reason that
    has no deadline at all. Everyone whose state is fresh returns False and
    flows through the fast toggle untouched.
    """
    if not info or info.get("status") != "OUT":
        return False
    when = when or datetime.now(TZ)
    reason = str(info.get("reason", "")).strip()
    ts = info.get("timestamp", "")

    due = effective_due_back(reason, ts)
    if due is not None:
        return minutes_late(due, when) >= FORGOT_THRESHOLD_MINUTES

    # No deadline for this reason: judge by raw time since sign-out.
    out_dt = parse_due(ts)
    if out_dt is None:
        return False
    try:
        hours = (when - out_dt).total_seconds() / 3600.0
    except Exception:
        return False
    return hours >= FORK_NO_DEADLINE_HOURS


def check_late_and_alert(df_out: pd.DataFrame):
    """Push once for each person who has just gone past their deadline.

    Dedupe is stored in the settings tab, keyed on the sign-out row's id, so
    two kiosks running the board never double-buzz you and nobody gets alerted
    twice for the same sign-out. Wrapped so a failure never breaks the board.
    """
    try:
        if df_out is None or df_out.empty:
            return

        # Read the dedupe list FRESH. A stale read here means two screens can
        # both decide nobody has been alerted yet and both fire.
        try:
            get_setting.clear()
        except Exception:
            pass

        raw = get_setting(LATE_ALERT_KEY)
        already_list = [x for x in raw.split(",") if x]
        already = set(already_list)

        new_alerts = []
        for _, row in df_out.iterrows():
            rid = str(row.get("id", "")).strip()
            mins = row_minutes_late(row)
            if mins > 0 and rid and rid not in already:
                notify_phone(
                    "Bauercrest: LATE",
                    f"{row.get('name','')} is {mins} min late ({row.get('reason','')})",
                )
                new_alerts.append(rid)

        if new_alerts:
            # Keep insertion order. Building this from a set would scramble the
            # order, and trimming an unordered list can drop the NEWEST ids,
            # which would make those people get alerted over and over.
            merged = already_list + [r for r in new_alerts if r not in already]
            set_setting(LATE_ALERT_KEY, ",".join(merged[-300:]))
    except Exception:
        pass


# Nightly "who is still out" summary to your phone, so a forget gets caught the
# same night instead of rotting into a giant late stamp days later. Fires once
# per night at this hour (24-hour clock, 0 = midnight). Change this one number
# to move it. NOTE: hour must be 0-23; datetime.hour never reaches 24, so a
# value of 24 here made this check always true and the summary never sent.
NIGHTLY_SUMMARY_HOUR = 0
NIGHTLY_SUMMARY_KEY = "nightly_summary_date"


def check_nightly_summary(df_out: pd.DataFrame):
    """Once per night at NIGHTLY_SUMMARY_HOUR, push who is still signed out.

    Deduped on the calendar date in the settings tab, so it fires a single time
    each night no matter how many board refreshes happen after the hour. Safe
    to call from any board tick. Never breaks the board on failure.
    """
    try:
        now = datetime.now(TZ)
        if now.hour < NIGHTLY_SUMMARY_HOUR:
            return

        today = now.strftime("%Y-%m-%d")
        try:
            get_setting.clear()
        except Exception:
            pass
        if get_setting(NIGHTLY_SUMMARY_KEY).strip() == today:
            return  # already sent tonight

        # Claim tonight FIRST, so two kiosks hitting the hour together do not
        # both send. Whoever writes the date first wins; the other sees it.
        set_setting(NIGHTLY_SUMMARY_KEY, today)

        if df_out is None or df_out.empty:
            notify_phone("Bauercrest: Nightly check", "All staff are signed IN. Nobody is out.")
            return

        people = []
        for _, row in df_out.iterrows():
            nm = str(row.get("name", "")).strip()
            rs = str(row.get("reason", "")).strip()
            people.append(f"{nm} ({rs})" if rs else nm)

        body = f"{len(people)} still signed OUT: " + ", ".join(people)
        notify_phone("Bauercrest: Still out tonight", body[:1500])
    except Exception:
        pass


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
    # staff_pins loads on EVERY page render, unconditionally, including the
    # Sign In/Out page itself - so a bare Sheets hiccup here (the exact
    # transient failure SHEETS_TIMEOUT_SECONDS exists to survive) must not
    # propagate as an uncaught exception. Uncaught, it would hit main()'s
    # top-level catch-all and replace the WHOLE kiosk with the "reconnecting"
    # screen, not just this one board, until someone manually reloads. Falling
    # back to an empty frame here instead lets the rest of the page degrade
    # the same way every other Sheets read in this file already does.
    try:
        sheet = get_worksheet(SHEET_STAFF)
        df = read_sheet_df(sheet)
    except Exception:
        df = pd.DataFrame(columns=["name", "pin", "active", "admin"])
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
    # Same reasoning as load_staff_df_cached above: never let a Sheets hiccup
    # here take down the whole kiosk instead of just leaving the driver list
    # empty for this render.
    try:
        sheet = get_worksheet(SHEET_DRIVERS)
        df = read_sheet_df(sheet)
    except Exception:
        df = pd.DataFrame(columns=["name", "passed_test"])
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
    except Exception:
        # Best effort only. The reader already tolerates messy headers and the
        # loaders fill missing columns in memory, so a failure here must never
        # halt the kiosk. Leaving a raw error on screen during a demo is worse
        # than quietly moving on.
        pass


@st.cache_data(ttl=10)
def load_logs_df_cached():
    try:
        sheet = get_worksheet(SHEET_LOGS)
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


@st.cache_data(ttl=600)
def get_log_headers():
    """Cached logs header order. Read once, reused for every write in the
    session, so appends do not re-read the header each time."""
    try:
        hdr = [h.strip() for h in get_worksheet(SHEET_LOGS).row_values(1) if str(h).strip()]
        return hdr or list(LOGS_HEADERS_REQUIRED)
    except Exception:
        return list(LOGS_HEADERS_REQUIRED)


@st.cache_data(ttl=600)
def get_van_headers():
    """Cached vans header order, same idea as the logs headers."""
    try:
        hdr = [h.strip() for h in get_vans_sheet().row_values(1) if str(h).strip()]
        return hdr or list(VANS_HEADERS_REQUIRED)
    except Exception:
        return list(VANS_HEADERS_REQUIRED)


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


def _ntfy_send(topic: str, title: str, message: str, priority: str = "", tags: str = ""):
    """Fire the push in the background so the sign-out never waits on it.

    Secrets are read here on the main thread, then the actual HTTP call runs
    on a daemon thread. If ntfy is slow or unreachable, the user still gets an
    instant response; the push lands when it lands.
    """
    if not topic:
        return
    server = str(st.secrets.get("ntfy_server", "https://ntfy.sh")).rstrip("/")
    headers = {"Title": title}
    if priority:
        headers["Priority"] = priority
    if tags:
        headers["Tags"] = tags
    url = f"{server}/{topic}"
    data = message.encode("utf-8")

    def _post():
        try:
            requests.post(url, data=data, headers=headers, timeout=5)
        except Exception:
            pass

    try:
        threading.Thread(target=_post, daemon=True).start()
    except Exception:
        # If a thread cannot start for any reason, fall back to a quick
        # direct call so the push is not lost entirely.
        try:
            requests.post(url, data=data, headers=headers, timeout=2)
        except Exception:
            pass


def append_log_row(name: str, reason: str, other_reason: str, action: str, status: str,
                   notify: bool = True, due_back=None, late: str = ""):
    """Write a log row mapped to the sheet's actual header order.

    Mapping by header (instead of fixed position) keeps rows aligned even if
    someone reorders columns in Google Sheets. Set notify=False for van-driven
    auto sign-outs, so they do not buzz the staff phone (the vans phone covers
    those).

    due_back is stamped on the OUT row so the board and the alerts know when
    this person is expected back. late is stamped on the IN row so the record
    permanently shows they came back late.
    """
    try:
        sheet = get_worksheet(SHEET_LOGS)

        row_dict = {
            "id": str(uuid.uuid4())[:8],
            "timestamp": datetime.now(TZ).isoformat(timespec="seconds"),
            "name": name,
            "reason": reason,
            "other_reason": other_reason or "",
            "action": action,
            "status": status,
            "due_back": due_back.isoformat(timespec="seconds") if due_back else "",
            "late": late or "",
        }
        headers = get_log_headers()
        row = [row_dict.get(h, "") for h in headers]
        sheet.append_row(row)
        clear_logs_cache()
        remember_status(name, status, reason, other_reason)
        # logs is the permanent record; this just keeps the fast-path status
        # cache in step with it. Never allowed to fail the sign-in/out itself.
        try:
            upsert_current_status_rows([row_dict])
        except Exception:
            pass

        # Phone push after a clean write. Sign-out shows the reason; the typed
        # detail wins when the reason is Other.
        if notify:
            if action == "OUT":
                detail = other_reason.strip() if (reason.startswith("Other") and other_reason.strip()) else reason
                notify_phone("Bauercrest: Signed OUT", f"{name}: {detail}")
            else:
                notify_phone("Bauercrest: Signed IN", name)

        # Handed back so the caller can offer an Undo on this exact row.
        return row_dict["id"]
    except (APIError, GSpreadException):
        st.error("Could not record this sign-in/sign-out due to a problem talking to Google Sheets.")
        st.stop()


# =================================================
# UNDO
# =================================================
# How long an action stays undoable. The kiosk session stays open all day, so
# without a window someone could hit Undo hours later and silently reverse a
# DIFFERENT counselor's action. Two minutes covers "I just made a mistake".
UNDO_WINDOW_SECONDS = 120

# How long a tapped van stays selected before the page resets itself. The
# kiosk is shared, so a van picked by someone who wandered off must not still
# be waiting when the next counselor walks up.
VAN_SELECT_TIMEOUT_SECONDS = 90

# How long the big "SIGNED IN/OUT" confirmation banner stays up before it
# clears itself. The Sign In/Out page never auto-refreshes on its own (typing
# must never be interrupted), so a pure CSS fade would still leave the banner
# holding its layout space open indefinitely if nobody touches the kiosk
# again. A small fragment ticks this down and removes the banner for real.
FLASH_DISPLAY_SECONDS = 5


def delete_log_row_by_id(row_id: str) -> bool:
    """Delete one log row by its id. True if it was removed.

    A real delete, not a reversing entry. An accidental sign-out should vanish
    from the record, not leave a confusing OUT/IN pair that could also produce
    a bogus lateness note.
    """
    row_id = str(row_id or "").strip()
    if not row_id:
        return False
    try:
        sheet = get_worksheet(SHEET_LOGS)
        headers = get_log_headers()
        id_col = (headers.index("id") + 1) if "id" in headers else 1

        # Search the id COLUMN only. A whole-sheet search could land on the
        # same string sitting in another column (a typed reason, a note) and
        # then either delete the wrong row or refuse and quietly do nothing.
        try:
            cell = sheet.find(row_id, in_column=id_col)
        except TypeError:
            cell = sheet.find(row_id)
            if cell and cell.col != id_col:
                cell = None

        if not cell or cell.col != id_col:
            return False

        sheet.delete_rows(cell.row)
        clear_logs_cache()
        # This bypasses append_log_row, so current_status was never told about
        # it. A deleted row can be someone's latest, so rebuild rather than
        # leave the fast-path cache pointing at a row that no longer exists.
        try:
            rebuild_current_status_from_logs()
        except Exception:
            pass
        return True
    except Exception:
        return False


def set_pending_undo(row_id: str, desc: str):
    """Remember the action just taken, so it can be undone for a short window."""
    if not row_id:
        return
    st.session_state["undo_action"] = {
        "id": row_id,
        "desc": desc,
        "at": datetime.now(TZ),
    }


def get_pending_undo():
    """The action still inside its undo window, or None. Expires itself."""
    u = st.session_state.get("undo_action")
    if not u:
        return None
    try:
        age = (datetime.now(TZ) - u["at"]).total_seconds()
    except Exception:
        st.session_state.pop("undo_action", None)
        return None
    if age > UNDO_WINDOW_SECONDS:
        st.session_state.pop("undo_action", None)
        return None
    return u


def clear_all_logs():
    try:
        sheet = get_worksheet(SHEET_LOGS)
        sheet.clear()
        sheet.insert_row(LOGS_HEADERS_REQUIRED, 1)
        clear_logs_cache()
        # The header order just changed, and every old sign-out id is gone, so
        # drop both caches or the next write could use the old column order.
        try:
            get_log_headers.clear()
            set_setting(LATE_ALERT_KEY, "")
        except Exception:
            pass
        try:
            rebuild_current_status_from_logs()
        except Exception:
            pass
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
        # Rewrite using the sheet's ACTUAL header order, not a hardcoded list.
        # Forcing a fixed column list would drop any extra column and could
        # shift data into the wrong columns.
        headers = get_log_headers()
        sheet.clear()
        sheet.insert_row(headers, 1)
        if not df_keep.empty:
            df_out = df_keep.copy()
            df_out["timestamp"] = df_out["timestamp"].astype(str)
            for h in headers:
                if h not in df_out.columns:
                    df_out[h] = ""
            rows = df_out[headers].astype(str).values.tolist()
            if rows:
                sheet.append_rows(rows)
        clear_logs_cache()
        get_log_headers.clear()
        try:
            rebuild_current_status_from_logs()
        except Exception:
            pass
    except (APIError, GSpreadException):
        st.error("Could not finish deleting selected log entries. Please try again later.")
        st.stop()

# =================================================
# LOGIC HELPERS
# =================================================
def _sorted_by_recency(df: pd.DataFrame) -> pd.DataFrame:
    """Sort log rows oldest -> newest, reliably.

    Three things this guards against:
    1. Timestamps are written to the SECOND, and batch writes (field trip, van)
       stamp many rows with the identical second. A plain sort can reorder
       ties, so the wrong row looks like the latest. We tie-break on the sheet
       row order, since rows are appended in order, so the later row wins.
    2. An unparseable timestamp (someone hand-edits the sheet) becomes NaT.
       With na_position="last" a NaT row would sort NEWEST and win, freezing a
       person's status. We push bad rows to the FRONT so they can never win.
    3. A stable sort keeps equal keys in their original order.
    """
    tmp = df.copy().reset_index(drop=True)
    tmp["_row"] = range(len(tmp))
    tmp["timestamp"] = pd.to_datetime(tmp["timestamp"], errors="coerce", format="mixed")
    tmp = tmp.sort_values(
        ["timestamp", "_row"],
        na_position="first",
        kind="stable",
    )
    return tmp


def get_currently_out(df: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe of people whose latest status is OUT.

    Carries id and due_back so the board can flag late people and the alert
    can dedupe on the specific sign-out row.
    """
    cols = ["name", "reason", "other_reason", "timestamp", "due_back", "id"]
    empty = pd.DataFrame(columns=cols)
    if df is None or df.empty:
        return empty

    tmp = _sorted_by_recency(df)
    for c in ["due_back", "id"]:
        if c not in tmp.columns:
            tmp[c] = ""
    last_actions = tmp.groupby("name", dropna=False).tail(1)
    out_rows = last_actions[last_actions["status"] == "OUT"].copy()

    if out_rows.empty:
        return empty

    return out_rows[cols]


# Marker stored in a van-driven sign-out's other_reason. Lets the van return
# sign back in only the people the van itself signed out, and never touch
# someone who was already out for their own reason.
VAN_SIGNOUT_TAG = "VAN_TRIP"

# Same idea as VAN_SIGNOUT_TAG, for a group activity leaving on foot or by a
# vehicle this app doesn't track (a field trip, a tournament bus). Lets the
# leader who took the group out be the one who brings them all back, without
# touching anyone who was already out for their own separate reason.
GROUP_SIGNOUT_TAG = "GROUP_TRIP"
GROUP_PURPOSES = ["Field Trip", "Tournament", "Activity", "Other"]


# =================================================
# LOG ARCHIVING (keep the live tab small, keep 10 years of history)
# =================================================
def get_logs_archive_sheet():
    """Get the archive tab, creating it (with the live header) if missing."""
    try:
        return get_worksheet(SHEET_LOGS_ARCHIVE)
    except WorksheetNotFound:
        ss = get_spreadsheet()
        sheet = ss.add_worksheet(title=SHEET_LOGS_ARCHIVE, rows=200, cols=len(LOGS_HEADERS_REQUIRED))
        sheet.update("A1", [list(LOGS_HEADERS_REQUIRED)])
        try:
            get_worksheet.clear()
        except Exception:
            pass
        return sheet


def _row_age_ok_to_archive(ts_value, cutoff):
    """True only if this timestamp is real AND older than the cutoff.

    A blank or unparseable timestamp returns False, so a row we cannot date is
    never archived. We only ever move rows we are certain are old.
    """
    dt = parse_due(ts_value)
    if dt is None:
        return False
    return dt < cutoff


def plan_archive(live_df: pd.DataFrame, raw_rows: list, header: list, now=None, out_names=None):
    """Decide which raw rows stay live and which move to the archive.

    The rule, in order of safety:
      - Keep EVERY row for anyone whose latest status is OUT. Removing any of
        their rows could strand them off the Who's Out board.
      - Keep any row newer than the keep-days window.
      - Keep any row we cannot confidently date (blank or bad timestamp).
      - Everything else is a closed-out, old row and moves to the archive.

    Works on the RAW cell rows so the archived record is byte-for-byte what was
    written, never a reformatted copy. Returns (keep_rows, archive_rows).

    out_names lets the caller pass in who is currently OUT from the fast
    current_status cache instead of scanning the full history here. Pass None
    to fall back to computing it directly from live_df (the original, slower
    but fully self-contained method) if that cache is ever unavailable.
    """
    now = now or datetime.now(TZ)
    cutoff = now - timedelta(days=ARCHIVE_KEEP_DAYS)

    if out_names is None:
        # Names whose latest row is OUT, judged on the real sheet data.
        out_names = set(get_currently_out(live_df)["name"].astype(str).str.strip().tolist())

    try:
        name_i = header.index("name")
    except ValueError:
        name_i = 2
    try:
        ts_i = header.index("timestamp")
    except ValueError:
        ts_i = 1

    keep_rows, archive_rows = [], []
    for raw in raw_rows:
        # Normalize the row to the header width so both tabs line up.
        row = list(raw) + [""] * (len(header) - len(raw))
        row = row[:len(header)]
        name = str(row[name_i]).strip() if name_i < len(row) else ""
        ts_value = row[ts_i] if ts_i < len(row) else ""

        if name in out_names:
            keep_rows.append(row)
        elif not _row_age_ok_to_archive(ts_value, cutoff):
            keep_rows.append(row)
        else:
            archive_rows.append(row)
    return keep_rows, archive_rows


def archive_old_logs():
    """Move old, closed-out log rows to the archive tab. Returns a result dict.

    Safety is everything here, since this touches thousands of rows:
      1. Read the live tab once (raw cells + a parsed frame for status).
      2. Decide keep vs archive with plan_archive.
      3. Copy the archive rows to the archive tab in chunks, then VERIFY the
         archive grew by exactly the number of rows we sent. If it did not,
         ABORT and leave the live tab completely untouched. Nothing is ever
         removed from live until its copy is confirmed present in the archive.
      4. Rewrite the live tab by overwriting from the top with the kept rows,
         then trimming the now-redundant tail. This never leaves the live tab
         empty: if the trim step fails, the kept rows are already in place and
         some old rows simply remain (harmless duplicates of the archive), so
         the board still works. It fails safe, never with data loss.
    """
    result = {"ok": False, "archived": 0, "kept": 0, "message": ""}
    try:
        live = get_worksheet(SHEET_LOGS)
        all_vals = live.get_all_values()
    except Exception:
        result["message"] = "Could not read the live logs tab. Nothing was changed."
        return result

    if not all_vals:
        result["message"] = "The logs tab looks empty. Nothing to archive."
        return result

    header = all_vals[0]
    raw_rows = all_vals[1:]
    if len(raw_rows) == 0:
        result["message"] = "No log rows yet. Nothing to archive."
        return result

    live_df = read_sheet_df(live)

    # Prefer the fast current_status cache for "who is out"; fall back to the
    # slower full-history scan (out_names=None) if it is not available for any
    # reason. Archiving safety must never depend on this cache being correct.
    try:
        clear_current_status_cache()
        fast_out_names = set(
            get_currently_out(load_current_status_df_cached())["name"]
            .astype(str).str.strip().tolist()
        )
    except Exception:
        fast_out_names = None

    keep_rows, archive_rows = plan_archive(live_df, raw_rows, header, out_names=fast_out_names)
    result["kept"] = len(keep_rows)

    if not archive_rows:
        result["ok"] = True
        result["message"] = f"Nothing old enough to archive. All {len(keep_rows)} rows are recent or still open."
        return result

    # --- Step 3: copy to archive, then verify it landed ---
    try:
        arch = get_logs_archive_sheet()
        before = len(arch.get_all_values())
        for i in range(0, len(archive_rows), 500):
            arch.append_rows(archive_rows[i:i + 500])
        after = len(arch.get_all_values())
    except Exception:
        result["message"] = "Could not write to the archive tab. Live logs were left untouched, nothing was lost."
        return result

    if after != before + len(archive_rows):
        result["message"] = (
            "Archive copy did not verify (expected "
            f"{before + len(archive_rows)} rows, found {after}). Live logs were "
            "left untouched. Check the logs_archive tab, then try again."
        )
        return result

    # --- Step 4: shrink the live tab, failing safe ---
    try:
        new_live = [header] + keep_rows
        live.update("A1", new_live)
        old_total = len(all_vals)
        new_total = len(new_live)
        if old_total > new_total:
            live.delete_rows(new_total + 1, old_total)
        clear_logs_cache()
        try:
            get_log_headers.clear()
        except Exception:
            pass
    except Exception:
        result["message"] = (
            f"Archived {len(archive_rows)} rows to logs_archive, but shrinking the "
            "live tab did not fully finish. Nothing was lost. Run it once more, "
            "or check the logs tab."
        )
        result["archived"] = len(archive_rows)
        return result

    result["ok"] = True
    result["archived"] = len(archive_rows)
    result["message"] = (
        f"Archived {len(archive_rows)} old rows to logs_archive. The live tab now "
        f"holds {len(keep_rows)} rows (everyone still out, plus the last "
        f"{ARCHIVE_KEEP_DAYS} days). Nothing was deleted, only moved."
    )
    return result


def get_vans_archive_sheet():
    """Get the vans archive tab, creating it (with the live header) if missing.
    Same idea as get_logs_archive_sheet, one tab down."""
    try:
        return get_worksheet(SHEET_VANS_ARCHIVE)
    except WorksheetNotFound:
        ss = get_spreadsheet()
        sheet = ss.add_worksheet(title=SHEET_VANS_ARCHIVE, rows=200, cols=len(VANS_HEADERS_REQUIRED))
        sheet.update("A1", [list(VANS_HEADERS_REQUIRED)])
        try:
            get_worksheet.clear()
        except Exception:
            pass
        return sheet


def plan_van_archive(vans_df: pd.DataFrame, raw_rows: list, header: list, now=None):
    """Same rule as plan_archive, keyed on van instead of name:
      - Keep EVERY row for any van whose latest status is OUT.
      - Keep any row newer than the keep-days window.
      - Keep any row we cannot confidently date.
      - Everything else moves to the archive.

    Only 3 vans exist, so this is cheap to compute directly every time; there
    is no fast-cache equivalent to current_status needed here.
    """
    now = now or datetime.now(TZ)
    cutoff = now - timedelta(days=ARCHIVE_KEEP_DAYS)

    status_map = compute_van_status(vans_df)
    out_vans = {v for v in VANS if status_map.get(v, {}).get("status") == "OUT"}

    try:
        van_i = header.index("van")
    except ValueError:
        van_i = 2
    try:
        ts_i = header.index("timestamp")
    except ValueError:
        ts_i = 1

    keep_rows, archive_rows = [], []
    for raw in raw_rows:
        row = list(raw) + [""] * (len(header) - len(raw))
        row = row[:len(header)]
        van = str(row[van_i]).strip() if van_i < len(row) else ""
        ts_value = row[ts_i] if ts_i < len(row) else ""

        if van in out_vans:
            keep_rows.append(row)
        elif not _row_age_ok_to_archive(ts_value, cutoff):
            keep_rows.append(row)
        else:
            archive_rows.append(row)
    return keep_rows, archive_rows


def archive_old_vans():
    """Move old, closed-out van log rows to the archive tab.

    Identical shape and identical safety guarantees to archive_old_logs:
    copy first, verify the copy landed, only then shrink the live tab. See
    archive_old_logs for the full reasoning.
    """
    result = {"ok": False, "archived": 0, "kept": 0, "message": ""}
    try:
        live = get_vans_sheet()
        all_vals = live.get_all_values()
    except Exception:
        result["message"] = "Could not read the live vans tab. Nothing was changed."
        return result

    if not all_vals:
        result["message"] = "The vans tab looks empty. Nothing to archive."
        return result

    header = all_vals[0]
    raw_rows = all_vals[1:]
    if len(raw_rows) == 0:
        result["message"] = "No van log rows yet. Nothing to archive."
        return result

    vans_df = read_sheet_df(live)
    keep_rows, archive_rows = plan_van_archive(vans_df, raw_rows, header)
    result["kept"] = len(keep_rows)

    if not archive_rows:
        result["ok"] = True
        result["message"] = f"Nothing old enough to archive. All {len(keep_rows)} rows are recent or still open."
        return result

    try:
        arch = get_vans_archive_sheet()
        before = len(arch.get_all_values())
        for i in range(0, len(archive_rows), 500):
            arch.append_rows(archive_rows[i:i + 500])
        after = len(arch.get_all_values())
    except Exception:
        result["message"] = "Could not write to the van archive tab. Live van logs were left untouched, nothing was lost."
        return result

    if after != before + len(archive_rows):
        result["message"] = (
            "Archive copy did not verify (expected "
            f"{before + len(archive_rows)} rows, found {after}). Live van logs "
            "were left untouched. Check the vans_archive tab, then try again."
        )
        return result

    try:
        new_live = [header] + keep_rows
        live.update("A1", new_live)
        old_total = len(all_vals)
        new_total = len(new_live)
        if old_total > new_total:
            live.delete_rows(new_total + 1, old_total)
        clear_vans_cache()
        try:
            get_van_headers.clear()
        except Exception:
            pass
    except Exception:
        result["message"] = (
            f"Archived {len(archive_rows)} rows to vans_archive, but shrinking the "
            "live tab did not fully finish. Nothing was lost. Run it once more, "
            "or check the vans tab."
        )
        result["archived"] = len(archive_rows)
        return result

    result["ok"] = True
    result["archived"] = len(archive_rows)
    result["message"] = (
        f"Archived {len(archive_rows)} old van rows to vans_archive. The live tab "
        f"now holds {len(keep_rows)} rows (every van still out, plus the last "
        f"{ARCHIVE_KEEP_DAYS} days). Nothing was deleted, only moved."
    )
    return result


def check_auto_archive():
    """Run the archive once a day on its own, so the live tab never grows
    unbounded just because nobody remembered to click the admin button.

    Deduped on the calendar date in the settings tab, exactly like the
    nightly summary, so it fires once per day no matter how many kiosks tick
    past the hour. Safe to call from any board tick; never breaks the board.
    """
    try:
        now = datetime.now(TZ)
        if now.hour < AUTO_ARCHIVE_HOUR:
            return

        today = now.strftime("%Y-%m-%d")
        try:
            get_setting.clear()
        except Exception:
            pass
        if get_setting(AUTO_ARCHIVE_KEY).strip() == today:
            return  # already ran today

        # Claim today FIRST, so two kiosks hitting the hour together do not
        # both kick off an archive run at once.
        set_setting(AUTO_ARCHIVE_KEY, today)
        archive_old_logs()
        archive_old_vans()
    except Exception:
        pass


def build_full_history_csv() -> str:
    """Live + archive, combined and sorted oldest to newest, as CSV text.

    This is the 10-year record in one file. Reads both tabs so a full history
    can be handed over at any time even after archiving has run many times.
    """
    frames = []
    try:
        frames.append(read_sheet_df(get_worksheet(SHEET_LOGS)))
    except Exception:
        pass
    try:
        frames.append(read_sheet_df(get_logs_archive_sheet()))
    except Exception:
        pass
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return ""
    full = pd.concat(frames, ignore_index=True)
    if "id" in full.columns:
        full = full.drop_duplicates(subset=["id"], keep="first")
    if "timestamp" in full.columns:
        full["_ts"] = pd.to_datetime(full["timestamp"], errors="coerce")
        full = full.sort_values("_ts", na_position="first").drop(columns=["_ts"])
    return full.to_csv(index=False)


def build_full_van_history_csv() -> str:
    """Live + archive van logs, combined and sorted oldest to newest.
    Same idea as build_full_history_csv, for the vans tab."""
    frames = []
    try:
        frames.append(read_sheet_df(get_vans_sheet()))
    except Exception:
        pass
    try:
        frames.append(read_sheet_df(get_vans_archive_sheet()))
    except Exception:
        pass
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return ""
    full = pd.concat(frames, ignore_index=True)
    if "id" in full.columns:
        full = full.drop_duplicates(subset=["id"], keep="first")
    if "timestamp" in full.columns:
        full["_ts"] = pd.to_datetime(full["timestamp"], errors="coerce")
        full = full.sort_values("_ts", na_position="first").drop(columns=["_ts"])
    return full.to_csv(index=False)


# =================================================
# CURRENT STATUS (fast path: one row per person, not the whole history)
# =================================================
def get_current_status_sheet():
    """Get the current_status tab, creating and backfilling it if missing.

    Backfilling on creation matters: without it, the tab would start empty
    even though real people may already be OUT in the live logs history, and
    every fast-path reader below would wrongly show them as IN the moment
    this feature ships.
    """
    try:
        return get_worksheet(SHEET_CURRENT_STATUS)
    except WorksheetNotFound:
        ss = get_spreadsheet()
        sheet = ss.add_worksheet(
            title=SHEET_CURRENT_STATUS, rows=200, cols=len(CURRENT_STATUS_HEADERS)
        )
        sheet.update("A1", [CURRENT_STATUS_HEADERS])
        try:
            get_worksheet.clear()
        except Exception:
            pass
        try:
            rebuild_current_status_from_logs(sheet)
        except Exception:
            pass
        return sheet


@st.cache_data(ttl=5)
def load_current_status_df_cached():
    try:
        sheet = get_current_status_sheet()
        df = read_sheet_df(sheet)
    except Exception:
        return pd.DataFrame(columns=CURRENT_STATUS_HEADERS)

    for c in CURRENT_STATUS_HEADERS:
        if c not in df.columns:
            df[c] = ""
    df["name"] = df["name"].astype(str).str.strip()
    df["status"] = df["status"].astype(str).str.strip().str.upper()
    df["reason"] = df["reason"].astype(str).str.strip()
    df["other_reason"] = df["other_reason"].astype(str)
    return df


def clear_current_status_cache():
    load_current_status_df_cached.clear()


def rebuild_current_status_from_logs(sheet=None):
    """Recompute current_status from scratch by scanning the full logs
    history. This is the safety net: current_status is only ever a cache, and
    this function is the proof it can always be thrown away and regenerated
    with no data loss, since logs remains the one and only permanent record.

    Also doubles as the one-time backfill when the tab is first created.
    """
    sheet = sheet or get_current_status_sheet()
    status_map = get_latest_status_map(load_logs_df_cached())
    rows = [CURRENT_STATUS_HEADERS]
    for name, info in status_map.items():
        if not name:
            continue
        rows.append([
            name,
            info.get("status", ""),
            info.get("reason", ""),
            info.get("other_reason", ""),
            info.get("timestamp", ""),
            info.get("due_back", ""),
            info.get("id", ""),
        ])
    sheet.clear()
    sheet.update("A1", rows)
    clear_current_status_cache()


def upsert_current_status_rows(rows: list) -> bool:
    """Write each row's person into current_status in place: update their one
    row if they already have one, append a new row if they don't.

    Reads the whole current_status tab first, but that tab only ever has one
    row per staff member (tens of rows), not the full append-only history
    (which only grows), so this stays cheap all season regardless of how much
    history has piled up in logs. Batches all updates into one API call and
    all new rows into one more, so a full van (many people at once) still
    costs at most two calls here, not one per person.

    Never raises: current_status is a cache, so a failure here must not break
    the actual sign-in/out, which already succeeded by the time this runs.
    """
    if not rows:
        return True
    try:
        sheet = get_current_status_sheet()
        # Reuse the cached read instead of a fresh get_all_values() call. The
        # caller almost always just did a fresh status read (get_status_fresh /
        # get_status_map_fresh) moments ago in this same click, so this is
        # usually served from cache for free - cutting a full extra network
        # round trip off of every single sign-in/out. read_sheet_df never
        # reorders rows, so the DataFrame's row i is always sheet row i+2.
        df = load_current_status_df_cached()
        header = df.columns.tolist() if not df.empty else list(CURRENT_STATUS_HEADERS)
        name_i = header.index("name") if "name" in header else 0

        row_num_by_name = {}
        if not df.empty:
            for i, nm in enumerate(df[header[name_i]].astype(str).str.strip()):
                if nm:
                    row_num_by_name[nm] = i + 2  # 1 header + 1-index

        updates = []
        appends = []
        append_index_by_name = {}
        last_col = chr(ord("A") + len(header) - 1)
        for rd in rows:
            name = str(rd.get("name", "")).strip()
            if not name:
                continue
            values = [rd.get(h, "") for h in header]
            rn = row_num_by_name.get(name)
            if rn:
                updates.append({"range": f"A{rn}:{last_col}{rn}", "values": [values]})
            elif name in append_index_by_name:
                # A second occurrence of this name earlier queued a brand-new
                # row rather than an update (they have no existing row yet).
                # Overwrite that queued row in place so the batch still ends
                # with exactly one row per name instead of appending a
                # duplicate that then desyncs current_status from logs.
                appends[append_index_by_name[name]] = values
            else:
                appends.append(values)
                append_index_by_name[name] = len(appends) - 1

        if updates:
            sheet.batch_update(updates)
        if appends:
            sheet.append_rows(appends)
        clear_current_status_cache()
        return True
    except Exception:
        return False


# =================================================
# WRITE MEMORY (fixes "the app forgot who is in/out")
# =================================================
# Google Sheets lags a moment behind a write. The app can append a sign-out row
# and then, a heartbeat later, re-read the sheet and get back data that does
# not yet include that row, so it thinks the person never signed out. To stop
# that, every write this session makes is remembered locally for a short time
# and merged into every status read. The app always trusts what it just did,
# even if Google has not caught up yet.
RECENT_WRITE_TTL_SECONDS = 45


def remember_status(name: str, status: str, reason: str = "", other_reason: str = ""):
    name = str(name or "").strip()
    if not name:
        return
    d = st.session_state.get("recent_status", {})
    d[name] = {
        "status": str(status or "").strip().upper(),
        "reason": reason or "",
        "other_reason": other_reason or "",
        "timestamp": datetime.now(TZ).isoformat(timespec="seconds"),
        "at": datetime.now(TZ),
    }
    st.session_state["recent_status"] = d


def merge_recent_writes(df: pd.DataFrame) -> pd.DataFrame:
    """Add this session's recent writes on top of the sheet data.

    Recent local rows carry a current timestamp and are appended last, so the
    robust recency sort ranks them newest and they win, exactly when Google has
    not yet surfaced them. Expired entries are pruned. If Google has already
    caught up, the duplicate says the same thing, so the result is unchanged.
    """
    d = st.session_state.get("recent_status", {})
    if not d:
        return df
    now = datetime.now(TZ)
    rows = []
    keep = {}
    for name, w in d.items():
        try:
            age = (now - w["at"]).total_seconds()
        except Exception:
            age = 1e9
        if age <= RECENT_WRITE_TTL_SECONDS:
            keep[name] = w
            rows.append({
                "id": "local",
                "timestamp": w["timestamp"],
                "name": name,
                "reason": w["reason"],
                "other_reason": w["other_reason"],
                "action": w["status"],
                "status": w["status"],
                "due_back": "",
                "late": "",
            })
    st.session_state["recent_status"] = keep
    if not rows:
        return df
    add = pd.DataFrame(rows)
    if df is None or df.empty:
        return add
    # Work on copies. df here is the process-wide cached logs frame, so adding
    # columns to it in place would poison the cache for every other reader.
    base = df.copy()
    for c in base.columns:
        if c not in add.columns:
            add[c] = ""
    for c in add.columns:
        if c not in base.columns:
            base[c] = ""
    return pd.concat([base, add[base.columns]], ignore_index=True)


def get_latest_status_map(df: pd.DataFrame) -> dict:
    """Map each name to their most recent log row: status, reason, other_reason.

    Stable sort keeps sheet order for same-second timestamps, so the last row
    written for a person wins.
    """
    if df is None or df.empty:
        return {}
    tmp = _sorted_by_recency(df)
    last = tmp.groupby("name", dropna=False).tail(1)
    out = {}
    for _, r in last.iterrows():
        out[str(r.get("name", "")).strip()] = {
            "status": str(r.get("status", "")).strip().upper(),
            "reason": str(r.get("reason", "")).strip(),
            "other_reason": str(r.get("other_reason", "")).strip(),
            "due_back": str(r.get("due_back", "")).strip(),
            "id": str(r.get("id", "")).strip(),
            "timestamp": str(r.get("timestamp", "")).strip(),
        }
    return out


def get_status_fresh(name: str):
    """Read this person's TRUE current status straight from the sheet.

    The sign in/out box is a TOGGLE, so a stale read does not just show old
    data, it picks the WRONG DIRECTION: it can sign someone OUT a second time
    when they meant to sign IN, leaving them stuck on the Who's Out board.

    Reads current_status (a handful of rows) instead of the full logs history,
    so this stays fast no matter how big the season's history has grown. Drops
    the cache first since Google Sheets itself can lag a moment behind a
    write. Returns a dict with status/reason/other_reason, or None if the
    person has no row yet.
    """
    try:
        clear_current_status_cache()
    except Exception:
        pass
    df = merge_recent_writes(load_current_status_df_cached())
    return get_latest_status_map(df).get((name or "").strip())


def get_status_map_fresh() -> dict:
    """Everyone's true current status, straight from the sheet.

    The mass actions (van checkout, field trip) decide who to sign out based on
    who is currently IN. Deciding that from a cache up to 10 seconds old can
    sign out someone who ALREADY signed themselves out seconds earlier, giving
    them two OUT rows and stranding them on the board. Same failure mode that
    hit the toggle. So these reads are always fresh, and read current_status
    instead of the full logs history for the same speed reason as above.
    """
    try:
        clear_current_status_cache()
    except Exception:
        pass
    return get_latest_status_map(merge_recent_writes(load_current_status_df_cached()))


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
        headers = get_log_headers()
        matrix = [[rd.get(h, "") for h in headers] for rd in rows]
        sheet.append_rows(matrix)
        clear_logs_cache()
        for rd in rows:
            remember_status(rd.get("name", ""), rd.get("status", ""), rd.get("reason", ""), rd.get("other_reason", ""))
        try:
            upsert_current_status_rows(rows)
        except Exception:
            pass
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
    status_map = get_status_map_fresh()
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
    status_map = get_status_map_fresh()
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


def signin_everyone_on_van(van_name: str):
    """Sign back in EVERYONE still stuck out under this van's tag.

    This is what runs when a van is returned. Instead of trusting a stored
    driver name from a possibly-stale frame, it reads the live state and clears
    anyone whose current status is a Van sign-out for THIS van. That means a
    returned van always frees whoever it stranded, no matter who drove it back,
    so you stop having to admin-fix people. Returns the names signed in.
    """
    tag = f"{VAN_SIGNOUT_TAG}|{van_name}"
    status_map = get_status_map_fresh()
    rows = []
    signed = []
    for name, info in status_map.items():
        name = (name or "").strip()
        if not name:
            continue
        if (
            info["status"] == "OUT"
            and info["reason"] == "Van"
            and info["other_reason"].strip() == tag
        ):
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
    append_log_rows_batch(rows)
    return signed


def auto_signout_for_group(party: list, purpose: str, other_purpose: str, tag: str):
    """Sign out everyone in a group activity's party who is currently IN.

    Same shape as auto_signout_for_van: anyone already OUT for their own
    reason is left untouched, everyone else gets one batched write tagged so
    the leader who took them out can find exactly this party to bring back.
    Returns the list of names signed out.
    """
    status_map = get_status_map_fresh()
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
            "reason": purpose,
            "other_reason": f"{tag}|{other_purpose}" if other_purpose else tag,
            "action": "OUT",
            "status": "OUT",
        })
        signed.append(name)
    ok = append_log_rows_batch(rows)
    return signed if ok else []


def signin_everyone_in_group(tag: str):
    """Sign back in EVERYONE still stuck out under this group trip's tag.

    Reads live state and clears anyone whose current status is a group
    sign-out carrying THIS exact tag, the same pattern as
    signin_everyone_on_van, so the leader bringing the group back always
    frees exactly their own party and nobody else's concurrent trip.
    """
    status_map = get_status_map_fresh()
    rows = []
    signed = []
    for name, info in status_map.items():
        name = (name or "").strip()
        if not name:
            continue
        if (
            info["status"] == "OUT"
            and info["other_reason"].strip().startswith(tag)
        ):
            rows.append({
                "id": str(uuid.uuid4())[:8],
                "timestamp": datetime.now(TZ).isoformat(timespec="seconds"),
                "name": name,
                "reason": info["reason"],
                "other_reason": info["other_reason"],
                "action": "IN",
                "status": "IN",
            })
            signed.append(name)
    append_log_rows_batch(rows)
    return signed


# Marker written into a log row when an admin signs someone in from the Admin
# page. Lets the history show a human corrected the board.
ADMIN_SIGNIN_TAG = "ADMIN_SIGNIN"


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
    except Exception:
        # Best effort, same as the logs header. Never halt on this.
        pass


@st.cache_data(ttl=10)
def load_vans_df_cached():
    # Same reasoning as load_staff_df_cached: an uncaught Sheets error here
    # would take down the whole kiosk via main()'s catch-all instead of just
    # leaving the Vans board empty. Every caller already treats an empty
    # frame as "no van data yet", so this is a safe, silent fallback.
    try:
        sheet = get_vans_sheet()
        return read_sheet_df(sheet)
    except Exception:
        return pd.DataFrame()


def clear_vans_cache():
    load_vans_df_cached.clear()


def append_vans_row(row_dict: dict):
    sheet = get_vans_sheet()
    headers = get_van_headers()
    row = [row_dict.get(h, "") for h in headers]
    sheet.append_row(row)
    clear_vans_cache()


def compute_van_status(vans_df: pd.DataFrame) -> dict:
    status_map = {v: {"status": "IN"} for v in VANS}
    if vans_df is None or vans_df.empty:
        return status_map

    tmp = vans_df.copy()
    for col in ["timestamp", "van", "status", "driver", "purpose", "passengers", "other_purpose", "action", "gas_left"]:
        if col not in tmp.columns:
            tmp[col] = ""

    # Same robust ordering used for people. Without this a single unreadable
    # timestamp sorted NEWEST and won, freezing a van as OUT forever, and a
    # checkout and checkin written in the same second were a coin flip.
    tmp = _sorted_by_recency(tmp)

    for v in VANS:
        rows = tmp[tmp["van"] == v]
        if rows.empty:
            continue
        last = rows.iloc[-1]
        st_val = str(last.get("status", "")).strip().upper()
        if st_val not in ("IN", "OUT"):
            st_val = "IN"
        # Gas is only recorded on a sign-in, so read the most recent row that
        # actually has a gas value, regardless of whether the van is out now.
        gas = ""
        gas_rows = rows[rows["gas_left"].astype(str).str.strip() != ""]
        if not gas_rows.empty:
            gas = str(gas_rows.iloc[-1].get("gas_left", "")).strip()
        status_map[v] = {
            "status": st_val,
            "driver": str(last.get("driver", "")).strip(),
            "purpose": str(last.get("purpose", "")).strip(),
            "other_purpose": str(last.get("other_purpose", "")).strip(),
            "passengers": str(last.get("passengers", "")).strip(),
            "gas": gas,
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
    """Hide internal tracking tags from public display.

    VAN_TRIP and GROUP_TRIP tags exist so a return action can find exactly
    who it stranded; they were never meant for a counselor reading the board
    to see. Before this, the board showed raw tags like "VAN_TRIP|Van 1" as
    someone's detail line, which is neither useful nor good-looking.
    """
    s = str(other_reason or "").strip()
    hidden_prefixes = (LEGACY_AUTO_TAG_PREFIX, VAN_SIGNOUT_TAG, GROUP_SIGNOUT_TAG)
    if any(s.startswith(f"{p}|") or s == p for p in hidden_prefixes):
        return ""
    return s


def render_out_cards(df_out: pd.DataFrame, forgot_zone: bool = False):
    cards = []
    df = df_out.sort_values("timestamp")
    for _, row in df.iterrows():
        name = esc(row.get("name", ""))
        reason = esc(row.get("reason", ""))
        details = esc(clean_other_reason(row.get("other_reason", "")))
        when = esc(format_board_time(row.get("timestamp")))

        # Late = past their due-back time and still not signed in. Recomputed
        # from the current reason, so a reason edit in the sheet corrects it.
        mins = row_minutes_late(row)
        is_late = mins > 0

        details_html = f"<div class='bc-meta'>{details}</div>" if details else ""

        # Show WHEN they're expected back, not just whether they're late. The
        # deadline was already being computed for the late chip; the board just
        # never displayed it, so a counselor watching the board could see
        # "LATE 12 MIN" but never see a due-back time before that happened.
        # Skip it in the forgot zone: those cards already say "NO SIGN-IN" and
        # a deadline hours or days in the past would only be noise there.
        due_html = ""
        if not forgot_zone:
            due = effective_due_back(row.get("reason", ""), row.get("timestamp", ""))
            if due is not None:
                due_word = "Due back" if not is_late else "Was due back"
                due_html = (
                    f"<div class='bc-meta'>{due_word} "
                    f"<span class='bc-time'>{esc(format_board_time(due))}</span></div>"
                )

        if forgot_zone:
            # A count in the thousands tells you nothing. Say what it means.
            late_chip = "<div class='bc-chip bc-chip-forgot'>NO SIGN-IN</div>"
            card_cls = "bc-card bc-card-forgot"
        elif is_late:
            late_chip = f"<div class='bc-chip bc-chip-late'>LATE {mins} MIN</div>"
            card_cls = "bc-card bc-card-late"
        else:
            late_chip = ""
            card_cls = "bc-card"

        cards.append(
            f"<div class='{card_cls}'>"
            f"<div class='bc-chip'>{reason}</div>"
            f"{late_chip}"
            f"<div class='bc-name'>{name}</div>"
            f"{details_html}"
            f"<div class='bc-meta'>Signed out at <span class='bc-time'>{when}</span></div>"
            f"{due_html}"
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
def render_stale_fork(reason: str, other_reason: str):
    """Show the In-or-Out question for someone who has been out a long time.

    Only appears when the toggle set a pending_fork, i.e. a code was entered for
    a person whose sign-out is hours stale. The app refuses to guess and lets
    the counselor say what is actually happening. Two clear buttons, no toggle.
    """
    fork = st.session_state.get("pending_fork")
    if not fork:
        return

    name = fork["name"]
    since = ""
    ts = fork.get("timestamp", "")
    dt = parse_due(ts)
    if dt is not None:
        since = format_board_time(dt)
    since_txt = f" since {since}" if since else " from earlier"

    st.markdown(
        "<div class='bc-fork'>"
        f"<div class='bc-fork-head'>{esc(name)} has been signed OUT{esc(since_txt)}.</div>"
        "<div class='bc-fork-sub'>The board still shows you out. What are you doing right now?</div>"
        "</div>",
        unsafe_allow_html=True,
    )
    c1, c2, c3 = st.columns([2, 2, 1])

    with c1:
        if st.button("Coming IN (I'm back)", key="fork_in", use_container_width=True):
            # Close the stale sign-out normally, stamping how late it ran.
            due = effective_due_back(fork.get("reason", ""), fork.get("timestamp", ""))
            mins = minutes_late(due)
            late_note = f"LATE {mins} min" if mins > 0 else ""
            new_id = append_log_row(
                name,
                fork.get("reason", ""),
                fork.get("other_reason", ""),
                action="IN",
                status="IN",
                late=late_note,
            )
            set_pending_undo(new_id, f"{name}'s sign-in")
            st.session_state.pop("pending_fork", None)
            st.session_state["log_flash_kind"] = "in"
            st.session_state["log_flash_word"] = f"{name.upper()} IS SIGNED IN"
            st.session_state["log_flash"] = "Welcome back. Your earlier sign-out is now closed."
            st.session_state["signio_nonce"] += 1
            st.rerun()

    with c2:
        if st.button("Signing OUT (leaving now)", key="fork_out", use_container_width=True):
            if reason == "Other (type reason)" and not other_reason.strip():
                st.session_state["fork_error"] = "Pick a reason above first, then tap Signing OUT."
                st.rerun()
            # Close the forgotten sign-out quietly, then open a fresh one now, so
            # the record is honest and they are not left marked in camp.
            append_log_row(
                name,
                fork.get("reason", ""),
                fork.get("other_reason", ""),
                action="IN",
                status="IN",
                late="AUTO-CLOSED (forgot to sign in)",
                notify=False,
            )
            due = compute_due_back(reason, datetime.now(TZ))
            new_id = append_log_row(name, reason, other_reason, action="OUT", status="OUT", due_back=due)
            set_pending_undo(new_id, f"{name}'s sign-out")
            st.session_state.pop("pending_fork", None)
            st.session_state["log_flash_kind"] = "out"
            st.session_state["log_flash_word"] = f"{name.upper()} IS SIGNED OUT"
            shown = reason if reason != "Other (type reason)" else other_reason
            st.session_state["log_flash"] = f"Got it. Signed you OUT now for {shown}. Your forgotten sign-in was cleaned up."
            st.session_state["signio_nonce"] += 1
            st.rerun()

    with c3:
        if st.button("Cancel", key="fork_cancel", use_container_width=True):
            st.session_state.pop("pending_fork", None)
            st.rerun()

    ferr = st.session_state.pop("fork_error", "")
    if ferr:
        st.error(ferr)


def whos_out_strip():
    """A compact live list of who is out, right under the sign box.

    Reuses the same cached logs read the board uses, with the session's own
    recent writes merged in, so it never adds a blocking call and never slows
    the code box. Forgot-zone people are shown separately, like the board.
    """
    try:
        df_out = get_currently_out(merge_recent_writes(load_current_status_df_cached()))
    except Exception:
        return

    st.markdown("<div class='bc-strip-title'>Signed out right now</div>", unsafe_allow_html=True)
    if df_out is None or df_out.empty:
        st.markdown("<div class='bc-strip-empty'>Everyone is in camp.</div>", unsafe_allow_html=True)
        return

    forgot, active = [], []
    for _, r in df_out.iterrows():
        item = {
            "name": str(r.get("name", "")).strip(),
            "reason": str(r.get("reason", "")).strip(),
            "other": clean_other_reason(r.get("other_reason", "")),
        }
        if row_minutes_late(r) >= FORGOT_THRESHOLD_MINUTES:
            forgot.append(item)
        else:
            active.append(item)

    def chip(it, forgot_zone=False):
        label = esc(it["name"])
        detail = esc(it["other"]) if it["other"] else esc(it["reason"])
        cls = "bc-chip-strip bc-chip-strip-forgot" if forgot_zone else "bc-chip-strip"
        tail = " · NO SIGN-IN" if forgot_zone else (f" · {detail}" if detail else "")
        return f"<span class='{cls}'>{label}{esc(tail)}</span>"

    if active:
        st.markdown("<div class='bc-strip'>" + "".join(chip(i) for i in active) + "</div>", unsafe_allow_html=True)
    if forgot:
        st.markdown(
            "<div class='bc-strip-forgot-label'>Probably forgot to sign in</div>"
            "<div class='bc-strip'>" + "".join(chip(i, True) for i in forgot) + "</div>",
            unsafe_allow_html=True,
        )


def page_sign_in_out(staff_pins: dict, staff_names: list):
    page_title("Camp Bauercrest Staff", "Sign In / Out")

    pin_lookup = build_pin_lookup(staff_pins)

    # Bumping this nonce changes the code field key, so the box comes back
    # empty after each use. The next counselor starts fresh.
    if "signio_nonce" not in st.session_state:
        st.session_state["signio_nonce"] = 0
    n = st.session_state["signio_nonce"]

    # A message set just now (this exact rerun) has no timestamp yet - stamp
    # it once, here, centrally, rather than touching every place that sets
    # log_flash. Expiry always clears the timestamp together with the message
    # (below), so "message present, no timestamp" only ever means "brand new".
    if st.session_state.get("log_flash") and "log_flash_at" not in st.session_state:
        st.session_state["log_flash_at"] = datetime.now(TZ)

    @st.fragment(run_every=1)
    def flash_ticker():
        msg = st.session_state.get("log_flash", "")
        if not msg:
            return
        at = st.session_state.get("log_flash_at")
        age = (datetime.now(TZ) - at).total_seconds() if at else FLASH_DISPLAY_SECONDS
        if age >= FLASH_DISPLAY_SECONDS:
            for k in ("log_flash", "log_flash_kind", "log_flash_word", "log_flash_ask", "log_flash_at"):
                st.session_state.pop(k, None)
            return
        big_flash(
            msg,
            st.session_state.get("log_flash_kind", "in"),
            st.session_state.get("log_flash_word", ""),
            st.session_state.get("log_flash_ask", ""),
        )

    flash_ticker()

    # Undo sits right under the banner, so the person who just made the
    # mistake sees it immediately. It disappears on its own after the window.
    undo = get_pending_undo()
    if undo:
        left = int(UNDO_WINDOW_SECONDS - (datetime.now(TZ) - undo["at"]).total_seconds())
        # The name goes ON the button. The kiosk is shared, so the next
        # counselor in line will see this too, and "Undo" alone invites them
        # to tap it. Naming the person makes it obvious whose action it is.
        uc1, uc2 = st.columns([2, 3])
        with uc1:
            if st.button(f"Undo {undo['desc']}", key="undo_btn", use_container_width=True):
                if delete_log_row_by_id(undo["id"]):
                    notify_phone("Bauercrest: UNDO", f"Undo: {undo['desc']}")
                    st.session_state["log_flash"] = f"Undone: {undo['desc']}"
                    st.session_state["log_flash_kind"] = "out"
                    st.session_state["log_flash_word"] = "UNDONE"
                else:
                    st.session_state["log_flash"] = "Nothing to undo. That record is already gone."
                st.session_state.pop("undo_action", None)
                st.session_state["signio_nonce"] += 1
                st.rerun()
        with uc2:
            st.caption(f"Only if that was a mistake. {max(left, 0)}s left.")

    # The reason carries over from whoever used the kiosk last (see note
    # below), which is exactly the thing someone in a hurry can miss. Read it
    # BEFORE the banner so the banner can say it out loud instead of making
    # people look down at a dropdown to find out what they are about to be
    # signed out for. The headline word itself ("SIGNING OUT") stays fixed and
    # short; the variable-length reason renders as its own smaller line below
    # it, so a long typed reason wraps gracefully instead of breaking the big
    # bold headline across several lines.
    pending_reason = st.session_state.get("signout_reason", REASONS[0])
    pending_other = st.session_state.get(f"signout_other_reason_{n}", "").strip()
    if pending_reason != "Other (type reason)":
        reason_line = pending_reason
    elif pending_other:
        reason_line = pending_other
    else:
        reason_line = "type it below"

    # Two-column status row, sized 3:2 since the OUT side carries the
    # variable reason text and the IN side never needs more than one line.
    st.markdown(
        "<div style='display:flex;gap:0.7rem;margin:0.2rem 0 0.9rem 0;align-items:stretch;'>"
        "<div class='bc-banner bc-banner-out' style='flex:3;margin:0;'>"
        "<div class='bc-banner-word'>SIGNING OUT</div>"
        f"<div class='bc-banner-reason'>{esc(reason_line)}</div>"
        "<div class='bc-banner-sub'>Not right? Change it below, then type your code</div>"
        "</div>"
        "<div class='bc-banner bc-banner-in' style='flex:2;margin:0;'>"
        "<div class='bc-banner-word'>COMING BACK</div>"
        "<div class='bc-banner-sub'>Type your code. Skip the reason</div>"
        "</div>"
        "</div>",
        unsafe_allow_html=True,
    )
    st.caption("One box does both. If you are in camp you go out. If you are out you come back in.")

    # Reason and code live in ONE card now, not two separate boxes of
    # different heights side by side - that split was exactly what made the
    # page feel unbalanced, with empty space under the shorter side. Fixed
    # keys, on purpose, for the reason itself. It stays on whatever the last
    # person picked, so a group all signing out for a Night Off does not have
    # to reset it every single time. Only the code box resets between people.
    # The banner above now names the pending reason out loud, so a counselor
    # who forgot to check it still sees it before typing their code.
    with st.container(border=True):
        reason = st.selectbox("Reason (only used if you are signing OUT)", REASONS, key="signout_reason")
        other_reason = ""
        if reason == "Other (type reason)":
            # Nonce-scoped, unlike the reason dropdown above. A shared dropdown
            # value ("Night Off") commonly applies to the next person too, but a
            # typed detail ("dentist appointment") almost never does, and being
            # free text makes a leftover value much easier to miss than a
            # dropdown that visibly still says the wrong thing. This clears it
            # after every action, same as the code box.
            other_reason = st.text_input("Type your reason", key=f"signout_other_reason_{n}")

        with st.form("signio_form", clear_on_submit=False):
            code_col, btn_col = st.columns([2, 1])
            with code_col:
                code = st.text_input("Your code", type="password", max_chars=4, key=f"signio_code_{n}")
            with btn_col:
                st.markdown("<div style='height:1.6rem'></div>", unsafe_allow_html=True)
                submitted = st.form_submit_button("Enter", use_container_width=True)

    if submitted:
        name, err = resolve_code(code, pin_lookup)
        if err:
            st.error(err)
        else:
            # A network round trip to Sheets sits behind this click, and a
            # kiosk with no other feedback just looks frozen for that moment.
            with st.spinner("One moment..."):
                # Decide direction from a FRESH read, never the cache. This is
                # a toggle, so a stale read would not just show old data, it
                # would flip the wrong way and sign someone OUT twice.
                info = get_status_fresh(name)
                is_out = bool(info and info["status"] == "OUT")

                if is_out:
                    due = effective_due_back(info.get("reason", ""), info.get("timestamp", ""))
                    mins = minutes_late(due)

                    # THE FORK. If signing them in would be surprising (stale
                    # sign-out, likely a forgotten sign-in), do not guess. Stop
                    # and ask whether they are coming in or leaving again now.
                    # Everyone whose state is fresh skips this entirely.
                    if is_surprising_signin(info):
                        st.session_state["pending_fork"] = {
                            "name": name,
                            "reason": info.get("reason", ""),
                            "other_reason": info.get("other_reason", ""),
                            "timestamp": info.get("timestamp", ""),
                            "mins": mins,
                        }
                        st.session_state["signio_nonce"] += 1
                        st.rerun()

                    # Normal, recent sign-in: carry their reason, note lateness.
                    late_note = f"LATE {mins} min" if mins > 0 else ""
                    new_id = append_log_row(
                        name,
                        info.get("reason", ""),
                        info.get("other_reason", ""),
                        action="IN",
                        status="IN",
                        late=late_note,
                    )
                    set_pending_undo(new_id, f"{name}'s sign-in")
                    st.session_state["log_flash_kind"] = "in"
                    st.session_state["log_flash_word"] = f"{name.upper()} IS SIGNED IN"
                    if mins > 0:
                        notify_phone(
                            "Bauercrest: Signed IN (LATE)",
                            f"{name} signed in {mins} min late ({info.get('reason','')})",
                        )
                        st.session_state["log_flash"] = f"Welcome back. You were {mins} min late."
                    else:
                        st.session_state["log_flash"] = "Welcome back to camp."
                    st.session_state["signio_nonce"] += 1
                    st.rerun()
                elif reason == "Other (type reason)" and not other_reason.strip():
                    st.error("Please type a reason for 'Other'.")
                else:
                    due = compute_due_back(reason, datetime.now(TZ))
                    new_id = append_log_row(name, reason, other_reason, action="OUT", status="OUT", due_back=due)
                    set_pending_undo(new_id, f"{name}'s sign-out")
                    st.session_state["log_flash_kind"] = "out"
                    st.session_state["log_flash_word"] = f"{name.upper()} IS SIGNED OUT"
                    st.session_state["log_flash"] = f"Reason: {reason if reason != 'Other (type reason)' else other_reason}. Sign back in when you return."
                    st.session_state["signio_nonce"] += 1
                    st.rerun()

    render_stale_fork(reason, other_reason)

    whos_out_strip()

    crest_footer()


def page_whos_out():
    page_title("The Big House Board", "Who's Out Right Now")

    @st.fragment(run_every=BOARD_REFRESH_SECONDS)
    def live_board():
        # This board is the screen most likely to sit unattended, so it gets
        # its own check on the fragment's normal refresh tick instead of
        # waiting for a full page rerun like the other pages.
        render_emergency_banner()
        live_clock_note()
        df_status = merge_recent_writes(load_current_status_df_cached())
        df_out = get_currently_out(df_status)

        # Alert on anyone who just crossed their deadline. Runs on the board's
        # one-minute tick, deduped in the sheet so it fires only once.
        check_late_and_alert(df_out)
        check_nightly_summary(df_out)
        check_auto_archive()

        # Split the board. Someone hours past their deadline almost certainly
        # forgot to sign in, and a huge minute count buries the person who is
        # genuinely late right now. Nothing is deleted, it just moves down.
        if df_out.empty:
            empty_note("No staff are currently signed out.")
        else:
            forgot_mask = df_out.apply(
                lambda r: row_minutes_late(r) >= FORGOT_THRESHOLD_MINUTES, axis=1
            )
            active = df_out[~forgot_mask]
            forgot = df_out[forgot_mask]

            if active.empty:
                empty_note("No staff are currently signed out.")
            else:
                render_out_cards(active)

            if not forgot.empty:
                st.markdown("")
                section_title(f"Probably Forgot To Sign In ({len(forgot)})")
                st.caption("No sign-in recorded well past their return time. An admin can clear these on the Admin page.")
                render_out_cards(forgot, forgot_zone=True)

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


def van_out_since(vans_df, van_name):
    """Who took this van and when, from its latest checkout row."""
    try:
        tmp = _sorted_by_recency(vans_df)
        rows = tmp[tmp["van"] == van_name]
        if rows.empty:
            return "", None
        outs = rows[rows["status"].astype(str).str.upper() == "OUT"]
        src = outs.iloc[-1] if not outs.empty else rows.iloc[-1]
        return str(src.get("driver", "")).strip(), src.get("timestamp")
    except Exception:
        return "", None


GAS_LEVELS = {
    "Full": (1.00, "#2E7D32"),
    "3/4": (0.75, "#4C9A2A"),
    "Half": (0.50, "#C7A008"),
    "1/4": (0.25, "#D08114"),
    "Low / Empty": (0.10, "#B3261E"),
}


def gas_tank_svg(level_word: str) -> str:
    """A little gas-tank graphic filled to match the recorded level.

    The fill height and color both track the word, so the reading is obvious at
    a glance: green and full up top, red and near-empty at the bottom.
    """
    word = str(level_word or "").strip()
    if word not in GAS_LEVELS:
        # Unknown or never recorded.
        return (
            "<div class='bc-gas'>"
            "<svg width='34' height='46' viewBox='0 0 34 46'>"
            "<rect x='4' y='6' width='26' height='36' rx='4' fill='none' "
            "stroke='#B9C0CC' stroke-width='2'/>"
            "</svg>"
            "<div class='bc-gas-word bc-gas-unknown'>No reading</div>"
            "</div>"
        )
    frac, color = GAS_LEVELS[word]
    inner_h = 32.0
    fill_h = max(2.0, inner_h * frac)
    fill_y = 8.0 + (inner_h - fill_h)
    return (
        "<div class='bc-gas'>"
        "<svg width='34' height='46' viewBox='0 0 34 46'>"
        f"<rect x='5' y='8' width='24' height='{inner_h}' rx='3' fill='#0f1b30' opacity='0.10'/>"
        f"<rect x='5' y='{fill_y:.1f}' width='24' height='{fill_h:.1f}' rx='3' fill='{color}'/>"
        "<rect x='4' y='6' width='26' height='36' rx='4' fill='none' stroke='#3A4A63' stroke-width='2'/>"
        "<rect x='24' y='2' width='7' height='6' rx='1.5' fill='#3A4A63'/>"
        "</svg>"
        f"<div class='bc-gas-word' style='color:{color};'>{esc(word)}</div>"
        "</div>"
    )


def render_van_tiles(status_map: dict, selected: str = ""):
    """The van picker. Each van is one tile showing its state.

    This IS the interface now. There is no separate sign-out form and sign-in
    form to choose between. You pick the van you are dealing with and the app
    works out whether you are taking it or bringing it back.
    """
    tiles = []
    for v in VANS:
        info = status_map.get(v, {"status": "IN"})
        out = info.get("status") == "OUT"
        sel = " bc-vantile-sel" if v == selected else ""
        state_cls = "bc-vantile-out" if out else "bc-vantile-in"
        state_word = "OUT" if out else "AT CAMP"
        action = "Tap to bring back" if out else "Tap to take out"
        who = f"<div class='bc-vantile-who'>{esc(info.get('driver',''))}</div>" if out and info.get("driver") else ""
        gas = gas_tank_svg(info.get("gas", ""))
        tiles.append(
            f"<div class='bc-vantile {state_cls}{sel}'>"
            f"<div class='bc-vantile-state'>{state_word}</div>"
            f"<div class='bc-vantile-name'>{esc(van_label(v))}</div>"
            f"{who}"
            f"<div class='bc-vantile-action'>{action}</div>"
            f"{gas}"
            f"</div>"
        )
    st.markdown(f"<div class='bc-vangrid'>{''.join(tiles)}</div>", unsafe_allow_html=True)


def page_vans(staff_pins: dict, staff_names: list, driver_names: list):
    page_title("Camp Vehicles", "Vans")

    if "van_form_nonce" not in st.session_state:
        st.session_state["van_form_nonce"] = 0
    van_nonce = st.session_state["van_form_nonce"]

    flash = st.session_state.pop("van_flash", "")
    if flash:
        big_flash(flash, "in")

    vans_df = load_vans_df_cached()
    status_map = compute_van_status(vans_df)

    # A van picked by someone who then walked away must not still be selected
    # when the next counselor arrives. The kiosk is one shared session, so the
    # selection expires on its own.
    selected = st.session_state.get("van_selected", "")
    picked_at = st.session_state.get("van_selected_at")
    if selected and picked_at is not None:
        try:
            if (datetime.now(TZ) - picked_at).total_seconds() > VAN_SELECT_TIMEOUT_SECONDS:
                selected = ""
                st.session_state["van_selected"] = ""
        except Exception:
            pass
    if selected and selected not in VANS:
        selected = ""

    st.caption("Tap the van you are dealing with. The app knows if you are taking it out or bringing it back.")
    render_van_tiles(status_map, selected)

    # The tiles above are the display. These buttons sit directly under them
    # and are what actually gets tapped, one per van, in the same order.
    cols = st.columns(len(VANS))
    for i, v in enumerate(VANS):
        with cols[i]:
            if st.button(van_label(v), key=f"vanpick_{v}", use_container_width=True):
                st.session_state["van_selected"] = v
                st.session_state["van_selected_at"] = datetime.now(TZ)
                st.rerun()

    if not selected:
        st.divider()
        empty_note("Pick a van above to take one out or bring one back.")
        crest_footer()
        return

    is_out = status_map.get(selected, {}).get("status") == "OUT"
    st.divider()

    # ---------------- BRING A VAN BACK ----------------
    if is_out:
        took_driver, took_at = van_out_since(vans_df, selected)
        sub = f"Taken by {took_driver}" if took_driver else "Bringing it back to camp"
        if took_at is not None:
            when = format_board_time(pd.to_datetime(took_at, errors="coerce"))
            if when:
                sub += f" at {when}"
        big_banner(f"BRINGING BACK {van_label(selected).upper()}", sub, "in")

        pin_lookup_in = build_pin_lookup(staff_pins)
        with st.form(f"van_back_form_{van_nonce}", clear_on_submit=True):
            back_code = st.text_input("Your code", type="password", max_chars=4)
            gas_left = st.selectbox("Gas left", ["Full", "3/4", "Half", "1/4", "Low / Empty"])
            back_go = st.form_submit_button(f"Bring {van_label(selected)} Back", use_container_width=True)

        if st.button("Pick a different van", key="van_cancel_in"):
            st.session_state["van_selected"] = ""
            st.rerun()

        if back_go:
            def do_bring_back():
                # The driving is done, so any active staff code can return a
                # van. Taking one OUT still requires a tested driver.
                who, err = resolve_code(back_code, pin_lookup_in)
                if err:
                    st.error(err)
                    return

                fresh_df = load_vans_df_cached()
                if compute_van_status(fresh_df).get(selected, {}).get("status") != "OUT":
                    st.error(f"{van_label(selected)} is already signed in.")
                    return

                last_purpose = ""
                last_other = ""
                try:
                    tmp = _sorted_by_recency(fresh_df)
                    vr = tmp[tmp["van"] == selected]
                    if not vr.empty:
                        outs = vr[vr["status"].astype(str).str.upper() == "OUT"]
                        src = outs.iloc[-1] if not outs.empty else vr.iloc[-1]
                        last_purpose = str(src.get("purpose", "")).strip()
                        last_other = str(src.get("other_purpose", "")).strip()
                except Exception:
                    pass

                row = {
                    "id": str(uuid.uuid4())[:8],
                    "timestamp": datetime.now(TZ).isoformat(timespec="seconds"),
                    "van": selected,
                    "driver": who,
                    "purpose": last_purpose,
                    "passengers": "",
                    "other_purpose": last_other,
                    "action": "CHECKIN",
                    "status": "IN",
                    "gas_left": gas_left,
                }
                try:
                    append_vans_row(row)
                except Exception:
                    st.error("Could not save the van sign-in. Please try again.")
                    return

                notify_vans(
                    "Bauercrest: Van IN",
                    f"{van_label(selected)} returned by {who}, gas: {gas_left}",
                )

                # Free everyone the van stranded on the camp board, read live.
                try:
                    freed = signin_everyone_on_van(selected)
                except Exception:
                    freed = []
                    st.warning("Van signed in, but linking riders back to the Who's Out board hit a snag.")

                # The person returning it is back at camp too, even if they
                # were not the one the van signed out.
                try:
                    if who not in freed:
                        info = get_status_fresh(who)
                        if info and info["status"] == "OUT" and info["reason"] == "Van":
                            append_log_row(who, "Van", info["other_reason"], action="IN", status="IN", notify=False)
                            freed = freed + [who]
                except Exception:
                    pass

                note = f" {len(freed)} signed back in at camp." if freed else ""
                st.session_state["van_form_nonce"] += 1
                st.session_state["van_selected"] = ""
                st.session_state["van_flash"] = f"{van_label(selected)} is back. Gas: {gas_left}.{note}"
                st.rerun()

            do_bring_back()

    # ---------------- TAKE A VAN OUT ----------------
    else:
        big_banner(f"TAKING OUT {van_label(selected).upper()}", "Signing this van off camp", "out")

        if not driver_names:
            st.warning("No eligible drivers found. Set drivers.passed_test=TRUE for cleared drivers.")
            if st.button("Pick a different van", key="van_cancel_nodrv"):
                st.session_state["van_selected"] = ""
                st.rerun()
            crest_footer()
            return

        pin_lookup = build_pin_lookup(staff_pins)
        with st.form(f"van_take_form_{van_nonce}", clear_on_submit=False):
            st.caption("You will be signed out of camp automatically. Passengers sign themselves out on the main page.")
            driver_code = st.text_input("Driver code", type="password", max_chars=4)
            purpose = st.selectbox("Purpose", VAN_PURPOSES)
            other_purpose = ""
            if purpose == "Other":
                other_purpose = st.text_input("Other purpose (required)")
            take_go = st.form_submit_button(f"Take {van_label(selected)} Out", use_container_width=True)

        if st.button("Pick a different van", key="van_cancel_out"):
            st.session_state["van_selected"] = ""
            st.rerun()

        if take_go:
            def do_take_out():
                driver, err = resolve_code(driver_code, pin_lookup)
                if err:
                    st.error(err)
                    return

                if driver not in driver_names:
                    st.error("This code is not cleared to drive a van.")
                    return

                # Guard against two people grabbing the same van at once.
                if compute_van_status(load_vans_df_cached()).get(selected, {}).get("status") == "OUT":
                    st.error(f"{van_label(selected)} was taken a moment ago. Pick another van.")
                    return

                if purpose == "Other" and not other_purpose.strip():
                    st.error("Please enter the other purpose.")
                    return

                row = {
                    "id": str(uuid.uuid4())[:8],
                    "timestamp": datetime.now(TZ).isoformat(timespec="seconds"),
                    "van": selected,
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

                ptext = other_purpose.strip() if (purpose == "Other" and other_purpose.strip()) else purpose
                notify_vans("Bauercrest: Van OUT", f"{van_label(selected)} - {driver} ({ptext})")

                # Sign the driver out of camp so the board matches the van.
                camp_note = ""
                try:
                    signed = auto_signout_for_van([driver], selected)
                    camp_note = f" {driver} signed out of camp." if signed else f" {driver} was already signed out."
                except Exception:
                    st.warning("Van saved and alert sent, but linking the driver to the Who's Out board hit a snag.")

                st.session_state["van_form_nonce"] += 1
                st.session_state["van_selected"] = ""
                st.session_state["van_flash"] = f"{van_label(selected)} is out under {driver}.{camp_note}"
                st.rerun()

            do_take_out()

    crest_footer()


def page_group_signout(staff_pins: dict, staff_names: list):
    """One leader's code signs a whole group out or back in at once.

    For a field trip, tournament, or activity leaving together without one
    of the tracked vans. Same trust model as the van flow: the leader's own
    PIN authorizes the whole party, exactly like a driver's code already
    does for everyone riding in a van. Nothing here is new risk the app
    didn't already accept for vans.
    """
    page_title("Field Trips & Activities", "Group Sign-Out")
    st.caption(
        "For a group leaving together without a tracked van — a field trip, a "
        "tournament, an off-site activity. One leader signs everyone out at "
        "once, and brings them all back the same way."
    )

    if "group_form_nonce" not in st.session_state:
        st.session_state["group_form_nonce"] = 0
    g_nonce = st.session_state["group_form_nonce"]

    flash = st.session_state.pop("group_flash", "")
    if flash:
        big_flash(flash, "in")

    pin_lookup = build_pin_lookup(staff_pins)
    leader = st.session_state.get("group_leader")

    # Step 1: who is this? We can't tell take-out from bring-back until we
    # know the leader's own current status, so identify them first.
    if not leader:
        with st.form(f"group_lookup_form_{g_nonce}", clear_on_submit=True):
            leader_code = st.text_input("Leader code", type="password", max_chars=4, key=f"group_leader_code_{g_nonce}")
            lookup_go = st.form_submit_button("Continue", use_container_width=True)
        if lookup_go:
            found, err = resolve_code(leader_code, pin_lookup)
            if err:
                st.error(err)
            else:
                st.session_state["group_leader"] = found
                st.rerun()
        crest_footer()
        return

    if st.button("Not you? Start over", key="group_cancel"):
        st.session_state.pop("group_leader", None)
        st.rerun()

    info = get_status_fresh(leader)
    is_leading_trip = bool(
        info and info["status"] == "OUT" and info["other_reason"].startswith(GROUP_SIGNOUT_TAG)
    )

    st.divider()

    # ---------------- BRING THE GROUP BACK ----------------
    if is_leading_trip:
        tag = info["other_reason"].strip()
        purpose = info["reason"]
        big_banner(f"BRINGING BACK {leader.upper()}'S GROUP", f"Signed out for {purpose}", "in")
        if st.button("Sign This Group Back In", key="group_bring_back", use_container_width=True):
            freed = signin_everyone_in_group(tag)
            notify_vans("Bauercrest: Group IN", f"{leader}'s group ({purpose}) is back: {len(freed)} signed in.")
            st.session_state["group_form_nonce"] += 1
            st.session_state.pop("group_leader", None)
            st.session_state["group_flash"] = (
                f"{len(freed)} signed back in: {', '.join(freed)}" if freed
                else "Nobody was still out under that trip."
            )
            st.rerun()

    # ---------------- TAKE THE GROUP OUT ----------------
    else:
        big_banner(f"{leader.upper()} TAKING A GROUP OUT", "Pick who's going, then confirm", "out")
        status_map = get_status_map_fresh()
        in_names = sorted(
            n for n in staff_names
            if n != leader and status_map.get(n, {}).get("status") != "OUT"
        )
        with st.form(f"group_take_form_{g_nonce}", clear_on_submit=False):
            purpose = st.selectbox("Purpose", GROUP_PURPOSES)
            other_purpose = ""
            if purpose == "Other":
                other_purpose = st.text_input("Other purpose (required)")
            party = st.multiselect("Who else is going (besides you)?", in_names)
            take_go = st.form_submit_button(
                f"Sign Out Group ({len(party) + 1} incl. you)", use_container_width=True
            )

        if take_go:
            if purpose == "Other" and not other_purpose.strip():
                st.error("Please enter the other purpose.")
            elif not party:
                st.error("Pick at least one other person. For just yourself, use the main Sign In / Out page.")
            else:
                full_party = [leader] + party
                trip_tag = f"{GROUP_SIGNOUT_TAG}|{uuid.uuid4().hex[:8]}"
                signed = auto_signout_for_group(full_party, purpose, other_purpose.strip(), trip_tag)
                ptext = other_purpose.strip() if (purpose == "Other" and other_purpose.strip()) else purpose
                notify_vans("Bauercrest: Group OUT", f"{leader} took {len(signed)} out: {ptext}")
                st.session_state["group_form_nonce"] += 1
                st.session_state.pop("group_leader", None)
                st.session_state["group_flash"] = (
                    f"{len(signed)} signed out for {ptext}: {', '.join(signed)}" if signed
                    else "Everyone selected was already signed out."
                )
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
    # CAMPWIDE EMERGENCY: the most urgent control on this page, so it sits
    # above everything else here.
    # -------------------------------------------------
    section_title("Campwide Emergency")

    em_flash = st.session_state.pop("emergency_flash", "")
    if em_flash:
        st.success(em_flash)
    em_err = st.session_state.pop("emergency_error", "")
    if em_err:
        st.error(em_err)

    if is_emergency_active():
        active_msg = get_emergency_message()
        st.error("EMERGENCY IS ACTIVE" + (f" — {active_msg}" if active_msg else ""))
        st.caption("Every page (Sign In/Out, Who's Out, Vans, Admin) is showing this banner right now.")
        with st.form("emergency_clear_form", clear_on_submit=True):
            em_clear_code = st.text_input("Your admin code", type="password", max_chars=4, key="emergency_clear_code")
            em_clear_go = st.form_submit_button("Clear Emergency")
        if em_clear_go:
            admin_name, err = resolve_admin_code(em_clear_code, staff_pins)
            if err:
                st.session_state["emergency_error"] = err
            else:
                set_emergency(False)
                notify_phone("Bauercrest: Emergency CLEARED", f"Cleared by {admin_name}.")
                st.session_state["emergency_flash"] = f"Emergency cleared by {admin_name}."
            st.rerun()
    else:
        st.caption(
            "Puts a red banner on every page, campwide, until an admin clears it. "
            "Use for a real emergency only — this is deliberately hard to miss."
        )
        with st.form("emergency_declare_form", clear_on_submit=True):
            em_msg = st.text_input(
                "Message shown to everyone (optional)",
                key="emergency_declare_msg",
                placeholder="e.g. Meet at the flagpole",
            )
            em_declare_code = st.text_input("Your admin code", type="password", max_chars=4, key="emergency_declare_code")
            em_confirm = st.checkbox("I understand this puts a red banner on every screen, campwide.")
            em_go = st.form_submit_button("DECLARE EMERGENCY")
        if em_go:
            admin_name, err = resolve_admin_code(em_declare_code, staff_pins)
            if err:
                st.session_state["emergency_error"] = err
            elif not em_confirm:
                st.session_state["emergency_error"] = "Tick the confirm box first."
            else:
                set_emergency(True, em_msg.strip())
                detail = f" {em_msg.strip()}" if em_msg.strip() else ""
                notify_phone("Bauercrest: EMERGENCY DECLARED", f"By {admin_name}.{detail}")
                st.session_state["emergency_flash"] = f"Emergency declared by {admin_name}."
            st.rerun()

    st.markdown("---")

    # -------------------------------------------------
    # ADMIN SIGN-IN: fix the board when someone forgot
    # -------------------------------------------------
    admin_flash = st.session_state.pop("admin_flash", "")
    if admin_flash:
        st.success(admin_flash)

    section_title("Sign Someone Back In")
    st.caption("For when a staff member forgot to sign in. Pick the person, type your admin code, and sign them in.")

    df_out_now = get_currently_out(load_current_status_df_cached())
    out_names_now = sorted(df_out_now["name"].tolist())

    if not out_names_now:
        empty_note("No staff are currently signed out.")
    else:
        with st.form("admin_signin_form", clear_on_submit=True):
            who = st.selectbox("Who is signed out?", out_names_now)
            admin_code = st.text_input("Your admin code", type="password", max_chars=4)
            do_signin = st.form_submit_button("Sign This Person In")

        if do_signin:
            admin_name, err = resolve_admin_code(admin_code, staff_pins)
            if err:
                st.error(err)
            else:
                row = df_out_now[df_out_now["name"] == who].iloc[0]
                # An admin fixing the board must still record that they were
                # late, recomputed from the current reason.
                mins = row_minutes_late(row)
                late_note = f"LATE {mins} min" if mins > 0 else ""
                append_log_row(
                    who,
                    row["reason"],
                    f"{ADMIN_SIGNIN_TAG}|{admin_name}",
                    action="IN",
                    status="IN",
                    late=late_note,
                )
                extra = f" LATE by {mins} min." if mins > 0 else ""
                st.session_state["admin_flash"] = f"{who} signed in by {admin_name}.{extra}"
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
            van_admin_code = st.text_input("Your admin code", type="password", max_chars=4, key="admin_van_code")
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

                # Free everyone stranded under this van's tag, read live.
                try:
                    signin_everyone_on_van(which_van)
                except Exception:
                    pass

                notify_vans("Bauercrest: Van IN", f"{van_label(which_van)} signed in by admin {admin_name}")
                st.session_state["admin_flash"] = f"{van_label(which_van)} signed in by {admin_name}."
                st.rerun()

    st.markdown("---")

    # -------------------------------------------------
    # ARCHIVE: keep the live tab small, keep history forever
    # -------------------------------------------------
    section_title("Archive Old Logs")
    st.caption(
        "Moves old, closed-out sign-outs to a separate logs_archive tab so the "
        "app stays fast. Everyone still signed out is kept, plus the last "
        f"{ARCHIVE_KEEP_DAYS} days. Nothing is ever deleted, only moved. This "
        f"now also runs automatically every night around {AUTO_ARCHIVE_HOUR}:00 "
        "AM — use the button below only if you want it to run right now."
    )

    arch_flash = st.session_state.pop("archive_flash", "")
    if arch_flash:
        st.success(arch_flash)
    arch_err = st.session_state.pop("archive_error", "")
    if arch_err:
        st.error(arch_err)

    with st.form("archive_form", clear_on_submit=True):
        st.caption("Type your admin code and confirm to archive.")
        arch_code = st.text_input("Your admin code", type="password", max_chars=4, key="archive_admin_code")
        arch_confirm = st.checkbox("I understand old closed-out rows will be moved to the archive tab.")
        arch_go = st.form_submit_button("Archive Old Logs Now")

    if arch_go:
        admin_name, err = resolve_admin_code(arch_code, staff_pins)
        if err:
            st.session_state["archive_error"] = err
            st.rerun()
        elif not arch_confirm:
            st.session_state["archive_error"] = "Tick the confirm box first."
            st.rerun()
        else:
            with st.spinner("Archiving. This can take a moment for thousands of rows."):
                res = archive_old_logs()
            if res["ok"]:
                notify_phone("Bauercrest: Logs archived", f"{admin_name} archived {res['archived']} rows.")
                st.session_state["archive_flash"] = res["message"]
            else:
                st.session_state["archive_error"] = res["message"]
            st.rerun()

    st.caption("Full 10-year record, live plus archive, in one file:")
    # Built only on demand. This reads BOTH tabs, which can be thousands of
    # rows, so doing it on every admin page load would make the page crawl.
    # The button prepares it, then the download appears.
    if st.button("Prepare full history file", key="prep_full_history"):
        try:
            with st.spinner("Gathering live and archived history..."):
                st.session_state["full_history_csv"] = build_full_history_csv()
        except Exception:
            st.session_state["full_history_csv"] = ""
            st.error("Could not build the full-history file right now. Try again in a moment.")

    full_csv = st.session_state.get("full_history_csv", "")
    if full_csv:
        st.download_button(
            "Download FULL History (live + archive) as CSV",
            data=full_csv,
            file_name="bauercrest_full_signout_history.csv",
            mime="text/csv",
            key="full_history_dl",
        )
    elif full_csv == "":
        st.caption("Press the button above to build the download.")

    st.markdown("---")

    # -------------------------------------------------
    # VAN ARCHIVE: same idea as the logs archive, for the vans tab
    # -------------------------------------------------
    section_title("Archive Old Van Logs")
    st.caption(
        "Moves old, closed-out van trips to a separate vans_archive tab, same "
        "idea as the logs archive above. Every van still out is kept, plus the "
        f"last {ARCHIVE_KEEP_DAYS} days. Nothing is ever deleted, only moved. "
        f"This also runs automatically every night around {AUTO_ARCHIVE_HOUR}:00 "
        "AM — use the button below only if you want it to run right now."
    )

    van_arch_flash = st.session_state.pop("van_archive_flash", "")
    if van_arch_flash:
        st.success(van_arch_flash)
    van_arch_err = st.session_state.pop("van_archive_error", "")
    if van_arch_err:
        st.error(van_arch_err)

    with st.form("van_archive_form", clear_on_submit=True):
        st.caption("Type your admin code and confirm to archive.")
        van_arch_code = st.text_input("Your admin code", type="password", max_chars=4, key="van_archive_admin_code")
        van_arch_confirm = st.checkbox("I understand old closed-out van rows will be moved to the archive tab.")
        van_arch_go = st.form_submit_button("Archive Old Van Logs Now")

    if van_arch_go:
        admin_name, err = resolve_admin_code(van_arch_code, staff_pins)
        if err:
            st.session_state["van_archive_error"] = err
            st.rerun()
        elif not van_arch_confirm:
            st.session_state["van_archive_error"] = "Tick the confirm box first."
            st.rerun()
        else:
            with st.spinner("Archiving van logs..."):
                van_res = archive_old_vans()
            if van_res["ok"]:
                notify_phone("Bauercrest: Van logs archived", f"{admin_name} archived {van_res['archived']} van rows.")
                st.session_state["van_archive_flash"] = van_res["message"]
            else:
                st.session_state["van_archive_error"] = van_res["message"]
            st.rerun()

    st.caption("Full van history, live plus archive, in one file:")
    if st.button("Prepare full van history file", key="prep_full_van_history"):
        try:
            with st.spinner("Gathering live and archived van history..."):
                st.session_state["full_van_history_csv"] = build_full_van_history_csv()
        except Exception:
            st.session_state["full_van_history_csv"] = ""
            st.error("Could not build the full van-history file right now. Try again in a moment.")

    full_van_csv = st.session_state.get("full_van_history_csv", "")
    if full_van_csv:
        st.download_button(
            "Download FULL Van History (live + archive) as CSV",
            data=full_van_csv,
            file_name="bauercrest_full_van_history.csv",
            mime="text/csv",
            key="full_van_history_dl",
        )
    elif full_van_csv == "":
        st.caption("Press the button above to build the download.")

    st.markdown("---")

    # -------------------------------------------------
    # STATUS CACHE: recovery tool for the current_status fast-path tab
    # -------------------------------------------------
    section_title("Rebuild Status Cache")
    st.caption(
        "The board and sign-in box read a small current_status tab instead of "
        "the full history, to stay fast. It updates itself on every sign-in/out "
        "automatically. Use this only if something looks wrong on the board "
        "after hand-editing rows directly in the logs sheet — it recomputes "
        "current_status from scratch by scanning the full history. Nothing in "
        "logs is ever touched by this."
    )
    if st.button("Rebuild Status Cache From History", key="rebuild_status_cache_btn"):
        try:
            with st.spinner("Recomputing current status from the full log history..."):
                clear_logs_cache()
                rebuild_current_status_from_logs()
            st.success("Status cache rebuilt from history.")
        except Exception:
            st.error("Could not rebuild the status cache right now. Please try again.")

    st.markdown("---")

    df_logs = load_logs_df_cached()

    section_title("Full Log History")
    if df_logs.empty:
        st.info("No logs recorded yet.")
    else:
        # Show only the most recent rows so this table stays fast no matter how
        # large the live tab is. The complete record is always available through
        # the full-history download and the archive tab.
        HISTORY_TABLE_LIMIT = 200

        # Filtering by name BEFORE the row cap, not after, is the whole point:
        # otherwise looking up one counselor's history means scrolling through
        # everyone else's most recent 200 rows hoping theirs are in there, or
        # downloading the CSV and searching it by hand.
        name_options = ["All"] + sorted(
            n for n in df_logs["name"].astype(str).str.strip().unique() if n
        )
        name_filter = st.selectbox("Filter by name", name_options, key="admin_history_name_filter")
        df_scope = df_logs if name_filter == "All" else df_logs[df_logs["name"] == name_filter]

        total_rows = len(df_scope)
        df_display = df_scope.copy()
        df_display["_ts"] = pd.to_datetime(df_display["timestamp"], errors="coerce", format="mixed")
        df_display = df_display.sort_values("_ts", na_position="first").drop(columns=["_ts"])
        if total_rows > HISTORY_TABLE_LIMIT:
            df_display = df_display.tail(HISTORY_TABLE_LIMIT)
            scope_label = "live" if name_filter == "All" else f"live {name_filter}"
            st.caption(f"Showing the most recent {HISTORY_TABLE_LIMIT} of {total_rows} {scope_label} rows. Use the downloads for the full record.")
        elif name_filter != "All":
            st.caption(f"{total_rows} row(s) for {name_filter}.")
        df_display["timestamp_str"] = df_display["timestamp"].apply(format_time)
        df_display = df_display.rename(columns={
            "id": "ID",
            "timestamp_str": "Time",
            "name": "Name",
            "reason": "Reason",
            "other_reason": "Other Details",
            "action": "Action",
            "status": "Status",
            "late": "Late",
        })
        cols_show = ["ID", "Time", "Name", "Reason", "Other Details", "Action", "Status", "Late"]
        for c in cols_show:
            if c not in df_display.columns:
                df_display[c] = ""
        df_display = df_display[cols_show]
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
def ensure_headers_once():
    """Fix the logs and vans header rows a single time per session.

    Headers used to be re-checked on every read and write, a network call each
    time. They only need checking once, so this runs at startup and then never
    again for the session. Wrapped and time-boxed so a slow Google response
    cannot freeze the kiosk on a blank screen; if it does not finish, the app
    still loads and the write paths ensure headers anyway.
    """
    if st.session_state.get("_headers_ensured"):
        return
    # Mark done FIRST. If a call below is slow and the user reloads, we do not
    # want to re-run this heavy startup step every time.
    st.session_state["_headers_ensured"] = True
    try:
        ensure_logs_header(get_worksheet(SHEET_LOGS))
        ensure_vans_header(get_vans_sheet())
        get_schedule_sheet()  # creates the schedule tab with camp defaults if missing
        get_current_status_sheet()  # creates + backfills current_status if missing
        get_log_headers.clear()
        get_van_headers.clear()
    except Exception:
        pass


def _main_body():
    st.set_page_config(
        page_title="Bauercrest Staff Sign-Out",
        page_icon="🏕️",
        layout="wide",
    )
    inject_css()
    ensure_headers_once()

    logo_path = Path("logo-header-2.png")
    if logo_path.exists():
        st.sidebar.image(str(logo_path), use_container_width=True)

    st.sidebar.title("Staff Sign-Out")
    st.sidebar.caption("Sign in and out with your 4-digit code.")

    # The idle timer sets this flag, then reruns. We apply it HERE, before the
    # page radio is created, because a widget's value cannot be changed after
    # the widget is drawn. Setting the radio's stored value first makes it come
    # up on the Sign In / Out page.
    if st.session_state.pop("force_home", False):
        st.session_state["main_page_radio"] = "Sign In / Out"
        # A kiosk that sat idle long enough to trigger this is a kiosk nobody
        # was actively using, so there is no "group still signing out"
        # convenience to protect. Reset it to a clean slate: default reason,
        # and a fresh code-box key in case someone walked away mid-type.
        st.session_state["signout_reason"] = REASONS[0]
        st.session_state["signio_nonce"] = st.session_state.get("signio_nonce", 0) + 1

    page = st.sidebar.radio(
        "Go to",
        ["Sign In / Out", "Who's Out", "Vans", "Group Sign-Out", "Admin / History"],
        key="main_page_radio",
    )

    st.sidebar.markdown("---")
    st.sidebar.caption("The Who's Out and Vans boards refresh on their own every minute.")

    staff_pins, staff_names, driver_names = get_staff_pins_and_lists()

    # Every real interaction reruns main(), so stamping here marks the last time
    # someone actually touched the kiosk. The idle watcher below compares
    # against this. Fragment-only refreshes do not run main(), so they do not
    # falsely count as activity.
    st.session_state["kiosk_active_at"] = datetime.now(TZ)

    render_emergency_banner()

    if page == "Sign In / Out":
        page_sign_in_out(staff_pins, staff_names)
    elif page == "Who's Out":
        page_whos_out()
    elif page == "Vans":
        page_vans(staff_pins, staff_names, driver_names)
    elif page == "Group Sign-Out":
        page_group_signout(staff_pins, staff_names)
    elif page == "Admin / History":
        page_admin_history(staff_pins)

    # Idle return-home watcher. Only on the counselor-facing display pages, so
    # the kiosk never sits parked on a board. Admin and the sign page are left
    # alone. Draws nothing.
    if page in ("Who's Out", "Vans"):
        @st.fragment(run_every=15)
        def idle_watch():
            try:
                last = st.session_state.get("kiosk_active_at")
                if last is None:
                    return
                if (datetime.now(TZ) - last).total_seconds() >= IDLE_RETURN_SECONDS:
                    st.session_state["force_home"] = True
                    st.session_state["van_selected"] = ""
                    st.rerun(scope="app")
            except Exception:
                pass

        idle_watch()

    # Late-alert heartbeat. Who's Out fires alerts from its own board fragment,
    # and the Sign page stays free of network work so typing is instant. That
    # leaves Vans, Group Sign-Out, and Admin, which have no board doing the
    # late check, so they get a light background tick that only fires
    # deadline alerts.
    if page in ("Vans", "Group Sign-Out", "Admin / History"):

        @st.fragment(run_every=BOARD_REFRESH_SECONDS)
        def heartbeat():
            try:
                out_now = get_currently_out(merge_recent_writes(load_current_status_df_cached()))
                check_late_and_alert(out_now)
                check_nightly_summary(out_now)
                check_auto_archive()
            except Exception:
                pass

        heartbeat()


def main():
    """Run the app behind a safety net.

    If anything unexpected throws, a counselor or an ACA visitor must never see
    a raw Python traceback. They see a calm message and a retry button, and the
    kiosk keeps working. set_page_config must run before this guard can draw,
    so the body owns it and we only catch what comes after.
    """
    try:
        _main_body()
    except Exception:
        try:
            st.markdown(
                "<div style='max-width:640px;margin:3rem auto;padding:1.6rem 1.8rem;"
                "border-radius:14px;background:#FBF3E4;border:2px solid #B07A1E;"
                "font-family:sans-serif;color:#6B4A0F;'>"
                "<div style='font-size:1.4rem;font-weight:800;margin-bottom:0.4rem;'>"
                "One moment, reconnecting</div>"
                "<div style='font-size:1rem;'>The sign-out board had a brief hiccup talking to "
                "its records. Tap the button to reload. Nobody's sign-in or sign-out was lost.</div>"
                "</div>",
                unsafe_allow_html=True,
            )
            if st.button("Reload", use_container_width=True):
                st.rerun()
        except Exception:
            pass


if __name__ == "__main__":
    main()
