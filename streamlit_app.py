import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz
import uuid

# ----------------------------
# Page config
# ----------------------------
st.set_page_config(page_title="Camp Bauercrest Sign Out", layout="wide")

TZ = pytz.timezone("America/New_York")
LOGO_PATH = "logo-header-2.png"  # adjust if needed (e.g., "assets/logo-header-2.png")

DEFAULT_STAFF_SHEET = "staff"
DEFAULT_VANS_SHEET = "vans"

VANS = ["Van 1", "Van 2", "Van 3"]
PURPOSE_OPTIONS = ["Period Off", "Night Off", "Day Off", "Other"]

VANS_REQUIRED_HEADERS = [
    "id", "timestamp", "van", "driver", "purpose", "passengers",
    "other_purpose", "action", "status"
]

# ----------------------------
# Safe header UI
# ----------------------------
def show_header():
    try:
        st.image(LOGO_PATH, width=220)
    except Exception:
        pass
    st.title("Camp Bauercrest Sign Out")

def now_iso():
    return datetime.now(TZ).isoformat(timespec="seconds")

def normalize_pin(pin) -> str:
    """
    Normalize any staff PIN / user input to a 4-digit string.
    Handles numbers, strings, and leading zeros.
    """
    s = str(pin).strip()
    if s.endswith(".0"):
        s = s[:-2]
    # remove accidental spaces
    s = s.replace(" ", "")
    return s.zfill(4)

# ----------------------------
# Google Sheets client (cached)
# ----------------------------
@st.cache_resource
def gs_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=scopes
    )
    return gspread.authorize(creds)

@st.cache_resource
def open_spreadsheet(spreadsheet_id: str):
    return gs_client().open_by_key(spreadsheet_id)

def get_or_create_ws(spreadsheet, title: str, required_headers: list[str]):
    """
    Ensures worksheet exists and contains required headers (adds missing headers to the end).
    Does NOT reorder existing columns.
    """
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows="1000", cols=str(max(12, len(required_headers) + 3)))
        ws.append_row(required_headers)
        return ws

    headers = ws.row_values(1)
    if not headers:
        ws.append_row(required_headers)
        return ws

    missing = [h for h in required_headers if h not in headers]
    if missing:
        new_headers = headers + missing
        ws.delete_rows(1)
        ws.insert_row(new_headers, 1)

    return ws

@st.cache_data(ttl=900)  # 15 minutes (kiosk-friendly)
def load_staff_cached(spreadsheet_id: str, staff_sheet_name: str) -> pd.DataFrame:
    ss = open_spreadsheet(spreadsheet_id)
    ws = get_or_create_ws(ss, staff_sheet_name, ["name", "pin", "active"])
    df = pd.DataFrame(ws.get_all_records())

    if df.empty:
        return df

    df["name"] = df["name"].astype(str).str.strip()
    df["pin"] = df["pin"].apply(normalize_pin)

    active = df["active"].astype(str).str.strip().str.lower()
    df = df[active.isin(["true", "yes", "1"])]

    return df

@st.cache_data(ttl=10)  # 10 seconds (prevents quota blowups, still feels live)
def load_vans_cached(spreadsheet_id: str, vans_sheet_name: str) -> pd.DataFrame:
    ss = open_spreadsheet(spreadsheet_id)
    ws = get_or_create_ws(ss, vans_sheet_name, VANS_REQUIRED_HEADERS)
    return pd.DataFrame(ws.get_all_records())

@st.cache_data(ttl=300)
def vans_headers_cached(spreadsheet_id: str, vans_sheet_name: str) -> list[str]:
    ss = open_spreadsheet(spreadsheet_id)
    ws = get_or_create_ws(ss, vans_sheet_name, VANS_REQUIRED_HEADERS)
    return [h.strip() for h in ws.row_values(1) if str(h).strip()]

def append_row_aligned(spreadsheet_id: str, vans_sheet_name: str, row_dict: dict):
    """
    Write one row aligned to the worksheet's actual header order.
    Prevents column drift permanently.
    """
    ss = open_spreadsheet(spreadsheet_id)
    ws = get_or_create_ws(ss, vans_sheet_name, VANS_REQUIRED_HEADERS)
    headers = vans_headers_cached(spreadsheet_id, vans_sheet_name)
    row = [row_dict.get(h, "") for h in headers]
    ws.append_row(row)

    # Clear caches so UI updates right after a write
    load_vans_cached.clear()
    vans_headers_cached.clear()

# ----------------------------
# Vans status logic
# ----------------------------
def compute_van_status(vans_df: pd.DataFrame) -> dict:
    status_map = {v: {"status": "IN"} for v in VANS}
    if vans_df is None or vans_df.empty:
        return status_map

    for col in ["timestamp", "van", "status", "driver", "purpose", "passengers", "other_purpose", "action"]:
        if col not in vans_df.columns:
            vans_df[col] = ""

    tmp = vans_df.copy()
    tmp["timestamp"] = pd.to_datetime(tmp["timestamp"], errors="coerce")
    tmp = tmp.sort_values("timestamp")

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
            "action": str(last.get("action", "")).strip(),
        }

    return status_map

def next_available_van(status_map: dict) -> str | None:
    for v in VANS:
        if status_map.get(v, {}).get("status") != "OUT":
            return v
    return None

# ----------------------------
# Vans page (driver code only; passengers names only)
# ----------------------------
def vans_page():
    # Secrets check
    if "SPREADSHEET_ID" not in st.secrets:
        st.error("Missing SPREADSHEET_ID in Streamlit secrets.")
        return
    if "gcp_service_account" not in st.secrets:
        st.error("Missing gcp_service_account in Streamlit secrets.")
        return

    spreadsheet_id = st.secrets["SPREADSHEET_ID"]
    staff_sheet_name = st.secrets.get("STAFF_SHEET_NAME", DEFAULT_STAFF_SHEET)
    vans_sheet_name = st.secrets.get("VANS_SHEET_NAME", DEFAULT_VANS_SHEET)

    st.header("Van Sign Out")
    st.caption("Driver enters their 4-digit code. Passengers are listed by name (no passenger codes).")

    staff_df = load_staff_cached(spreadsheet_id, staff_sheet_name)
    if staff_df.empty:
        st.error("Staff sheet is empty or not readable. Check the 'staff' tab and service account access.")
        return

    pin_map = dict(zip(staff_df["name"], staff_df["pin"]))
    names = sorted(pin_map.keys())

    vans_df = load_vans_cached(spreadsheet_id, vans_sheet_name)
    status_map = compute_van_status(vans_df)

    out_vans = [v for v in VANS if status_map.get(v, {}).get("status") == "OUT"]
    available = next_available_van(status_map)

    flash = st.session_state.pop("van_flash", "")
    if flash:
        st.success(flash)

    st.subheader("Vans Out Right Now")
    if not out_vans:
        st.info("All vans are currently in.")
    else:
        for v in out_vans:
            info = status_map[v]
            purpose = info.get("purpose", "")
            if purpose == "Other" and info.get("other_purpose"):
                purpose = f"Other: {info.get('other_purpose')}"
            st.write(
                f"**{v}** — Driver: **{info.get('driver','')}** | "
                f"Purpose: **{purpose}** | Passengers: {info.get('passengers','')}"
            )

    st.divider()

    # SIGN OUT (Form = fewer reruns and fewer API reads)
    st.subheader("Sign Out a Van")
    if available is None:
        st.warning("No vans available. All three vans are currently out.")
    else:
        st.info(f"Next available: **{available}**")

        with st.form("van_signout_form", clear_on_submit=True):
            c1, c2 = st.columns([2, 1])
            with c1:
                driver = st.selectbox("Driver", options=names)
            with c2:
                driver_code = st.text_input("Driver 4-digit code", type="password")

            purpose = st.selectbox("Purpose", PURPOSE_OPTIONS)
            other_purpose = ""
            if purpose == "Other":
                other_purpose = st.text_input("Other purpose (required)")

            passengers = st.multiselect(
                "Passengers (select everyone riding with the driver)",
                options=[n for n in names if n != driver],
            )

            submitted = st.form_submit_button("Sign Out Van", use_container_width=True)

        if submitted:
            if normalize_pin(driver_code) != pin_map.get(driver, "----"):
                st.error("Wrong driver code.")
                return
            if purpose == "Other" and not other_purpose.strip():
                st.error("Please enter the other purpose.")
                return

            # Pull last known checkout details for this van so passengers appear on CHECKIN too
last_passengers = ""
last_purpose = ""
last_other_purpose = ""

try:
    tmp = vans_df.copy()
    tmp["timestamp"] = pd.to_datetime(tmp["timestamp"], errors="coerce")
    tmp = tmp.sort_values("timestamp")
    van_rows = tmp[tmp["van"] == van_to_in]
    if not van_rows.empty:
        # Look for the most recent OUT row (preferred)
        out_rows = van_rows[van_rows["status"].astype(str).str.upper() == "OUT"]
        src = out_rows.iloc[-1] if not out_rows.empty else van_rows.iloc[-1]
        last_passengers = str(src.get("passengers", "")).strip()
        last_purpose = str(src.get("purpose", "")).strip()
        last_other_purpose = str(src.get("other_purpose", "")).strip()
except Exception:
    pass

row = {
    "id": str(uuid.uuid4())[:8],
    "timestamp": now_iso(),
    "van": van_to_in,
    "driver": return_driver,
    "purpose": last_purpose,
    "passengers": last_passengers,
    "other_purpose": last_other_purpose,
    "action": "CHECKIN",
    "status": "IN",
}

    append_row_aligned(spreadsheet_id, vans_sheet_name, row)
    st.session_state["van_flash"] = f"{available} signed out under {driver}."
    st.rerun()

    # SIGN IN (only if a van is out)
    if out_vans:
        st.divider()
        st.subheader("Sign In a Van")

        van_to_in = out_vans[0] if len(out_vans) == 1 else st.selectbox("Which van is returning?", out_vans)

        with st.form("van_signin_form", clear_on_submit=True):
            c1, c2 = st.columns([2, 1])
            with c1:
                return_driver = st.selectbox("Driver returning the van", options=names)
            with c2:
                return_driver_code = st.text_input("Driver 4-digit code", type="password")

            submitted_in = st.form_submit_button("Sign In Van", use_container_width=True)

        if submitted_in:
            if normalize_pin(return_driver_code) != pin_map.get(return_driver, "----"):
                st.error("Wrong driver code.")
                return

            row = {
                "id": str(uuid.uuid4())[:8],
                "timestamp": now_iso(),
                "van": van_to_in,
                "driver": return_driver,
                "purpose": "",
                "passengers": "",
                "other_purpose": "",
                "action": "CHECKIN",
                "status": "IN",
            }
            append_row_aligned(spreadsheet_id, vans_sheet_name, row)
            st.session_state["van_flash"] = f"{van_to_in} signed back in under {return_driver}."
            st.rerun()

# ----------------------------
# Main app (so it actually loads)
# ----------------------------
def main():
    show_header()

    page = st.sidebar.radio(
        "Navigation",
        ["Sign In / Out", "Who’s Out", "Vans", "Admin / History"],
        index=2
    )

    if page == "Vans":
        vans_page()
    else:
        st.info("Vans is fixed + quota-safe now. Next step is merging this into your old pages.")
        st.write("Pick **Vans** from the sidebar to test.")

if __name__ == "__main__":
    main()
