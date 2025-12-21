import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz
import uuid

# ----------------------------
# Config
# ----------------------------
st.set_page_config(page_title="Camp Bauercrest Sign Out", layout="wide")

TZ = pytz.timezone("America/New_York")

LOGO_PATH = "logo-header-2.png"  # keep in repo root (or change path)

# Sheets
DEFAULT_LOGS_SHEET = "logs"
DEFAULT_STAFF_SHEET = "staff"
DEFAULT_VANS_SHEET = "vans"

VANS = ["Van 1", "Van 2", "Van 3"]

PURPOSE_OPTIONS = ["Period Off", "Night Off", "Day Off", "Other"]

# Vans sheet headers this app will create if missing.
# If your sheet already exists with more columns, this app will still work.
VANS_REQUIRED_HEADERS = [
    "id", "timestamp", "van", "driver", "purpose", "passengers",
    "other_purpose", "action", "status"
]

# ----------------------------
# Helpers: Google Sheets
# ----------------------------
@st.cache_resource
def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds_info = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def get_spreadsheet():
    client = get_gspread_client()
    spreadsheet_id = st.secrets.get("SPREADSHEET_ID", "")
    if not spreadsheet_id:
        st.error("Missing SPREADSHEET_ID in Streamlit secrets.")
        st.stop()
    return client.open_by_key(spreadsheet_id)

def get_or_create_worksheet(spreadsheet, title: str, headers: list[str]):
    """Create worksheet if missing. If exists, ensure header row contains at least the required headers."""
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows="1000", cols=str(max(10, len(headers) + 5)))
        ws.append_row(headers)
        return ws

    # Ensure header row exists
    existing_headers = ws.row_values(1)
    if not existing_headers:
        ws.append_row(headers)
        return ws

    # If required headers missing, add them to the end (do not reorder user columns)
    missing = [h for h in headers if h not in existing_headers]
    if missing:
        new_headers = existing_headers + missing
        ws.delete_rows(1)
        ws.insert_row(new_headers, 1)
    return ws

def sheet_headers(ws) -> list[str]:
    headers = ws.row_values(1)
    return [h.strip() for h in headers if h.strip()]

def append_row_by_headers(ws, row_dict: dict):
    """
    Appends a row aligned to the worksheet's header row.
    Prevents column drift permanently.
    """
    headers = sheet_headers(ws)
    if not headers:
        raise ValueError("Worksheet has no header row.")

    row = []
    for h in headers:
        row.append(row_dict.get(h, ""))

    ws.append_row(row)

def load_logs_df(spreadsheet):
    logs_name = st.secrets.get("SHEET_NAME", DEFAULT_LOGS_SHEET)
    ws = get_or_create_worksheet(spreadsheet, logs_name, [
        "id", "timestamp", "name", "reason", "other_reason", "action", "status"
    ])
    records = ws.get_all_records()
    return pd.DataFrame(records)

def load_staff(spreadsheet) -> pd.DataFrame:
    staff_ws_name = st.secrets.get("STAFF_SHEET_NAME", DEFAULT_STAFF_SHEET)
    ws = get_or_create_worksheet(spreadsheet, staff_ws_name, ["name", "pin", "active"])
    df = pd.DataFrame(ws.get_all_records())
    if df.empty:
        return df

    # Normalize
    df["name"] = df["name"].astype(str).str.strip()

    # Normalize pins: treat as 4-digit strings (handles leading zeros)
    df["pin"] = df["pin"].astype(str).str.strip()
    df["pin"] = df["pin"].str.replace(".0", "", regex=False)  # if read as float-looking
    df["pin"] = df["pin"].str.zfill(4)

    if "active" in df.columns:
        active = df["active"].astype(str).str.strip().str.lower()
        df = df[active.isin(["true", "yes", "1"])]
    return df

def get_staff_pin_map(staff_df: pd.DataFrame) -> dict[str, str]:
    if staff_df.empty:
        return {}
    return dict(zip(staff_df["name"], staff_df["pin"]))

def now_iso():
    return datetime.now(TZ).isoformat(timespec="seconds")

# ----------------------------
# UI helpers
# ----------------------------
def show_header():
    # Logo + Title
    try:
        st.image(LOGO_PATH, width=220)
    except Exception:
        pass
    st.title("Camp Bauercrest Sign Out")

def require_driver_auth(name: str, pin_input: str, pin_map: dict[str, str]) -> bool:
    # Normalize user input to 4-digit
    pin_input = str(pin_input).strip().zfill(4)
    expected = pin_map.get(name)
    return expected is not None and expected == pin_input

# ----------------------------
# Vans logic
# ----------------------------
def load_vans_df(spreadsheet) -> tuple[pd.DataFrame, gspread.Worksheet]:
    vans_ws_name = st.secrets.get("VANS_SHEET_NAME", DEFAULT_VANS_SHEET)
    ws = get_or_create_worksheet(spreadsheet, vans_ws_name, VANS_REQUIRED_HEADERS)
    df = pd.DataFrame(ws.get_all_records())
    return df, ws

def compute_van_status(vans_df: pd.DataFrame) -> dict[str, dict]:
    """
    Returns status per van based on latest record.
    Output: { "Van 1": {"status":"IN/OUT", "driver":..., "timestamp":..., "purpose":..., "passengers":...}, ... }
    """
    status_map = {v: {"status": "IN"} for v in VANS}
    if vans_df.empty:
        return status_map

    # Normalize expected columns if missing
    for col in ["timestamp", "van", "status", "driver", "purpose", "passengers", "other_purpose", "action"]:
        if col not in vans_df.columns:
            vans_df[col] = ""

    # Keep original timestamp logic (you asked not to change it)
    # But ensure sort doesn't crash if timestamp empty
    tmp = vans_df.copy()
    tmp["timestamp"] = pd.to_datetime(tmp["timestamp"], errors="coerce")
    tmp = tmp.sort_values("timestamp")

    for v in VANS:
        rows = tmp[tmp["van"] == v]
        if rows.empty:
            continue
        last = rows.iloc[-1]
        st_val = str(last.get("status", "")).strip().upper()
        if st_val not in ["IN", "OUT"]:
            # If status is malformed, assume IN (safer for UI), but still show last entry info
            st_val = "IN"
        status_map[v] = {
            "status": st_val,
            "driver": str(last.get("driver", "")).strip(),
            "timestamp": last.get("timestamp"),
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

def vans_page(spreadsheet, staff_df: pd.DataFrame):
    st.header("Van Sign Out")

    pin_map = get_staff_pin_map(staff_df)
    names = sorted(pin_map.keys())

    vans_df, vans_ws = load_vans_df(spreadsheet)
    status_map = compute_van_status(vans_df)

    out_vans = [v for v in VANS if status_map.get(v, {}).get("status") == "OUT"]
    available = next_available_van(status_map)

    # Flash success message after rerun
    if st.session_state.get("van_flash"):
        st.success(st.session_state["van_flash"])
        st.session_state["van_flash"] = ""

    st.caption("Driver must enter their own 4-digit code. Passengers are listed by name (no codes).")

    st.subheader("Vans Out Right Now")
    if not out_vans:
        st.info("All vans are currently in.")
    else:
        for v in out_vans:
            info = status_map[v]
            purpose = info.get("purpose", "")
            if purpose == "Other" and info.get("other_purpose"):
                purpose = f'Other: {info.get("other_purpose")}'
            st.write(
                f"**{v}** — Driver: **{info.get('driver','')}** | Purpose: **{purpose}** | "
                f"Passengers: {info.get('passengers','')}"
            )

    st.divider()

    # ----------------------------
    # SIGN OUT (always visible)
    # ----------------------------
    st.subheader("Sign Out a Van")
    if available is None:
        st.warning("No vans available. All three vans are currently out.")
    else:
        st.info(f"Next available: **{available}**")

        col1, col2 = st.columns([2, 1])
        with col1:
            driver = st.selectbox("Driver", options=names, key="van_driver")
        with col2:
            driver_code = st.text_input("Driver 4-digit code", type="password", key="van_driver_code")

        purpose = st.selectbox("Purpose", PURPOSE_OPTIONS, key="van_purpose")
        other_purpose = ""
        if purpose == "Other":
            other_purpose = st.text_input("Other purpose (required)", key="van_other_purpose")

        passengers = st.multiselect(
            "Passengers (select everyone riding with the driver)",
            options=[n for n in names if n != driver],
            key="van_passengers",
        )

        signout_clicked = st.button("Sign Out Van", use_container_width=True)

        if signout_clicked:
            # Validate
            if not require_driver_auth(driver, driver_code, pin_map):
                st.error("Wrong driver code.")
                st.stop()
            if purpose == "Other" and not other_purpose.strip():
                st.error("Please enter the other purpose.")
                st.stop()

            row = {
                "id": str(uuid.uuid4())[:8],
                "timestamp": now_iso(),
                "van": available,
                "driver": driver,
                "purpose": purpose,
                "passengers": ", ".join(passengers),
                "other_purpose": other_purpose.strip(),
                "action": "CHECKOUT",
                "status": "OUT",
            }

            # Append aligned to actual headers (prevents column shifting)
            append_row_by_headers(vans_ws, row)

            st.session_state["van_flash"] = f"{available} signed out under {driver}."
            st.rerun()

    # ----------------------------
    # SIGN IN (only if something is out)
    # ----------------------------
    if out_vans:
        st.divider()
        st.subheader("Sign In a Van")

        # If only one van is out, no selection required.
        if len(out_vans) == 1:
            van_to_in = out_vans[0]
            st.info(f"Signing in: **{van_to_in}**")
        else:
            van_to_in = st.selectbox("Which van is returning?", options=out_vans, key="van_to_in")

        col1, col2 = st.columns([2, 1])
        with col1:
            return_driver = st.selectbox("Driver returning the van", options=names, key="van_return_driver")
        with col2:
            return_driver_code = st.text_input("Driver 4-digit code", type="password", key="van_return_driver_code")

        signin_clicked = st.button("Sign In Van", use_container_width=True)

        if signin_clicked:
            if not require_driver_auth(return_driver, return_driver_code, pin_map):
                st.error("Wrong driver code.")
                st.stop()

            # Preserve previous purpose/passengers info in log? Not needed for checkin.
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
            append_row_by_headers(vans_ws, row)

            st.session_state["van_flash"] = f"{van_to_in} signed back in under {return_driver}."
            st.rerun()

# ----------------------------
# Main Nav (keep your existing pages simple)
# ----------------------------
def main():
    spreadsheet = get_spreadsheet()
    staff_df = load_staff(spreadsheet)

    show_header()

    page = st.sidebar.radio(
        "Navigation",
        ["Sign In / Out", "Who’s Out", "Vans", "Admin / History"],
        index=0
    )

    # NOTE: I’m keeping the other pages minimal here; you can paste your existing logic
    # for Sign In/Out, Who’s Out, Admin/History below if this file is replacing your whole app.
    # If your existing app already has those pages working, copy those sections into this file.

    if page == "Vans":
        vans_page(spreadsheet, staff_df)
    else:
        st.info("This file includes the fixed Vans page. Paste your existing non-van pages here (Sign In/Out, Who’s Out, Admin/History).")

if __name__ == "__main__":
    main()
