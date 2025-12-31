import streamlit as st
import pandas as pd
from datetime import datetime
from pathlib import Path
import pytz
import uuid

import gspread
from gspread.exceptions import APIError, GSpreadException
from google.oauth2.service_account import Credentials

# -------------------------------------------------
# PAGE CONFIG (must be called once, at top-level)
# -------------------------------------------------
st.set_page_config(page_title="Bauercrest Staff Sign-Out", layout="wide")

# -------------------------------------------------
# CONFIG / CONSTANTS
# -------------------------------------------------
EASTERN = pytz.timezone("US/Eastern")

REASONS = ["Day Off", "Period Off", "Night Off", "Other (type reason)"]
VANS = ["Van 1", "Van 2", "Van 3"]
VAN_PURPOSES = ["Period Off", "Night Off", "Day Off", "Other"]

LOGS_HEADERS = ["id", "timestamp", "name", "reason", "other_reason", "action", "status"]
VANS_HEADERS_REQUIRED = ["id", "timestamp", "van", "driver", "purpose", "passengers", "other_purpose", "action", "status"]
STAFF_HEADERS_REQUIRED = ["name", "pin", "active"]

# Read config from Streamlit secrets (preferred), with safe fallbacks
SPREADSHEET_ID = st.secrets.get("SPREADSHEET_ID", "")  # required
SHEET_NAME = st.secrets.get("SHEET_NAME", "logs")
VANS_SHEET_NAME = st.secrets.get("VANS_SHEET_NAME", "vans")
STAFF_SHEET_NAME = st.secrets.get("STAFF_SHEET_NAME", "staff")
ADMIN_PASSWORD = st.secrets.get("ADMIN_PASSWORD", "")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# -------------------------------------------------
# UTIL
# -------------------------------------------------
def normalize_pin(pin) -> str:
    """Normalize any PIN/user input to a 4-digit string (handles leading zeros)."""
    s = str(pin).strip().replace(" ", "")
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(4)

def now_iso() -> str:
    return datetime.now(EASTERN).isoformat(timespec="seconds")

def safe_stop(msg: str):
    st.error(msg)
    st.stop()

# -------------------------------------------------
# GOOGLE SHEETS HELPERS (quota-safe)
# -------------------------------------------------
@st.cache_resource
def get_gspread_client():
    """Authorize and cache the gspread client."""
    if "gcp_service_account" not in st.secrets:
        safe_stop("Missing Streamlit secret: gcp_service_account")
    creds_info = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(creds)

@st.cache_resource
def get_spreadsheet():
    """Open and cache the spreadsheet object."""
    if not SPREADSHEET_ID:
        safe_stop("Missing Streamlit secret: SPREADSHEET_ID")
    client = get_gspread_client()
    return client.open_by_key(SPREADSHEET_ID)

def get_worksheet(name: str):
    ss = get_spreadsheet()
    return ss.worksheet(name)

def ensure_header(sheet, required_headers):
    """Ensure header row exists and contains required headers (adds missing to end)."""
    try:
        headers = sheet.row_values(1)
        if not headers:
            sheet.insert_row(required_headers, 1)
            return
        missing = [h for h in required_headers if h not in headers]
        if missing:
            new_headers = headers + missing
            sheet.delete_rows(1)
            sheet.insert_row(new_headers, 1)
    except (APIError, GSpreadException) as e:
        safe_stop(f"Google Sheets header error: {e}")

# -------------------
# STAFF (from sheet)
# -------------------
@st.cache_data(ttl=900)  # 15 min; kiosk-friendly
def load_staff_df_cached() -> pd.DataFrame:
    """Load staff (name, pin, active) from staff sheet."""
    try:
        sheet = get_worksheet(STAFF_SHEET_NAME)
        ensure_header(sheet, STAFF_HEADERS_REQUIRED)
        df = pd.DataFrame(sheet.get_all_records())
    except Exception as e:
        safe_stop(f"Could not read staff sheet: {e}")

    if df.empty:
        return pd.DataFrame(columns=["name", "pin", "active"])

    df["name"] = df.get("name", "").astype(str).str.strip()
    df["pin"] = df.get("pin", "").apply(normalize_pin)

    active = df.get("active", True).astype(str).str.strip().str.lower()
    df = df[active.isin(["true", "yes", "1"])].copy()

    # Drop blanks
    df = df[df["name"] != ""]
    return df

def get_pin_map_and_names():
    staff_df = load_staff_df_cached()
    pin_map = dict(zip(staff_df["name"], staff_df["pin"]))
    names = sorted(pin_map.keys())
    return pin_map, names

# -------------------
# LOGS (people)
# -------------------
def empty_logs_df():
    return pd.DataFrame(columns=LOGS_HEADERS)

@st.cache_data(ttl=5)  # fast board, low quota
def load_logs_df_cached() -> pd.DataFrame:
    """Load logs into a DataFrame (cached)."""
    try:
        sheet = get_worksheet(SHEET_NAME)
        ensure_header(sheet, LOGS_HEADERS)
        records = sheet.get_all_records()
    except APIError:
        # Temporary quota / API issue -> return empty, but don't crash kiosk UI
        return empty_logs_df()
    except Exception:
        return empty_logs_df()

    if not records:
        return empty_logs_df()

    df = pd.DataFrame(records)

    # Coerce types safely
    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    else:
        df["id"] = pd.Series(dtype="Int64")

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    else:
        df["timestamp"] = pd.NaT

    # Ensure required columns exist
    for c in LOGS_HEADERS:
        if c not in df.columns:
            df[c] = ""
    return df[LOGS_HEADERS]

def clear_logs_cache():
    load_logs_df_cached.clear()

def append_log_row(name: str, reason: str, other_reason: str, action: str, status: str):
    """Append a new people log row, then clear cache."""
    try:
        sheet = get_worksheet(SHEET_NAME)
        ensure_header(sheet, LOGS_HEADERS)

        df = load_logs_df_cached()
        next_id = 1 if df.empty or df["id"].isna().all() else int(df["id"].max()) + 1
        row = [next_id, now_iso(), name, reason, other_reason, action, status]
        sheet.append_row(row)
        clear_logs_cache()
    except (APIError, GSpreadException) as e:
        safe_stop(f"Could not record log due to Google Sheets error: {e}")

def clear_all_logs():
    try:
        sheet = get_worksheet(SHEET_NAME)
        sheet.clear()
        ensure_header(sheet, LOGS_HEADERS)
        clear_logs_cache()
    except (APIError, GSpreadException) as e:
        safe_stop(f"Could not clear logs: {e}")

def delete_logs_by_ids(ids_to_delete):
    try:
        sheet = get_worksheet(SHEET_NAME)
        df = load_logs_df_cached()
    except Exception as e:
        safe_stop(f"Could not load logs to delete: {e}")

    if df.empty:
        return

    df_keep = df[~df["id"].isin(ids_to_delete)].copy()

    try:
        sheet.clear()
        ensure_header(sheet, LOGS_HEADERS)
        if not df_keep.empty:
            df_out = df_keep.copy()
            df_out["timestamp"] = df_out["timestamp"].astype(str)
            rows = df_out[LOGS_HEADERS].values.tolist()
            sheet.append_rows(rows)
        clear_logs_cache()
    except (APIError, GSpreadException) as e:
        safe_stop(f"Could not delete selected entries: {e}")

# -------------------
# LOGIC HELPERS
# -------------------
def get_currently_out(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["name", "reason", "other_reason", "timestamp"])
    df_sorted = df.sort_values("timestamp")
    last_actions = df_sorted.groupby("name").tail(1)
    out_rows = last_actions[last_actions["status"].astype(str).str.upper() == "OUT"].copy()
    if out_rows.empty:
        return pd.DataFrame(columns=["name", "reason", "other_reason", "timestamp"])
    return out_rows[["name", "reason", "other_reason", "timestamp"]]

def format_time(dt):
    if pd.isna(dt):
        return ""
    try:
        return dt.strftime("%Y-%m-%d %I:%M %p")
    except Exception:
        return str(dt)

# -------------------
# VANS
# -------------------
@st.cache_data(ttl=5)
def load_vans_df_cached() -> pd.DataFrame:
    try:
        sheet = get_worksheet(VANS_SHEET_NAME)
        ensure_header(sheet, VANS_HEADERS_REQUIRED)
        df = pd.DataFrame(sheet.get_all_records())
    except APIError:
        return pd.DataFrame(columns=VANS_HEADERS_REQUIRED)
    except Exception:
        return pd.DataFrame(columns=VANS_HEADERS_REQUIRED)

    if df.empty:
        return pd.DataFrame(columns=VANS_HEADERS_REQUIRED)

    # Normalize timestamp parsing
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    for c in VANS_HEADERS_REQUIRED:
        if c not in df.columns:
            df[c] = ""
    return df[VANS_HEADERS_REQUIRED]

def clear_vans_cache():
    load_vans_df_cached.clear()

def append_vans_row(row_dict: dict):
    """Append one row aligned to LIVE headers (no cached headers on write)."""
    try:
        sheet = get_worksheet(VANS_SHEET_NAME)
        ensure_header(sheet, VANS_HEADERS_REQUIRED)
        headers = [h.strip() for h in sheet.row_values(1) if str(h).strip()]
        row = [row_dict.get(h, "") for h in headers]
        sheet.append_row(row)
        clear_vans_cache()
    except (APIError, GSpreadException) as e:
        safe_stop(f"Could not write vans log: {e}")

def compute_van_status(vans_df: pd.DataFrame) -> dict:
    status_map = {v: {"status": "IN"} for v in VANS}
    if vans_df is None or vans_df.empty:
        return status_map

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
        }
    return status_map

def next_available_van(status_map: dict) -> str | None:
    for v in VANS:
        if status_map.get(v, {}).get("status") != "OUT":
            return v
    return None

# -------------------------------------------------
# PAGES
# -------------------------------------------------
def page_sign_in_out():
    st.header("Staff Sign-Out / Sign-In")

    pin_map, staff_names = get_pin_map_and_names()

    df_logs = load_logs_df_cached()
    df_out = get_currently_out(df_logs)

    # SIGN OUT
    st.subheader("Sign Out")
    with st.form("people_signout_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            name = st.selectbox("Your name", [""] + staff_names, index=0)
        with c2:
            reason = st.selectbox("Reason for going out", REASONS)

        other_reason = ""
        if reason == "Other (type reason)":
            other_reason = st.text_input("Type your reason")

        pin = st.text_input("4-digit code", type="password", max_chars=4)
        submitted = st.form_submit_button("Sign Out")

    if submitted:
        if not name:
            st.error("Please select your name.")
        elif name not in pin_map:
            st.error("Name not recognized in staff list.")
        elif normalize_pin(pin_map[name]) != normalize_pin(pin):
            st.error("Incorrect code.")
        elif reason == "Other (type reason)" and not other_reason.strip():
            st.error("Please type a reason for 'Other'.")
        elif not df_out.empty and name in df_out["name"].values:
            st.error(f"{name} is already signed out.")
        else:
            append_log_row(name, reason, other_reason, action="OUT", status="OUT")
            st.success(f"{name} signed OUT successfully.")
            st.rerun()

    st.markdown("---")

    # SIGN IN
    st.subheader("Sign In")

    # Recompute using cache (cheap)
    df_logs = load_logs_df_cached()
    df_out = get_currently_out(df_logs)

    if df_out.empty:
        st.info("No one is currently signed out.")
        return

    out_names = df_out["name"].tolist()
    with st.form("people_signin_form", clear_on_submit=True):
        c3, c4 = st.columns(2)
        with c3:
            name_in = st.selectbox("Who is signing back in?", [""] + out_names, index=0)
        with c4:
            pin_in = st.text_input("4-digit code", type="password", max_chars=4)
        submitted_in = st.form_submit_button("Sign In")

    if submitted_in:
        if not name_in:
            st.error("Please select your name.")
        elif name_in not in pin_map:
            st.error("Name not recognized in staff list.")
        elif normalize_pin(pin_map[name_in]) != normalize_pin(pin_in):
            st.error("Incorrect code.")
        else:
            row = df_out[df_out["name"] == name_in].iloc[0]
            last_reason = row["reason"]
            last_other = row["other_reason"]
            append_log_row(name_in, last_reason, last_other, action="IN", status="IN")
            st.success(f"{name_in} signed IN successfully.")
            st.rerun()

def page_whos_out():
    st.header("Who’s Out Right Now?")

    df_logs = load_logs_df_cached()
    df_out = get_currently_out(df_logs)

    if df_out.empty:
        st.info("No staff are currently out.")
        return

    df_display = df_out.copy()
    df_display["When"] = df_display["timestamp"].apply(format_time)
    df_display = df_display.rename(columns={
        "name": "Name",
        "reason": "Reason",
        "other_reason": "Other Details",
    })
    df_display = df_display[["Name", "Reason", "Other Details", "When"]]
    st.dataframe(df_display, use_container_width=True)

def page_vans():
    st.header("Vans")

    pin_map, staff_names = get_pin_map_and_names()

    flash = st.session_state.pop("van_flash", "")
    if flash:
        st.success(flash)

    vans_df = load_vans_df_cached()
    status_map = compute_van_status(vans_df)
    out_vans = [v for v in VANS if status_map.get(v, {}).get("status") == "OUT"]
    available = next_available_van(status_map)

    st.subheader("Vans Out Right Now")
    if not out_vans:
        st.info("All vans are currently in.")
    else:
        for v in out_vans:
            info = status_map[v]
            purpose = info.get("purpose", "")
            if purpose == "Other" and info.get("other_purpose"):
                purpose = f"Other: {info.get('other_purpose')}"
            st.write(f"**{v}** — Driver: **{info.get('driver','')}** | Purpose: **{purpose}** | Passengers: {info.get('passengers','')}")

    st.divider()
    st.subheader("Sign Out a Van")

    if available is None:
        st.warning("No vans available. All vans are currently out.")
    else:
        st.info(f"Next available: **{available}**")

        with st.form("van_signout_form", clear_on_submit=True):
            driver = st.selectbox("Driver", options=staff_names)
            driver_code = st.text_input("Driver 4-digit code", type="password", max_chars=4)
            purpose = st.selectbox("Purpose", VAN_PURPOSES)
            other_purpose = ""
            if purpose == "Other":
                other_purpose = st.text_input("Other purpose (required)")
            passengers = st.multiselect(
                "Passengers (select everyone riding with the driver)",
                options=[n for n in staff_names if n != driver],
            )
            submitted = st.form_submit_button("Sign Out Van", use_container_width=True)

        if submitted:
            if not driver:
                st.error("Please select a driver.")
                return
            if normalize_pin(driver_code) != normalize_pin(pin_map.get(driver, "")):
                st.error("Wrong driver code.")
                return
            if purpose == "Other" and not other_purpose.strip():
                st.error("Please enter the other purpose.")
                return

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
            append_vans_row(row)
            st.session_state["van_flash"] = f"{available} signed out under {driver}."
            st.rerun()

    # SIGN IN (only if a van is out)
    if out_vans:
        st.divider()
        st.subheader("Sign In a Van")

        van_to_in = out_vans[0] if len(out_vans) == 1 else st.selectbox("Which van is returning?", out_vans)

        with st.form("van_signin_form", clear_on_submit=True):
            return_driver = st.selectbox("Driver returning the van", options=staff_names)
            return_driver_code = st.text_input("Driver 4-digit code", type="password", max_chars=4)
            submitted_in = st.form_submit_button("Sign In Van", use_container_width=True)

        if submitted_in:
            if normalize_pin(return_driver_code) != normalize_pin(pin_map.get(return_driver, "")):
                st.error("Wrong driver code.")
                return

            # Copy last OUT row details so CHECKIN row also shows passengers/purpose
            last_passengers = ""
            last_purpose = ""
            last_other_purpose = ""
            try:
                tmp = vans_df.copy()
                tmp["timestamp"] = pd.to_datetime(tmp["timestamp"], errors="coerce")
                tmp = tmp.sort_values("timestamp")
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
                "timestamp": now_iso(),
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

def page_admin_history():
    st.header("Admin / History")

    if not ADMIN_PASSWORD:
        st.warning("ADMIN_PASSWORD is not set in Streamlit secrets. Admin area is disabled.")
        return

    if "admin_authenticated" not in st.session_state:
        st.session_state.admin_authenticated = False

    if not st.session_state.admin_authenticated:
        st.info("Admin access is password protected.")
        pw = st.text_input("Enter admin password", type="password", key="admin_pw_input")
        if st.button("Unlock Admin", key="admin_pw_btn"):
            if pw == ADMIN_PASSWORD:
                st.session_state.admin_authenticated = True
                st.success("Access granted.")
                st.rerun()
            else:
                st.error("Incorrect password.")
        st.stop()

    with st.expander("Admin Session", expanded=False):
        st.caption("You are logged in to the admin area.")
        if st.button("Lock Admin Area", key="admin_logout_btn"):
            st.session_state.admin_authenticated = False
            st.success("Admin area locked again.")
            st.rerun()

    df_logs = load_logs_df_cached()

    st.subheader("Full Log History")
    if df_logs.empty:
        st.info("No logs recorded yet.")
        return

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

    csv_data = df_display.to_csv(index=False)
    st.download_button("Download Full Log as CSV", data=csv_data, file_name="signout_log.csv", mime="text/csv")

    st.markdown("---")
    st.subheader("Delete Specific Log Entries (for testing / pre-season only)")

    ids = df_logs["id"].dropna().astype(int).tolist()
    if not ids:
        st.info("No deletable entries.")
    else:
        id_to_label = {}
        for _, row in df_logs.iterrows():
            if pd.isna(row["id"]):
                continue
            label = f"{int(row['id'])} – {row['name']} – {format_time(row['timestamp'])} – {row['action']}"
            id_to_label[int(row["id"])] = label

        selected_labels = st.multiselect("Select entries to delete", list(id_to_label.values()))
        selected_ids = [log_id for log_id, label in id_to_label.items() if label in selected_labels]

        if selected_ids and st.button("Delete Selected Entries"):
            delete_logs_by_ids(selected_ids)
            st.success(f"Deleted {len(selected_ids)} log(s).")
            st.rerun()

    st.markdown("---")
    st.subheader("Delete ALL Logs (for testing / pre-season only)")
    st.error("WARNING: This will delete ALL sign-in/out records from Google Sheets.")

    confirm_all = st.checkbox("I understand this will permanently delete all logs.")
    if confirm_all and st.button("Delete ALL Logs"):
        clear_all_logs()
        st.success("All logs cleared.")
        st.rerun()

# -------------------------------------------------
# BOARD MODE (for always-on monitor)
# -------------------------------------------------
def page_board_mode():
    st.title("Bauercrest Sign-Out Board")
    st.caption(f"Auto-refreshes. Last refresh: {datetime.now(EASTERN).strftime('%I:%M:%S %p')}")

    # People out
    st.subheader("Who’s Out")
    df_out = get_currently_out(load_logs_df_cached())
    if df_out.empty:
        st.info("No staff are currently out.")
    else:
        df_display = df_out.copy()
        df_display["When"] = df_display["timestamp"].apply(format_time)
        df_display = df_display.rename(columns={"name": "Name", "reason": "Reason", "other_reason": "Other Details"})
        st.dataframe(df_display[["Name", "Reason", "Other Details", "When"]], use_container_width=True, hide_index=True)

    # Vans out
    st.subheader("Vans Out")
    vans_df = load_vans_df_cached()
    status_map = compute_van_status(vans_df)
    out_vans = [v for v in VANS if status_map.get(v, {}).get("status") == "OUT"]
    if not out_vans:
        st.info("All vans are currently in.")
    else:
        rows = []
        for v in out_vans:
            info = status_map[v]
            purpose = info.get("purpose", "")
            if purpose == "Other" and info.get("other_purpose"):
                purpose = f"Other: {info.get('other_purpose')}"
            rows.append({"Van": v, "Driver": info.get("driver", ""), "Purpose": purpose, "Passengers": info.get("passengers", "")})
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # Light auto-refresh without hammering sheets (Streamlit reruns on interaction; this is for monitor)
    st.markdown(" ")
    st.caption("Tip: Leave this page open on the monitor. It will stay quota-safe.")

# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main():
    st.sidebar.title("Bauercrest Staff Sign-Out")

    logo_path = Path("logo-header-2.png")
    if logo_path.exists():
        st.sidebar.image(str(logo_path), use_container_width=True)

    st.sidebar.caption("Track who’s out of camp, safely and clearly.")

    # Board mode via URL param: ?mode=board
    qp = st.query_params
    if qp.get("mode", "").lower() == "board":
        page_board_mode()
        return

    page = st.sidebar.radio("Go to", ["Sign In / Out", "Who’s Out", "Vans", "Admin / History"], key="main_page_radio")

    if page == "Sign In / Out":
        page_sign_in_out()
    elif page == "Who’s Out":
        page_whos_out()
    elif page == "Vans":
        page_vans()
    else:
        page_admin_history()

if __name__ == "__main__":
    main()
