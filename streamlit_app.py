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
# CONFIG
# -------------------------------------------------

SPREADSHEET_ID = "1oS7KMged-KMGkeT9BHq1He8_K1oXMNuCvWQig21S5Xg"
SHEET_NAME = "logs"
VANS_SHEET_NAME = "vans"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Staff list and PINs (4-digit codes)
STAFF_PINS = {
    "Jaden Pollack": "1723",
    "Ethan Goldberg": "3841",
    "Colby Karp": "5927",
    "Jordan Bornstein": "4068",
    "Dylan Israel": "8315",
    "Zach Baum": "2194",
    "Darren Sands": "6450",
    "Ethan Esterson": "9032",
    "Asher Schiillin": "5179",
    "Brody Masters": "7624",
    "Matt Schultz": "4482",
    "Max Pollack": "3901",
    "Will Carp": "6842",
    "Josh Poscover": "5589",
    "Evan Ashe": "7136",
    "Riley Schneller": "8240",
    "Joey Rosenfeld": "9675",
    "Justin Feldman": "1358",
}
STAFF_NAMES = list(STAFF_PINS.keys())

REASONS = [
    "Day Off",
    "Period Off",
    "Night Off",
    "Other (type reason)",
]

VANS = ["Van 1", "Van 2", "Van 3"]
VAN_PURPOSES = ["Period Off", "Night Off", "Day Off", "Other"]

ADMIN_PASSWORD = "Hyaffa26"
EASTERN = pytz.timezone("US/Eastern")


# -------------------------------------------------
# GOOGLE SHEETS HELPERS
# -------------------------------------------------

@st.cache_resource
def get_gspread_client():
    """Authorize and cache the gspread client (prevents quota blowups from repeated auth)."""
    creds_info = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_resource
def get_spreadsheet():
    """Open and cache the Google Spreadsheet object."""
    client = get_gspread_client()
    return client.open_by_key(SPREADSHEET_ID)


def get_worksheet(worksheet_name: str):
    """Return a worksheet by name (not cached; cheap)."""
    ss = get_spreadsheet()
    return ss.worksheet(worksheet_name)


def get_sheet():
    """Backwards-compatible: return the main logs worksheet."""
    return get_worksheet(SHEET_NAME)



def ensure_header(sheet):
    """Ensure the logs sheet has a header row."""
    try:
        values = sheet.get_all_values()
    except (APIError, GSpreadException) as e:
        st.error("Problem reading the sign-out log header from Google Sheets. "
                 "Please try reloading the page.")
        st.stop()

    if not values:
        header = ["id", "timestamp", "name", "reason", "other_reason", "action", "status"]
        sheet.append_row(header)


def empty_logs_df():
    return pd.DataFrame(
        columns=["id", "timestamp", "name", "reason", "other_reason", "action", "status"]
    )


def load_logs_df():
    """Load all logs into a pandas DataFrame, with error handling."""
    try:
        sheet = get_sheet()
    except Exception:
        st.error("Could not connect to Google Sheets. Check your internet "
                 "connection and try again.")
        return empty_logs_df()

    ensure_header(sheet)

    try:
        records = sheet.get_all_records()  # Uses first row as header
    except GSpreadException as e:
        st.error("Problem reading the sign-out log from Google Sheets "
                 "(sheet format issue). Please check the 'logs' sheet header "
                 "or try again.")
        return empty_logs_df()
    except APIError as e:
        st.error("Temporary error talking to Google Sheets. Please wait a "
                 "moment and reload the page.")
        return empty_logs_df()

    if not records:
        return empty_logs_df()

    df = pd.DataFrame(records)

    # Coerce types
    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    else:
        df["id"] = pd.Series(dtype="Int64")

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    else:
        df["timestamp"] = pd.NaT

    return df


def append_log_row(name: str, reason: str, other_reason: str, action: str, status: str):
    """Append a new log row to the sheet, safely."""
    try:
        sheet = get_sheet()
        df = load_logs_df()
        next_id = 1 if df.empty or df["id"].isna().all() else int(df["id"].max()) + 1

        now = datetime.now(EASTERN)
        timestamp_str = now.isoformat()

        row = [next_id, timestamp_str, name, reason, other_reason, action, status]
        sheet.append_row(row)
    except (APIError, GSpreadException):
        st.error("Could not record this sign-in/sign-out due to a problem "
                 "talking to Google Sheets. Please tell a senior staff member "
                 "and try again in a minute.")
        st.stop()


def clear_all_logs():
    """Delete all logs from the sheet (re-add header)."""
    try:
        sheet = get_sheet()
        sheet.clear()
        ensure_header(sheet)
    except (APIError, GSpreadException):
        st.error("Could not clear logs in Google Sheets. Please try again later.")
        st.stop()


def delete_logs_by_ids(ids_to_delete):
    """Delete specific logs by id (rewrite sheet)."""
    try:
        sheet = get_sheet()
        df = load_logs_df()
    except (APIError, GSpreadException):
        st.error("Could not update logs in Google Sheets. Please try again later.")
        st.stop()

    if df.empty:
        return

    df_keep = df[~df["id"].isin(ids_to_delete)].copy()

    try:
        sheet.clear()
        ensure_header(sheet)
        if not df_keep.empty:
            df_out = df_keep.copy()
            cols = ["id", "timestamp", "name", "reason", "other_reason", "action", "status"]
            for col in cols:
                if col not in df_out.columns:
                    df_out[col] = ""
            df_out["timestamp"] = df_out["timestamp"].astype(str)
            rows = df_out[cols].values.tolist()
            if rows:
                sheet.append_rows(rows)
    except (APIError, GSpreadException):
        st.error("Could not finish deleting selected log entries. Please try again later.")
        st.stop()


# -------------------------------------------------
# LOGIC HELPERS
# -------------------------------------------------

def get_currently_out(df: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe of people whose latest status is OUT."""
    if df.empty:
        return pd.DataFrame(columns=["name", "reason", "other_reason", "timestamp"])

    df_sorted = df.sort_values("timestamp")
    last_actions = df_sorted.groupby("name").tail(1)
    out_rows = last_actions[last_actions["status"] == "OUT"].copy()

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


# ------------# -------------------------------------------------
# VANS HELPERS (quota-safe + correct logging)
# -------------------------------------------------

VANS_HEADERS_REQUIRED = ["id", "timestamp", "van", "driver", "purpose", "passengers", "other_purpose", "action", "status"]

def normalize_pin(pin: str) -> str:
    s = str(pin).strip().replace(" ", "")
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(4)

def get_vans_sheet():
    return get_worksheet(VANS_SHEET_NAME)

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
    """Quota-safe: cache reads briefly so typing doesn't hammer Sheets."""
    sheet = get_vans_sheet()
    ensure_vans_header(sheet)
    return pd.DataFrame(sheet.get_all_records())

def clear_vans_cache():
    load_vans_df_cached.clear()

def append_vans_row(row_dict: dict):
    """
    Append one row aligned to the LIVE header row.
    (We do NOT cache headers for writes — avoids 'passengers' silently disappearing.)
    """
    sheet = get_vans_sheet()
    ensure_vans_header(sheet)

    headers = [h.strip() for h in sheet.row_values(1) if str(h).strip()]
    if "passengers" not in headers:
        headers = headers + ["passengers"]
        sheet.delete_rows(1)
        sheet.insert_row(headers, 1)

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

def page_vans():
    st.header("Vans")

    # Flash message
    flash = st.session_state.pop("van_flash", "")
    if flash:
        st.success(flash)

    # Load vans + compute status
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
            driver = st.selectbox("Driver", options=sorted(STAFF_PINS.keys()))
            driver_code = st.text_input("Driver 4-digit code", type="password")
            purpose = st.selectbox("Purpose", VAN_PURPOSES)
            other_purpose = ""
            if purpose == "Other":
                other_purpose = st.text_input("Other purpose (required)")
            passengers = st.multiselect(
                "Passengers (select everyone riding with the driver)",
                options=[n for n in sorted(STAFF_PINS.keys()) if n != driver],
            )
            submitted = st.form_submit_button("Sign Out Van", use_container_width=True)

        if submitted:
            if normalize_pin(driver_code) != normalize_pin(STAFF_PINS.get(driver, "")):
                st.error("Wrong driver code.")
                return
            if purpose == "Other" and not other_purpose.strip():
                st.error("Please enter the other purpose.")
                return

            row = {
                "id": str(uuid.uuid4())[:8],
                "timestamp": datetime.now(CAMP_TZ).isoformat(timespec="seconds"),
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
            return_driver = st.selectbox("Driver returning the van", options=sorted(STAFF_PINS.keys()))
            return_driver_code = st.text_input("Driver 4-digit code", type="password")
            submitted_in = st.form_submit_button("Sign In Van", use_container_width=True)

        if submitted_in:
            if normalize_pin(return_driver_code) != normalize_pin(STAFF_PINS.get(return_driver, "")):
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
                "timestamp": datetime.now(CAMP_TZ).isoformat(timespec="seconds"),
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

-------------------------------------
# PAGES
# -------------------------------------------------

def page_sign_in_out():
    st.header("Staff Sign-Out / Sign-In")

    df_logs = load_logs_df()
    df_out = get_currently_out(df_logs)

    # --- Sign Out Section ---
    st.subheader("Sign Out")

    col1, col2 = st.columns(2)

    with col1:
        name = st.selectbox("Your name", [""] + STAFF_NAMES, index=0, key="signout_name")
    with col2:
        reason = st.selectbox("Reason for going out", REASONS, key="signout_reason")

    other_reason = ""
    if reason == "Other (type reason)":
        other_reason = st.text_input("Type your reason", key="signout_other_reason")

    pin = st.text_input("4-digit code", type="password", max_chars=4, key="signout_pin")

    already_out = False
    if name:
        if not df_out.empty and name in df_out["name"].values:
            already_out = True
            st.warning(f"{name} is already signed out. They should sign back in first.")

    if st.button("Sign Out", key="signout_button"):
        if not name:
            st.error("Please select your name.")
        elif name not in STAFF_PINS:
            st.error("Name not recognized in staff list.")
        elif STAFF_PINS[name] != pin:
            st.error("Incorrect code.")
        elif reason == "Other (type reason)" and not other_reason.strip():
            st.error("Please type a reason for 'Other'.")
        elif already_out:
            st.error(f"{name} is already signed out.")
        else:
            append_log_row(name, reason, other_reason, action="OUT", status="OUT")
            st.success(f"{name} signed OUT successfully.")

    st.markdown("---")

    # --- Sign In Section ---
    st.subheader("Sign In")

    df_logs = load_logs_df()
    df_out = get_currently_out(df_logs)

    if df_out.empty:
        st.info("No one is currently signed out.")
    else:
        out_names = df_out["name"].tolist()
        col3, col4 = st.columns(2)
        with col3:
            name_in = st.selectbox("Who is signing back in?", [""] + out_names,
                                   index=0, key="signin_name")
        with col4:
            pin_in = st.text_input("4-digit code", type="password", max_chars=4,
                                   key="signin_pin")

        if st.button("Sign In", key="signin_button"):
            if not name_in:
                st.error("Please select your name.")
            elif name_in not in STAFF_PINS:
                st.error("Name not recognized in staff list.")
            elif STAFF_PINS[name_in] != pin_in:
                st.error("Incorrect code.")
            else:
                row = df_out[df_out["name"] == name_in].iloc[0]
                last_reason = row["reason"]
                last_other = row["other_reason"]
                append_log_row(name_in, last_reason, last_other, action="IN", status="IN")
                st.success(f"{name_in} signed IN successfully.")


def page_whos_out():
    st.header("Who’s Out Right Now?")

    df_logs = load_logs_df()
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


def page_admin_history():
    st.header("Admin / History")

    if "admin_authenticated" not in st.session_state:
        st.session_state.admin_authenticated = False

    if not st.session_state.admin_authenticated:
        st.info("Admin access is password protected.")
        pw = st.text_input("Enter admin password", type="password", key="admin_pw_input")
        col_pw_btn, _ = st.columns([1, 3])
        with col_pw_btn:
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

    df_logs = load_logs_df()

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
    st.download_button(
        "Download Full Log as CSV",
        data=csv_data,
        file_name="signout_log.csv",
        mime="text/csv",
    )

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

        selected_labels = st.multiselect(
            "Select entries to delete",
            list(id_to_label.values()),
            key="admin_delete_specific_multiselect",
        )

        selected_ids = [
            log_id for log_id, label in id_to_label.items()
            if label in selected_labels
        ]

        if selected_ids and st.button("Delete Selected Entries", key="admin_delete_specific_button"):
            delete_logs_by_ids(selected_ids)
            st.success(f"Deleted {len(selected_ids)} log(s).")
            st.rerun()

    st.markdown("---")

    st.subheader("Delete ALL Logs (for testing / pre-season only)")
    st.error(
        "WARNING: This will delete ALL sign-in/out records from Google Sheets. "
        "Do NOT use this during the actual camp season if you need 10-year records."
    )

    confirm_all = st.checkbox(
        "I understand this will permanently delete all logs.",
        key="admin_confirm_delete_all_logs",
    )
    if confirm_all and st.button("Delete ALL Logs", key="admin_delete_all_logs_button"):
        clear_all_logs()
        st.success("All logs cleared.")
        st.rerun()


# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():
    st.set_page_config(
        page_title="Bauercrest Staff Sign-Out",
        layout="wide",
    )

    st.sidebar.title("Bauercrest Staff Sign-Out")

    logo_path = Path("logo-header-2.png")
    if logo_path.exists():
        st.sidebar.image(str(logo_path), use_column_width=True)

    st.sidebar.caption("Track who’s out of camp, safely and clearly.")

    page = st.sidebar.radio(
        "Go to",
        ["Sign In / Out", "Who’s Out", "Vans", "Admin / History"],
        key="main_page_radio",
    )

    if page == "Sign In / Out":
        page_sign_in_out()
    elif page == "Who’s Out":
        page_whos_out()
    elif page == "Vans":
        page_vans()
    elif page == "Admin / History":
        page_admin_history()


if __name__ == "__main__":
    main()
