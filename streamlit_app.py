import streamlit as st
import pandas as pd
from datetime import datetime
import pytz

import gspread
from gspread.exceptions import APIError, GSpreadException
from google.oauth2.service_account import Credentials

# -------------------------------------------------
# CONFIG
# -------------------------------------------------

# Defaults (can be overridden by Streamlit Secrets if present)
DEFAULT_SHEET_NAME = "logs"
DEFAULT_STAFF_SHEET_NAME = "staff"
DEFAULT_VANS_SHEET_NAME = "vans"

# Admin password can stay in secrets; fallback to this only if you really want.
DEFAULT_ADMIN_PASSWORD = "Hyaffa26"

EASTERN = pytz.timezone("US/Eastern")

REASONS = [
    "Day Off",
    "Period Off",
    "Night Off",
    "Other (type reason)",
]

VANS = ["Van 1", "Van 2", "Van 3"]
VAN_PURPOSES = [
    "Airport/Bus",
    "Medical/Pharmacy",
    "Supplies/Errand",
    "Trip Support",
    "Other (type purpose)",
]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]

# -------------------------------------------------
# SECRETS / SETTINGS
# -------------------------------------------------

def get_setting(key: str, default=None):
    # st.secrets behaves like a dict; using get keeps local dev easy.
    try:
        return st.secrets.get(key, default)
    except Exception:
        return default

SPREADSHEET_ID = get_setting("SPREADSHEET_ID", None)
SHEET_NAME = get_setting("SHEET_NAME", DEFAULT_SHEET_NAME)
STAFF_SHEET_NAME = get_setting("STAFF_SHEET_NAME", DEFAULT_STAFF_SHEET_NAME)
VANS_SHEET_NAME = get_setting("VANS_SHEET_NAME", DEFAULT_VANS_SHEET_NAME)
ADMIN_PASSWORD = get_setting("ADMIN_PASSWORD", DEFAULT_ADMIN_PASSWORD)

# -------------------------------------------------
# GOOGLE SHEETS HELPERS
# -------------------------------------------------

@st.cache_resource(show_spinner=False)
def get_gspread_client():
    """Create and cache the gspread client using Streamlit secrets."""
    try:
        creds_dict = st.secrets["gcp_service_account"]
    except Exception:
        st.error("Missing Google service account credentials. Add 'gcp_service_account' to Streamlit Secrets.")
        st.stop()

    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

def get_spreadsheet():
    if not SPREADSHEET_ID:
        st.error("Missing SPREADSHEET_ID. Add it to Streamlit Secrets.")
        st.stop()
    client = get_gspread_client()
    try:
        return client.open_by_key(SPREADSHEET_ID)
    except Exception:
        st.error("Could not open the Google Spreadsheet. Check SPREADSHEET_ID and sharing permissions.")
        st.stop()

def get_or_create_worksheet(ws_name: str, headers: list[str]):
    ss = get_spreadsheet()
    try:
        ws = ss.worksheet(ws_name)
    except Exception:
        # Create worksheet if missing
        try:
            ws = ss.add_worksheet(title=ws_name, rows=1000, cols=max(10, len(headers)))
        except Exception:
            st.error(f"Could not find or create worksheet '{ws_name}'.")
            st.stop()

    ensure_header(ws, headers)
    return ws

def ensure_header(ws, expected_headers: list[str]):
    """Ensure the first row is the expected header row."""
    try:
        first = ws.row_values(1)
    except Exception:
        first = []

    if [h.strip() for h in first] != expected_headers:
        # If sheet is empty, write header
        if len(first) == 0:
            ws.append_row(expected_headers)
        else:
            # Don't overwrite a non-empty sheet silently
            # (This protects your existing logger integration)
            missing = [h for h in expected_headers if h not in first]
            if missing:
                st.error(
                    f"Worksheet '{ws.title}' header doesn't match expected columns. "
                    f"Missing columns: {missing}. Please fix the header row."
                )
                st.stop()

# -------------------------------------------------
# DATA LOADERS
# -------------------------------------------------

def empty_logs_df():
    return pd.DataFrame(columns=["id", "timestamp", "name", "reason", "other_reason", "action", "status"])

def empty_vans_df():
    return pd.DataFrame(columns=[
        "id", "timestamp", "van", "driver", "destination", "purpose",
        "passengers", "expected_return", "notes", "action", "status"
    ])

@st.cache_data(ttl=30, show_spinner=False)
def load_staff_pins() -> dict[str, str]:
    """Load active staff and their 4-digit PINs from the STAFF worksheet."""
    ss = get_spreadsheet()
    try:
        ws = ss.worksheet(STAFF_SHEET_NAME)
    except Exception:
        st.error(f"Could not find a worksheet named '{STAFF_SHEET_NAME}'.")
        st.stop()

    try:
        records = ws.get_all_records()
    except Exception:
        st.error("Could not read the staff sheet.")
        st.stop()

    df = pd.DataFrame(records)
    if df.empty:
        return {}

    needed = {"name", "pin", "active"}
    if not needed.issubset(set(df.columns)):
        st.error(f"Staff sheet must have headers: name, pin, active (found: {list(df.columns)})")
        st.stop()

    df["name"] = df["name"].astype(str).str.strip()
    df["pin"] = df["pin"].astype(str).str.strip()

    # treat TRUE/true/Yes/1 as active
    df["active"] = df["active"].astype(str).str.strip().str.lower().isin(["true", "yes", "1"])
    df = df[df["active"]].copy()

    pins = dict(zip(df["name"], df["pin"]))
    return pins

def load_logs_df():
    """Load staff logs into a DataFrame (timestamp logic unchanged)."""
    try:
        ws = get_or_create_worksheet(
            SHEET_NAME,
            ["id", "timestamp", "name", "reason", "other_reason", "action", "status"],
        )
    except Exception:
        return empty_logs_df()

    try:
        records = ws.get_all_records()
    except GSpreadException:
        st.error("Problem reading the sign-out log from Google Sheets. Please check the logs sheet header.")
        return empty_logs_df()

    df = pd.DataFrame(records)
    if df.empty:
        return empty_logs_df()

    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    else:
        df["id"] = pd.Series(dtype="Int64")

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    else:
        df["timestamp"] = pd.NaT

    return df

def load_vans_df():
    """Load van logs into a DataFrame."""
    ws = get_or_create_worksheet(
        VANS_SHEET_NAME,
        ["id", "timestamp", "van", "driver", "destination", "purpose",
         "passengers", "expected_return", "notes", "action", "status"],
    )

    try:
        records = ws.get_all_records()
    except GSpreadException:
        st.error("Problem reading the vans sheet. Please check the vans sheet header.")
        return empty_vans_df()

    df = pd.DataFrame(records)
    if df.empty:
        return empty_vans_df()

    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    else:
        df["id"] = pd.Series(dtype="Int64")

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    else:
        df["timestamp"] = pd.NaT

    return df

# -------------------------------------------------
# BUSINESS LOGIC
# -------------------------------------------------

def format_time(ts):
    if pd.isna(ts):
        return ""
    try:
        # keep it simple, camp-friendly
        return ts.tz_localize("UTC").tz_convert(EASTERN).strftime("%a %I:%M %p")
    except Exception:
        try:
            return ts.strftime("%a %I:%M %p")
        except Exception:
            return str(ts)

def get_currently_out(df: pd.DataFrame) -> pd.DataFrame:
    """Return a dataframe of people whose latest status is OUT (timestamp logic unchanged)."""
    if df.empty:
        return pd.DataFrame(columns=["name", "reason", "other_reason", "timestamp"])

    df_sorted = df.sort_values("timestamp")
    last_actions = df_sorted.groupby("name").tail(1)
    out_rows = last_actions[last_actions["status"] == "OUT"].copy()

    if out_rows.empty:
        return pd.DataFrame(columns=["name", "reason", "other_reason", "timestamp"])

    return out_rows[["name", "reason", "other_reason", "timestamp"]]

def get_vans_out(df: pd.DataFrame) -> pd.DataFrame:
    """Return vans whose latest status is OUT."""
    if df.empty:
        return pd.DataFrame(columns=["van", "driver", "purpose", "timestamp", "passengers"])

    df_sorted = df.sort_values("timestamp")
    last_actions = df_sorted.groupby("van").tail(1)
    out_rows = last_actions[last_actions["status"] == "OUT"].copy()

    if out_rows.empty:
        return pd.DataFrame(columns=["van", "driver", "purpose", "timestamp", "passengers"])

    keep = ["van", "driver", "purpose", "other_purpose", "passengers", "timestamp"]
    keep = [c for c in keep if c in out_rows.columns]
    understand = out_rows[keep].copy()
    return understand

def append_staff_log(name: str, reason: str, other_reason: str, action: str, status: str):
    try:
        ws = get_or_create_worksheet(
            SHEET_NAME,
            ["id", "timestamp", "name", "reason", "other_reason", "action", "status"],
        )
        df = load_logs_df()
        next_id = 1 if df.empty or df["id"].isna().all() else int(df["id"].max()) + 1

        now = datetime.now(EASTERN)
        timestamp_str = now.isoformat()

        row = [next_id, timestamp_str, name, reason, other_reason, action, status]
        ws.append_row(row)
    except (APIError, GSpreadException):
        st.error("Could not record this sign-in/sign-out due to a problem talking to Google Sheets.")

def append_van_log(
    van: str,
    driver: str,
    purpose: str,
    other_purpose: str,
    passengers: str,
    action: str,
    status: str,
):
    """Append a van log row to the vans worksheet."""
    try:
        ws = get_or_create_worksheet(
            VANS_SHEET_NAME,
            ["id", "timestamp", "van", "driver", "purpose", "other_purpose", "passengers", "action", "status"],
        )

        df = load_vans_df()
        next_id = 1 if df.empty or df["id"].isna().all() else int(df["id"].max()) + 1

        now = datetime.now(EASTERN)
        timestamp_str = now.isoformat()

        row = [next_id, timestamp_str, van, driver, purpose, other_purpose, passengers, action, status]
        ws.append_row(row)
    except (APIError, GSpreadException):
        st.error("Could not record this van log due to a problem talking to Google Sheets.")

def page_sign_in_out():
    st.header("Staff Sign In / Out")

    staff_pins = load_staff_pins()
    staff_names = sorted(staff_pins.keys())

    if not staff_names:
        st.warning("No active staff found in the staff sheet.")
        return

    st.caption("Use your 4-digit code. This logs to the main camp sign-out sheet.")

    # --- Sign Out Section ---
    st.subheader("Sign Out")

    col1, col2 = st.columns(2)
    with col1:
        name = st.selectbox("Your name", [""] + staff_names, index=0, key="signout_name")
    with col2:
        reason = st.selectbox("Reason for going out", REASONS, key="signout_reason")

    other_reason = ""
    if reason == "Other (type reason)":
        other_reason = st.text_input("Type your reason", key="signout_other_reason")

    pin = st.text_input("Enter your 4-digit code", type="password", max_chars=4, key="signout_pin")

    if st.button("Sign Out", key="signout_btn"):
        if not name:
            st.warning("Select your name.")
            return
        if name not in staff_pins:
            st.error("Name not recognized. Check the staff sheet.")
            return
        if staff_pins[name] != str(pin).strip():
            st.error("Incorrect code.")
            return

        append_staff_log(name, reason, other_reason, action="SIGN_OUT", status="OUT")
        st.success(f"{name} signed out.")
        st.cache_data.clear()

    st.divider()

    # --- Sign In Section ---
    st.subheader("Sign In")

    name_in = st.selectbox("Your name ", [""] + staff_names, index=0, key="signin_name")
    pin_in = st.text_input("Enter your 4-digit code ", type="password", max_chars=4, key="signin_pin")

    if st.button("Sign In", key="signin_btn"):
        if not name_in:
            st.warning("Select your name.")
            return
        if staff_pins.get(name_in) != str(pin_in).strip():
            st.error("Incorrect code.")
            return

        append_staff_log(name_in, reason="", other_reason="", action="SIGN_IN", status="IN")
        st.success(f"{name_in} signed in.")
        st.cache_data.clear()

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
        "other_reason": "Other Reason",
    })
    cols = ["Name", "Reason", "Other Reason", "When"]
    cols = [c for c in cols if c in df_display.columns]
    st.dataframe(df_display[cols], use_container_width=True)

def page_vans():
    st.header("Van Sign Out")

    staff_pins = load_staff_pins()
    staff_names = sorted(staff_pins.keys())

    if not staff_names:
        st.warning("No active staff found in the staff sheet.")
        return

    st.caption("Driver + every passenger must enter their own 4-digit code.")

    # Load van logs and compute current status
    df_vans = load_vans_df()
    df_vans_out = get_vans_out(df_vans)

    st.subheader("Vans Out Right Now")
    if df_vans_out.empty:
        st.info("All vans are currently in.")
        vans_out_set = set()
    else:
        df_disp = df_vans_out.copy()
        df_disp["When"] = df_disp["timestamp"].apply(format_time)
        # Make purpose display nice
        if "other_purpose" in df_disp.columns:
            df_disp["Purpose"] = df_disp.apply(
                lambda r: r["purpose"] if str(r.get("purpose", "")).strip().lower() != "other" else f'Other: {r.get("other_purpose", "")}',
                axis=1,
            )
        else:
            df_disp["Purpose"] = df_disp.get("purpose", "")

        rename = {
            "van": "Van",
            "driver": "Driver",
            "passengers": "Passengers",
        }
        df_disp = df_disp.rename(columns=rename)
        show_cols = [c for c in ["Van", "Driver", "Passengers", "Purpose", "When"] if c in df_disp.columns]
        st.dataframe(df_disp[show_cols], use_container_width=True)

        vans_out_set = set(df_vans_out["van"].astype(str).tolist())

    st.divider()

    VANS = ["Van 1", "Van 2", "Van 3"]
    van = st.selectbox("Select a van", VANS, key="van_select")

    is_out = van in vans_out_set

    # -------------------------------
    # CHECK OUT (Sign Out) - only when van is IN
    # -------------------------------
    st.subheader("Check Out a Van")

    if is_out:
        st.info(f"{van} is currently OUT. Use the Return section below to sign it back in.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            driver = st.selectbox("Driver", [""] + staff_names, index=0, key="van_driver")
        with col2:
            driver_pin = st.text_input("Driver 4-digit code", type="password", max_chars=4, key="van_driver_pin")

        purpose_choice = st.selectbox(
            "Purpose",
            ["Period Off", "Night Off", "Day Off", "Other"],
            key="van_purpose",
        )
        other_purpose = ""
        if purpose_choice == "Other":
            other_purpose = st.text_input("Other purpose (required)", key="van_other_purpose")

        st.markdown("**Passengers (each must enter their code)**")

        passenger_names = [n for n in staff_names if n != driver]
        passengers_selected = st.multiselect(
            "Add passengers",
            passenger_names,
            key="van_passengers",
        )

        passenger_pins = {}
        if passengers_selected:
            for pname in passengers_selected:
                passenger_pins[pname] = st.text_input(
                    f"{pname} 4-digit code",
                    type="password",
                    max_chars=4,
                    key=f"van_pin_{pname}",
                )

        if st.button("Sign Out Van", key="van_checkout_btn"):
            # Validate driver
            if not driver:
                st.warning("Select a driver.")
                return
            if staff_pins.get(driver) != str(driver_pin).strip():
                st.error("Incorrect driver code.")
                return

            # Validate passengers (each must enter correct code)
            for pname in passengers_selected:
                if staff_pins.get(pname) != str(passenger_pins.get(pname, "")).strip():
                    st.error(f"Incorrect code for {pname}.")
                    return

            # Validate purpose
            if purpose_choice == "Other" and not other_purpose.strip():
                st.warning("Please enter the other purpose.")
                return

            passengers_str = ", ".join(passengers_selected) if passengers_selected else ""

            append_van_log(
                van=van,
                driver=driver,
                purpose=purpose_choice,
                other_purpose=other_purpose.strip(),
                passengers=passengers_str,
                action="CHECKOUT",
                status="OUT",
            )
            st.success(f"{van} signed out under {driver}.")

            st.cache_data.clear()
            st.rerun()

    st.divider()

    # -------------------------------
    # RETURN (Sign In) - only when van is OUT
    # -------------------------------
    st.subheader("Return a Van")

    if not is_out:
        st.info(f"{van} is currently IN. Return is only available when the van is out.")
        return_driver = st.selectbox("Driver returning the van", [""] + staff_names, index=0, key="van_return_driver_disabled")
        st.text_input("Driver 4-digit code", type="password", max_chars=4, key="van_return_driver_pin_disabled")
        st.button("Sign In Van", key="van_return_btn_disabled", disabled=True)
        return

    col1, col2 = st.columns(2)
    with col1:
        return_driver = st.selectbox("Driver returning the van", [""] + staff_names, index=0, key="van_return_driver")
    with col2:
        return_driver_pin = st.text_input("Driver 4-digit code", type="password", max_chars=4, key="van_return_driver_pin")

    if st.button("Sign In Van", key="van_return_btn"):
        if not return_driver:
            st.warning("Select a driver.")
            return
        if staff_pins.get(return_driver) != str(return_driver_pin).strip():
            st.error("Incorrect driver code.")
            return

        append_van_log(
            van=van,
            driver=return_driver,
            purpose="",
            other_purpose="",
            passengers="",
            action="RETURN",
            status="IN",
        )
        st.success(f"{van} signed in (returned by {return_driver}).")

        st.cache_data.clear()
        st.rerun()

def page_admin_history():
    st.header("Admin / History")

    if "admin_authenticated" not in st.session_state:
        st.session_state.admin_authenticated = False

    if not st.session_state.admin_authenticated:
        password = st.text_input("Admin password", type="password", key="admin_pw")
        if st.button("Unlock Admin Area", key="admin_unlock_btn"):
            if password == ADMIN_PASSWORD:
                st.session_state.admin_authenticated = True
                st.success("Admin unlocked.")
                st.rerun()
            else:
                st.error("Incorrect admin password.")
        return

    with st.expander("Admin Session", expanded=False):
        st.caption("You are logged in to the admin area.")
        if st.button("Lock Admin Area", key="admin_logout_btn"):
            st.session_state.admin_authenticated = False
            st.success("Admin area locked again.")
            st.rerun()

    st.subheader("Staff Log History")
    df_logs = load_logs_df()
    if df_logs.empty:
        st.info("No staff logs recorded yet.")
    else:
        df_disp = df_logs.copy()
        df_disp["When"] = df_disp["timestamp"].apply(format_time)
        st.dataframe(df_disp.sort_values("timestamp", ascending=False), use_container_width=True)

    st.divider()

    st.subheader("Van Log History")
    df_vans = load_vans_df()
    if df_vans.empty:
        st.info("No van logs recorded yet.")
    else:
        df_disp = df_vans.copy()
        df_disp["When"] = df_disp["timestamp"].apply(format_time)
        st.dataframe(df_disp.sort_values("timestamp", ascending=False), use_container_width=True)

# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():
    st.set_page_config(page_title="Bauercrest Sign Out", layout="wide")
    st.image("logo-header-2.png", use_container_width=True)


    st.title("Camp Bauercrest Sign Out")

    page = st.sidebar.radio(
        "Navigation",
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
