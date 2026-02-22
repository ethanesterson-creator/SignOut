import streamlit as st
import pandas as pd
from datetime import datetime
from pathlib import Path
import pytz
import uuid

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
SHEET_DAYS_OFF = "days_off"  # you create this tab

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

REASONS = ["Day Off", "Period Off", "Night Off", "Other (type reason)"]

VANS = ["Van 1", "Van 2", "Van 3"]
VAN_PURPOSES = ["Period Off", "Night Off", "Day Off", "Field Trip", "Tournament", "Other"]

# Auto Day Off behavior
AUTO_DAY_OFF_TAG_PREFIX = "AUTO_DAY_OFF"  # we append today's date to prevent duplicates
AUTO_DAY_OFF_START_HOUR = 7  # local time; adjust if you want auto-outs to start later

# Vans sheet required headers
VANS_HEADERS_REQUIRED = [
    "id", "timestamp", "van", "driver", "purpose", "passengers",
    "other_purpose", "action", "status"
]

# =================================================
# SMALL UTILS
# =================================================
def normalize_pin(pin: str) -> str:
    s = str(pin).strip().replace(" ", "")
    if s.endswith(".0"):
        s = s[:-2]
    return s.zfill(4)

def format_time(dt):
    if pd.isna(dt):
        return ""
    try:
        return dt.strftime("%Y-%m-%d %I:%M %p")
    except Exception:
        return str(dt)

def kiosk_autorefresh(seconds: int):
    """Simple meta refresh to keep kiosk view fresh."""
    if seconds and seconds > 0:
        st.components.v1.html(
            f"<meta http-equiv='refresh' content='{int(seconds)}'>",
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

# =================================================
# LOGS SHEET HELPERS
# =================================================
LOGS_HEADERS_REQUIRED = ["id", "timestamp", "name", "reason", "other_reason", "action", "status"]

def ensure_logs_header(sheet):
    try:
        headers = sheet.row_values(1)
        if not headers:
            sheet.insert_row(LOGS_HEADERS_REQUIRED, 1)
            return
        # add missing columns (append to end)
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

    # normalize columns
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
    try:
        sheet = get_worksheet(SHEET_LOGS)
        ensure_logs_header(sheet)

        row = [
            str(uuid.uuid4())[:8],
            datetime.now(TZ).isoformat(timespec="seconds"),
            name,
            reason,
            other_reason or "",
            action,
            status,
        ]
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
# DAYS OFF AUTO SIGN-OUT (ANY WEEKDAY)
# =================================================
@st.cache_data(ttl=60)
def load_days_off_df_cached():
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

    # active: blank treated as TRUE
    a = df["active"].astype(str).str.upper().str.strip()
    df["active"] = a.isin(["TRUE", "1", "YES", "Y", ""])

    df = df[df["name"] != ""].copy()
    return df

def maybe_auto_day_off_signouts(staff_pins: dict):
    """
    If today matches entries in days_off tab, ensure those staff have an OUT entry
    for today tagged with AUTO_DAY_OFF|YYYY-MM-DD (so refresh doesn't duplicate).
    """
    now = datetime.now(TZ)
    if now.hour < AUTO_DAY_OFF_START_HOUR:
        return

    today_weekday = normalize_weekday(now.strftime("%A"))
    today_str = now.date().isoformat()
    tag_today = f"{AUTO_DAY_OFF_TAG_PREFIX}|{today_str}"

    df_days = load_days_off_df_cached()
    if df_days.empty:
        return

    names_today = df_days[(df_days["active"]) & (df_days["weekday"] == today_weekday)]["name"].tolist()
    if not names_today:
        return

    df_logs = load_logs_df_cached()

    # Already ran today?
    if not df_logs.empty:
        tmp = df_logs.copy()
        tmp["timestamp"] = pd.to_datetime(tmp["timestamp"], errors="coerce")
        tmp = tmp[tmp["timestamp"].dt.date == now.date()]
        if "other_reason" in tmp.columns and (tmp["other_reason"].astype(str) == tag_today).any():
            return

    df_out = get_currently_out(df_logs)
    currently_out = set(df_out["name"].tolist()) if not df_out.empty else set()

    # write OUT for each eligible name
    for n in names_today:
        if n in staff_pins and n not in currently_out:
            append_log_row(n, "Day Off", tag_today, action="OUT", status="OUT")

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

def next_available_van(status_map: dict) -> str | None:
    for v in VANS:
        if status_map.get(v, {}).get("status") != "OUT":
            return v
    return None

# =================================================
# PAGES
# =================================================
def page_sign_in_out(staff_pins: dict, staff_names: list[str]):
    st.header("Staff Sign-Out / Sign-In")

    df_logs = load_logs_df_cached()
    df_out = get_currently_out(df_logs)

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

    pin = st.text_input("4-digit code", type="password", max_chars=4, key="signout_pin")

    already_out = False
    if name and (not df_out.empty) and name in df_out["name"].values:
        already_out = True
        st.warning(f"{name} is already signed out. They should sign back in first.")

    if st.button("Sign Out", key="signout_button"):
        if not name:
            st.error("Please select your name.")
        elif name not in staff_pins:
            st.error("Name not recognized (inactive or missing from staff sheet).")
        elif normalize_pin(pin) != normalize_pin(staff_pins.get(name, "")):
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

    df_logs = load_logs_df_cached()
    df_out = get_currently_out(df_logs)

    if df_out.empty:
        st.info("No one is currently signed out.")
    else:
        out_names = sorted(df_out["name"].tolist())
        col3, col4 = st.columns(2)
        with col3:
            name_in = st.selectbox("Who is signing back in?", [""] + out_names, index=0, key="signin_name")
        with col4:
            pin_in = st.text_input("4-digit code", type="password", max_chars=4, key="signin_pin")

        if st.button("Sign In", key="signin_button"):
            if not name_in:
                st.error("Please select your name.")
            elif name_in not in staff_pins:
                st.error("Name not recognized (inactive or missing from staff sheet).")
            elif normalize_pin(pin_in) != normalize_pin(staff_pins.get(name_in, "")):
                st.error("Incorrect code.")
            else:
                row = df_out[df_out["name"] == name_in].iloc[0]
                last_reason = row["reason"]
                last_other = row["other_reason"]
                append_log_row(name_in, last_reason, last_other, action="IN", status="IN")
                st.success(f"{name_in} signed IN successfully.")

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

def page_vans(staff_pins: dict, staff_names: list[str], driver_names: list[str]):
    st.header("Vans")

    # Form nonce to keep widget keys stable after submit
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
st.subheader("Sign Out a Van")

if available is None:
    st.warning("No vans available. All vans are currently out.")
    return

st.info(f"Next available: **{available}**")

# ✅ Move this check OUTSIDE the form
if not driver_names:
    st.warning("No eligible drivers found. Set drivers.passed_test=TRUE for cleared drivers.")
    return

with st.form("van_signout_form", clear_on_submit=False):
    driver = st.selectbox(
        "Driver (must be driving-tested)",
        options=driver_names,
        key=f"van_driver_{van_nonce}"
    )
    driver_code = st.text_input("Driver 4-digit code", type="password", key=f"van_driver_code_{van_nonce}")
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
            passengers_selected = st.session_state.get(f"van_passengers_{van_nonce}", passengers) or []
            passengers_selected = [p for p in passengers_selected if p != driver]

            if driver not in driver_names:
                st.error("This staff member is not cleared to drive a van.")
                return

            if normalize_pin(driver_code) != normalize_pin(staff_pins.get(driver, "")):
                st.error("Wrong driver code.")
                return

            if purpose == "Other" and not other_purpose.strip():
                st.error("Please enter the other purpose.")
                return

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

    if out_vans:
        st.divider()
        st.subheader("Sign In a Van")

        van_to_in = out_vans[0] if len(out_vans) == 1 else st.selectbox("Which van is returning?", out_vans)

        with st.form("van_signin_form", clear_on_submit=True):
            return_driver = st.selectbox("Driver returning the van", options=staff_names)
            return_driver_code = st.text_input("Driver 4-digit code", type="password")
            submitted_in = st.form_submit_button("Sign In Van", use_container_width=True)

        if submitted_in:
            if normalize_pin(return_driver_code) != normalize_pin(staff_pins.get(return_driver, "")):
                st.error("Wrong driver code.")
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

def page_admin_history():
    st.header("Admin / History")

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

    st.subheader("Full Log History")
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

    # Vans history
    st.markdown("---")
    df_vans = load_vans_df_cached()
    st.subheader("Van Log History")
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
    st.subheader("Delete Specific Log Entries (for testing / pre-season only)")

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
    st.subheader("Delete ALL Logs (for testing / pre-season only)")
    st.error("WARNING: This will delete ALL sign-in/out records from Google Sheets.")

    confirm_all = st.checkbox("I understand this will permanently delete all logs.", key="admin_confirm_delete_all_logs")
    if confirm_all and st.button("Delete ALL Logs", key="admin_delete_all_logs_button"):
        clear_all_logs()
        st.success("All logs cleared.")
        st.rerun()

# =================================================
# MAIN
# =================================================
def main():
    st.set_page_config(page_title="Bauercrest Staff Sign-Out", layout="wide")
    st.sidebar.title("Bauercrest Staff Sign-Out")

    logo_path = Path("logo-header-2.png")
    if logo_path.exists():
        st.sidebar.image(str(logo_path), use_column_width=True)

    st.sidebar.caption("Sign in and out with your 4-digit code.")

    # Kiosk controls
    with st.sidebar.expander("Kiosk Settings", expanded=False):
        auto_refresh_on = st.checkbox("Auto-refresh kiosk", value=True)
        refresh_seconds = st.slider("Refresh every (seconds)", 10, 120, 30, step=5)
        if auto_refresh_on:
            kiosk_autorefresh(refresh_seconds)

    # Load staff + drivers (from sheets)
    staff_pins, staff_names, driver_names = get_staff_pins_and_lists()

    # Auto day-off signouts (any weekday)
    maybe_auto_day_off_signouts(staff_pins)

    page = st.sidebar.radio("Go to", ["Sign In / Out", "Who’s Out", "Vans", "Admin / History"], key="main_page_radio")

    if page == "Sign In / Out":
        page_sign_in_out(staff_pins, staff_names)
    elif page == "Who’s Out":
        page_whos_out()
    elif page == "Vans":
        page_vans(staff_pins, staff_names, driver_names)
    elif page == "Admin / History":
        page_admin_history()

if __name__ == "__main__":
    main()
