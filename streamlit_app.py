
import streamlit as st
import pandas as pd
from datetime import datetime
from pathlib import Path
import pytz

import gspread
from gspread.exceptions import APIError, GSpreadException
from google.oauth2.service_account import Credentials

# =================================================
# CONFIG
# =================================================
EASTERN = pytz.timezone("America/New_York")

# Sheet names (override via Streamlit secrets if you want)
SPREADSHEET_ID = st.secrets.get("SPREADSHEET_ID", "1oS7KMged-KMGkeT9BHq1He8_K1oXMNuCvWQig21S5Xg")
LOG_SHEET_NAME = st.secrets.get("SHEET_NAME", "logs")
STAFF_SHEET_NAME = st.secrets.get("STAFF_SHEET_NAME", "staff")
VANS_SHEET_NAME = st.secrets.get("VANS_SHEET_NAME", "vans")

# Admin password (optional). If not set, Admin tab will still render but won't unlock.
ADMIN_PASSWORD = st.secrets.get("ADMIN_PASSWORD", "")

VANS = ["Van 1", "Van 2", "Van 3"]

PEOPLE_REASONS = ["Period Off", "Night Off", "Day Off", "Medical", "Program", "Other"]
VAN_PURPOSES = ["Period Off", "Night Off", "Day Off", "Other"]

LOG_HEADERS = ["id", "timestamp", "name", "reason", "other_reason", "action", "status"]
VAN_HEADERS = ["id", "timestamp", "van", "driver", "purpose", "other_reason", "action", "passengers"]

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# =================================================
# HELPERS
# =================================================
def normalize_pin(pin) -> str:
    """Normalize 4-digit codes as strings. Preserves leading zeros by zfill."""
    if pin is None:
        return ""
    s = str(pin).strip()
    # If it came in as "123.0" from sheets, fix it.
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    # Keep digits only
    s = "".join(ch for ch in s if ch.isdigit())
    if not s:
        return ""
    return s.zfill(4)


@st.cache_resource
def _gspread_client():
    creds_info = st.secrets.get("gcp_service_account")
    if not creds_info:
        st.error("Missing Google service account credentials in Streamlit secrets: gcp_service_account")
        st.stop()
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    return gspread.authorize(creds)


def get_worksheet(ws_name: str):
    client = _gspread_client()
    try:
        sh = client.open_by_key(SPREADSHEET_ID)
    except Exception:
        st.error("Could not open the Google Spreadsheet. Check SPREADSHEET_ID and sharing permissions.")
        st.stop()

    try:
        return sh.worksheet(ws_name)
    except Exception:
        # If missing (only for vans), create it
        if ws_name == VANS_SHEET_NAME:
            try:
                ws = sh.add_worksheet(title=ws_name, rows=2000, cols=20)
                ws.append_row(VAN_HEADERS)
                return ws
            except Exception:
                st.error("Could not create the vans worksheet. Check spreadsheet permissions.")
                st.stop()
        st.error(f"Worksheet '{ws_name}' not found.")
        st.stop()


def ensure_header(sheet, expected_headers):
    try:
        values = sheet.get_all_values()
    except (APIError, GSpreadException):
        st.error("Problem reading the Google Sheet.")
        st.stop()

    if not values:
        sheet.append_row(expected_headers)
        return

    header = values[0]
    if header != expected_headers:
        # Don't destroy existing data; just warn loudly.
        st.warning(
            f"Header mismatch in '{sheet.title}'. Expected {expected_headers} but found {header}. "
            "The app may not behave correctly until this is fixed."
        )


@st.cache_data(ttl=30)
def load_staff_df() -> pd.DataFrame:
    sheet = get_worksheet(STAFF_SHEET_NAME)
    records = sheet.get_all_records()
    df = pd.DataFrame(records)

    if df.empty:
        return pd.DataFrame(columns=["name", "pin", "active"])

    # Normalize expected columns
    for col in ["name", "pin", "active"]:
        if col not in df.columns:
            df[col] = ""

    df["name"] = df["name"].astype(str).str.strip()
    df["pin"] = df["pin"].apply(normalize_pin)

    # Active parsing
    df["active"] = df["active"].astype(str).str.strip().str.lower().isin(["true", "yes", "1"])
    df = df[df["active"]]
    df = df[df["name"] != ""]
    return df.sort_values("name")


def staff_pin_map() -> dict:
    df = load_staff_df()
    return dict(zip(df["name"], df["pin"]))


@st.cache_data(ttl=20)
def load_logs_df() -> pd.DataFrame:
    sheet = get_worksheet(LOG_SHEET_NAME)
    ensure_header(sheet, LOG_HEADERS)
    records = sheet.get_all_records()
    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=LOG_HEADERS)

    # parse timestamp
    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    except Exception:
        df["timestamp"] = pd.NaT
    return df


def append_people_log(name: str, reason: str, other_reason: str, action: str, status: str):
    sheet = get_worksheet(LOG_SHEET_NAME)
    ensure_header(sheet, LOG_HEADERS)

    df = load_logs_df()
    next_id = 1 if df.empty or df["id"].isna().all() else int(pd.to_numeric(df["id"], errors="coerce").max()) + 1

    now = datetime.now(EASTERN)
    timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")

    row = [next_id, timestamp_str, name, reason, other_reason, action, status]
    sheet.append_row(row, value_input_option="USER_ENTERED")
    # bust cache
    load_logs_df.clear()


@st.cache_data(ttl=20)
def load_vans_df() -> pd.DataFrame:
    sheet = get_worksheet(VANS_SHEET_NAME)
    ensure_header(sheet, VAN_HEADERS)
    records = sheet.get_all_records()
    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=VAN_HEADERS)

    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    except Exception:
        df["timestamp"] = pd.NaT
    return df


def append_van_log(van: str, driver: str, purpose: str, other_reason: str, action: str, passengers: list[str]):
    sheet = get_worksheet(VANS_SHEET_NAME)
    ensure_header(sheet, VAN_HEADERS)

    df = load_vans_df()
    next_id = 1 if df.empty or df["id"].isna().all() else int(pd.to_numeric(df["id"], errors="coerce").max()) + 1

    now = datetime.now(EASTERN)
    timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")

    passengers_str = ", ".join(passengers) if passengers else ""
    row = [next_id, timestamp_str, van, driver, purpose, other_reason, action, passengers_str]
    sheet.append_row(row, value_input_option="USER_ENTERED")
    load_vans_df.clear()


def current_people_out(df_logs: pd.DataFrame) -> pd.DataFrame:
    if df_logs.empty:
        return df_logs

    df = df_logs.copy()
    if "timestamp" in df.columns:
        df_sorted = df.sort_values("timestamp")
    else:
        df_sorted = df

    last_actions = df_sorted.groupby("name", as_index=False).tail(1)
    # Expect status column values like OUT/IN
    out_now = last_actions[last_actions["status"].astype(str).str.upper() == "OUT"]
    return out_now.sort_values("timestamp", ascending=False)


def current_vans_status(df_vans: pd.DataFrame) -> dict:
    """Return {van: 'OUT'/'IN'/None} based on last action for each van."""
    status = {v: None for v in VANS}
    if df_vans.empty:
        return status

    df = df_vans.copy()
    if "timestamp" in df.columns:
        df = df.sort_values("timestamp")
    last = df.groupby("van", as_index=False).tail(1)
    for _, row in last.iterrows():
        v = str(row.get("van", "")).strip()
        s = str(row.get("action", "")).strip().upper()
        if v in status:
            status[v] = "OUT" if s == "OUT" else "IN"
    return status


def first_available_van(van_status: dict) -> str | None:
    for v in VANS:
        if van_status.get(v) != "OUT":
            return v
    return None


def render_logo():
    logo_path = Path("logo-header-2.png")
    if logo_path.exists():
        st.image(str(logo_path), width=220)
    else:
        # Don't fail the app if the logo file isn't present
        st.markdown("")


# =================================================
# UI
# =================================================
st.set_page_config(page_title="Bauercrest Sign Out", layout="centered")

render_logo()
st.title("Sign Out")

tabs = st.tabs(["People", "Vans", "Admin"])


# -------------------------
# PEOPLE TAB
# -------------------------
with tabs[0]:
    staff_df = load_staff_df()
    pins = staff_pin_map()
    staff_names = staff_df["name"].tolist()

    st.subheader("Staff Sign Out / Sign In")

    if not staff_names:
        st.error("No active staff found in the staff sheet.")
        st.stop()

    # Show who's out (unchanged timestamp logic)
    df_logs = load_logs_df()
    out_now = current_people_out(df_logs)

    st.markdown("#### Who’s Out Right Now")
    if out_now.empty:
        st.info("No one is currently signed out.")
    else:
        show = out_now[["name", "reason", "other_reason", "timestamp"]].copy()
        show["timestamp"] = show["timestamp"].astype(str)
        st.dataframe(show, use_container_width=True, hide_index=True)

    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        name = st.selectbox("Name", staff_names, key="people_name")
    with col2:
        pin = st.text_input("4-digit code", type="password", key="people_pin")

    reason = st.selectbox("Reason", PEOPLE_REASONS, key="people_reason")
    other_reason = ""
    if reason == "Other":
        other_reason = st.text_input("If Other, write reason", key="people_other_reason")

    # Determine current status
    current_status = "IN"
    if not out_now.empty and name in out_now["name"].values:
        current_status = "OUT"

    action_label = "Sign Out" if current_status == "IN" else "Sign In"

    if st.button(action_label, type="primary", key="people_submit"):
        pin_norm = normalize_pin(pin)
        expected = pins.get(name, "")
        if not expected:
            st.error("Name not recognized. Check the staff sheet.")
        elif pin_norm != expected:
            st.error("Wrong code entered.")
        else:
            if current_status == "IN":
                append_people_log(name=name, reason=reason, other_reason=other_reason, action="OUT", status="OUT")
                st.success(f"{name} successfully signed out.")
            else:
                append_people_log(name=name, reason="", other_reason="", action="IN", status="IN")
                st.success(f"{name} successfully signed in.")


# -------------------------
# VANS TAB
# -------------------------
with tabs[1]:
    st.subheader("Van Sign Out")

    staff_df = load_staff_df()
    pins = staff_pin_map()
    staff_names = staff_df["name"].tolist()

    df_vans = load_vans_df()
    van_status = current_vans_status(df_vans)
    out_vans = [v for v, s in van_status.items() if s == "OUT"]
    available_van = first_available_van(van_status)

    st.markdown("#### Vans Out Right Now")
    if not out_vans:
        st.info("No vans are currently signed out.")
    else:
        # show the most recent OUT row per van
        df = df_vans.copy()
        df = df.sort_values("timestamp")
        last = df.groupby("van", as_index=False).tail(1)
        last_out = last[last["action"].astype(str).str.upper() == "OUT"]
        if last_out.empty:
            st.info("No vans are currently signed out.")
        else:
            show = last_out[["van", "driver", "purpose", "other_reason", "passengers", "timestamp"]].copy()
            show["timestamp"] = show["timestamp"].astype(str)
            st.dataframe(show, use_container_width=True, hide_index=True)

    st.divider()

    # --- SIGN OUT SECTION (no van selection; auto-assign)
    st.markdown("### Sign Out a Van")
    if available_van is None:
        st.warning("All vans are currently signed out. Please sign one back in before signing out another.")
    else:
        st.caption(f"Next available: **{available_van}** (auto-assigned)")

        with st.form("van_sign_out_form", clear_on_submit=False):
            driver = st.selectbox("Driver", staff_names, key="van_driver")
            driver_pin = st.text_input("Driver 4-digit code", type="password", key="van_driver_pin")

            purpose = st.selectbox("Purpose", VAN_PURPOSES, key="van_purpose")
            other = ""
            if purpose == "Other":
                other = st.text_input("If Other, write purpose", key="van_other_purpose")

            st.markdown("**Passengers** (each passenger must enter their own code)")
            passenger_count = st.number_input("Number of passengers (not including driver)", min_value=0, max_value=12, value=0, step=1)

            passengers = []
            passenger_pins = []
            used_names = {driver}

            for i in range(int(passenger_count)):
                c1, c2 = st.columns([2, 1])
                with c1:
                    # filter out already used
                    options = [n for n in staff_names if n not in used_names]
                    p_name = st.selectbox(f"Passenger {i+1}", options, key=f"p_name_{i}")
                with c2:
                    p_pin = st.text_input(f"Code {i+1}", type="password", key=f"p_pin_{i}")
                used_names.add(p_name)
                passengers.append(p_name)
                passenger_pins.append(p_pin)

            submit = st.form_submit_button("Sign Out Van", type="primary")

        if submit:
            # Validate driver
            if normalize_pin(driver_pin) != pins.get(driver, ""):
                st.error("Wrong driver code entered.")
            else:
                # Validate passengers
                bad = []
                for p_name, p_pin in zip(passengers, passenger_pins):
                    if normalize_pin(p_pin) != pins.get(p_name, ""):
                        bad.append(p_name)

                if bad:
                    st.error("Wrong code entered for: " + ", ".join(bad))
                else:
                    if purpose == "Other" and not other.strip():
                        st.error("Please write the purpose for Other.")
                    else:
                        append_van_log(
                            van=available_van,
                            driver=driver,
                            purpose=purpose,
                            other_reason=other.strip(),
                            action="OUT",
                            passengers=passengers,
                        )
                        st.success(f"{available_van} successfully signed out under {driver}.")

    # --- SIGN IN SECTION (only appears when at least one van out)
    if out_vans:
        st.divider()
        st.markdown("### Sign In a Van")

        # No initial van dropdown; render one card/button per out van.
        df = df_vans.copy().sort_values("timestamp")
        last = df.groupby("van", as_index=False).tail(1)
        last_out = last[last["action"].astype(str).str.upper() == "OUT"].set_index("van")

        for v in out_vans:
            info = last_out.loc[v] if v in last_out.index else None
            label = v
            if info is not None:
                label = f"{v} (out under {info.get('driver','')})"

            with st.expander(label, expanded=True):
                returning_driver = st.selectbox("Returning driver", staff_names, key=f"ret_driver_{v}")
                returning_pin = st.text_input("Returning driver 4-digit code", type="password", key=f"ret_pin_{v}")

                if st.button(f"Sign In {v}", key=f"sign_in_btn_{v}"):
                    if normalize_pin(returning_pin) != pins.get(returning_driver, ""):
                        st.error("Wrong code entered.")
                    else:
                        append_van_log(
                            van=v,
                            driver=returning_driver,
                            purpose="",
                            other_reason="",
                            action="IN",
                            passengers=[],
                        )
                        st.success(f"{v} successfully signed in.")


# -------------------------
# ADMIN TAB
# -------------------------
with tabs[2]:
    st.subheader("Admin")

    if not ADMIN_PASSWORD:
        st.info("Admin password not configured. Add ADMIN_PASSWORD in Streamlit secrets to enable admin actions.")
    else:
        admin_pw = st.text_input("Admin password", type="password")
        if admin_pw != ADMIN_PASSWORD:
            st.warning("Enter admin password to view admin tools.")
        else:
            st.success("Admin unlocked.")

            st.markdown("### View People Logs")
            df_logs = load_logs_df()
            st.dataframe(df_logs.sort_values("timestamp", ascending=False), use_container_width=True, hide_index=True)

            st.markdown("### View Van Logs")
            df_vans = load_vans_df()
            st.dataframe(df_vans.sort_values("timestamp", ascending=False), use_container_width=True, hide_index=True)

            st.caption("Tip: If you edit the staff sheet, changes may take up to ~30 seconds to reflect here due to caching.")
