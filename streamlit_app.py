import streamlit as st
import pandas as pd
from datetime import datetime
from pathlib import Path

# -----------------------------------
# Basic settings you can customize
# -----------------------------------

DATA_FILE = "signouts.csv"
ADMIN_PASSWORD = "crest123"  # CHANGE THIS to something you and directors know

# Optional: put common counselor names here, or leave empty.
COUNSELOR_NAMES = [
    "Counselor A",
    "Counselor B",
    "Counselor C",
    "Counselor D",
]

BUNK_CHOICES = [str(i) for i in range(1, 21)]  # bunks 1–20; edit as needed

DATA_COLUMNS = [
    "record_id",
    "counselor_name",
    "bunk",
    "reason",
    "destination",
    "time_out",
    "expected_return",
    "time_in",
    "status",
]


# -----------------------------------
# Helpers for loading / saving data
# -----------------------------------

def load_data() -> pd.DataFrame:
    path = Path(DATA_FILE)
    if path.exists():
        df = pd.read_csv(path)
        # parse dates if present
        for col in ["time_out", "expected_return", "time_in"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df
    else:
        return pd.DataFrame(columns=DATA_COLUMNS)


def save_data(df: pd.DataFrame) -> None:
    df.to_csv(DATA_FILE, index=False)


def next_record_id(df: pd.DataFrame) -> int:
    if df.empty:
        return 1
    else:
        return int(df["record_id"].max()) + 1


# -----------------------------------
# UI Components
# -----------------------------------

def show_sign_out_in_page():
    st.title("Counselor Sign-Out")

    st.caption("This app should stay open at the Big House. "
               "Counselors must sign out when leaving camp and sign back in when they return.")

    df = load_data()

    tab_out, tab_in = st.tabs(["Sign OUT", "Sign IN"])

    # ------------- SIGN OUT TAB -------------
    with tab_out:
        st.subheader("Sign OUT")

        with st.form("sign_out_form", clear_on_submit=True):
            # Name selection
            col_name1, col_name2 = st.columns([2, 1])
            with col_name1:
                use_dropdown = st.checkbox("Choose name from list", value=True)
            with col_name2:
                st.write("")  # spacer

            if use_dropdown and COUNSELOR_NAMES:
                counselor_name = st.selectbox("Counselor name", COUNSELOR_NAMES)
            else:
                counselor_name = st.text_input("Counselor name")

            bunk = st.selectbox("Bunk", BUNK_CHOICES + ["Other"], index=0)

            reason = st.selectbox(
                "Reason for leaving",
                [
                    "Day off",
                    "Medical appointment",
                    "Errand for camp",
                    "Family emergency",
                    "Other",
                ],
            )

            destination = st.text_input("Destination (where are you going?)")

            col_time1, col_time2 = st.columns(2)
            with col_time1:
                expected_return_time_str = st.text_input(
                    "Expected return time (e.g. 6:30 PM)", ""
                )
            with col_time2:
                now_str = datetime.now().strftime("%Y-%m-%d %I:%M %p")
                st.caption(f"Time OUT will be recorded as: **{now_str}**")

            confirmed = st.checkbox(
                "I confirm my bunk is covered while I am out.",
            )

            submitted = st.form_submit_button("Sign OUT")

            if submitted:
                if not counselor_name:
                    st.error("Please enter your name.")
                elif not destination:
                    st.error("Please enter your destination.")
                elif not confirmed:
                    st.error("You must confirm your bunk is covered.")
                else:
                    # create new record
                    record_id = next_record_id(df)
                    time_out = datetime.now()

                    # try to parse expected return
                    if expected_return_time_str.strip():
                        try:
                            today_str = datetime.now().strftime("%Y-%m-%d")
                            full_str = f"{today_str} {expected_return_time_str}"
                            expected_return = datetime.strptime(
                                full_str, "%Y-%m-%d %I:%M %p"
                            )
                        except Exception:
                            expected_return = pd.NaT
                    else:
                        expected_return = pd.NaT

                    new_row = {
                        "record_id": record_id,
                        "counselor_name": counselor_name,
                        "bunk": bunk,
                        "reason": reason,
                        "destination": destination,
                        "time_out": time_out,
                        "expected_return": expected_return,
                        "time_in": pd.NaT,
                        "status": "OUT",
                    }

                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    save_data(df)

                    st.success(f"{counselor_name} signed OUT at {time_out.strftime('%I:%M %p')}.")

    # ------------- SIGN IN TAB -------------
    with tab_in:
        st.subheader("Sign IN")

        df = load_data()  # reload in case it changed
        out_now = df[df["status"] == "OUT"].copy()

        if out_now.empty:
            st.info("No counselors are currently signed OUT.")
        else:
            out_now_display = out_now.copy()
            out_now_display["time_out"] = out_now_display["time_out"].dt.strftime("%Y-%m-%d %I:%M %p")
            out_now_display["expected_return"] = out_now_display["expected_return"].dt.strftime("%Y-%m-%d %I:%M %p")
            out_now_display = out_now_display[
                ["record_id", "counselor_name", "bunk", "destination", "time_out", "expected_return"]
            ]
            st.write("Currently OUT:")
            st.dataframe(out_now_display, use_container_width=True)

            for _, row in out_now.iterrows():
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.write(
                        f"**{row['counselor_name']}** (Bunk {row['bunk']}) – "
                        f"OUT since {row['time_out'].strftime('%I:%M %p')} "
                        f"to {row['destination']}"
                    )
                with col2:
                    if st.button(
                        "Sign IN",
                        key=f"sign_in_{row['record_id']}",
                    ):
                        df.loc[df["record_id"] == row["record_id"], "time_in"] = datetime.now()
                        df.loc[df["record_id"] == row["record_id"], "status"] = "IN"
                        save_data(df)
                        st.success(f"{row['counselor_name']} signed back IN.")
                        st.experimental_rerun()


def show_admin_page():
    st.title("Admin / History")

    st.caption("For leadership only: view and export full sign-out history.")

    password = st.text_input("Admin password", type="password")
    if password != ADMIN_PASSWORD:
        st.info("Enter the admin password to view data.")
        return

    df = load_data()
    if df.empty:
        st.info("No records yet.")
        return

    # nice formatting
    df_display = df.copy()
    for col in ["time_out", "expected_return", "time_in"]:
        if col in df_display.columns:
            df_display[col] = df_display[col].apply(
                lambda x: x.strftime("%Y-%m-%d %I:%M %p") if pd.notna(x) else ""
            )

    st.subheader("All Records")
    st.dataframe(df_display, use_container_width=True)

    # Download CSV
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download CSV",
        data=csv_bytes,
        file_name="counselor_signout_history.csv",
        mime="text/csv",
    )

    st.markdown("---")
    st.subheader("Danger zone")

    confirm_clear = st.checkbox("I understand this will delete ALL records.")
    if confirm_clear and st.button("Clear ALL data"):
        empty_df = pd.DataFrame(columns=DATA_COLUMNS)
        save_data(empty_df)
        st.success("All data cleared.")
        st.experimental_rerun()


# -----------------------------------
# Main
# -----------------------------------

def main():
    st.set_page_config(
        page_title="Counselor Sign-Out",
        page_icon="🚪",
        layout="wide",
    )

    page = st.sidebar.radio(
        "Go to",
        ["Sign Out / In", "Admin"],
    )

    if page == "Sign Out / In":
        show_sign_out_in_page()
    elif page == "Admin":
        show_admin_page()


if __name__ == "__main__":
    main()
