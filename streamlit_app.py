import streamlit as st
import pandas as pd
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo  # timezone support

# --------------- Settings ---------------

DATA_FILE = "signouts.csv"  # CSV stored next to this file

REASONS = [
    "Day off",
    "Period off",
    "Night off",
    "Other",
]

# Staff list used for the name dropdown.
# Streamlit lets you type a few letters to filter this dropdown.
# >>> Add / remove names in this list as needed. <<<
STAFF_NAMES = [
    "Ethan Esterson",
    "Asher Schiillin",
    "Jaden Pollack",
    "Ethan Goldberg",
    "Colby Karp",
    "Jordan Bornstein",
    "Dylan Israel",
    "Zach Baum",
    "Darren Sands",
    "Brody Masters",
    "Matt Schultz",
    "Max Pollack",
    "Will Carp",
    "Josh Poscover",
    "Evan Ashe",
    "Riley Schneller",
    "Joey Rosenfeld",
    "Justin Feldman",
]

HISTORY_PASSWORD = "Hyaffa26"  # password to view/download/delete history

DATA_COLUMNS = [
    "record_id",
    "name",
    "reason",
    "other_reason",
    "full_reason",
    "time_out",
    "time_in",
    "status",
]

EASTERN = ZoneInfo("America/New_York")


def now_eastern_naive() -> datetime:
    """Return current time in Eastern, but without timezone info (stored as plain Eastern)."""
    return datetime.now(EASTERN).replace(tzinfo=None)


# ------------- Data helpers -------------


def load_data() -> pd.DataFrame:
    path = Path(DATA_FILE)
    if path.exists():
        df = pd.read_csv(path)
        for col in ["time_out", "time_in"]:
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
    return int(df["record_id"].max()) + 1


# ------------- Page: Sign In / Out -------------


def page_sign_in_out():
    df = load_data()

    st.title("Counselor Sign-Out")
    st.caption(
        "This app should stay open at the Big House. "
        "Counselors MUST sign out when leaving camp and sign back in when they return."
    )

    # ---------- Sign OUT section ----------
    st.subheader("Sign OUT")

    with st.form("sign_out_form", clear_on_submit=True):
        # Name dropdown with type-to-search
        name_options = ["-- Select name --"] + STAFF_NAMES + ["Other (not listed)"]
        name_choice = st.selectbox(
            "Your name (type to search)",
            name_options,
        )

        manual_name = ""
        if name_choice == "Other (not listed)":
            manual_name = st.text_input("Type your name")

        # Final name value we will use
        if name_choice == "-- Select name --":
            name = manual_name.strip()  # will be empty if not filled
        elif name_choice == "Other (not listed)":
            name = manual_name.strip()
        else:
            name = name_choice.strip()

        reason = st.selectbox("Reason for leaving", REASONS)

        other_reason = ""
        if reason == "Other":
            other_reason = st.text_input("Describe reason")

        submitted = st.form_submit_button("Sign OUT")

        if submitted:
            if not name:
                st.error("Please select or type your name.")
            elif reason == "Other" and not other_reason.strip():
                st.error("Please type the reason for 'Other'.")
            else:
                record_id = next_record_id(df)
                time_out = now_eastern_naive()  # Eastern, stored as plain datetime

                if reason == "Other":
                    full_reason = other_reason.strip()
                else:
                    full_reason = reason

                new_row = {
                    "record_id": record_id,
                    "name": name,
                    "reason": reason,
                    "other_reason": other_reason.strip(),
                    "full_reason": full_reason,
                    "time_out": time_out,
                    "time_in": pd.NaT,
                    "status": "OUT",
                }

                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                save_data(df)

                st.success(
                    f"{name} signed OUT at {time_out.strftime('%Y-%m-%d %I:%M %p')} "
                    f"for: {full_reason}"
                )

    st.markdown("---")

    # ---------- Currently OUT section ----------
    st.subheader("Currently OUT")

    df = load_data()  # reload in case someone just signed out
    out_now = df[df["status"] == "OUT"].copy()

    if out_now.empty:
        st.info("No counselors are currently signed out.")
    else:
        # Nice display table
        display = out_now.copy()
        display["time_out"] = display["time_out"].apply(
            lambda x: x.strftime("%Y-%m-%d %I:%M %p") if pd.notna(x) else ""
        )
        display = display[["record_id", "name", "full_reason", "time_out"]]
        display = display.rename(
            columns={
                "record_id": "ID",
                "name": "Name",
                "full_reason": "Reason",
                "time_out": "Time OUT",
            }
        )
        st.dataframe(display, use_container_width=True)

        st.write("Click **Sign IN** next to your name when you return:")

        for _, row in out_now.iterrows():
            col1, col2 = st.columns([4, 1])
            with col1:
                out_time_display = (
                    row["time_out"].strftime("%I:%M %p") if pd.notna(row["time_out"]) else "Unknown"
                )
                st.write(
                    f"**{row['name']}** – {row['full_reason']} "
                    f"(OUT since {out_time_display})"
                )
            with col2:
                if st.button("Sign IN", key=f"sign_in_{row['record_id']}"):
                    now_in = now_eastern_naive()
                    df.loc[df["record_id"] == row["record_id"], "time_in"] = now_in
                    df.loc[df["record_id"] == row["record_id"], "status"] = "IN"
                    save_data(df)
                    st.success(f"{row['name']} signed back IN at {now_in.strftime('%I:%M %p')}.")
                    st.rerun()

    # ---------- History (password protected) ----------
    with st.expander("History (for leadership – password required)"):
        password = st.text_input("Enter password to view history", type="password")

        if password == HISTORY_PASSWORD:
            df_hist = load_data()
            if df_hist.empty:
                st.write("No records yet.")
            else:
                hist = df_hist.copy()
                for col in ["time_out", "time_in"]:
                    hist[col] = hist[col].apply(
                        lambda x: x.strftime("%Y-%m-%d %I:%M %p") if pd.notna(x) else ""
                    )
                st.dataframe(hist, use_container_width=True)

                # Download CSV
                csv_bytes = hist.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download history CSV",
                    data=csv_bytes,
                    file_name="counselor_signout_history.csv",
                    mime="text/csv",
                )

                st.markdown("---")
                st.subheader("Delete logs")

                # Delete selected records
                # Build friendly labels for selection
                options = []
                for _, r in df_hist.iterrows():
                    out_str = r["time_out"].strftime("%Y-%m-%d %I:%M %p") if pd.notna(r["time_out"]) else "Unknown"
                    label = f"ID {r['record_id']} – {r['name']} – {r['full_reason']} – OUT {out_str}"
                    options.append((label, int(r["record_id"])))

                if options:
                    labels = [o[0] for o in options]
                    label_to_id = {o[0]: o[1] for o in options}
                    selected_labels = st.multiselect("Select records to delete", labels)
                    selected_ids = [label_to_id[l] for l in selected_labels]

                    if selected_ids and st.button("Delete selected records"):
                        df_new = df_hist[~df_hist["record_id"].isin(selected_ids)]
                        save_data(df_new)
                        st.success(f"Deleted {len(selected_ids)} record(s).")
                        st.rerun()

                st.markdown("#### Delete ALL logs")
                confirm_all = st.checkbox("I understand this will delete EVERY record.")
                if confirm_all and st.button("Delete ALL logs now"):
                    empty_df = pd.DataFrame(columns=DATA_COLUMNS)
                    save_data(empty_df)
                    st.success("All logs deleted.")
                    st.rerun()

        elif password:
            st.error("Incorrect password.")


# ------------- Page: Out Board -------------


def page_out_board():
    st.title("Who’s Out Right Now")

    df = load_data()
    out_now = df[df["status"] == "OUT"].copy()

    if out_now.empty:
        st.info("No counselors are currently signed out.")
        return

    out_now = out_now.sort_values("time_out")

    st.markdown("### Currently OUT")
    for _, row in out_now.iterrows():
        out_time_display = (
            row["time_out"].strftime("%I:%M %p") if pd.notna(row["time_out"]) else "Unknown"
        )
        st.markdown(
            f"**{row['name']}** — {row['full_reason']} &nbsp;&nbsp; "
            f"(OUT since {out_time_display})"
        )


# ------------- Main -------------


def main():
    st.set_page_config(
        page_title="Counselor Sign-Out",
        page_icon="🚪",
        layout="wide",
    )

    # Bauercrest logo in sidebar if present
    logo_path = Path("logo-header-2.png")  # change if your logo file name is different
    if logo_path.exists():
        st.sidebar.image(str(logo_path), use_column_width=True)

    page = st.sidebar.radio(
        "Go to",
        ["Sign In / Out", "Out Board"],
    )

    if page == "Sign In / Out":
        page_sign_in_out()
    elif page == "Out Board":
        page_out_board()


if __name__ == "__main__":
    main()
