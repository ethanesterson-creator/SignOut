# Setting up SignOut for a new camp

SignOut is a single Streamlit app backed by one Google Sheet. Every
camp-specific detail — identity, timezone, van fleet, notifications — is
configured through `secrets.toml`, not code. This doc walks through taking
the app from zero to a working kiosk for a new camp.

## 1. Create the Google Sheet and service account

1. Create a new Google Sheet. This will hold all of the camp's data — it can
   start completely empty; the app builds every tab it needs (see step 4).
2. In Google Cloud Console, create a service account with the **Google
   Sheets API** enabled, and generate a JSON key for it.
3. Share the new Sheet with the service account's email address (found in
   the JSON key as `client_email`), with **Editor** access.
4. Copy the Sheet's ID out of its URL — the long string between `/d/` and
   `/edit`.

## 2. Configure `secrets.toml`

Create `.streamlit/secrets.toml` (this file is gitignored — never commit it)
with at minimum:

```toml
spreadsheet_id = "your-sheet-id-here"
admin_password = "choose-a-password"

[gcp_service_account]
# paste the entire contents of the service account JSON key here
```

Then set whichever of these apply — every one has a Bauercrest-shaped
default, so a new camp only needs to override what's actually different:

```toml
camp_name = "Camp Example"
camp_tagline = "Somewhere, ST &middot; Est. 1985"
camp_logo_path = "logo-header-2.png"   # a PNG in the repo root, sidebar logo
camp_notify_prefix = "Example"          # prefixes every push notification title
camp_csv_prefix = "example"             # prefixes every downloaded CSV filename
camp_timezone = "US/Eastern"            # any pytz timezone name
pin_salt = "any-random-string"          # unique per camp; see step 5

vans = ["Van 1", "Van 2"]
[van_labels]
"Van 1" = "Van 1 (Blue)"
"Van 2" = "Van 2 (Green)"

# Optional push notifications via ntfy.sh (or a self-hosted ntfy server).
# Leave unset and notifications are simply skipped.
ntfy_topic = "your-camp-main-phone-topic"
ntfy_topic_vans = "your-camp-vans-phone-topic"
ntfy_server = "https://ntfy.sh"
```

## 3. Run the app and unlock Admin

Start the app (`streamlit run streamlit_app.py`) and open **Admin /
History**. Log in with the `admin_password` you set above — this works
before any staff exist, since it's a shared password from secrets, not a
staff PIN.

## 4. Create the sheet's tabs

Open the **New Camp Setup** expander at the top of the Admin page and click
**Create Any Missing Tabs**. This builds every tab the app needs — `staff`,
`drivers`, `days_off`, `logs`, `vans`, plus the tabs the app manages on its
own (`settings`, `schedule`, `current_status`, `logs_archive`,
`vans_archive`, `audit_log`) — with the correct headers. It only creates
what's missing, so it's safe to run again later.

## 5. Add staff

In the `staff` tab, add one row per counselor: `name`, `pin` (a 4-digit
code), `active` (TRUE/blank), `admin` (TRUE for anyone who should have admin
access via their own code, blank otherwise). Staff can start signing in and
out immediately with these PINs.

Once real staff are entered, go to Admin → **Staff PIN Security** and run
**Hash All Plaintext PINs Now**. This replaces every plaintext PIN in the
sheet with a one-way hash, so anyone who later gets read access to the
sheet can't see anyone's actual code. Set a unique `pin_salt` per camp
(step 2) before doing this — it's what keeps one camp's hashes from being
replayable against another's.

## 6. Add drivers and days off (optional)

- `drivers`: `name`, `passed_test` (TRUE for anyone cleared to drive a van).
- `days_off`: `name`, `weekday`, `active` — for the passive Day Off display
  on the Who's Out board. Skip this tab entirely if the camp doesn't use it.

## 7. Set the schedule (optional)

The `schedule` tab is seeded with a placeholder schedule in step 4. Edit it
directly in Google Sheets with the camp's actual period times — no code
change needed. The app re-reads it live.

That's it — the kiosk is ready for staff to use.
