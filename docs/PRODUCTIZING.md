# Selling SignOut to other camps: plan and status

Decisions (2026-10-13): every camp gets its **own deployment and its own Google Sheet**
(no shared multi-tenant app, no connected data), priced **per summer**, operated by
Ethan, who wants every failure alert on his phone. One codebase, never forks.

Principle: **structural settings** (name, timezone, reasons and their deadline rules,
vans, thresholds) live in the deployment's `secrets.toml` via `camp_config.py`.
**Day-to-day data** (staff, drivers, days off, period schedule) lives in the camp's own
Sheet, which the camp edits.

## Roadmap (working back from June 2027)

| When | Work |
|---|---|
| Oct-Dec 2026 | Make it packageable: config, Sheet template + intake form, provisioning checklist -> script, monitoring/alerts, staged updates + rollback, nightly Sheet backups |
| Jan-Mar 2027 | 2-3 pilot camps (free/discounted), ACA contact, LLC + Stripe + terms/privacy |
| Apr-May 2027 | White-glove onboarding of each camp, test day with their real staff |
| Jun-Aug 2027 | Live; Ethan watches alerts |
| Sep+ | Hibernate servers, archive, send renewals |

## Status

- [x] Audit: what is Bauercrest-specific (below)
- [x] `camp_config.py`: reasons + deadline rules, van purposes, default schedule, board
      label, thresholds, hours, spreadsheet id guard. Defaults = Bauercrest, proven
      identical to the old hardcoded rules by `tests/test_camp_config.py`.
- [ ] Brand color per camp (theme.py is owned by the UI work; needs a coordinated change)
- [x] Intake form (`provision/intake_template.toml`) and onboarding generator
      (`provision/new_camp.py`): validates the intake, writes a ready `secrets.toml`
      (random admin password, PIN salt and notification topics), staff import with
      unique codes, the director email and a per-camp checklist. Never talks to Google.
      Tested; also checked by booting the app as a fake "Camp Example".
- [ ] Hosting chosen + one-command deploy of a camp's instance
- [ ] Health reporting + alerts (uptime, app heartbeat, daily digest) to Ethan's phone
- [ ] Staged updates (Bauercrest first) + rollback
- [ ] Nightly Sheet backups
- [ ] Pick product name + domain, host, price structure

## Audit: what was Bauercrest-specific

Already configurable before this work: camp name, tagline, logo, notification prefix,
CSV prefix, timezone, PIN salt, vans and van labels, ntfy topics, admin password.

Hardcoded, now configurable via `camp_config.py`: the reason list; how each reason
sets its deadline (Period Off / Night Off / Day Off rules); van purposes; default
period schedule; the "Big House board" label; forgot-to-sign-in threshold; stale-fork
hours; nightly summary hour; auto-archive hour and keep-days; the Sheet ID (a camp
other than Bauercrest now **must** set its own).

Still Bauercrest-specific (not yet changed):
- Navy brand color and the flag/"signal locker" look (theme.py)
- The logo file `logo-header-2.png` is Bauercrest's (set `camp_logo_path` per camp)
- Sidebar text "Staff Sign-Out" and a few UI strings (generic enough to keep)
- Group sign-out and Day Off board assume the same reason vocabulary

## Per-camp settings reference (secrets.toml)

```toml
camp_name = "Camp Example"            # required for any camp other than Bauercrest
spreadsheet_id = "..."               # required for any camp other than Bauercrest
camp_board_label = "The Lodge board"
forgot_threshold_minutes = 180
fork_no_deadline_hours = 14
nightly_summary_hour = 0              # 0-23
auto_archive_hour = 3                 # 0-23
archive_keep_days = 3
van_purposes = ["Period Off", "Field Trip", "Other"]
default_schedule = [["Period 1", "09:00", "09:50"], ["Period 2", "10:00", "10:50"]]

# Rules: none | period_end | next_clock | day_after
[[reasons]]
name = "Period Off"
rule = "period_end"
grace_min = 5            # back this many minutes before the period ends

[[reasons]]
name = "Night Off"
rule = "next_clock"
hour = 0
minute = 15              # back the next time the clock reads 12:15 AM

[[reasons]]
name = "Day Off"
rule = "day_after"
hour = 7
minute = 45              # back 7:45 AM the morning after the day off
nextday_cutoff_hour = 12 # signing out at/after noon means tomorrow is the day off

[[reasons]]
name = "Other (type reason)"   # shows a "type a reason" box
```


## Onboarding a camp (today)

1. Copy `provision/intake_template.toml` to `provision/intake_<camp>.toml`, fill it in.
2. Create the camp's own Google service account; put its key path in the intake.
3. `.venv/Scripts/python.exe provision/new_camp.py provision/intake_<camp>.toml`
   -> `camps/<camp>/director_email.txt` and `CHECKLIST.md`. Send the email.
4. When the director replies with the Sheet link, add it (and the staff CSV) to the intake,
   run the command again -> `secrets.toml`, `staff_import.csv`, `credentials.txt`.
5. Follow `CHECKLIST.md`. `camps/` is git-ignored: it holds secrets.

Known limit: the Google Sheets API allows roughly 300 reads/minute per Google Cloud
project across all its service accounts. With one shared project that caps the fleet at
roughly 10-20 camps (each kiosk's background refresh uses part of it; not yet measured).
Beyond that, give each camp its own Google Cloud project or request a quota increase.
