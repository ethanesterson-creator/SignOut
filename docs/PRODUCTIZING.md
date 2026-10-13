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

## Status (2026-10-13)

### Done
- [x] Decisions: separate deployment + data per camp, per-summer price, Ethan operates
- [x] `camp_config.py`: reasons + deadline rules, vans, schedule, thresholds, hours
- [x] Onboarding generator `provision/new_camp.py` + intake form
- [x] Hosting trial on Render Starter ($7/mo): memory flat ~150 MB of 512 MB, CPU ~0 -> **Starter is enough**
- [x] `start.sh` + `docs/DEPLOY_RENDER.md`
- [x] Operator alerts (`opswatch.py`): Sheets unreachable, saves stuck, app errors, restarts, morning check-in
- [x] `stable` branch + update/rollback procedure (`docs/OPERATIONS.md`)
- [x] Nightly backup script (`provision/backup_camp.py`), tested against the test Sheet
- [x] Kiosk clock moved off Streamlit's deprecated embed to `st.iframe` (identical look, still ticking)
- [x] Camp discovery script for calls and visits (`docs/CAMP_DISCOVERY.md`)
- [x] Drafted: email to boss (ACA contact's address), email to the ACA contact (Ethan sends)

### Brand (decided 2026-10-13)
Product name **Headcount**, domain **headcountcamp.com** (bought). Planned addresses: `<camp>.headcountcamp.com` per camp,
`demo.headcountcamp.com` for the demo, the bare domain for a landing page later. Still to do: trademark search
(uspto.gov) before spending on a logo; professional email on the domain. The app itself still says "SignOut".

- [x] Demo camp: separate Google project + Sheet created, seeded with 36 fake staff by `provision/seed_sheet.py`; verified locally (sign out 327 ms, sign in 277 ms). Next: deploy it on Render as `demo.headcountcamp.com`.

### Needs you (small)
- [x] Ops channel + ntfy subscribed (done)
- [x] Outside uptime checker (Better Stack free) on the Render trial (done)
- [ ] Schedule `backup_camp.py` nightly (Windows Task Scheduler) once a real camp exists
- [x] Render trial follows `main` (done)
- [ ] Send the two emails; record a 2-minute demo video
- [x] Name + domain (Headcount / headcountcamp.com)
- [ ] Price, LLC, Stripe

### Harder, in the order I'd do them
1. ~~Code-guessing protection~~ **Built (branch `security/code-guess-limit`), tested live.** 10 wrong codes in 2
   minutes, or 25 in an hour, pauses code entry for 60 seconds (longer on repeats, up to 15 min); the admin
   password is stricter (5 in 5 min). You get a "Repeated wrong codes" alert. It slows and flags guessing;
   it does not make a 4-digit code strong. A guesser still has a decent chance of hitting SOME staff code
   over hours. Bigger fix if a camp needs it: 6-digit codes (a decision for later).
2. **Camp look: colors + logo.** theme.py hardcodes Bauercrest navy in many places; make it driven
   by a few per-camp colors without changing Bauercrest's look.
3. **A public demo camp** with fake staff, for the ACA contact and pilots to click through.
4. **Different schedule per day** (weekday/weekend/trip day). Wait for discovery calls to confirm demand.
5. **Fleet view:** one page listing every camp, its version, last check-in and health.
6. **Self-serve onboarding:** a form the director fills, that triggers the generator and deploy.
7. **Billing and renewal:** Stripe subscription per summer, off-season hibernate, renewal reminders.
8. **Legal:** terms of service, privacy policy, liability wording, insurance check.
9. **Optional alert channels:** SMS/email to a camp director's own phone (cost per message).
10. **Support:** a runbook of common fixes, a support inbox, a simple status page.

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
