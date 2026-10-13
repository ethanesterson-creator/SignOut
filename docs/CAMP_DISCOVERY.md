# Camp discovery script

For a call or a camp visit. Goal: learn how THIS camp works, decide whether SignOut fits, and
come away with everything needed to fill the intake form (`provision/intake_template.toml`).
Ask open questions, write down their words. The `->` after a question says where the answer lands.

**Bauercrest, in one breath (use as the example when they go blank):** about 60 counselors sign out
with a 4-digit code on a shared Chromebook in the main building. Reasons carry deadlines
(Period Off back before the period ends, Night Off back by 12:15 AM, Day Off back 7:45 the next
morning). The board shows who is out and who is late. Three vans, each tracked with driver,
passengers and gas. Group sign-outs for trips. Any admin can declare a campwide emergency that
takes over every screen. The office gets a phone alert when someone is late.

---

## 0. Open (2 min)
- Walk me through what happens today when a counselor leaves camp, from "I'm heading out" to "I'm back".
- Who needs to know, and how do they find out?
- What went wrong last summer that made you wish you had something better?
  -> *the real reason they'd buy; use their words in your pitch*

## 1. Who and how many
- How many staff in a typical summer? How many on camp at the same time?  -> *size; over ~300 needs a check*
- Do staff turn over mid-summer (second-session hires, CITs, volunteers)? Who adds and removes people?  -> *staff import / admin*
- Who would be tracked: counselors only, or also specialists, kitchen, maintenance, nurses, CITs?
- **Do you want campers tracked?** -> *Not supported, and a different level of responsibility. Say so plainly.*
- Who are the "office" people who'd run it day to day? How many admins?  -> *admin flags*

## 2. Leaving camp: the rules
- What are the reasons someone can leave? Names they already use ("period off", "night off", "day off", "town run", "doctor"...)  -> *`[[reasons]]` names*
- For each one: is there a time they have to be back? How is it decided?
  - back before a period ends  -> `period_end` (minutes before)
  - back at a clock time that night or morning  -> `next_clock`
  - a full day off, back the next morning  -> `day_after` (what time, and where does "noon" cutoff fall)
  - no deadline  -> `none`
- Is there a "type your own reason" option you want, or should it be a fixed list only?  -> *keep or drop "Other"*
- What counts as "late"? When do you stop calling it late and assume they forgot to sign in? (Bauercrest: 3 hours)  -> *`forgot_threshold_minutes`*
- Who gets notified when someone is late, and how fast?  -> *alert recipients*
- Any limits (max people out at once, no one leaves during certain activities)?  -> *note it: not supported today*

## 3. The daily schedule
- Do you run periods or blocks? Names and times.  -> *schedule*
- Is the schedule the same every day? What about weekends, trip days, rainy days, special events?
  -> *Today there is ONE schedule for every day. If they have several, that's a feature to build; note it.*
- When does the "day" start and end? Curfew time?  -> *Night Off due time*

## 4. Vehicles
- Do staff drive camp vehicles? How many, what do you call them (vans, buses, trucks, a boat)?  -> *`vans`, `van_labels`*
- Anything you'd want to record per trip: driver, passengers, destination or purpose, mileage, gas?  -> *Bauercrest: driver, passengers, purpose, gas left*
- Who is allowed to drive? Is there a "passed the driving test" list?  -> *drivers tab*
- Do you want vans tracked at all, or only people? (Fine to switch it off.)
- What are the common trip purposes?  -> *`van_purposes`*

## 5. Groups and trips
- Does a leader take a group off camp (field trips, tournaments, hikes)? Do you want one person to sign out the whole group at once?  -> *group sign-out*

## 6. Days off
- Do staff have scheduled days off by weekday? Do you want a "who's on day off today" board?  -> *days_off tab*

## 7. Emergencies
- If there's an emergency, how do you reach and account for everyone today?
- Would a screen takeover ("campwide emergency in effect", message from the director, on every kiosk) help?  -> *emergency banner*
- Who is allowed to declare it?  -> *admin flags*

## 8. The device and the room
- Where would it live (office, dining hall, front gate, health center)? One spot or several?
- What device: Chromebook, tablet, old laptop, a TV with a keyboard? Screen size?  -> *kiosk fit check; Bauercrest is a small Chromebook*
- Is the wifi reliable there? What happens when it drops?  -> *offline risk*
- Do counselors have phones on them for the lateness alerts, or should alerts go to a person's phone only?  -> *notifications*
- Staff codes: 4-digit PIN OK? Any concern about someone using another person's code? (Bauercrest: one code per person, honor system.)

## 9. How it should look
- What are the camp's main colors? Do you have a logo file (PNG)? Any brand rules?  -> *colors and logo*
- Name and tagline for the footer (town, "Est." year)?  -> *`camp_name`, `camp_tagline`*
- What do you call the main spot, the "Big House" equivalent?  -> *`camp_board_label`*
- Anything on the screen that would feel wrong at your camp (words, flags, tone)?

## 10. Their tech
- **Do you use Google (Gmail / Google Drive) at the camp office?** Personal Gmail, or Google Workspace? -> *Needed: the camp's data lives in a Google Sheet they own.*
- If Workspace: can staff share a Sheet with an outside address? (Some schools and districts block it.)  -> *the likeliest setup snag*
- Who at camp is comfortable creating a Sheet and clicking Share? It takes 5 minutes.
- Do you already keep a staff list in a spreadsheet? Can we have it?  -> *staff import*

## 11. Data and trust
- What does your board or insurer say about where staff data is kept?
- Are you comfortable with the data living in your Google account, with us having access only as a service?
- How long do you keep records? Who can see the history?  -> *archive keep days*
- Anything you can't have stored (home addresses, phone numbers)? We store names, scrambled codes, and sign-out records only.

## 12. Buying
- Who decides? Anyone else who has to approve (board, parent organization, accountant)?
- When do you finalize summer purchases and budget?  -> *timing*
- What do you pay for software today, and how (invoice, card, purchase order)?
- What would make this an easy yes? What would make it a no?
- Would you run it free or discounted this first summer, in exchange for feedback and a quote you can share?  -> *pilot terms*

## 13. Close
- Read back what you heard in their words. Ask what you missed.
- "If this worked perfectly, what would be different on a Tuesday in July?"
- Agree the next step and the date: a short demo, a trial, or a second call with the director.

---

## Capture sheet (fill in as they talk, then paste into the intake)
```
Camp:                      Director:                 Email:
Town / tagline:            Timezone:
Staff total / on camp:     Admins:                   Mid-season hires? Y/N
Colors:                    Logo file? Y/N            Board name:
Reasons + rule:            1.            2.            3.            4.
Late/forgot threshold:     Alerts go to:
Schedule same daily? Y/N   Periods:
Vehicles? Y/N  Names:                 Per-trip details:
Driver list? Y/N           Trip purposes:
Group sign-out? Y/N        Days-off board? Y/N       Emergency banner? Y/N
Device / screen:           Wifi OK? Y/N
Google: personal or Workspace?         Can share outside? Y/N
Decision maker:            Budget time:              Pilot OK? Y/N
Wanted but we don't do:
```

## Fit check: be honest early
| Looks like a fit | Pause or pass |
|---|---|
| Staff-only sign-out, one or two kiosks | They want camper tracking or check-in |
| Uses Google or can create a Sheet | Microsoft-only, no way to use Google Sheets |
| A device with a browser and decent wifi | No reliable internet where the kiosk would sit |
| Under about 300 staff | A different schedule every day (not built yet; note it) |
| Wants the director alerted by phone | Needs text-message or email alerts only (today it's the ntfy phone app) |

## What we can't do yet (don't promise these)
- More than one daily schedule (one schedule applies to every day).
- Camper tracking.
- Text-message or email alerts (phone app alerts only).
- Colors and logo that fully match a camp's brand (Bauercrest navy today; this is on the build list).
- Languages other than English.
- Anything that isn't Google Sheets as the data store.
