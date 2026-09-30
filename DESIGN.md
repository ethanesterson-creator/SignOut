# SignOut: the signal locker

Status: **v2 (immersive) implemented** on `design/v2-immersive`. v2 supersedes the light/white v1 below: the canvas is now deep navy with gradient light, film grain, glass surfaces, Big Shoulders Display + Geist type, waving cloth flags (28 sliced columns on a travelling sine wave), a swaying signal-pennant string under every title, a live header clock, staggered entrances and a larger hoist confirmation. Status-by-shape and the colour roles are unchanged. (v1 text follows and is partly outdated.)

Previous status: **implemented** on branch `design/full-visual-revamp` (approved, with navy and white as the anchor). Tokens and CSS live in `theme.py`; render helpers in `streamlit_app.py`. Replaces the earlier collegiate / parchment system (in git history).

Built-state notes vs. the proposal: the "out, due soon" swallowtail was not needed and was dropped; the board shows a due-back time instead. The sidebar stays (Streamlit radio) but is restyled as the rail. Text inputs use Streamlit test-ids (`stTextInputRootElement`, etc.) rather than baseweb attributes, which this Streamlit version no longer emits.

Concept roll: seed key `d34b0ea9` (operate, direction scope); assigned candidate 6 of 7 (signal flags / muster board). Challengers were weighed and declined; none reached the build.

## The one idea: the signal locker

A camp on a lake speaks in flags. SignOut becomes a **ship's muster board**, built from the international maritime signal-flag system. That system is already navy and white, it was designed to be read across water at a glance, and one flag says exactly what this app says.

> **Blue Peter** (flag "P"): a navy field with a white center. Hoisted in harbor, it means *all aboard, we are about to sail*.

That is the sign-out moment. The whole interface is one disciplined grammar:

| Meaning | Flag shape | Fill | Never color alone |
|---|---|---|---|
| **In camp** | Square flag | White field, navy border | Hollow square |
| **Out** (on time) | Blue Peter | Navy field, white center | Solid square with inset |
| **Out, due soon** | Swallowtail | Navy + gold | Notched end |
| **Late / no sign-in** | Burgee (pennant) | Red + white bands | Pointed triangle |
| **Emergency** | Full-width banner, red and white halves | Red | Bar across every page |

Status is carried by **shape first, color second**, so it reads from across the room and for color-blind viewers. The shapes are small inline SVG, authored here. They are not emoji and not an icon library.

### What the design refuses
- Rounded SaaS cards in a 3-up grid, soft shadows, pills everywhere, centered everything.
- The current cream/parchment ground, gold outline pills, and "varsity" patches.
- Gradients, glass, and emoji-as-icons (the tab favicon `🏕️` becomes a flag).

## Color: Committed navy, one white, two signals

Strategy: **Committed.** Navy owns whole regions (the side rail, the active state, each person's flag). Everything else is white and thin rules. Bauercrest navy stays the anchor, as you asked, and remains the default that camp secrets can override.

```css
:root {
  --navy:        #13294B;  /* Bauercrest. rail, headlines, primary action */
  --navy-deep:   #0B1B33;  /* text on white, pressed states */
  --navy-tint:   #E8EDF5;  /* quiet fills, hover, table banding */
  --white:       #FFFFFF;  /* the page. sailcloth, not cream */
  --rule:        #C9D3E3;  /* hairlines and input borders */
  --ink-2:       #4A5B77;  /* secondary text. 7:1 on white */

  --signal-red:  #C8102E;  /* late, emergency, destructive */
  --red-tint:    #FCEBEE;
  --signal-gold: #F2B705;  /* due soon. FILL ONLY, never text on white */
  --gold-tint:   #FFF6D6;
  --gold-ink:    #5C4300;  /* text that sits on gold tints */
}
```

- Green is retired. "In camp" is white and quiet, because the normal state should not shout. Only exceptions earn color.
- Red and gold appear only for things a person must act on. A healthy board is almost entirely navy and white.
- Light, not dark. The scene is a shared computer in a bright room or hall in daylight; white wins on glare.
- Contrast targets: body 7:1, large text 4.5:1 minimum, all status pairs checked in the final pass.

## Typography

- **Barlow Condensed** (700/800/600): names on the board, page titles, flag labels. A road-sign-and-DIN-lineage face: tall, stencil-like in rhythm, readable at 3 meters. Used uppercase and tight for labels, mixed case for names.
- **Barlow** (400/500/600): body, forms, admin tables. Same skeleton as the display, so it never looks like two fonts fighting.
- **Tabular figures** (`font-variant-numeric: tabular-nums`) on every time, count, and PIN box, so the board does not jitter when it refreshes.
- Replaces Archivo and Public Sans. Type scale on a 1.25 ratio with a kiosk base of **18px** (up from ~16px): 14 / 18 / 22 / 28 / 36 / 48 / 64.

## Layout and composition

- **Left rail becomes the signal locker:** a slim full-height navy column. Logo at the top, five destinations as big tap rows (minimum 56px) with a flag glyph that fills when active, and the auto-refresh note at the foot. Navigation stays a Streamlit radio, so the routing and idle-return logic are untouched.
- **Left-aligned and asymmetric.** Each page has a strong left spine: title, the live clock as a tabular readout, then content. No centered hero stacks.
- **Who's Out is the flagship screen:** a board, not cards. One row per person: flag, name at display size, reason, left time, due-back time, and a minutes-late readout. Rows sort by urgency (late first), and row size scales with urgency the way a festival bill scales by billing. Healthy rows are quiet and compact; late rows are tall and loud. Empty state: a single calm line, "Everyone is in camp", with the in-camp flag.
- **Sign In / Out:** the code entry is the hero. Four big PIN cells (tabular, ghost-slot style) instead of one password field look. Reason selector becomes a row of large choices rather than a dropdown where that's possible without changing the underlying widget value.
- **Vans:** each van is a plate with its flag state, odometer/gas readout, and one obvious action. No dead "No reading" grey blocks; empty readings are hidden or shown as deliberate blanks.
- **Admin:** denser and calmer. Same tokens, a tighter table density, sticky section headers, quiet chrome.
- Spacing: 4px base, rhythm of 8 / 16 / 24 / 40 / 64. Corners: 4px (sharp, sign-like), not 14px bubbles.

## Motion

One signature, used sparingly: **the hoist.** When a sign-out succeeds, the full-screen confirmation raises a Blue Peter up a short halyard (translateY, about 450ms, ease-out-quart). A sign-in lowers it. Lateness ticks use a stepped update, not a fade. Everything else is 120-180ms state transitions. `prefers-reduced-motion` turns the hoist into an instant state change. The existing `st.fragment` refresh and flash-banner timing is not touched.

## States (all designed)

- **Empty:** Everyone is in camp / no vans out / no history yet, each with the matching flag.
- **Loading:** flag-line skeleton rows, no spinners centered on a blank page.
- **Error:** plain-language panel with a Reload button (the existing safety-net screen restyled).
- **Success:** the hoist banner with name, reason, and due-back time, readable at arm's length.
- **Emergency:** full-width red-and-white banner pinned to every page, unmistakable, no motion.

## Copy (in scope: you approved UX and copy changes)

Plain, short, camp-voiced. Examples: "Signing out" / "Coming back" stay. "Probably forgot to sign in" becomes "No sign-in on record". "Type your code. Skip the reason" becomes "Enter your code. No reason needed." All functions, data fields, and routes keep their meaning.

## Non-negotiables carried over

- Navy and white remain the anchor; camp name, tagline, logo, and color stay secret-configurable.
- No change to data, sheet schema, PIN handling, routes, fragments, or idle/undo logic.
- Keyboard focus rings on every control, 44px minimum targets (56px on the kiosk), no hover-only information.

## Build order (after approval)

1. Tokens and base components: fonts, color, type scale, buttons, inputs, flag SVG set, banner, empty/loading/error/success.
2. Rail and page frame.
3. Screens one at a time: Sign In / Out, Who's Out, Vans, Group Sign-Out, Admin / History. Each checked in the browser at phone and desktop widths.
4. Final review pass, before/after screenshots, and a list of anything unverified.

## Honest risks

- The flag grammar only works if it is executed with discipline. If the glyphs are sloppy it reads as a gimmick, so they get drawn, not approximated.
- Streamlit limits layout control (the rail cannot move to the top, some widget internals resist CSS). The design works inside those limits, and I will list anything that fell short.
- Admin tables are data-heavy; there the flags stay small and the design goes quiet on purpose.
