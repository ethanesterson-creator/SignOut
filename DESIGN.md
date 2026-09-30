# SignOut design system

This documents the visual and interaction language for SignOut, so future
changes (by a person or an automated routine) stay consistent instead of
drifting page by page. It formalizes what's already working in the app and
gives names to the things that were previously ad hoc.

## Who this is for

Two very different users, both time-pressured:

- **Counselors** — tap in/out on a shared tablet mid-activity, or check the
  Who's Out/Vans boards at a glance while walking past. Fast, thumb-driven,
  often outdoors or in a bright room. They are not "using an app," they're
  glancing at a board and typing 4 digits.
- **Camp office / admins** — sit down at the Admin page to review history,
  archive, or handle an exception. Slower, more deliberate, more text-heavy.

The design has to work as a glanceable public kiosk display *and* as a
denser admin tool, without becoming two different products.

## Do not change

- **The navy identity.** `#13294B` (and its `_DEEP`/`_SOFT` variants) is
  Bauercrest's actual brand color and should stay the default for any camp
  that doesn't override it via secrets. Independently cross-checking this
  against a "utility / operations / government-service" color palette
  reference landed on almost the same navy family (`#0F172A` primary) —
  this isn't a generic SaaS blue, it's already the right choice for a
  trustworthy, institutional, high-contrast utility tool.
- **Archivo (headings) + Public Sans (body).** Already loaded via
  `@import` in `APP_CSS`. Public Sans in particular is built for exactly
  this kind of high-legibility civic/utility context — don't swap it for
  something more "designed," it would make the board harder to read at a
  glance, which is the one thing it can't afford to be.
- **The `st.fragment`-based refresh/flash-banner timing.** This is verified
  interaction design (see git history), not just a visual choice — don't
  "simplify" it back to a CSS-only animation.

## Color tokens

Formalize the existing navy + status colors as named tokens instead of
repeated hex literals. Values below match what's already in the CSS almost
everywhere — this is mostly naming what exists, plus filling real gaps.

```css
:root {
  /* Brand (from CAMP_* / theme constants — already externalized) */
  --color-navy:        #13294B;
  --color-navy-deep:    #0B1B33;
  --color-navy-soft:    #1E3A66;
  --color-cloud:        #F5F7FA;  /* page background */
  --color-line:         #D8DFE9;  /* borders */
  --color-mist:         #5C6B82;  /* secondary text */
  --color-white:        #FFFFFF;

  /* Status — semantics already correct in the code, just not tokenized */
  --color-success:       #2E7D32;   /* signed IN / at camp / on time */
  --color-success-bg:    #E7F4EA;
  --color-success-strong:#1B5E20;

  --color-warning:       #B07A1E;   /* late / forgot to sign in */
  --color-warning-bg:    #FBF3E4;
  --color-warning-strong:#6B4A0F;

  --color-danger:        #B3261E;   /* emergency / delete / error */
  --color-danger-bg:     #FDECEC;
  --color-danger-strong: #7A1620;
}
```

Every hardcoded status hex in `streamlit_app.py` (there are ~22 today)
should eventually reference these instead of repeating the literal. This
also becomes the real Phase 3 unlock: right now only the navy is
camp-configurable — a future `camp_accent_color` secret could remap the
brand tokens without touching status colors at all, since those are
semantic (green/amber/red mean the same thing at every camp).

## Typography

| Role | Font | Weight | Size |
|---|---|---|---|
| Display / big banner | Archivo | 800 | 2rem |
| Page title | Archivo | 700 | 1.55rem |
| Section title | Archivo | 600 | 1.15–1.35rem |
| Body | Public Sans | 400–500 | 0.9–1.05rem |
| Label / eyebrow | Public Sans | 600, uppercase, +tracking | 0.72–0.85rem |

Keep Archivo strictly for short, high-emphasis text (banners, titles,
numbers). Public Sans for everything a person actually reads. Don't
introduce a third family.

## Spacing scale

Replace ad hoc values (today: 15+ distinct padding/margin/gap values like
0.16rem, 0.18rem, 0.22rem, 0.28rem...) with a single 4px-based scale:

```css
--space-1: 0.25rem;  /* 4px  - tight (chip padding) */
--space-2: 0.5rem;   /* 8px  - default gap */
--space-3: 0.75rem;  /* 12px */
--space-4: 1rem;     /* 16px - default card padding */
--space-5: 1.5rem;   /* 24px */
--space-6: 2rem;     /* 32px - section spacing */
```

Every existing value should round to the nearest step above, not be
preserved exactly — the goal is a shared rhythm, not pixel-perfect
backward compatibility with today's incidental values.

## Corner radius

One rule, not five: **`10px` for cards and containers, `999px` (pill) for
badges/tags/chips only.** Collapse the current 8/12/14px variants into
10px unless there's a specific reason (there isn't one visible today).

```css
--radius-card: 10px;
--radius-pill: 999px;
```

## Layout & responsive rules

The current CSS has no explicit breakpoints, which is the direct cause of
the overflow issues found in review (Vans board 3-up layout, Admin page
long captions/tables clipping past the viewport edge). Going forward:

- Never rely on a fixed multi-column layout (`st.columns(3)` for the vans
  board) without a narrower fallback. Below ~900px, stack to a single
  column rather than compressing three columns into less space than they
  need.
- Long captions and section headings must wrap, never clip. If a heading
  is truncating, the fix is a layout fix, not a shorter heading.
- Tables (`st.dataframe`) need either a horizontal-scroll affordance that's
  visually obvious, or fewer default-visible columns with a "show more."
- Target breakpoints: 375px (phone), 768px (tablet portrait — the
  literal kiosk device), 1024px+ (desktop/admin).

## Componentizing native Streamlit widgets

`st.dataframe` tables and `st.bar_chart` charts currently render in
Streamlit's default styling, visually disconnected from the hand-styled
cards and banners everywhere else on the page. When touched next:

- Chart colors should pull from the token palette above (success green /
  navy / warning amber), not Streamlit's default categorical palette.
- Table styling should at minimum inherit the app's border color
  (`--color-line`) and font (Public Sans) via Streamlit's theming config
  (`.streamlit/config.toml` `[theme]` section) rather than being left as
  browser/Streamlit defaults next to fully custom-CSS neighbors.

## Motion

Keep it minimal and functional — this is a utility board, not a marketing
site:

- Transitions: 150–200ms ease, for hover/press states only.
- No entrance animations on the boards themselves (Who's Out, Vans) — they
  auto-refresh every 20–60 seconds; anything that animates on refresh would
  be distracting on a passive display.
- Respect `prefers-reduced-motion` for anything added beyond what exists.

## Accessibility baseline

- Text contrast: 4.5:1 minimum, checked against the token backgrounds
  above (the navy/cloud/status combinations already meet this).
- Touch targets: 44×44px minimum on anything tappable — relevant given the
  primary device is a touchscreen tablet, not a mouse-driven desktop.
- Visible focus states on every interactive element (inputs, buttons,
  nav radio items) for keyboard/switch-device use in the admin office.

## What NOT to do

- Don't introduce a third font family or a second accent color "for
  variety" — the palette is intentionally narrow because this is a
  glanceable utility board, not a brand showcase.
- Don't add decorative motion, gradients, or shadows — flat, high-contrast,
  fast-loading is the right style for this product and audience (per the
  Digital Signage/Kiosk and Minimalism/Swiss Style reference profiles this
  was checked against), not a stylistic accident to "modernize" away.
- Don't chase a phone-first redesign at the cost of the kiosk/tablet
  layout — the tablet board is the primary surface, phones are secondary.
