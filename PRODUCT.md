# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

Existing: Streamlit (single `streamlit_app.py`) backed by one Google Sheet via gspread. Styling is injected CSS (`APP_CSS`) plus Streamlit widgets; revamp stays inside this stack. No rewrite of logic, data, or routes.

## Users

- **Counselors (primary):** use a shared computer kiosk (almost exclusively) to sign themselves in/out with a 4-digit code, mid-activity, often in a rush. Also glance at the Who's Out and Vans boards.
- **Camp office / admins:** sit down at Admin / History to review history, archive, handle exceptions, manage PINs.

## Product Purpose

SignOut tells a summer camp who is off-site, where, since when, and whether they are late, so nobody is unaccounted for. Success: a counselor finishes a sign-out in seconds, and anyone walking past can read who is out at a glance.

## Positioning

A camp-specific sign-out board (vans, groups, schedules, emergency mode, late alerts) configured per camp through secrets, possibly sold to other camps later.

## Operating Context

Shared kiosk computer, frequently idle; returns to the home page after idle. Boards auto-refresh each minute. Five pages: Sign In / Out, Who's Out, Vans, Group Sign-Out, Admin / History. Emergency banner appears on every page.

## Capabilities and Constraints

- Must not change functions, routes, data, or logic.
- `st.fragment` refresh and flash-banner timing is verified behavior; preserve it.
- Camp name, tagline, logo, colors are configurable per camp via secrets.
- Copy, labels, and screen-level layout/flow may be improved (user confirmed).

## Brand Commitments

Bauercrest: **navy and white stays the anchor** (user-confirmed). Logo at `logo-header-2.png`.

## Evidence on Hand

No testimonials, customer counts, or stats; none may be fabricated.

## Product Principles

1. Glanceable first: state (out / late / in) reads from across a room.
2. Fast taps, no ambiguity: sign-out is a few large, unmistakable actions.
3. Calm under pressure: emergencies and errors are clear without panic.
4. Per-camp identity without per-camp code.
5. Data and behavior are sacred; only presentation and wording move.

## Accessibility & Inclusion

Bright room, distance reading, large tap targets, high contrast; never color alone for status.
