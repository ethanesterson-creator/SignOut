"""SignOut visual system: the "signal locker".

Navy and white are the anchor. Status is carried by flag *shape* first and
colour second (see DESIGN.md). This module holds the CSS and the flag SVGs so
streamlit_app.py stays about behaviour. Nothing here touches data or logic.
"""

NAVY = "#13294B"        # Bauercrest navy: rail, headlines, primary action
NAVY_DEEP = "#0B1B33"   # text on white, pressed states
NAVY_TINT = "#E8EDF5"   # quiet fills, hover
WHITE = "#FFFFFF"
RULE = "#C9D3E3"
RULE_STRONG = "#6B7F9E"
INK_2 = "#4A5B77"
RED = "#C8102E"
RED_TINT = "#FCEBEE"
RED_INK = "#8A0B20"
GOLD = "#F2B705"
GOLD_TINT = "#FFF6D6"
GOLD_INK = "#5C4300"


# ---------------------------------------------------------------- flags
def flag(kind: str, height: int = 28, label: str = "") -> str:
    """Inline SVG signal flag. kind: in | out | late | forgot | emergency.

    Shapes differ on purpose (square, inset square, pennant, split square) so
    status never depends on colour alone. Decorative unless a label is given.
    """
    w = int(height * 1.5)
    if kind == "out":  # Blue Peter: navy field, white centre
        body = (
            f"<rect width='48' height='32' fill='{NAVY}'/>"
            f"<rect x='14' y='8' width='20' height='16' fill='{WHITE}'/>"
        )
    elif kind == "late":  # burgee: white hoist band, red pennant
        body = (
            f"<polygon points='0,0 14,4.67 14,27.33 0,32' fill='{WHITE}' stroke='{RED}' stroke-width='2' stroke-linejoin='round'/>"
            f"<polygon points='14,4.67 48,16 14,27.33' fill='{RED}'/>"
        )
    elif kind == "forgot":  # split square, gold over white
        body = (
            f"<rect x='1.5' y='1.5' width='45' height='29' fill='{WHITE}' stroke='{GOLD_INK}' stroke-width='3'/>"
            f"<polygon points='3,3 45,3 3,29' fill='{GOLD}'/>"
        )
    elif kind == "emergency":  # red / white halves
        body = (
            f"<rect width='24' height='32' fill='{WHITE}'/>"
            f"<rect x='24' width='24' height='32' fill='{RED}'/>"
        )
    else:  # in camp: hollow square
        body = f"<rect x='1.5' y='1.5' width='45' height='29' fill='{WHITE}' stroke='{NAVY}' stroke-width='3'/>"
    aria = f"role='img' aria-label='{label}'" if label else "aria-hidden='true'"
    return (
        f"<svg class='sg-flag' viewBox='0 0 48 32' width='{w}' height='{height}' "
        f"{aria} focusable='false'>{body}</svg>"
    )


def _data_uri(svg: str) -> str:
    return "url(\"data:image/svg+xml;utf8," + svg.replace("#", "%23").replace("'", "%27") + "\")"


APP_CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow:wght@400;500;600;700&family=Barlow+Condensed:wght@600;700;800&display=swap');

:root {{
    --navy: {NAVY};
    --navy-deep: {NAVY_DEEP};
    --navy-tint: {NAVY_TINT};
    --white: {WHITE};
    --rule: {RULE};
    --rule-strong: {RULE_STRONG};
    --ink-2: {INK_2};
    --red: {RED};
    --red-tint: {RED_TINT};
    --red-ink: {RED_INK};
    --gold: {GOLD};
    --gold-tint: {GOLD_TINT};
    --gold-ink: {GOLD_INK};

    --font-display: 'Barlow Condensed', 'Arial Narrow', sans-serif;
    --font-body: 'Barlow', system-ui, sans-serif;

    --r: 4px;
    --s-1: 0.25rem;
    --s-2: 0.5rem;
    --s-3: 1rem;
    --s-4: 1.5rem;
    --s-5: 2.5rem;
    --s-6: 4rem;

    --ease-out: cubic-bezier(0.16, 1, 0.3, 1);
}}

/* ---------- base ---------- */
html {{ font-size: 112.5%; }}  /* 18px kiosk base */
.stApp {{
    background: var(--white);
    font-family: var(--font-body);
    color: var(--navy-deep);
    line-height: 1.45;
    overflow-x: hidden;
}}
.stApp p, .stApp label, .stApp li, .stApp input, .stApp textarea {{ font-family: var(--font-body); }}
::selection {{ background: var(--navy); color: var(--white); }}
#MainMenu, footer {{ visibility: hidden; }}
[data-testid="stAppDeployButton"] {{ display: none; }}
header[data-testid="stHeader"] {{ background: var(--white); }}
.block-container {{
    padding-top: 4.5rem;
    padding-bottom: 2rem;
    max-width: 1180px;
}}

/* Themed scrollbars */
* {{ scrollbar-width: thin; scrollbar-color: var(--rule-strong) transparent; }}
*::-webkit-scrollbar {{ width: 10px; height: 10px; }}
*::-webkit-scrollbar-thumb {{ background: var(--rule-strong); border-radius: 0; }}

/* st.columns: let rows wrap instead of overflowing on narrow screens */
div[data-testid="stHorizontalBlock"] {{ flex-wrap: wrap; row-gap: var(--s-3); }}
div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"] {{ min-width: 0; flex: 1 1 200px; }}

h1, h2, h3, .stApp h1, .stApp h2, .stApp h3 {{
    font-family: var(--font-display);
    color: var(--navy);
    letter-spacing: 0;
}}
[data-testid="stCaptionContainer"], .stApp small {{
    color: var(--ink-2);
    font-size: 0.95rem;
    opacity: 1;
}}
[data-testid="stCaptionContainer"] p {{ color: var(--ink-2); opacity: 1; }}
hr {{ border: 0; border-top: 1px solid var(--rule); margin: var(--s-4) 0; }}

/* ---------- focus ---------- */
:focus-visible {{ outline: 3px solid var(--navy); outline-offset: 2px; }}
.stButton > button:focus-visible,
.stFormSubmitButton > button:focus-visible,
.stDownloadButton > button:focus-visible {{ outline: 3px solid var(--navy); outline-offset: 3px; }}
section[data-testid="stSidebar"] :focus-visible,
section[data-testid="stSidebar"] label[data-testid="stRadioOption"]:has(input:focus-visible) {{
    outline: 3px solid var(--gold);
    outline-offset: 2px;
}}

/* ---------- rail (sidebar) ---------- */
section[data-testid="stSidebar"] {{
    background: var(--navy);
    border-right: 0;
}}
section[data-testid="stSidebar"] * {{ color: var(--white); }}
section[data-testid="stSidebar"] [data-testid="stSidebarContent"] {{ padding: var(--s-2) var(--s-3); }}
section[data-testid="stSidebar"] h1 {{
    font-family: var(--font-display);
    font-weight: 800;
    font-size: 1.9rem;
    text-transform: uppercase;
    letter-spacing: 0.02em;
    line-height: 1;
    padding: var(--s-3) 0 var(--s-1) 0;
}}
section[data-testid="stSidebar"] [data-testid="stCaptionContainer"] {{
    color: #B9C7DE;
    font-size: 0.85rem;
}}
section[data-testid="stSidebar"] [data-testid="stCaptionContainer"] p {{ color: #B9C7DE; }}
section[data-testid="stSidebar"] [data-testid="stIconMaterial"],
[data-testid="stIconMaterial"] {{ font-family: 'Material Symbols Rounded' !important; }}
section[data-testid="stSidebar"] hr {{ border-top-color: rgba(255,255,255,0.2); }}
/* The "Go to" label is redundant next to five obvious destinations */
section[data-testid="stSidebar"] [data-testid="stRadio"] > label {{ display: none; }}
section[data-testid="stSidebar"] [data-testid="stRadioGroup"] {{ gap: 4px; }}
section[data-testid="stSidebar"] label[data-testid="stRadioOption"] {{
    min-height: 60px;
    padding: 0 var(--s-3) 0 calc(var(--s-3) + 28px);
    border-radius: var(--r);
    position: relative;
    display: flex;
    align-items: center;
    cursor: pointer;
    transition: background 140ms var(--ease-out);
}}
/* hide the stock radio dot */
section[data-testid="stSidebar"] label[data-testid="stRadioOption"] > div > div > div:first-child {{ display: none; }}
section[data-testid="stSidebar"] label[data-testid="stRadioOption"]::before {{
    content: "";
    position: absolute;
    left: var(--s-3);
    top: 50%;
    border-radius: 0;
    transform: translateY(-50%);
    width: 6px;
    height: 28px;
    background: #8FA3C4;
}}
section[data-testid="stSidebar"] label[data-testid="stRadioOption"] p {{
    font-family: var(--font-display);
    font-weight: 700;
    font-size: 1.35rem;
    letter-spacing: 0.01em;
    text-transform: uppercase;
    color: #DCE5F3;
    white-space: nowrap;
}}
section[data-testid="stSidebar"] label[data-testid="stRadioOption"]:hover {{ background: rgba(255,255,255,0.08); }}
section[data-testid="stSidebar"] label[data-testid="stRadioOption"][data-selected="true"] {{ background: var(--white); }}
section[data-testid="stSidebar"] label[data-testid="stRadioOption"][data-selected="true"] p {{ color: var(--navy); }}
section[data-testid="stSidebar"] label[data-testid="stRadioOption"][data-selected="true"]::before {{
    background: var(--navy);
}}
section[data-testid="stSidebar"] [data-testid="stExpander"] {{ background: rgba(255,255,255,0.08); border: 0; }}

/* ---------- buttons ---------- */
.stButton > button, .stFormSubmitButton > button {{
    background: var(--navy);
    color: var(--white);
    border: 2px solid var(--navy);
    border-radius: var(--r);
    font-family: var(--font-display);
    font-weight: 700;
    font-size: 1.3rem;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    padding: 0 var(--s-4);
    min-height: 56px;
    transition: background 120ms var(--ease-out), color 120ms var(--ease-out), transform 90ms var(--ease-out);
}}
.stButton > button p, .stFormSubmitButton > button p {{ font-family: var(--font-display); font-size: 1.3rem; font-weight: 700; color: inherit; }}
.stButton > button:hover, .stFormSubmitButton > button:hover {{
    background: var(--navy-deep);
    border-color: var(--navy-deep);
    color: var(--white);
}}
.stButton > button:active, .stFormSubmitButton > button:active {{ transform: translateY(1px); background: var(--navy-deep); }}
.stButton > button:disabled {{ background: var(--navy-tint); border-color: var(--rule); color: var(--ink-2); }}
/* secondary (Streamlit "secondary" buttons that are not primary actions stay navy; downloads are outlined) */
.stDownloadButton > button {{
    background: var(--white);
    color: var(--navy);
    border: 2px solid var(--navy);
    border-radius: var(--r);
    font-family: var(--font-display);
    font-weight: 700;
    font-size: 1.15rem;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    min-height: 52px;
}}
.stApp .stDownloadButton button p {{ font-family: var(--font-display) !important; font-size: 1.15rem !important; font-weight: 700 !important; letter-spacing: 0.04em; text-transform: uppercase; color: var(--navy); }}
.stDownloadButton > button:hover {{ background: var(--navy-tint); color: var(--navy); }}

/* ---------- inputs ---------- */
[data-testid="stWidgetLabel"] p, .stApp label p {{
    font-family: var(--font-body);
    font-weight: 600;
    font-size: 0.95rem;
    color: var(--navy-deep);
}}
[data-testid="stTextInputRootElement"],
[data-testid="stSelectbox"] div:has(> input),
[data-testid="stMultiSelect"] div[role="group"]:has(> [data-testid="stMultiSelectTagsContainer"]),
[data-testid="stNumberInputContainer"],
[data-testid="stTextAreaRootElement"] {{
    background: var(--white) !important;
    border: 2px solid var(--rule-strong) !important;
    border-radius: var(--r) !important;
    min-height: 56px;
    height: auto;
}}
[data-testid="stTextInputRootElement"]:focus-within,
[data-testid="stSelectbox"] div:has(> input):focus-within,
[data-testid="stMultiSelect"] div[role="group"]:focus-within,
[data-testid="stNumberInputContainer"]:focus-within,
[data-testid="stTextAreaRootElement"]:focus-within {{
    border-color: var(--navy) !important;
    box-shadow: 0 0 0 3px rgba(19,41,75,0.18);
}}
.stTextInput input, .stSelectbox input, .stMultiSelect input {{
    font-family: var(--font-body);
    font-size: 1.1rem;
    color: var(--navy-deep);
}}
/* Code entry: the most-tapped field in the app. Big, tabular, spaced. */
.stTextInput input[type="password"] {{
    font-family: var(--font-display);
    font-size: 2.2rem;
    font-weight: 700;
    letter-spacing: 0.5em;
    font-variant-numeric: tabular-nums;
    height: 64px;
}}
[data-testid="stTextInputRootElement"]:has(input[type="password"]) {{ min-height: 72px; }}
[data-baseweb="tag"] {{ background: var(--navy) !important; border-radius: var(--r); }}
[data-baseweb="tag"] * {{ color: var(--white) !important; }}
[data-baseweb="popover"] li[role="option"] {{ font-family: var(--font-body); min-height: 48px; }}

/* ---------- forms and containers ---------- */
[data-testid="stForm"] {{
    background: var(--white);
    border: 2px solid var(--navy);
    border-radius: var(--r);
    padding: var(--s-4);
}}
div[data-testid="stExpander"] {{
    border: 1px solid var(--rule);
    border-radius: var(--r);
    background: var(--white);
}}
div[data-testid="stExpander"] summary {{ min-height: 52px; }}
div[data-testid="stExpander"] summary p {{ font-weight: 600; font-size: 1rem; }}

/* ---------- alerts ---------- */
[data-testid="stAlertContainer"] {{ border-radius: var(--r); border: 2px solid var(--navy); background: var(--navy-tint) !important; }}
[data-testid="stAlertContainer"] p {{ color: var(--navy-deep); font-weight: 500; }}
[data-testid="stAlertContainer"]:has([data-testid="stAlertContentError"]) {{ background: var(--red-tint) !important; border-color: var(--red); }}
[data-testid="stAlertContainer"]:has([data-testid="stAlertContentError"]) p {{ color: var(--red-ink); font-weight: 600; }}
[data-testid="stAlertContainer"]:has([data-testid="stAlertContentWarning"]) {{ background: var(--gold-tint) !important; border-color: var(--gold-ink); }}
[data-testid="stAlertContainer"]:has([data-testid="stAlertContentWarning"]) p {{ color: var(--gold-ink); font-weight: 600; }}
[data-testid="stAlertContainer"]:has([data-testid="stAlertContentSuccess"]) {{ background: var(--white) !important; border-color: var(--navy); }}
[data-testid="stAlertContainer"]:has([data-testid="stAlertContentSuccess"]) p {{ color: var(--navy); font-weight: 600; }}

/* ---------- tabs ---------- */
[role="tablist"] {{ gap: var(--s-1); border-bottom: 2px solid var(--rule); }}
[data-testid="stTab"] {{ min-height: 56px; padding: 0 var(--s-3); position: relative; }}
.stApp [data-testid="stTab"] [data-testid="stMarkdownContainer"] p {{
    font-family: var(--font-display) !important;
    font-weight: 700 !important;
    font-size: 1.35rem !important;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    color: var(--ink-2);
}}
.stApp [data-testid="stTab"][aria-selected="true"] [data-testid="stMarkdownContainer"] p {{ color: var(--navy); }}
[data-testid="stMultiSelectTagsContainer"] {{ border: 0 !important; background: transparent !important; }}
[data-testid="stTab"][aria-selected="true"]::after,
[data-testid="stTab"] .react-aria-SelectionIndicator {{
    content: "";
    position: absolute; left: 0; right: 0; bottom: -2px; height: 4px;
    background: var(--navy);
}}
[data-testid="stTab"]:hover p {{ color: var(--navy); }}

/* ---------- metrics ---------- */
[data-testid="stMetricValue"] {{
    font-family: var(--font-display);
    font-weight: 700;
    font-variant-numeric: tabular-nums;
    color: var(--navy);
    font-size: 2.6rem;
}}
[data-testid="stMetricLabel"] p {{ color: var(--ink-2); font-weight: 600; }}
[data-testid="stCheckbox"] label {{ min-height: 44px; }}
[data-testid="stDataFrame"] {{ border: 1px solid var(--rule); border-radius: var(--r); }}

/* ================= custom components ================= */

/* Page head: title with a hoist line under it. Context sits right, in the title block. */
.sg-head {{
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    gap: var(--s-3);
    padding-bottom: var(--s-2);
    border-bottom: 4px solid var(--navy);
    margin-bottom: var(--s-4);
}}
.sg-mast {{ display: block; width: 14px; height: 44px; background: var(--navy); flex: none; }}
.sg-head-l {{ display: flex; align-items: center; gap: var(--s-3); min-width: 0; }}
.sg-title {{
    font-family: var(--font-display);
    font-size: 3.4rem;
    font-weight: 800;
    color: var(--navy);
    line-height: 0.98;
    letter-spacing: 0;
    text-wrap: balance;
}}
.sg-head-r {{
    font-family: var(--font-display);
    font-weight: 600;
    font-size: 1.05rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--ink-2);
    text-align: right;
    padding-bottom: 6px;
}}
.sg-section {{
    font-family: var(--font-display);
    font-size: 1.7rem;
    font-weight: 700;
    color: var(--navy);
    margin: var(--s-5) 0 var(--s-2) 0;
    display: flex;
    align-items: center;
    gap: var(--s-2);
    line-height: 1.1;
}}
.sg-count {{
    font-variant-numeric: tabular-nums;
    background: var(--navy);
    color: var(--white);
    font-size: 1rem;
    padding: 2px 10px;
    border-radius: var(--r);
}}

/* live readout */
.sg-live {{
    display: flex; align-items: center; gap: var(--s-2);
    font-family: var(--font-body); font-size: 0.95rem; font-weight: 600;
    color: var(--ink-2); font-variant-numeric: tabular-nums;
    margin: 0 0 var(--s-3) 0;
}}
.sg-live i {{ width: 10px; height: 10px; background: var(--navy); display: inline-block; animation: sgBlink 2s steps(2, jump-none) infinite; }}
@keyframes sgBlink {{ 0% {{ opacity: 1; }} 100% {{ opacity: 0.25; }} }}

/* Blue Peter sits on navy in several places: give it a white keyline there */
.sg-mode-out .sg-flag, .sg-banner-out .sg-flag, .sg-flash-out .sg-flag, .sg-van-out .sg-flag {{
    outline: 2px solid var(--white);
    outline-offset: 0;
}}

.sg-mode-sub, .sg-mode-reason, .sg-banner-sub, .sg-flash-sub, .sg-detail, .sg-reason, .sg-van-meta, .sg-van-who,
.sg-note, .sg-fork-sub, .sg-empty span, .sg-emergency-msg, .sg-live, .sg-strip-empty, .sg-chip small {{ font-family: var(--font-body); }}

/* ---------- the board ---------- */
.sg-board {{ border-top: 2px solid var(--navy); }}
.sg-boardhead, .sg-row {{
    display: grid;
    grid-template-columns: 64px minmax(0, 1.5fr) minmax(0, 1.2fr) 150px 170px;
    column-gap: var(--s-3);
    align-items: center;
}}
.sg-boardhead {{
    font-family: var(--font-display);
    font-size: 0.95rem;
    font-weight: 600;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--ink-2);
    padding: var(--s-2) var(--s-3);
    border-bottom: 1px solid var(--rule);
}}
.sg-row {{
    padding: var(--s-3);
    border-bottom: 1px solid var(--rule);
    background: var(--white);
}}
.sg-row .sg-name {{
    font-family: var(--font-display);
    font-size: 2rem;
    font-weight: 700;
    color: var(--navy);
    line-height: 1.05;
    overflow-wrap: anywhere;
}}
.sg-row .sg-reason {{ font-weight: 600; color: var(--navy-deep); }}
.sg-row .sg-detail {{ color: var(--ink-2); font-size: 0.92rem; }}
.sg-row .sg-time {{
    font-family: var(--font-display);
    font-size: 1.5rem;
    font-weight: 700;
    color: var(--navy);
    font-variant-numeric: tabular-nums;
    line-height: 1.1;
}}
.sg-row .sg-timelabel {{ display: none; }}
.sg-row .sg-status {{ justify-self: end; text-align: right; }}
.sg-badge {{
    display: inline-block;
    font-family: var(--font-display);
    font-weight: 700;
    font-size: 1.15rem;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    padding: 4px 12px;
    border-radius: var(--r);
    background: var(--navy);
    color: var(--white);
    white-space: nowrap;
}}
/* Late: tall and loud, the way a headliner is billed */
.sg-row.sg-late {{
    background: var(--red-tint);
    padding-top: var(--s-4);
    padding-bottom: var(--s-4);
    border-top: 2px solid var(--red);
    border-bottom: 2px solid var(--red);
}}
.sg-row.sg-late .sg-name {{ font-size: 2.7rem; color: var(--red-ink); }}
.sg-row.sg-late .sg-time {{ color: var(--red-ink); }}
.sg-row.sg-late .sg-badge {{ background: var(--red); font-size: 1.6rem; padding: 6px 14px; }}
/* Forgot: compact and calm; needs cleanup, not alarm */
.sg-row.sg-forgot {{ background: var(--gold-tint); padding-top: var(--s-2); padding-bottom: var(--s-2); }}
.sg-row.sg-forgot .sg-name {{ font-size: 1.5rem; color: var(--gold-ink); }}
.sg-row.sg-forgot .sg-time {{ font-size: 1.15rem; color: var(--gold-ink); }}
.sg-row.sg-forgot .sg-badge {{ background: var(--gold); color: var(--gold-ink); font-size: 0.95rem; }}

.sg-empty {{
    display: flex; align-items: center; gap: var(--s-3);
    padding: var(--s-4) var(--s-3);
    border-top: 2px solid var(--navy);
    border-bottom: 1px solid var(--rule);
    font-family: var(--font-display);
    font-size: 1.6rem;
    font-weight: 700;
    color: var(--navy);
}}
.sg-empty span {{ font-family: var(--font-body); font-weight: 500; font-size: 1rem; color: var(--ink-2); display: block; }}

/* Day-off list: plain text run, no pills */
.sg-dayoff {{
    display: flex; flex-wrap: wrap; gap: var(--s-1) var(--s-4);
    font-family: var(--font-display); font-size: 1.35rem; font-weight: 600; color: var(--navy);
    padding: var(--s-2) 0;
}}

/* ---------- sign in/out mode strip ---------- */
.sg-mode {{
    display: grid;
    grid-template-columns: minmax(0, 3fr) minmax(0, 2fr);
    border: 2px solid var(--navy);
    border-radius: var(--r);
    overflow: hidden;
    margin-bottom: var(--s-2);
}}
.sg-mode-out {{
    background: var(--navy); color: var(--white);
    padding: var(--s-3) var(--s-4);
    display: flex; align-items: center; gap: var(--s-3);
}}
.sg-mode-in {{
    background: var(--white); color: var(--navy);
    padding: var(--s-3) var(--s-4);
    display: flex; align-items: center; gap: var(--s-3);
}}
.sg-mode-word {{ font-family: var(--font-display); font-size: 2.2rem; font-weight: 800; line-height: 1; text-transform: uppercase; color: inherit; }}
.sg-mode-reason {{ font-size: 1.15rem; font-weight: 600; color: inherit; margin-top: 6px; overflow-wrap: anywhere; }}
.sg-mode-sub {{ font-size: 0.95rem; opacity: 0.85; margin-top: 2px; color: inherit; }}
.sg-mode-in .sg-mode-sub {{ color: var(--ink-2); opacity: 1; }}

/* ---------- big banner (van / group steps) ---------- */
.sg-banner {{
    display: flex; align-items: center; gap: var(--s-3);
    border-radius: var(--r);
    padding: var(--s-3) var(--s-4);
    margin: var(--s-2) 0 var(--s-3) 0;
}}
.sg-banner-out {{ background: var(--navy); color: var(--white); }}
.sg-banner-in {{ background: var(--white); color: var(--navy); border: 2px solid var(--navy); }}
.sg-banner-word {{ font-family: var(--font-display); font-size: 2.2rem; font-weight: 800; text-transform: uppercase; line-height: 1.05; color: inherit; overflow-wrap: anywhere; }}
.sg-banner-sub {{ font-size: 1rem; margin-top: 4px; color: inherit; opacity: 0.88; }}
.sg-banner-in .sg-banner-sub {{ color: var(--ink-2); opacity: 1; }}

/* ---------- the hoist: confirmation ---------- */
.sg-flash {{
    display: grid;
    grid-template-columns: 96px minmax(0, 1fr);
    align-items: center;
    gap: var(--s-4);
    border-radius: var(--r);
    padding: var(--s-4);
    margin-bottom: var(--s-3);
}}
.sg-flash-out {{ background: var(--navy); color: var(--white); }}
.sg-flash-in {{ background: var(--white); color: var(--navy); border: 3px solid var(--navy); }}
.sg-halyard {{ position: relative; width: 96px; height: 96px; overflow: hidden; }}
.sg-halyard::before {{ content: ""; position: absolute; left: 6px; top: 0; bottom: 0; width: 4px; background: currentColor; opacity: 0.55; }}
.sg-halyard .sg-flag {{ position: absolute; left: 10px; top: 6px; width: 72px; height: 48px; transform-origin: left center; }}
.sg-flash-out .sg-halyard .sg-flag {{ animation: sgHoist 700ms var(--ease-out) both; }}
.sg-flash-in .sg-halyard .sg-flag {{ animation: sgStrike 700ms var(--ease-out) both; }}
@keyframes sgHoist {{ from {{ transform: translateY(84px); }} to {{ transform: translateY(0); }} }}
@keyframes sgStrike {{ from {{ transform: translateY(0); }} to {{ transform: translateY(84px); }} }}
.sg-flash-word {{ font-family: var(--font-display); font-size: 3rem; font-weight: 800; line-height: 1; text-transform: uppercase; color: inherit; overflow-wrap: anywhere; }}
.sg-flash-sub {{ font-size: 1.2rem; font-weight: 600; margin-top: 6px; color: inherit; }}
.sg-flash-ask {{ margin-top: var(--s-2); padding-top: var(--s-2); border-top: 2px solid currentColor; font-family: var(--font-display); font-size: 1.3rem; font-weight: 700; color: inherit; }}

/* stale sign-in fork */
.sg-fork {{
    background: var(--gold-tint); border: 2px solid var(--gold-ink);
    border-radius: var(--r); padding: var(--s-3) var(--s-4); margin: var(--s-2) 0 var(--s-3) 0;
    display: flex; gap: var(--s-3); align-items: center;
}}
.sg-fork-head {{ font-family: var(--font-display); font-size: 1.7rem; font-weight: 700; color: var(--gold-ink); line-height: 1.1; }}
.sg-fork-sub {{ font-size: 1.05rem; font-weight: 600; color: var(--gold-ink); margin-top: 4px; }}

/* "Signed out right now" strip under the sign box */
.sg-strip-title {{ font-family: var(--font-display); font-size: 1.5rem; font-weight: 700; color: var(--navy); margin: var(--s-5) 0 var(--s-2) 0; }}
.sg-strip-empty {{ color: var(--ink-2); font-size: 1.05rem; display: flex; align-items: center; gap: var(--s-2); }}
.sg-strip {{ display: flex; flex-wrap: wrap; gap: var(--s-2); margin-bottom: var(--s-2); }}
.sg-chip {{
    display: inline-flex; align-items: center; gap: 8px;
    border: 2px solid var(--navy); border-radius: var(--r);
    padding: 4px 12px 4px 8px; background: var(--white); color: var(--navy);
    font-family: var(--font-display); font-weight: 700; font-size: 1.15rem;
}}
.sg-chip small {{ font-family: var(--font-body); font-weight: 500; font-size: 0.85rem; color: var(--ink-2); }}
.sg-chip-forgot {{ border-color: var(--gold-ink); background: var(--gold-tint); color: var(--gold-ink); }}
.sg-chip-forgot small {{ color: var(--gold-ink); }}
.sg-strip-label {{ font-family: var(--font-display); font-size: 1.25rem; font-weight: 700; color: var(--gold-ink); margin: var(--s-2) 0 var(--s-1) 0; }}

/* ---------- fleet (vans) ---------- */
.sg-fleet {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: var(--s-3); margin-bottom: var(--s-2); }}
.sg-van {{
    border: 2px solid var(--navy); border-radius: var(--r); background: var(--white); color: var(--navy);
    padding: var(--s-3) var(--s-3) var(--s-3) var(--s-3);
    display: flex; flex-direction: column; gap: 6px; min-height: 120px;
}}
.sg-van.sg-van-out {{ background: var(--navy); color: var(--white); }}
.sg-van.sg-van-sel {{ box-shadow: 0 0 0 4px var(--gold); }}
.sg-van-top {{ display: flex; align-items: center; justify-content: space-between; }}
.sg-van-state {{ font-family: var(--font-display); font-size: 1.2rem; font-weight: 700; color: inherit; }}
.sg-van-name {{ font-family: var(--font-display); font-size: 2.1rem; font-weight: 800; line-height: 1.05; color: inherit; }}
.sg-van-who {{ font-size: 1.05rem; font-weight: 600; color: inherit; }}
.sg-van-meta {{ font-size: 0.95rem; opacity: 0.9; color: inherit; }}
.sg-van-hint {{ font-size: 0.9rem; opacity: 0.8; margin-top: auto; color: inherit; }}
.sg-gas {{ display: flex; align-items: center; gap: 10px; margin-top: 6px; color: inherit; }}
.sg-gas-seg {{ display: inline-flex; gap: 3px; }}
.sg-gas-seg i {{ width: 18px; height: 12px; border: 2px solid currentColor; display: inline-block; opacity: 0.45; }}
.sg-gas-seg i.on {{ background: currentColor; opacity: 1; }}
.sg-gas-seg.low i.on {{ background: var(--red); border-color: var(--red); }}
.sg-van-out .sg-gas-seg.low i.on {{ background: var(--gold); border-color: var(--gold); }}
.sg-gas-word {{ font-family: var(--font-display); font-size: 1.1rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.04em; color: inherit; }}

.sg-van-board {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: var(--s-3); }}

.sg-note {{
    border-top: 2px solid var(--navy); border-bottom: 1px solid var(--rule);
    padding: var(--s-3); color: var(--ink-2); font-size: 1.05rem;
}}

/* Quick confirmation line (admin, inline) */
.sg-inline-flash {{ border: 2px solid var(--navy); border-radius: var(--r); padding: var(--s-2) var(--s-3); font-weight: 600; color: var(--navy); margin-bottom: var(--s-2); animation: sgFade 2.4s var(--ease-out) forwards; }}
@keyframes sgFade {{ 0% {{ opacity: 0; }} 10% {{ opacity: 1; }} 75% {{ opacity: 1; }} 100% {{ opacity: 0; }} }}

/* ---------- emergency ---------- */
.sg-emergency {{
    display: grid; grid-template-columns: 72px minmax(0, 1fr); gap: var(--s-3); align-items: center;
    background: var(--red); color: var(--white);
    border-radius: var(--r); padding: var(--s-3) var(--s-4); margin: 0 0 var(--s-4) 0;
    border: 3px solid var(--red-ink);
}}
.sg-emergency .sg-flag {{ width: 72px; height: 48px; outline: 2px solid var(--white); }}
.sg-emergency-word {{ font-family: var(--font-display); font-size: 2.3rem; font-weight: 800; letter-spacing: 0.03em; text-transform: uppercase; line-height: 1; color: var(--white); }}
.sg-emergency-msg {{ font-size: 1.25rem; font-weight: 600; margin-top: 6px; color: var(--white); }}

.sg-footer {{
    margin-top: var(--s-5); padding-top: var(--s-2); border-top: 1px solid var(--rule);
    font-family: var(--font-display); font-size: 0.95rem; font-weight: 600;
    letter-spacing: 0.12em; text-transform: uppercase; color: var(--ink-2);
}}

/* ---------- responsive ---------- */
@media (max-width: 900px) {{
    html {{ font-size: 100%; }}
    .block-container {{ padding-left: 1rem; padding-right: 1rem; }}
    .sg-title {{ font-size: 2.4rem; }}
    .sg-head {{ flex-direction: column; align-items: flex-start; gap: var(--s-1); }}
    .sg-head-r {{ text-align: left; padding-bottom: 0; }}
    .sg-boardhead {{ display: none; }}
    .sg-row {{ grid-template-columns: 52px minmax(0, 1fr); row-gap: 4px; }}
    .sg-row .sg-flagcell {{ grid-row: 1 / span 3; }}
    .sg-row .sg-time::before {{ content: "Out "; font-family: var(--font-body); font-size: 0.85rem; font-weight: 500; color: var(--ink-2); }}
    .sg-row .sg-status .sg-time::before {{ content: "Due back "; }}
    .sg-row .sg-status .sg-detail {{ display: none; }}
    .sg-row.sg-forgot .sg-time::before, .sg-row.sg-late .sg-time::before {{ color: inherit; }}
    .sg-row > * {{ grid-column: 2; }}
    .sg-row > .sg-flagcell {{ grid-column: 1; }}
    .sg-row .sg-status {{ justify-self: start; text-align: left; }}
    .sg-row .sg-name {{ font-size: 1.7rem; }}
    .sg-row.sg-late .sg-name {{ font-size: 2rem; }}
    .sg-mode {{ grid-template-columns: 1fr; }}
    .sg-mode-word {{ font-size: 1.8rem; }}
    .sg-flash {{ grid-template-columns: 72px minmax(0, 1fr); padding: var(--s-3); gap: var(--s-3); }}
    .sg-flash-word {{ font-size: 2rem; }}
    .sg-halyard {{ width: 72px; }}
    .sg-halyard .sg-flag {{ width: 54px; height: 36px; }}
    .sg-emergency {{ grid-template-columns: 1fr; }}
    .sg-emergency-word {{ font-size: 1.7rem; }}
}}

@media (prefers-reduced-motion: reduce) {{
    .sg-flash .sg-halyard .sg-flag, .sg-live i, .sg-inline-flash {{ animation: none !important; }}
}}
</style>
"""
