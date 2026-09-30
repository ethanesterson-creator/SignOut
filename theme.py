"""SignOut visual system v2: "the signal locker", drenched in Bauercrest navy.

A deep navy atmosphere with depth and motion. Status is carried by flag *shape*
(cloth flags that actually wave); colour only reinforces it. Navy and white are
the anchor; red and gold are reserved for things a person must act on.

This module only holds presentation: the CSS, the flag SVGs, and the bunting.
Nothing here touches data or logic.
"""

NAVY = "#13294B"
WHITE = "#FFFFFF"
RED = "#E5213F"
GOLD = "#F2B705"
GOLD_INK = "#3D2C00"
SKY = "#5C86C8"


# ---------------------------------------------------------------- flags
def _shape(kind: str):
    """(body svg, overlay polygon/rect for cloth folds) for a 48x32 flag."""
    if kind == "out":  # Blue Peter: navy field, white centre
        return (
            f"<rect x='1' y='1' width='46' height='30' fill='{NAVY}' stroke='{WHITE}' stroke-width='2'/>"
            f"<rect x='14' y='8' width='20' height='16' fill='{WHITE}'/>",
            "<rect width='48' height='32'/>",
        )
    if kind == "late":  # burgee: white hoist band, red pennant
        return (
            f"<polygon points='0,0 14,4.67 14,27.33 0,32' fill='{WHITE}'/>"
            f"<polygon points='14,4.67 48,16 14,27.33' fill='{RED}'/>",
            "<polygon points='0,0 48,16 0,32'/>",
        )
    if kind == "forgot":  # split square, gold over white
        return (
            f"<rect width='48' height='32' fill='{WHITE}'/>"
            f"<polygon points='0,0 48,0 0,32' fill='{GOLD}'/>",
            "<rect width='48' height='32'/>",
        )
    if kind == "emergency":  # red / white halves
        return (
            f"<rect width='24' height='32' fill='{WHITE}'/><rect x='24' width='24' height='32' fill='{RED}'/>",
            "<rect width='48' height='32'/>",
        )
    # in camp: white field with navy frame
    return (
        f"<rect width='48' height='32' fill='{WHITE}'/>"
        f"<rect x='4' y='4' width='40' height='24' fill='none' stroke='{NAVY}' stroke-width='3'/>",
        "<rect width='48' height='32'/>",
    )


_flag_seq = [0]


def flag(kind: str, height: int = 28, label: str = "", wave: bool = False) -> str:
    """Inline SVG signal flag. kind: in | out | late | forgot | emergency.

    Shapes differ on purpose (plain, inset square, pennant, split square) so
    status never depends on colour alone. wave=True renders real rippling cloth:
    the flag is cut into vertical slices that ride a travelling sine wave, so
    edges stay crisp. Decorative unless a label is given.
    """
    w = int(height * 1.5)
    body, overlay = _shape(kind)
    aria = f"role='img' aria-label='{label}'" if label else "aria-hidden='true'"
    if not wave:
        return (
            f"<svg class='sg-flag' viewBox='0 0 48 32' width='{w}' height='{height}' {aria} focusable='false'>{body}</svg>"
        )
    _flag_seq[0] += 1
    fid = f"f{_flag_seq[0]}"
    n = 28
    sw = 48 / n
    clips = "".join(
        f"<clipPath id='{fid}c{i}'><rect x='{i * sw - 0.15:.2f}' y='-4' width='{sw + 0.3:.2f}' height='40'/></clipPath>"
        for i in range(n)
    )
    slices = "".join(
        f"<g clip-path='url(#{fid}c{i})'><g class='sg-ripple' style='--a:{0.1 + 1.6 * i / (n - 1):.2f}px;animation-delay:{-i * 0.075:.2f}s'>"
        f"<use href='#{fid}'/></g></g>"
        for i in range(n)
    )
    return (
        f"<svg class='sg-flag sg-cloth' viewBox='0 -3 48 38' width='{w}' height='{int(height * 38 / 32)}' {aria} focusable='false'>"
        f"<defs><g id='{fid}'>{body}<g fill='url(#sgFold)' style='mix-blend-mode:overlay'>{overlay}</g></g>{clips}</defs>"
        f"{slices}</svg>"
    )


# Shared SVG defs (cloth folds + wave filter). Injected once per page.
SVG_DEFS = """
<svg width="0" height="0" style="position:absolute" aria-hidden="true" focusable="false">
  <defs>
    <linearGradient id="sgFold" x1="0" x2="1" y1="0" y2="0">
      <stop offset="0" stop-color="#000" stop-opacity=".30"/>
      <stop offset=".12" stop-color="#fff" stop-opacity=".35"/>
      <stop offset=".26" stop-color="#000" stop-opacity=".28"/>
      <stop offset=".44" stop-color="#fff" stop-opacity=".30"/>
      <stop offset=".62" stop-color="#000" stop-opacity=".30"/>
      <stop offset=".80" stop-color="#fff" stop-opacity=".30"/>
      <stop offset="1" stop-color="#000" stop-opacity=".28"/>
    </linearGradient>

  </defs>
</svg>
"""


def bunting(width: int = 1200) -> str:
    """A dressed-ship string of signal pennants that sways. Decorative."""
    n = 26
    sag = 16
    pts = []
    for i in range(n):
        x = 30 + i * ((width - 60) / (n - 1))
        t = x / width
        y = 6 + sag * 4 * t * (1 - t)
        pts.append((x, y))
    path = "M0,4 " + " ".join(f"L{x:.1f},{y:.1f}" for x, y in pts) + f" L{width},4"
    kinds = [
        (WHITE, None),
        (SKY, None),
        (RED, None),
        (NAVY, WHITE),
        (GOLD, None),
        (WHITE, RED),
        (SKY, WHITE),
    ]
    flags = []
    for i, (x, y) in enumerate(pts):
        c1, c2 = kinds[(i * 3 + 1) % len(kinds)]
        delay = -((i * 0.37) % 3.6)
        dur = 3.2 + (i % 4) * 0.45
        if i % 3 == 1:  # pennant (triangle)
            shape = f"<polygon points='-7,0 7,0 0,26' fill='{c1}'/>"
            if c2:
                shape += f"<polygon points='-3.5,0 3.5,0 0,13' fill='{c2}'/>"
        else:  # square signal flag
            shape = f"<rect x='-8' y='0' width='16' height='15' fill='{c1}'/>"
            if c2:
                shape += f"<rect x='-3.5' y='3.5' width='7' height='8' fill='{c2}'/>"
        flags.append(
            f"<g transform='translate({x:.1f},{y:.1f})'>"
            f"<g class='sg-sway' style='animation-duration:{dur:.2f}s;animation-delay:{delay:.2f}s'>{shape}</g></g>"
        )
    return (
        f"<svg class='sg-bunting' viewBox='0 0 {width} 46' preserveAspectRatio='xMidYMin slice' aria-hidden='true' focusable='false'>"
        f"<path d='{path}' fill='none' stroke='rgba(255,255,255,0.55)' stroke-width='1.4'/>"
        + "".join(flags)
        + "</svg>"
    )


APP_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Big+Shoulders+Display:wght@700;800;900&family=Schibsted+Grotesk:wght@400;500;600;700&family=Red+Hat+Mono:wght@500;600&display=swap');

:root {
    --bg-0: #081529;
    --bg-1: #0B1B33;
    --navy: #13294B;
    --navy-2: #1E3A66;
    --navy-3: #2C538F;
    --ink: #FFFFFF;
    --ink-2: #B9C8E4;
    --ink-3: #8EA3C7;
    --line: rgba(255,255,255,0.14);
    --line-strong: rgba(255,255,255,0.32);
    --glass: rgba(255,255,255,0.055);
    --glass-2: rgba(255,255,255,0.10);
    --red: #E5213F;
    --red-glow: rgba(229,33,63,0.30);
    --gold: #F2B705;
    --gold-ink: #3D2C00;

    --font-display: 'Big Shoulders Display', 'Arial Narrow', sans-serif;
    --font-body: 'Schibsted Grotesk', system-ui, -apple-system, sans-serif;
    --font-mono: 'Red Hat Mono', ui-monospace, monospace;

    --r: 16px;
    --r-sm: 10px;
    --s-1: 0.25rem; --s-2: 0.5rem; --s-3: 1rem; --s-4: 1.5rem; --s-5: 2.5rem; --s-6: 4rem;
    --ease-out: cubic-bezier(0.16, 1, 0.3, 1);
    --ease-io: cubic-bezier(0.65, 0, 0.35, 1);
}

/* ================= atmosphere ================= */
html { font-size: 100%; }
.stApp {
    font-family: var(--font-body);
    color: var(--ink);
    line-height: 1.5;
    overflow-x: hidden;
    background:
        radial-gradient(1100px 620px at 88% -8%, rgba(76,118,196,0.42), transparent 62%),
        radial-gradient(900px 560px at -6% 108%, rgba(44,83,143,0.55), transparent 60%),
        linear-gradient(180deg, #0D2144 0%, #0A1A34 55%, #081529 100%);
    background-attachment: fixed;
}
/* slow drifting light, like sun off water */
.stApp::before {
    content: "";
    position: fixed; inset: -20%;
    z-index: 0; pointer-events: none;
    background:
        radial-gradient(600px 400px at 30% 30%, rgba(120,160,230,0.14), transparent 70%),
        radial-gradient(500px 360px at 75% 70%, rgba(92,134,200,0.12), transparent 70%);
    animation: sgDrift 38s var(--ease-io) infinite alternate;
}
/* film grain keeps the gradients from banding and adds depth */
.stApp::after {
    content: "";
    position: fixed; inset: 0; z-index: 0; pointer-events: none; opacity: 0.07; mix-blend-mode: overlay;
    background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='180' height='180'><filter id='n'><feTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='2' stitchTiles='stitch'/></filter><rect width='180' height='180' filter='url(%23n)'/></svg>");
}
@keyframes sgDrift { from { transform: translate3d(-3%, -2%, 0) scale(1); } to { transform: translate3d(4%, 3%, 0) scale(1.12); } }

.stApp p, .stApp label, .stApp li, .stApp input, .stApp textarea { font-family: var(--font-body); }
::selection { background: var(--gold); color: var(--gold-ink); }
#MainMenu, footer { visibility: hidden; }
[data-testid="stAppDeployButton"] { display: none; }
header[data-testid="stHeader"] { background: transparent; }
.block-container, [data-testid="stMainBlockContainer"] {
    position: relative; z-index: 1;
    padding-top: 3rem; padding-bottom: 1rem; max-width: 1240px;
}
* { scrollbar-width: thin; scrollbar-color: rgba(255,255,255,0.28) transparent; }
*::-webkit-scrollbar { width: 10px; height: 10px; }
*::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.28); border-radius: 10px; }

div[data-testid="stHorizontalBlock"] { flex-wrap: wrap; row-gap: var(--s-3); gap: var(--s-3); }
div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"] { min-width: 0; flex: 1 1 200px; }

h1, h2, h3, .stApp h1, .stApp h2, .stApp h3 { font-family: var(--font-display); color: var(--ink); letter-spacing: 0.01em; }
[data-testid="stCaptionContainer"], [data-testid="stCaptionContainer"] p { color: var(--ink-2); font-size: 0.95rem; opacity: 1; }
hr { border: 0; border-top: 1px solid var(--line); margin: var(--s-4) 0; }

/* every block rises into place on first paint; identical re-renders never replay it */
[data-testid="stMainBlockContainer"] [data-testid="stVerticalBlock"] > [data-testid="stElementContainer"] {
    animation: sgRise 640ms var(--ease-out) both;
}
[data-testid="stElementContainer"]:nth-child(2) { animation-delay: 60ms; }
[data-testid="stElementContainer"]:nth-child(3) { animation-delay: 120ms; }
[data-testid="stElementContainer"]:nth-child(4) { animation-delay: 180ms; }
[data-testid="stElementContainer"]:nth-child(5) { animation-delay: 240ms; }
[data-testid="stElementContainer"]:nth-child(6) { animation-delay: 300ms; }
[data-testid="stElementContainer"]:nth-child(n+7) { animation-delay: 360ms; }
@keyframes sgRise { from { opacity: 0; transform: translateY(18px); } to { opacity: 1; transform: none; } }

/* ================= focus ================= */
:focus-visible { outline: 3px solid var(--gold); outline-offset: 3px; }

/* ================= rail ================= */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, rgba(6,14,29,0.78), rgba(10,24,48,0.62));
    -webkit-backdrop-filter: blur(22px) saturate(140%);
    backdrop-filter: blur(22px) saturate(140%);
    border-right: 1px solid var(--line);
    overflow: hidden;
}
section[data-testid="stSidebar"][aria-expanded="true"] { width: 250px !important; min-width: 250px !important; }
/* two layers of water drifting along the foot of the rail */
section[data-testid="stSidebar"]::after {
    content: ""; position: absolute; left: 0; right: 0; bottom: 0; height: 120px; pointer-events: none;
    background:
        url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='600' height='60' viewBox='0 0 600 60' preserveAspectRatio='none'><path d='M0 30 Q75 6 150 30 T300 30 T450 30 T600 30 V60 H0Z' fill='rgba(92,134,200,0.22)'/></svg>") repeat-x 0 60px / 600px 60px,
        url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='600' height='60' viewBox='0 0 600 60' preserveAspectRatio='none'><path d='M0 34 Q75 58 150 34 T300 34 T450 34 T600 34 V60 H0Z' fill='rgba(255,255,255,0.08)'/></svg>") repeat-x 0 76px / 600px 60px;
    animation: sgWaves 14s linear infinite;
}
@keyframes sgWaves { from { background-position: 0 60px, 0 76px; } to { background-position: 600px 60px, -600px 76px; } }
section[data-testid="stSidebar"] [data-testid="stSidebarContent"] { padding: var(--s-2) var(--s-3); position: relative; z-index: 1; }
section[data-testid="stSidebar"] * { color: var(--ink); }
section[data-testid="stSidebar"] h1 {
    font-family: var(--font-display); font-weight: 900; font-size: 1.6rem; text-transform: uppercase; white-space: nowrap;
    letter-spacing: 0.02em; line-height: 0.95; padding: var(--s-2) 0 2px 0;
}
section[data-testid="stSidebar"] [data-testid="stCaptionContainer"] p { color: var(--ink-2); font-size: 0.88rem; }
section[data-testid="stSidebar"] [data-testid="stIconMaterial"], [data-testid="stIconMaterial"] { font-family: 'Material Symbols Rounded' !important; }
section[data-testid="stSidebar"] hr { border-top-color: var(--line); }
section[data-testid="stSidebar"] [data-testid="stRadio"] > label { display: none; }
section[data-testid="stSidebar"] [data-testid="stRadioGroup"] { gap: 3px; }
section[data-testid="stSidebar"] [data-testid="stImage"] img { max-width: 150px; margin: 0 auto; display: block; }
section[data-testid="stSidebar"] label[data-testid="stRadioOption"] {
    min-height: 46px; padding: 0 var(--s-3) 0 38px; border-radius: 12px; position: relative;
    display: flex; align-items: center; cursor: pointer; border: 1px solid transparent;
    transition: background 200ms var(--ease-out), border-color 200ms var(--ease-out), transform 200ms var(--ease-out);
}
section[data-testid="stSidebar"] label[data-testid="stRadioOption"] > div > div > div:first-child { display: none; }
section[data-testid="stSidebar"] label[data-testid="stRadioOption"] p {
    font-family: var(--font-display); font-weight: 800; font-size: 1.3rem; letter-spacing: 0.03em; text-transform: uppercase;
    color: var(--ink-2); white-space: nowrap; transition: color 200ms var(--ease-out), letter-spacing 300ms var(--ease-out);
}
section[data-testid="stSidebar"] label[data-testid="stRadioOption"]:hover { background: var(--glass-2); transform: translateX(3px); }
section[data-testid="stSidebar"] label[data-testid="stRadioOption"]:hover p { color: var(--ink); }
section[data-testid="stSidebar"] label[data-testid="stRadioOption"][data-selected="true"] {
    background: linear-gradient(180deg, rgba(255,255,255,0.20), rgba(255,255,255,0.09));
    border-color: var(--line-strong);
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.35), 0 10px 30px -10px rgba(0,0,0,0.6);
}
section[data-testid="stSidebar"] label[data-testid="stRadioOption"][data-selected="true"] p { color: var(--ink); letter-spacing: 0.05em; }
section[data-testid="stSidebar"] label[data-testid="stRadioOption"][data-selected="true"]::before {
    content: ""; position: absolute; left: 18px; top: 50%; width: 8px; height: 8px; margin-top: -4px; border-radius: 50%;
    background: var(--gold); box-shadow: 0 0 14px 3px rgba(242,183,5,0.7);
}

/* ================= buttons ================= */
.stButton > button, .stFormSubmitButton > button, .stDownloadButton > button {
    position: relative; overflow: hidden;
    background: var(--ink); color: var(--navy);
    border: 0; border-radius: 14px;
    font-family: var(--font-display); font-weight: 800; font-size: 1.25rem; letter-spacing: 0.05em; text-transform: uppercase;
    padding: 0 var(--s-3); min-height: 48px;
    box-shadow: 0 1px 0 rgba(255,255,255,0.6) inset, 0 14px 28px -14px rgba(0,0,0,0.7);
    transition: transform 220ms var(--ease-out), box-shadow 220ms var(--ease-out), background 160ms var(--ease-out);
}
.stApp .stButton > button p, .stApp .stFormSubmitButton > button p, .stApp .stDownloadButton > button p {
    font-family: var(--font-display) !important; font-weight: 800 !important; font-size: 1.25rem !important;
    letter-spacing: 0.05em; text-transform: uppercase; color: var(--navy);
}
.stButton > button::after, .stFormSubmitButton > button::after {
    content: ""; position: absolute; top: 0; bottom: 0; left: -60%; width: 40%;
    background: linear-gradient(105deg, transparent, rgba(92,134,200,0.45), transparent);
    transform: skewX(-18deg); transition: left 700ms var(--ease-out);
}
.stButton > button:hover, .stFormSubmitButton > button:hover { background: #fff; color: var(--navy); transform: translateY(-2px); box-shadow: 0 1px 0 rgba(255,255,255,0.6) inset, 0 22px 34px -14px rgba(0,0,0,0.75); }
.stButton > button:hover::after, .stFormSubmitButton > button:hover::after { left: 130%; }
.stButton > button:active, .stFormSubmitButton > button:active { transform: translateY(1px) scale(0.985); }
.stButton > button:disabled { background: var(--glass-2); color: var(--ink-3); box-shadow: none; }
.stDownloadButton > button { background: transparent; color: var(--ink); border: 1.5px solid var(--line-strong); box-shadow: none; min-height: 44px; }
.stApp .stDownloadButton > button p { color: var(--ink); font-size: 1.25rem !important; }
.stDownloadButton > button:hover { background: var(--glass-2); transform: translateY(-2px); }

/* ================= inputs ================= */
[data-testid="stWidgetLabel"] p, .stApp label p { font-family: var(--font-body); font-weight: 600; font-size: 0.92rem; color: var(--ink-2); letter-spacing: 0.02em; }
[data-testid="stTextInputRootElement"],
[data-testid="stSelectbox"] div:has(> input),
[data-testid="stMultiSelect"] div[role="group"]:has(> [data-testid="stMultiSelectTagsContainer"]),
[data-testid="stNumberInputContainer"],
[data-testid="stTextAreaRootElement"] {
    background: rgba(255,255,255,0.07) !important;
    border: 1px solid var(--line-strong) !important;
    border-radius: 12px !important; min-height: 46px; height: auto;
    transition: border-color 180ms var(--ease-out), box-shadow 180ms var(--ease-out), background 180ms var(--ease-out);
}
[data-testid="stTextInputRootElement"]:hover, [data-testid="stSelectbox"] div:has(> input):hover { background: rgba(255,255,255,0.10) !important; }
[data-testid="stTextInputRootElement"]:focus-within,
[data-testid="stSelectbox"] div:has(> input):focus-within,
[data-testid="stMultiSelect"] div[role="group"]:focus-within,
[data-testid="stNumberInputContainer"]:focus-within,
[data-testid="stTextAreaRootElement"]:focus-within {
    border-color: #fff !important; background: rgba(255,255,255,0.12) !important;
    box-shadow: 0 0 0 4px rgba(255,255,255,0.16), 0 0 34px -4px rgba(92,134,200,0.7);
}
.stTextInput input, .stSelectbox input, .stMultiSelect input { font-family: var(--font-body); font-size: 1.1rem; color: var(--ink); }
.stTextInput input[type="password"] {
    font-family: var(--font-display); font-size: 2rem; font-weight: 800; letter-spacing: 0.55em;
    font-variant-numeric: tabular-nums; height: 52px; color: var(--ink);
}
[data-testid="stTextInputRootElement"]:has(input[type="password"]) { min-height: 56px; }
[data-testid="stMultiSelectTagsContainer"] { border: 0 !important; background: transparent !important; }
[data-baseweb="tag"] { background: var(--navy-3) !important; border-radius: 8px; }
[data-baseweb="tag"] * { color: #fff !important; }
[data-baseweb="popover"] li[role="option"] { font-family: var(--font-body); min-height: 50px; }

/* ================= forms / containers ================= */
[data-testid="stForm"] {
    background: linear-gradient(180deg, rgba(255,255,255,0.10), rgba(255,255,255,0.04));
    border: 1px solid var(--line-strong); border-radius: 18px; padding: var(--s-3);
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.22), 0 30px 60px -30px rgba(0,0,0,0.75);
    -webkit-backdrop-filter: blur(10px); backdrop-filter: blur(10px);
}
div[data-testid="stExpander"] { border: 1px solid var(--line); border-radius: var(--r); background: var(--glass); }
div[data-testid="stExpander"] summary { min-height: 56px; }
div[data-testid="stExpander"] summary p { font-weight: 600; font-size: 1rem; color: var(--ink); }
[data-testid="stAlertContainer"] { border-radius: var(--r); border: 1px solid var(--line-strong); background: var(--glass-2) !important; }
[data-testid="stAlertContainer"] p { color: var(--ink); font-weight: 500; }
[data-testid="stAlertContainer"]:has([data-testid="stAlertContentError"]) { background: rgba(229,33,63,0.18) !important; border-color: var(--red); }
[data-testid="stAlertContainer"]:has([data-testid="stAlertContentWarning"]) { background: rgba(242,183,5,0.16) !important; border-color: var(--gold); }
[data-testid="stAlertContainer"]:has([data-testid="stAlertContentSuccess"]) { background: rgba(255,255,255,0.12) !important; border-color: #fff; }

[role="tablist"] { gap: var(--s-1); border-bottom: 1px solid var(--line); }
[data-testid="stTab"] { min-height: 58px; padding: 0 var(--s-3); position: relative; }
.stApp [data-testid="stTab"] [data-testid="stMarkdownContainer"] p {
    font-family: var(--font-display) !important; font-weight: 800 !important; font-size: 1.5rem !important;
    text-transform: uppercase; letter-spacing: 0.04em; color: var(--ink-3);
}
.stApp [data-testid="stTab"][aria-selected="true"] [data-testid="stMarkdownContainer"] p { color: var(--ink); }
[data-testid="stTab"][aria-selected="true"]::after, [data-testid="stTab"] .react-aria-SelectionIndicator {
    content: ""; position: absolute; left: 0; right: 0; bottom: -1px; height: 4px; background: #fff; border-radius: 4px 4px 0 0;
    box-shadow: 0 0 18px 2px rgba(255,255,255,0.5);
}
[data-testid="stMetricValue"] { font-family: var(--font-display); font-weight: 900; font-variant-numeric: tabular-nums; color: var(--ink); font-size: 3.4rem; }
[data-testid="stMetricLabel"] p { color: var(--ink-2); font-weight: 600; }
[data-testid="stCheckbox"] label { min-height: 46px; }
[data-testid="stDataFrame"] { border: 1px solid var(--line); border-radius: var(--r); overflow: hidden; }

/* ================= signature pieces ================= */
.sg-flag { display: block; flex: none; overflow: visible; border-radius: 2px; filter: drop-shadow(0 6px 10px rgba(0,0,0,0.45)); }
.sg-flag.sg-cloth { filter: drop-shadow(0 10px 16px rgba(0,0,0,0.5)); border-radius: 0; }
.sg-ripple { animation: sgRipple 2.6s ease-in-out infinite; }
@keyframes sgRipple { 0% { transform: translateY(calc(var(--a) * -1)); } 50% { transform: translateY(var(--a)); } 100% { transform: translateY(calc(var(--a) * -1)); } }

/* page head: huge title, a string of swaying signal pennants, context at right */
.sg-head { position: relative; margin-bottom: var(--s-3); padding-bottom: var(--s-2); border-bottom: 1px solid var(--line-strong); }
.sg-head-row { display: flex; align-items: flex-end; justify-content: space-between; gap: var(--s-3); }
.sg-title {
    font-family: var(--font-display); font-weight: 900; text-transform: uppercase; letter-spacing: 0.01em;
    font-size: clamp(2.4rem, 4.2vw, 3.4rem); line-height: 0.95; color: var(--ink); text-wrap: balance;
    text-shadow: 0 6px 40px rgba(0,0,0,0.45);
    animation: sgTitleIn 900ms var(--ease-out) both;
}
@keyframes sgTitleIn { from { opacity: 0; transform: translateY(26px); letter-spacing: 0.08em; filter: blur(8px); } to { opacity: 1; transform: none; letter-spacing: 0.01em; filter: none; } }
.sg-head-r { font-family: var(--font-display); font-weight: 700; font-size: 1rem; letter-spacing: 0.14em; text-transform: uppercase; color: var(--ink-2); text-align: right; padding-bottom: 4px; white-space: nowrap; }
.sg-bunting { display: block; width: 100%; height: 46px; margin-top: 6px; overflow: visible; }
.sg-sway { transform-box: fill-box; transform-origin: 50% 0; animation: sgSway 3.6s ease-in-out infinite alternate; }
@keyframes sgSway { from { transform: rotate(-7deg); } to { transform: rotate(7deg); } }

.sg-section {
    font-family: var(--font-display); font-size: 1.6rem; font-weight: 800; text-transform: uppercase; letter-spacing: 0.03em;
    color: var(--ink); margin: var(--s-3) 0 var(--s-1) 0; display: flex; align-items: center; gap: var(--s-2); line-height: 1;
}
.sg-count { font-family: var(--font-mono); font-size: 1rem; background: var(--glass-2); border: 1px solid var(--line-strong); padding: 3px 12px; border-radius: 999px; }

.sg-live { display: inline-flex; align-items: center; gap: 10px; font-family: var(--font-mono); font-size: 0.9rem; font-weight: 500; color: var(--ink-2); margin: 0 0 var(--s-3) 0; }
.sg-live i { width: 9px; height: 9px; border-radius: 50%; background: #6EE7A8; box-shadow: 0 0 0 0 rgba(110,231,168,0.6); animation: sgPing 2.2s ease-out infinite; }
@keyframes sgPing { 0% { box-shadow: 0 0 0 0 rgba(110,231,168,0.55); } 80%, 100% { box-shadow: 0 0 0 12px rgba(110,231,168,0); } }

/* ---------- the board ---------- */
.sg-board { display: flex; flex-direction: column; gap: 6px; }
.sg-boardhead, .sg-row { display: grid; grid-template-columns: 56px minmax(0, 1.5fr) minmax(0, 1.2fr) 130px 160px; column-gap: var(--s-3); align-items: center; }
.sg-boardhead { font-family: var(--font-display); font-size: 1rem; font-weight: 700; letter-spacing: 0.14em; text-transform: uppercase; color: var(--ink-3); padding: 0 var(--s-4); }
.sg-row {
    position: relative; padding: 10px var(--s-3); border-radius: 14px;
    background: linear-gradient(90deg, rgba(255,255,255,0.09), rgba(255,255,255,0.035));
    border: 1px solid var(--line);
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.12);
    transition: transform 260ms var(--ease-out), background 260ms var(--ease-out), border-color 260ms var(--ease-out);
    animation: sgRowIn 700ms var(--ease-out) both;
}
.sg-row:nth-child(2) { animation-delay: 80ms; } .sg-row:nth-child(3) { animation-delay: 160ms; } .sg-row:nth-child(4) { animation-delay: 240ms; }
.sg-row:nth-child(5) { animation-delay: 320ms; } .sg-row:nth-child(n+6) { animation-delay: 400ms; }
@keyframes sgRowIn { from { opacity: 0; transform: translateX(-28px); } to { opacity: 1; transform: none; } }
.sg-row:hover { transform: translateX(6px); background: linear-gradient(90deg, rgba(255,255,255,0.15), rgba(255,255,255,0.05)); border-color: var(--line-strong); }
.sg-row .sg-name { font-family: var(--font-display); font-size: 1.8rem; font-weight: 800; color: var(--ink); line-height: 0.95; letter-spacing: 0.01em; overflow-wrap: anywhere; }
.sg-row .sg-reason { font-weight: 600; color: var(--ink); font-size: 0.95rem; }
.sg-row .sg-detail { color: var(--ink-2); font-size: 0.82rem; }
.sg-row .sg-time { font-family: var(--font-mono); font-size: 1rem; font-weight: 600; color: var(--ink); font-variant-numeric: tabular-nums; line-height: 1.15; }
.sg-row .sg-status { justify-self: end; text-align: right; }
.sg-badge { display: inline-block; font-family: var(--font-display); font-weight: 800; font-size: 1.1rem; letter-spacing: 0.08em; text-transform: uppercase; padding: 4px 12px; border-radius: 999px; background: var(--glass-2); border: 1px solid var(--line-strong); color: var(--ink); white-space: nowrap; }
/* late: a loud, tall row with a live red glow */
.sg-row.sg-late {
    padding-top: var(--s-3); padding-bottom: var(--s-3);
    background: radial-gradient(700px 160px at 12% 50%, var(--red-glow), transparent 70%), linear-gradient(90deg, rgba(229,33,63,0.26), rgba(229,33,63,0.08));
    border-color: rgba(229,33,63,0.75);
}
.sg-row.sg-late .sg-name { font-size: 2.5rem; }
.sg-row.sg-late .sg-badge { background: var(--red); border-color: #ff6b81; font-size: 1.5rem; padding: 6px 16px; animation: sgBeat 2.4s ease-in-out infinite; box-shadow: 0 10px 30px -8px rgba(229,33,63,0.9); }
@keyframes sgBeat { 0%, 100% { transform: scale(1); } 50% { transform: scale(1.06); } }
.sg-row.sg-forgot { padding-top: var(--s-2); padding-bottom: var(--s-2); background: linear-gradient(90deg, rgba(242,183,5,0.16), rgba(242,183,5,0.05)); border-color: rgba(242,183,5,0.5); }
.sg-row.sg-forgot .sg-name { font-size: 1.4rem; }
.sg-row.sg-forgot .sg-time { font-size: 0.9rem; }
.sg-row.sg-forgot .sg-badge { background: var(--gold); border-color: var(--gold); color: var(--gold-ink); font-size: 0.9rem; }

.sg-empty { display: flex; align-items: center; gap: var(--s-3); padding: var(--s-4); border-radius: 18px; border: 1px dashed var(--line-strong); background: var(--glass); font-family: var(--font-display); font-size: 1.8rem; font-weight: 800; text-transform: uppercase; letter-spacing: 0.02em; line-height: 1; }
.sg-empty span { font-family: var(--font-body); font-weight: 400; text-transform: none; letter-spacing: 0; font-size: 1.05rem; color: var(--ink-2); display: block; margin-top: 8px; }
.sg-dayoff { display: flex; flex-wrap: wrap; gap: var(--s-1) var(--s-4); font-family: var(--font-display); font-size: 1.25rem; font-weight: 700; letter-spacing: 0.03em; text-transform: uppercase; color: var(--ink); padding: 2px 0; }
.sg-note { padding: var(--s-4); border-radius: var(--r); border: 1px dashed var(--line-strong); color: var(--ink-2); font-size: 1.05rem; background: var(--glass); }

/* ---------- sign in/out hero ---------- */
.sg-mode { display: grid; grid-template-columns: minmax(0, 3fr) minmax(0, 2fr); gap: var(--s-2); margin-bottom: var(--s-2); }
.sg-mode-out, .sg-mode-in { position: relative; overflow: hidden; border-radius: 20px; padding: var(--s-3) var(--s-4); display: flex; align-items: center; gap: var(--s-3); min-height: 0; }
.sg-mode-out {
    background: radial-gradient(600px 300px at 0% 0%, rgba(92,134,200,0.55), transparent 65%), linear-gradient(135deg, #1E3A66, #13294B 70%);
    border: 1px solid var(--line-strong); box-shadow: inset 0 1px 0 rgba(255,255,255,0.3), 0 40px 70px -30px rgba(0,0,0,0.85);
}
.sg-mode-in { background: var(--glass); border: 1px solid var(--line); opacity: 0.92; }
.sg-mode-word { font-family: var(--font-display); font-size: 2.6rem; font-weight: 900; line-height: 0.9; text-transform: uppercase; color: var(--ink); letter-spacing: 0.01em; }
.sg-mode-in .sg-mode-word { font-size: 1.8rem; color: var(--ink-2); }
.sg-mode-reason { display: inline-block; margin-top: 6px; padding: 3px 12px; border-radius: 999px; background: #fff; color: var(--navy); font-weight: 700; font-size: 0.95rem; overflow-wrap: anywhere; }
.sg-mode-sub { font-size: 0.85rem; color: var(--ink-2); margin-top: 4px; }

/* ---------- banners (van / group steps) ---------- */
.sg-banner { display: flex; align-items: center; gap: var(--s-4); border-radius: 18px; padding: var(--s-3) var(--s-4); margin: var(--s-2) 0 var(--s-2) 0; border: 1px solid var(--line-strong); }
.sg-banner-out { background: radial-gradient(500px 240px at 0% 0%, rgba(92,134,200,0.5), transparent 65%), linear-gradient(135deg, #1E3A66, #13294B); box-shadow: inset 0 1px 0 rgba(255,255,255,0.3), 0 30px 60px -30px rgba(0,0,0,0.8); }
.sg-banner-in { background: var(--glass-2); }
.sg-banner-word { font-family: var(--font-display); font-size: 2rem; font-weight: 900; text-transform: uppercase; line-height: 0.92; color: var(--ink); overflow-wrap: anywhere; }
.sg-banner-sub { font-size: 0.95rem; margin-top: 4px; color: var(--ink-2); }

/* ---------- the hoist: confirmation ---------- */
.sg-flash { position: relative; overflow: hidden; display: grid; grid-template-columns: 110px minmax(0, 1fr); align-items: center; gap: var(--s-4); border-radius: 22px; padding: var(--s-3) var(--s-4); margin-bottom: var(--s-3); border: 1px solid var(--line-strong); }
.sg-flash-out { background: radial-gradient(700px 360px at 10% 0%, rgba(92,134,200,0.65), transparent 65%), linear-gradient(135deg, #24467A, #13294B 75%); box-shadow: inset 0 1px 0 rgba(255,255,255,0.35), 0 50px 90px -30px rgba(0,0,0,0.9); }
.sg-flash-in { background: radial-gradient(700px 360px at 10% 0%, rgba(255,255,255,0.28), transparent 65%), linear-gradient(135deg, #2C538F, #1E3A66 75%); box-shadow: inset 0 1px 0 rgba(255,255,255,0.4), 0 50px 90px -30px rgba(0,0,0,0.9); }
/* a burst ring rolls out as the flag reaches the top */
.sg-flash::after { content: ""; position: absolute; left: 55px; top: 50%; width: 40px; height: 40px; margin: -20px 0 0 -20px; border-radius: 50%; border: 2px solid rgba(255,255,255,0.7); opacity: 0; animation: sgRing 1100ms 420ms var(--ease-out) both; pointer-events: none; }
@keyframes sgRing { 0% { opacity: 0.9; transform: scale(0.4); } 100% { opacity: 0; transform: scale(14); } }
.sg-halyard { position: relative; width: 110px; height: 110px; overflow: hidden; }
.sg-halyard::before { content: ""; position: absolute; left: 10px; top: 0; bottom: 0; width: 5px; background: linear-gradient(180deg, #fff, rgba(255,255,255,0.4)); border-radius: 4px; box-shadow: 0 0 14px rgba(255,255,255,0.5); }
.sg-halyard .sg-flag { position: absolute; left: 12px; top: 6px; width: 90px; height: 60px; transform-origin: left center; }
.sg-flash-out .sg-halyard .sg-flag { animation: sgHoist 1100ms var(--ease-out) both; }
.sg-flash-in .sg-halyard .sg-flag { animation: sgStrike 1000ms var(--ease-out) both; }
@keyframes sgHoist { from { transform: translateY(104px); } to { transform: translateY(0); } }
@keyframes sgStrike { from { transform: translateY(0); } to { transform: translateY(44px); } }
.sg-flash-word { font-family: var(--font-display); font-size: clamp(1.8rem, 3.6vw, 2.8rem); font-weight: 900; line-height: 0.95; text-transform: uppercase; color: var(--ink); overflow-wrap: anywhere; text-shadow: 0 6px 40px rgba(0,0,0,0.4); animation: sgRise 800ms 250ms var(--ease-out) both; }
.sg-flash-sub { font-size: 1.05rem; font-weight: 500; margin-top: 6px; color: var(--ink-2); animation: sgRise 800ms 400ms var(--ease-out) both; }
.sg-flash-ask { margin-top: var(--s-3); padding-top: var(--s-2); border-top: 1px solid var(--line-strong); font-family: var(--font-display); font-size: 1.6rem; font-weight: 700; color: var(--ink); }

.sg-fork { display: flex; gap: var(--s-4); align-items: center; border-radius: 18px; padding: var(--s-3) var(--s-4); margin: var(--s-2) 0 var(--s-2) 0; background: linear-gradient(90deg, rgba(242,183,5,0.22), rgba(242,183,5,0.06)); border: 1px solid var(--gold); }
.sg-fork-head { font-family: var(--font-display); font-size: 1.6rem; font-weight: 800; line-height: 1; text-transform: uppercase; color: var(--ink); }
.sg-fork-sub { font-size: 0.95rem; color: var(--ink-2); margin-top: 6px; }

.sg-strip-title { font-family: var(--font-display); font-size: 1.4rem; font-weight: 800; text-transform: uppercase; color: var(--ink); margin: var(--s-3) 0 var(--s-2) 0; letter-spacing: 0.03em; }
.sg-strip-empty { color: var(--ink-2); font-size: 0.95rem; display: flex; align-items: center; gap: var(--s-2); }
.sg-strip { display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: var(--s-2); }
.sg-chip { display: inline-flex; align-items: center; gap: 10px; border: 1px solid var(--line-strong); border-radius: 999px; padding: 3px 12px 3px 8px; background: var(--glass-2); color: var(--ink); font-family: var(--font-display); font-weight: 800; font-size: 1.05rem; letter-spacing: 0.02em; transition: transform 200ms var(--ease-out), background 200ms; }
.sg-chip:hover { transform: translateY(-2px); background: rgba(255,255,255,0.16); }
.sg-chip small { font-family: var(--font-body); font-weight: 500; font-size: 0.85rem; color: var(--ink-2); letter-spacing: 0; }
.sg-chip-forgot { border-color: var(--gold); background: rgba(242,183,5,0.14); }
.sg-strip-label { font-family: var(--font-display); font-size: 1.05rem; font-weight: 800; color: var(--gold); margin: var(--s-2) 0 var(--s-1) 0; }

/* ---------- fleet (vans) ---------- */
.sg-fleet, .sg-van-board { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: var(--s-2); margin-bottom: var(--s-2); }
.sg-van {
    position: relative; overflow: hidden; border-radius: 18px; padding: var(--s-3); min-height: 120px;
    display: flex; flex-direction: column; gap: 8px; color: var(--ink);
    background: linear-gradient(180deg, rgba(255,255,255,0.11), rgba(255,255,255,0.04)); border: 1px solid var(--line-strong);
    box-shadow: inset 0 1px 0 rgba(255,255,255,0.22), 0 30px 50px -28px rgba(0,0,0,0.8);
    transition: transform 280ms var(--ease-out), border-color 280ms var(--ease-out);
    animation: sgRise 700ms var(--ease-out) both;
}
.sg-van:hover { transform: translateY(-5px); border-color: rgba(255,255,255,0.6); }
.sg-van.sg-van-out { background: radial-gradient(400px 220px at 100% 0%, rgba(255,255,255,0.6), transparent 65%), linear-gradient(160deg, #fff 0%, #DCE6F7 100%); color: var(--navy); border-color: #fff; }
.sg-van.sg-van-out * { color: var(--navy); }
.sg-van.sg-van-sel { box-shadow: 0 0 0 3px var(--gold), 0 0 50px -4px rgba(242,183,5,0.6); }
.sg-van-top { display: flex; align-items: center; justify-content: space-between; }
.sg-van-state { font-family: var(--font-display); font-size: 1.1rem; font-weight: 800; letter-spacing: 0.04em; color: var(--ink-2); }
.sg-van-name { font-family: var(--font-display); font-size: 2rem; font-weight: 900; line-height: 0.92; text-transform: uppercase; color: inherit; }
.sg-van-name small { display: block; font-family: var(--font-body); font-size: 0.8rem; font-weight: 600; letter-spacing: 0.16em; text-transform: uppercase; opacity: 0.7; margin-top: 8px; }
.sg-van-who { font-size: 0.95rem; font-weight: 600; color: inherit; }
.sg-van-meta { font-size: 0.85rem; opacity: 0.85; color: inherit; }
.sg-gas { display: flex; align-items: center; gap: 12px; margin-top: auto; color: inherit; }
.sg-gas-seg { display: inline-flex; gap: 4px; }
.sg-gas-seg i { width: 20px; height: 10px; border-radius: 4px; border: 1.5px solid currentColor; display: inline-block; opacity: 0.4; }
.sg-gas-seg i.on { background: currentColor; opacity: 1; box-shadow: 0 0 14px -2px currentColor; }
.sg-gas-seg.low i.on { background: var(--red); border-color: var(--red); box-shadow: 0 0 14px 0 rgba(229,33,63,0.8); }
.sg-gas-word { font-family: var(--font-display); font-size: 1.05rem; font-weight: 800; letter-spacing: 0.05em; text-transform: uppercase; color: inherit; }

.sg-inline-flash { border: 1px solid var(--line-strong); background: var(--glass-2); border-radius: var(--r); padding: var(--s-2) var(--s-3); font-weight: 600; margin-bottom: var(--s-2); animation: sgFade 2.4s var(--ease-out) forwards; }
@keyframes sgFade { 0% { opacity: 0; } 10% { opacity: 1; } 75% { opacity: 1; } 100% { opacity: 0; } }

/* ---------- emergency: the loudest thing in the app, and still ---------- */
.sg-emergency { display: grid; grid-template-columns: 72px minmax(0, 1fr); gap: var(--s-3); align-items: center; background: linear-gradient(135deg, #F0294A, #B8112F); color: #fff; border-radius: 18px; padding: var(--s-3) var(--s-4); margin: 0 0 var(--s-3) 0; border: 2px solid #ff9aa9; box-shadow: 0 40px 80px -30px rgba(229,33,63,0.85), inset 0 1px 0 rgba(255,255,255,0.4); }
.sg-emergency .sg-flag { width: 72px; height: 48px; }
.sg-emergency-word { font-family: var(--font-display); font-size: 2.2rem; font-weight: 900; letter-spacing: 0.02em; text-transform: uppercase; line-height: 0.95; color: #fff; }
.sg-emergency-msg { font-size: 1.1rem; font-weight: 600; margin-top: 8px; color: #fff; }

.sg-footer { margin-top: var(--s-2); padding-top: var(--s-1); border-top: 1px solid var(--line); font-family: var(--font-display); font-size: 0.85rem; font-weight: 700; letter-spacing: 0.18em; text-transform: uppercase; color: var(--ink-3); }

/* Who's Out shows vans as a compact strip so the whole board fits one screen */
.sg-van-board { grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: var(--s-2); }
.sg-van-board .sg-van { min-height: 0; padding: 10px var(--s-3); gap: 2px; border-radius: 14px; }
.sg-van-board .sg-van-name { font-size: 1.5rem; }
.sg-van-board .sg-van-name small { display: inline; margin: 0 0 0 8px; }
.sg-van-board .sg-van-meta { font-size: 0.8rem; }
.sg-van-board .sg-van-top { position: absolute; right: 12px; top: 10px; }
.sg-van-board .sg-van-state { display: none; }
.sg-van-board .sg-van { padding-right: 70px; }
.sg-live { margin: 0 0 var(--s-2) 0; }
.sg-empty { padding: var(--s-3); font-size: 1.5rem; }
.sg-empty span { margin-top: 2px; font-size: 0.9rem; }
.sg-dayoff { font-size: 1.05rem; gap: 0 var(--s-3); }

/* the live clock lives in an iframe pinned to the header, out of the page flow */
[data-testid="stElementContainer"]:has(iframe) { position: fixed !important; top: 4px; right: 230px; width: 300px; height: 50px; z-index: 50; margin: 0; animation: none !important; }
[data-testid="stElementContainer"]:has(iframe) iframe { width: 300px; height: 50px; border: 0; }

/* ---------- responsive ---------- */
@media (max-width: 900px) {
    html { font-size: 100%; }
    .block-container, [data-testid="stMainBlockContainer"] { padding-left: 1rem; padding-right: 1rem; }
    .sg-head-row { flex-direction: column; align-items: flex-start; gap: 4px; }
    .sg-head-r { text-align: left; padding-bottom: 0; }
    .sg-boardhead { display: none; }
    .sg-row { grid-template-columns: 56px minmax(0, 1fr); row-gap: 4px; padding: var(--s-3); }
    .sg-row .sg-time::before { content: "Out "; font-family: var(--font-body); font-size: 0.85rem; font-weight: 500; color: var(--ink-2); }
    .sg-row .sg-status .sg-time::before { content: "Due back "; }
    .sg-row .sg-status .sg-detail { display: none; }
    .sg-row > * { grid-column: 2; }
    .sg-row > .sg-flagcell { grid-column: 1; grid-row: 1 / span 3; }
    .sg-row .sg-status { justify-self: start; text-align: left; }
    .sg-row .sg-name { font-size: 2.1rem; }
    .sg-row.sg-late .sg-name { font-size: 2.6rem; }
    .sg-mode { grid-template-columns: 1fr; }
    .sg-mode-out, .sg-mode-in { padding: var(--s-3); min-height: 0; gap: var(--s-3); }
    .sg-mode-word { font-size: 3rem; }
    .sg-mode-in .sg-mode-word { font-size: 2.2rem; }
    .sg-flash { grid-template-columns: 96px minmax(0, 1fr); padding: var(--s-3); gap: var(--s-3); }
    .sg-halyard { width: 96px; height: 120px; }
    .sg-halyard .sg-flag { width: 78px; height: 52px; }
    .sg-emergency { grid-template-columns: 1fr; }
    .sg-emergency-word { font-size: 2.3rem; }
    [data-testid="stElementContainer"]:has(iframe) { display: none; }
}
@media (prefers-reduced-motion: reduce) {
    .stApp::before, section[data-testid="stSidebar"]::after, .sg-sway, .sg-row, .sg-row.sg-late .sg-badge, .sg-live i,
    .sg-flash .sg-halyard .sg-flag, .sg-flash::after, .sg-title, .sg-van, .sg-inline-flash,
    [data-testid="stElementContainer"] { animation: none !important; }
    .sg-ripple { animation: none !important; }
}
</style>
"""
