"""Stamp the CSS and JS links in index.html with a short hash of each file.

    python site/stamp_assets.py

Run it before committing any change to styles.css or script.js. The link becomes
styles.css?v=<hash>, and the hash changes whenever the file does, so a browser or CDN
can never pair a new page with an old stylesheet (which makes the layout look broken).
"""
import hashlib
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
html = (HERE / "index.html").read_text(encoding="utf-8")
for name in ("styles.css", "script.js"):
    h = hashlib.sha256((HERE / name).read_bytes()).hexdigest()[:8]
    html, n = re.subn(rf'({re.escape(name)})(\?v=[0-9a-f]+)?(")', rf'\1?v={h}\3', html)
    print(f"{name}: v={h} ({n} link{'s' if n != 1 else ''} updated)")
(HERE / "index.html").write_text(html, encoding="utf-8")
