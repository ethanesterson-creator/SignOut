"""Keep a camp's settings in the repo and only its secrets in the host's secret file.

    # At startup (start.sh does this when CAMP_SLUG is set):
    python provision/merge_settings.py merge .streamlit/secrets.toml deploy_config/<slug>.toml

    # Once, to split an existing full secrets.toml into the two files:
    python provision/merge_settings.py split camps/<slug>/secrets.toml deploy_config/<slug>.toml camps/<slug>/secrets.private.toml

Why: every setting change used to mean re-pasting the whole secrets file into the host.
Now the settings (name, reasons, schedule, switches, alert recipients) live in
deploy_config/<slug>.toml, which is committed, so changing one is a normal push.
Keys that must stay private can never be read from the committed file.
"""

import sys
import tomllib
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))
import new_camp  # noqa: E402

# Anything here lives only in the host's secret file. A committed settings file that
# tries to set one of these is rejected, so a secret can never end up in the repo.
PRIVATE_KEYS = {
    "admin_password", "pin_salt", "ntfy_topic", "ntfy_topic_vans", "ops_ntfy_topic",
    "ops_ntfy_server", "resend_api_key", "spreadsheet_id",
}
SA_KEY = "gcp_service_account"
TABLE_KEYS = ("van_labels", "reasons")


class SettingsError(ValueError):
    pass


def check_public(public: dict):
    bad = sorted((set(public) & PRIVATE_KEYS) | ({SA_KEY} & set(public)))
    if bad:
        raise SettingsError(
            "The committed settings file may not contain private keys: " + ", ".join(bad)
            + ". Keep those in the host's secret file."
        )


def merge(secrets_text: str, public: dict) -> str:
    """Combined settings text. For any setting in both files, the committed file wins;
    private keys always come from the secret file."""
    check_public(public)
    sec = tomllib.loads(secrets_text)
    sa = sec.pop(SA_KEY, None)
    if not sa:
        raise SettingsError("The secret file has no [gcp_service_account] section.")
    private = {k: sec.pop(k) for k in list(sec) if k in PRIVATE_KEYS}
    settings = {**sec, **public}
    flat = {**private, **{k: v for k, v in settings.items() if k not in TABLE_KEYS}}
    return new_camp.render_toml(
        flat, settings.get("van_labels"), settings.get("reasons", []), sa,
        header="# Built at startup by provision/merge_settings.py. Contains secrets.",
    )


def split(full_text: str):
    """(private_text, public_text) from a complete secrets.toml."""
    full = tomllib.loads(full_text)
    sa = full.pop(SA_KEY)
    private = {k: full.pop(k) for k in list(full) if k in PRIVATE_KEYS}
    public_flat = {k: v for k, v in full.items() if k not in TABLE_KEYS}
    private_text = new_camp.render_toml(
        private, service_account=sa,
        header="# Private settings only. Paste this into the host's secret file. Everything else is in deploy_config/.",
    )
    public_text = new_camp.render_toml(
        public_flat, full.get("van_labels"), full.get("reasons", []),
        header="# This camp's settings. Committed to the repo. No secrets here.",
    )
    return private_text, public_text


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    try:
        if argv[:1] == ["merge"] and len(argv) == 3:
            sp, pp = Path(argv[1]), Path(argv[2])
            public = tomllib.loads(pp.read_text(encoding="utf-8"))
            sp.write_text(merge(sp.read_text(encoding="utf-8"), public), encoding="utf-8")
            print(f"Settings merged from {pp.name}.")
            return 0
        if argv[:1] == ["split"] and len(argv) == 4:
            private_text, public_text = split(Path(argv[1]).read_text(encoding="utf-8"))
            Path(argv[2]).parent.mkdir(parents=True, exist_ok=True)
            Path(argv[2]).write_text(public_text, encoding="utf-8")
            Path(argv[3]).write_text(private_text, encoding="utf-8")
            print(f"Wrote {argv[2]} (commit this) and {argv[3]} (private; paste into the host).")
            return 0
    except (SettingsError, FileNotFoundError, tomllib.TOMLDecodeError) as e:
        print(f"merge_settings: {e}", file=sys.stderr)
        return 1
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
