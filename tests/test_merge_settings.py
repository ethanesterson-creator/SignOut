"""Tests for provision/merge_settings.py: settings in the repo, secrets in the host.

Run:  python tests/test_merge_settings.py   (or pytest)
"""
import os
import sys
import tomllib

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "provision"))
import camp_config  # noqa: E402
import merge_settings as ms  # noqa: E402

FULL = """
admin_password = "pw-secret"
pin_salt = "salt-secret"
ntfy_topic = "topic-secret"
resend_api_key = "re_secret"
spreadsheet_id = "SHEET123"
camp_name = "Camp Example"
camp_timezone = "US/Eastern"
due_soon_minutes = 10
vans = ["Van 1", "Van 2"]
default_schedule = [["Period 1", "09:00", "09:50"], ["Period 2", "10:00", "10:50"]]

[van_labels]
"Van 1" = "Van 1"
"Van 2" = "Van 2"

[[reasons]]
name = "Period Off"
rule = "period_end"
grace_min = 5

[[reasons]]
name = "Night Off"
rule = "next_clock"
hour = 23
minute = 30

[gcp_service_account]
type = "service_account"
client_email = "x@y.iam.gserviceaccount.com"
private_key = "-----BEGIN PRIVATE KEY-----abc-----END PRIVATE KEY-----"
"""


def test_split_keeps_every_secret_out_of_the_public_file():
    private_text, public_text = ms.split(FULL)
    for secret in ("pw-secret", "salt-secret", "topic-secret", "re_secret", "SHEET123", "PRIVATE KEY"):
        assert secret not in public_text, secret
        assert secret in private_text, secret
    pub = tomllib.loads(public_text)
    assert pub["camp_name"] == "Camp Example" and pub["due_soon_minutes"] == 10
    assert [r["name"] for r in pub["reasons"]] == ["Period Off", "Night Off"]
    assert pub["van_labels"]["Van 2"] == "Van 2"
    priv = tomllib.loads(private_text)
    assert "camp_name" not in priv and priv["gcp_service_account"]["client_email"].startswith("x@")


def test_split_then_merge_gives_back_the_same_configuration():
    private_text, public_text = ms.split(FULL)
    merged = tomllib.loads(ms.merge(private_text, tomllib.loads(public_text)))
    original = tomllib.loads(FULL)
    assert merged == original
    cfg = camp_config.load(merged)
    assert cfg.name == "Camp Example" and cfg.due_soon_minutes == 10 and cfg.spreadsheet_id == "SHEET123"
    assert cfg.reasons[1].hour == 23 and cfg.vans == ("Van 1", "Van 2")


def test_committed_file_wins_for_settings_and_a_change_is_picked_up():
    private_text, public_text = ms.split(FULL)
    public = tomllib.loads(public_text)
    public["due_soon_minutes"] = 20
    public["compact_admin"] = True
    merged = tomllib.loads(ms.merge(private_text, public))
    assert merged["due_soon_minutes"] == 20 and merged["compact_admin"] is True
    assert merged["admin_password"] == "pw-secret"                        # private key untouched


def test_old_style_full_secret_file_still_works_during_the_move():
    """A host that still has the whole old secrets.toml: committed settings override it."""
    public = {"camp_name": "Camp Renamed", "server_late_check": True}
    merged = tomllib.loads(ms.merge(FULL, public))
    assert merged["camp_name"] == "Camp Renamed" and merged["server_late_check"] is True
    assert merged["due_soon_minutes"] == 10 and merged["admin_password"] == "pw-secret"


def test_a_committed_file_can_never_carry_a_private_key():
    for key in ("admin_password", "resend_api_key", "spreadsheet_id", "ntfy_topic", "gcp_service_account"):
        try:
            ms.merge(FULL, {key: "x"})
        except ms.SettingsError as e:
            assert key in str(e)
        else:
            raise AssertionError(f"{key} should be refused in the committed file")


def test_missing_service_account_is_a_clear_error():
    try:
        ms.merge('camp_name = "x"', {})
    except ms.SettingsError as e:
        assert "gcp_service_account" in str(e)
    else:
        raise AssertionError("expected SettingsError")


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print("PASS", t.__name__)
    print("ALL PASS")
