"""Tests for emailalert and the alert-email settings. No network: the HTTP call is faked.

Run:  python tests/test_emailalert.py   (or pytest)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import camp_config as cc  # noqa: E402
import emailalert  # noqa: E402


class _Resp:
    def __init__(self, code):
        self.status_code = code


def test_send_builds_the_right_request():
    seen = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        seen.update(url=url, json=json, headers=headers, timeout=timeout)
        return _Resp(200)

    ok = emailalert.send("key_123", "Headcount <alerts@headcountcamp.com>", ["office@camp.org"],
                         "subj", "body", post=fake_post, background=False)
    assert ok is True
    assert seen["url"] == emailalert.RESEND_URL
    assert seen["json"] == {"from": "Headcount <alerts@headcountcamp.com>", "to": ["office@camp.org"],
                            "subject": "subj", "text": "body"}
    assert seen["headers"]["Authorization"] == "Bearer key_123"
    assert seen["timeout"]


def test_send_never_raises_and_skips_when_unconfigured():
    def boom(*a, **k):
        raise RuntimeError("network down")

    assert emailalert.send("k", "a@b.co", ["x@y.org"], "s", "t", post=boom, background=False) is False
    assert emailalert.send("", "a@b.co", ["x@y.org"], "s", "t", background=False) is False   # no key
    assert emailalert.send("k", "", ["x@y.org"], "s", "t", background=False) is False        # no sender
    assert emailalert.send("k", "a@b.co", [], "s", "t", background=False) is False           # no recipients
    assert emailalert.send("k", "a@b.co", ["x@y.org"], "s", "t",
                           post=lambda *a, **k: _Resp(500), background=False) is False       # service error


def test_messages_read_plainly():
    subj, body = emailalert.late_message("Camp Young Judaea", "Casey Hayes", 12, "Night Off", "Tue 12:15 AM")
    assert subj == "Camp Young Judaea: Casey Hayes is 12 min late"
    assert "Night Off" in body and "Tue 12:15 AM" in body and "sign back in" in body
    subj, body = emailalert.back_in_message("Camp Young Judaea", "Casey Hayes", "LATE 12 min")
    assert subj == "Camp Young Judaea: Casey Hayes is back" and "LATE 12 min" in body


def test_alert_email_settings():
    d = cc.load({})
    assert d.alert_email_to == () and d.alert_email_from == "" and d.late_alerts_only is False
    c = cc.load({"alert_email_to": ["yjoffice@campyj.org", " rrosen@campyj.org "],
                 "alert_email_from": "Headcount <alerts@headcountcamp.com>", "late_alerts_only": True})
    assert c.alert_email_to == ("yjoffice@campyj.org", "rrosen@campyj.org") and c.late_alerts_only is True
    assert cc.load({"alert_email_to": "a@b.org, c@d.org"}).alert_email_to == ("a@b.org", "c@d.org")
    for bad in ("not-an-email", "two@@signs.org", "@nouser.org", "spaces in@x.org", "nodot@localhost"):
        try:
            cc.load({"alert_email_to": [bad]})
        except cc.ConfigError:
            pass
        else:
            raise AssertionError(f"expected ConfigError for {bad!r}")


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print("PASS", t.__name__)
    print("ALL PASS")
