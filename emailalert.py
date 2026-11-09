"""Alert emails, sent through Resend (https://resend.com).

Kept free of Streamlit so it can be tested without a browser or a real account.
The API key is passed in by the caller (it lives in the deployment's secrets),
and nothing here ever raises: a slow or broken email service must never delay or
break a sign-out. A failed send simply does not happen.
"""

import threading

import requests

RESEND_URL = "https://api.resend.com/emails"


def late_message(camp: str, name: str, mins: int, reason: str, due_text: str = ""):
    """(subject, body) for 'someone has just gone past their deadline'."""
    subject = f"{camp}: {name} is {mins} min late"
    lines = [f"{name} is {mins} minutes late.", f"Reason: {reason}"]
    if due_text:
        lines.append(f"Was due back: {due_text}")
    lines += ["", "You will get one more email when they sign back in.", f"Sent by Headcount for {camp}."]
    return subject, "\n".join(lines)


def back_in_message(camp: str, name: str, late_note: str):
    """(subject, body) for 'a late person has signed back in'. late_note is 'LATE 12 min'."""
    detail = str(late_note or "").strip()
    subject = f"{camp}: {name} is back"
    lines = [f"{name} signed back in."]
    if detail:
        lines.append(f"They were marked: {detail}.")
    lines += ["", f"Sent by Headcount for {camp}."]
    return subject, "\n".join(lines)


def send(api_key: str, sender: str, to, subject: str, text: str, post=None, background: bool = True):
    """Send one email. Returns None when it runs in the background, or True/False
    when background=False (used by tests). Never raises.
    """
    to = [t for t in (to or []) if t]
    if not (api_key and sender and to):
        return False if not background else None
    post = post or requests.post
    payload = {"from": sender, "to": to, "subject": subject, "text": text}
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    def _go():
        try:
            r = post(RESEND_URL, json=payload, headers=headers, timeout=8)
            return 200 <= getattr(r, "status_code", 0) < 300
        except Exception:
            return False

    if not background:
        return _go()
    try:
        threading.Thread(target=_go, daemon=True).start()
    except Exception:
        pass
    return None
