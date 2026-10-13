"""Operator alerts: tells the person running the app when a camp's kiosk has a problem.

This is for the operator (you), not the camp's staff. It runs on its own thread,
checks the app's health every half minute, and sends a push to a private ops
topic. Every message is tagged with the camp's name, so one phone can follow
every camp.

What it does:
  * A check must stay failing for a few minutes before it alerts (a one-second
    Google hiccup is not worth waking anyone), then sends ONE alert, a reminder
    every half hour while it lasts, and a recovery message when it clears.
  * One-off events (an app error) are rate limited so a crash loop sends a few
    messages, not hundreds.
  * A hard daily cap keeps a runaway from burning ntfy's free daily allowance.
  * A short morning digest says the camp is alive and how it did yesterday.

Nothing here touches Streamlit, so it is safe on a background thread and testable
on its own. If no ops topic is configured it does nothing at all.
"""
import logging
import threading
import time

import requests

log = logging.getLogger("opswatch")

PRIORITY_NORMAL = "default"
PRIORITY_HIGH = "high"
PRIORITY_URGENT = "urgent"
PRIORITY_LOW = "low"


def ntfy_sender(topic: str, server: str = "https://ntfy.sh", timeout: int = 6):
    """A send(title, message, priority, tags) function that posts to ntfy."""
    url = f"{server.rstrip('/')}/{topic}"

    def send(title: str, message: str, priority: str = PRIORITY_NORMAL, tags: str = ""):
        headers = {"Title": title.encode("ascii", "replace").decode("ascii"), "Priority": priority}
        if tags:
            headers["Tags"] = tags
        requests.post(url, data=message.encode("utf-8"), headers=headers, timeout=timeout)

    return send


# The running watchdog for this process. It lives here, not in the app script, because
# Streamlit re-runs the script on every click and would otherwise lose the reference.
CURRENT = None


def event(key, title, detail="", **kw):
    """Report a one-off event through the running watchdog, if there is one. Never raises."""
    try:
        if CURRENT is not None:
            return CURRENT.event(key, title, detail, **kw)
    except Exception:
        pass
    return False


class Watchdog:
    def __init__(self, camp, send, clock=time.time, min_fail=180, repeat=1800,
                 event_cooldown=600, daily_cap=40):
        self.camp = camp
        self._send = send
        self.clock = clock
        self.min_fail = min_fail            # seconds a check must keep failing before the first alert
        self.repeat = repeat                # seconds between reminders while still failing
        self.event_cooldown = event_cooldown
        self.daily_cap = daily_cap
        self.probes = {}                    # key -> (label, fn() -> (ok, detail))
        self._state = {}                    # key -> {"bad_since", "alerted", "last_alert", "detail"}
        self._events = {}                   # key -> {"last", "suppressed"}
        self._day = None
        self._count = 0
        self._capped = False
        self.digest_fn = None               # () -> str | None
        self.digest_hour = 7
        self.digest_window = 3              # hours: a restart later in the day does not send one
        self._digest_day = None
        self._lock = threading.Lock()

    # -- sending -----------------------------------------------------------
    def _today(self):
        return time.strftime("%Y-%m-%d", time.localtime(self.clock()))

    def emit(self, title, message, priority=PRIORITY_NORMAL, tags=""):
        """Send with the daily cap applied. Never raises."""
        with self._lock:
            today = self._today()
            if today != self._day:
                self._day, self._count, self._capped = today, 0, False
            if self._count >= self.daily_cap:
                if not self._capped:
                    self._capped = True
                    self._count += 1
                    title, message, priority, tags = (
                        f"[{self.camp}] Alert limit reached",
                        f"{self.daily_cap} alerts sent today; staying quiet until tomorrow. "
                        "Something is badly wrong. Check the camp.",
                        PRIORITY_HIGH, "warning",
                    )
                else:
                    return False
            else:
                self._count += 1
        try:
            self._send(f"[{self.camp}] {title}" if not title.startswith("[") else title, message, priority, tags)
            return True
        except Exception as e:  # noqa: BLE001
            log.warning("ops alert not sent: %r", e)
            return False

    # -- continuous checks -------------------------------------------------
    def observe(self, key, ok, label, detail=""):
        """Feed one check result. Alerts after sustained failure, once, with
        reminders and a recovery message."""
        now = self.clock()
        st = self._state.setdefault(key, {"bad_since": None, "alerted": False, "last_alert": 0, "detail": ""})
        if ok:
            if st["alerted"] and st["bad_since"] is not None:
                mins = max(1, int((now - st["bad_since"]) // 60))
                self.emit(f"Recovered: {label}", f"Back to normal after about {mins} min.", PRIORITY_NORMAL,
                          "white_check_mark")
            st.update(bad_since=None, alerted=False, detail="")
            return
        st["detail"] = detail
        if st["bad_since"] is None:
            st["bad_since"] = now
        down = now - st["bad_since"]
        if not st["alerted"] and down >= self.min_fail:
            st["alerted"], st["last_alert"] = True, now
            self.emit(label, f"{detail}\nFailing for about {int(down // 60)} min.", PRIORITY_HIGH, "rotating_light")
        elif st["alerted"] and now - st["last_alert"] >= self.repeat:
            st["last_alert"] = now
            self.emit(f"Still failing: {label}", f"{detail}\nFailing for about {int(down // 60)} min.",
                      PRIORITY_HIGH, "rotating_light")

    def add_probe(self, key, label, fn):
        self.probes[key] = (label, fn)

    # -- one-off events ----------------------------------------------------
    def event(self, key, title, detail="", priority=PRIORITY_HIGH, tags="warning"):
        """A single thing that happened (an app error). Same key within the
        cooldown is counted, not sent."""
        now = self.clock()
        ev = self._events.setdefault(key, {"last": None, "suppressed": 0})
        if ev["last"] is not None and now - ev["last"] < self.event_cooldown:
            ev["suppressed"] += 1
            return False
        extra = f"\n({ev['suppressed']} more since the last alert)" if ev["suppressed"] else ""
        ev["last"], ev["suppressed"] = now, 0
        return self.emit(title, f"{detail}{extra}", priority, tags)

    # -- the loop ------------------------------------------------------------
    def tick(self):
        for key, (label, fn) in list(self.probes.items()):
            try:
                ok, detail = fn()
            except Exception as e:  # noqa: BLE001 - a broken probe is itself a problem
                ok, detail = False, f"The health check itself failed: {e!r}"
            self.observe(key, bool(ok), label, detail)
        self._maybe_digest()

    def _maybe_digest(self):
        if self.digest_fn is None:
            return
        now = self.clock()
        lt = time.localtime(now)
        today = self._today()
        if not (self.digest_hour <= lt.tm_hour < self.digest_hour + self.digest_window) or self._digest_day == today:
            return
        self._digest_day = today
        try:
            text = self.digest_fn()
        except Exception as e:  # noqa: BLE001
            text = f"Could not build the daily summary: {e!r}"
        if text:
            self.emit("Daily check-in", text, PRIORITY_LOW, "sunrise")

    def run_forever(self, interval=30):
        while True:
            try:
                self.tick()
            except Exception:  # noqa: BLE001
                log.exception("opswatch tick failed")
            time.sleep(interval)

    def start(self, interval=30):
        threading.Thread(target=self.run_forever, args=(interval,), daemon=True, name="opswatch").start()
