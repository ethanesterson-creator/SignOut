"""Process-wide data layer that keeps the kiosk from ever waiting on Google.

Every click used to pay for Google Sheets round trips: expired caches were
refetched in-line (six sequential reads, 100-250ms each), toggles forced a
fresh read, and writes ran before the page could respond. Three pieces here
take all of that off the path a person is waiting on:

* Store - stale-while-revalidate cache. Reads always return what is in memory
  immediately; an expired value is refreshed on a background thread, and a
  keepalive thread keeps recently used stores warm so a kiosk that was just
  sitting there never wakes up cold. Only the very first load, or a value
  that is genuinely too old to trust, ever blocks.
* submit/cancel - a single ordered write queue with retry. The click records
  its effect in memory first (see the RECENT / VANS_PENDING overlays), the
  Sheets writes happen right behind it, and a failed write keeps retrying
  instead of being lost.
* RECENT / VANS_PENDING - the app's own not-yet-confirmed writes, layered over
  whatever the sheet says, so the board and the sign-in toggle already show
  the truth the instant a button is pressed.

Nothing in this module touches Streamlit's session or UI APIs: it is used from
background threads.
"""
import logging
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor

# Background threads have no script run context; the cached-resource lookups
# some loaders do from them are fine, but log a warning per call otherwise.
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)
logging.getLogger("streamlit.runtime.caching.cache_resource_api").setLevel(logging.ERROR)

log = logging.getLogger("livecache")

# Sheet handles, captured on the main thread so background threads never have
# to go through Streamlit's cache machinery to find a worksheet.
HANDLES = {}

ACTIVE_WINDOW_SECONDS = 15 * 60
_last_touch = time.time()
_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="livecache")
_STORES = {}
_registry_lock = threading.Lock()
_keepalive_started = False


def _touch():
    global _last_touch
    _last_touch = time.time()


def _copy(v):
    try:
        return v.copy()
    except Exception:
        return v


class Store:
    def __init__(self, name, ttl, max_stale):
        self.name = name
        self.ttl = ttl
        self.max_stale = max_stale
        self.loader = None
        self.value = None
        self.loaded_at = 0.0
        self.dirty = False
        self._refreshing = False
        self._flag_lock = threading.Lock()
        self._load_lock = threading.Lock()
        self.last_error = ""

    # -- loading -------------------------------------------------------
    def _load_into_store(self):
        v = self.loader()
        self.value = v
        self.loaded_at = time.time()
        self.dirty = False
        self.last_error = ""
        return v

    def _load_blocking(self, default):
        with self._load_lock:
            # Someone else may have loaded while we waited for the lock.
            if self.value is not None and (time.time() - self.loaded_at) <= self.ttl:
                return self.value
            try:
                return self._load_into_store()
            except Exception as e:
                self.last_error = repr(e)
                log.warning("load %s failed: %r", self.name, e)
                if self.value is not None:
                    return self.value
                return default

    def _refresh_job(self):
        try:
            with self._load_lock:
                try:
                    self._load_into_store()
                except Exception as e:
                    self.last_error = repr(e)
                    log.warning("refresh %s failed: %r", self.name, e)
                    # Back off a few seconds instead of hammering a sick API.
                    self.loaded_at = max(self.loaded_at, time.time() - self.ttl + 5)
        finally:
            with self._flag_lock:
                self._refreshing = False

    def refresh_now(self):
        """Re-read the sheet right now and wait for it. For admin operations
        that must act on the true sheet state, never the click path."""
        with self._load_lock:
            try:
                return self._load_into_store()
            except Exception as e:
                self.last_error = repr(e)
                return self.value

    def kick(self):
        with self._flag_lock:
            if self._refreshing:
                return
            self._refreshing = True
        try:
            _pool.submit(self._refresh_job)
        except Exception:
            with self._flag_lock:
                self._refreshing = False

    # -- public --------------------------------------------------------
    def get(self, default=None):
        _touch()
        if self.value is None:
            return self._load_blocking(default)
        age = time.time() - self.loaded_at
        if age > self.max_stale:
            return self._load_blocking(default)
        if self.dirty or age > self.ttl:
            self.kick()
        return self.value

    def set(self, value):
        """Write-through: make a change visible immediately. The next refresh
        still re-reads the sheet (callers keep an overlay for anything the
        sheet does not have yet)."""
        self.value = value

    def invalidate(self, kick=False):
        self.dirty = True
        if kick:
            self.kick()


def register(name, ttl, max_stale, loader):
    with _registry_lock:
        s = _STORES.get(name)
        if s is None:
            s = Store(name, ttl, max_stale)
            _STORES[name] = s
        s.ttl, s.max_stale, s.loader = ttl, max_stale, loader
    _ensure_keepalive()
    return s


def cached(name, ttl, max_stale, default):
    """Decorator: turn a loader that RAISES on failure into a non-blocking
    accessor. `default` is a zero-arg factory used only when the very first
    load fails. The accessor hands back a copy, so callers can mutate freely."""

    def deco(fn):
        store = register(name, ttl, max_stale, fn)

        def accessor():
            return _copy(store.get(default=default()))

        accessor.store = store
        accessor.clear = lambda: store.invalidate(kick=True)
        accessor.__name__ = getattr(fn, "__name__", name)
        return accessor

    return deco


def prewarm(timeout=25):
    """Load every cold store at once instead of one after another."""
    cold = [s for s in list(_STORES.values()) if s.value is None]
    if not cold:
        return
    futs = [_pool.submit(s._load_blocking, None) for s in cold]
    deadline = time.time() + timeout
    for f in futs:
        try:
            f.result(timeout=max(0.1, deadline - time.time()))
        except Exception:
            pass


def _keepalive():
    while True:
        time.sleep(1)
        try:
            active = (time.time() - _last_touch) < ACTIVE_WINDOW_SECONDS
            now = time.time()
            for s in list(_STORES.values()):
                if s.value is None or s.loader is None:
                    continue
                limit = s.ttl if active else max(s.ttl * 6, 90)
                if now - s.loaded_at > limit or s.dirty and active:
                    s.kick()
        except Exception:
            pass


def _ensure_keepalive():
    global _keepalive_started
    with _registry_lock:
        if _keepalive_started:
            return
        _keepalive_started = True
    threading.Thread(target=_keepalive, daemon=True, name="livecache-keepalive").start()


# ---------------------------------------------------------------------------
# Ordered, retrying write queue
# ---------------------------------------------------------------------------
class _Job:
    __slots__ = ("tag", "fn", "tries", "created", "started")

    def __init__(self, fn, tag):
        self.fn = fn
        self.tag = tag
        self.tries = 0
        self.started = False
        self.created = time.time()


_jobs = deque()
_cv = threading.Condition()
_running = None
_worker_started = False
_stats = {"done": 0, "failed_attempts": 0, "last_error": ""}


def submit(fn, tag=None):
    """Queue a write. `fn(attempt)` runs on the writer thread, strictly in the
    order jobs were submitted, and is retried with backoff until it succeeds -
    so it must be safe to run again (the app's jobs check for ids already
    written)."""
    global _worker_started
    job = _Job(fn, tag)
    with _cv:
        _jobs.append(job)
        _cv.notify_all()
        if not _worker_started:
            _worker_started = True
            threading.Thread(target=_worker, daemon=True, name="livecache-writer").start()
    return job


def cancel(tag):
    """Drop a queued job that has not started yet (an undo that beat its own
    write to the sheet). False if it already ran, is running, or was retried."""
    if not tag:
        return False
    with _cv:
        for j in list(_jobs):
            if j.tag == tag and not j.started:
                _jobs.remove(j)
                _cv.notify_all()
                return True
    return False


def _worker():
    global _running
    while True:
        with _cv:
            while not _jobs:
                _cv.wait()
            job = _jobs.popleft()
            _running = job
        try:
            job.started = True
            job.fn(job.tries)
            ok = True
        except Exception as e:  # noqa: BLE001 - retry anything
            ok = False
            job.tries += 1
            _stats["failed_attempts"] += 1
            _stats["last_error"] = repr(e)
            log.warning("write job failed (attempt %s): %r", job.tries, e)
        with _cv:
            _running = None
            if ok:
                _stats["done"] += 1
            else:
                _jobs.appendleft(job)
            _cv.notify_all()
        if not ok:
            time.sleep(min(1.5 * (2 ** min(job.tries - 1, 4)), 20))


def pending_info():
    """(jobs not yet confirmed, age in seconds of the oldest, last error)."""
    with _cv:
        items = list(_jobs) + ([_running] if _running else [])
    if not items:
        return 0, 0.0, _stats["last_error"]
    oldest = max(time.time() - j.created for j in items)
    return len(items), oldest, _stats["last_error"]


def drain(timeout=6.0):
    """Wait for queued writes to land (used before admin operations that read
    or rewrite the sheets directly). True if the queue emptied in time."""
    end = time.time() + timeout
    with _cv:
        while _jobs or _running:
            left = end - time.time()
            if left <= 0:
                return False
            _cv.wait(timeout=min(left, 0.5))
    return True


# ---------------------------------------------------------------------------
# Overlays: this app's own writes the sheet may not show yet
# ---------------------------------------------------------------------------
CONFIRMED_KEEP_SECONDS = 20
PENDING_GIVE_UP_SECONDS = 15 * 60

_RECENT = {}          # person name -> status entry
_VANS = {}            # row id -> van row
_overlay_lock = threading.Lock()


def _live(entry, now):
    c = entry.get("confirmed_at")
    if c is not None:
        return now - c <= CONFIRMED_KEEP_SECONDS
    return now - entry["at"] <= PENDING_GIVE_UP_SECONDS


def recent_put(name, info):
    """Record a person's status as the app just set it. `info` carries the
    current_status fields (status, reason, other_reason, timestamp, due_back,
    id). Stays authoritative until its write is confirmed and a short grace
    period has passed."""
    entry = dict(info)
    entry["name"] = name
    entry["at"] = time.time()
    entry["confirmed_at"] = None
    with _overlay_lock:
        _RECENT[name] = entry


def recent_confirm(name, row_id):
    with _overlay_lock:
        e = _RECENT.get(name)
        if e is not None and e.get("id") == row_id:
            e["confirmed_at"] = time.time()


def recent_entries():
    now = time.time()
    with _overlay_lock:
        for k in [k for k, e in _RECENT.items() if not _live(e, now)]:
            del _RECENT[k]
        return [dict(e) for e in _RECENT.values()]


def drop_confirmed_overlays():
    """After an admin rebuild/delete rewrote the sheets, whatever this app had
    already saved is part of the sheet again - stop layering old copies over
    it. Writes still waiting in the queue stay layered on top."""
    with _overlay_lock:
        for d in (_RECENT, _VANS):
            for k in [k for k, e in d.items() if e.get("confirmed_at") is not None]:
                del d[k]


def vans_put(row):
    entry = dict(row)
    entry["at"] = time.time()
    entry["confirmed_at"] = None
    with _overlay_lock:
        _VANS[str(row.get("id", ""))] = entry


def vans_confirm(row_id):
    with _overlay_lock:
        e = _VANS.get(str(row_id))
        if e is not None:
            e["confirmed_at"] = time.time()


def vans_entries():
    now = time.time()
    with _overlay_lock:
        for k in [k for k, e in _VANS.items() if not _live(e, now)]:
            del _VANS[k]
        return [dict(e) for e in _VANS.values()]


_once = set()


def once(key):
    """True the first time `key` is seen in this process, False afterwards."""
    with _registry_lock:
        if key in _once:
            return False
        _once.add(key)
        return True
