# Running the fleet

How you keep every camp healthy without being at any of them.

## Who follows which branch
| Deployment | Follows | Why |
|---|---|---|
| Bauercrest (live kiosk) | `main` | First to get every change, so problems show up on your own camp |
| Every other camp | `stable` | Only moves forward after Bauercrest has run the change cleanly |

`stable` starts equal to `main`. Nothing about Bauercrest's current hosting changes.

## Shipping a change (every time)
1. Work on a branch, run the tests, merge to `main`.
2. Watch Bauercrest for at least one full day: your alerts channel quiet, the board behaving.
   During camp season, do this on a quiet day, not before a trip day.
3. Move `stable` forward, which updates every other camp:
   ```bash
   git checkout stable && git pull && git merge --ff-only main && git push origin stable
   ```
   Render redeploys each camp on its own. Zero-downtime deploys on paid plans means a kiosk
   is not taken offline by an update.
4. Watch the alerts channel for the next hour.

## Rolling back
- **One camp misbehaving:** Render dashboard > that service > Events/Deploys > pick the last good
  deploy > **Rollback**. Takes about a minute. Does not touch the camp's data.
- **A bad change that reached `stable`:** roll `stable` back and redeploy:
  ```bash
  git checkout stable && git reset --hard <last-good-commit> && git push --force-with-lease origin stable
  ```
  (`stable` is the one branch where this is allowed; never do it to `main`.)
- A rollback never loses sign-in/out data: the data lives in the camp's Google Sheet, not on the server.

## Your alerts
- **Channel:** a private ntfy topic only you subscribe to (`provision/ops.local.toml`, see
  `provision/ops.example.toml`). Every message is titled `[Camp Name] ...`.
- **From the app itself:** can't reach Google Sheets; sign-ins/outs stuck waiting to save; an
  app error; the app started or restarted; a short morning check-in.
- **From outside the app (set up once per camp):** a free uptime service pinging
  `https://<camp address>/_stcore/health` every few minutes, alerting you by email and push.
  This is the only thing that catches "the whole app is down", because a dead app cannot
  report itself. Check the service's current free limits when you sign up.
- **How to read them:** a camp name in the title tells you which one. A "Recovered" message
  means it fixed itself. "Still failing" repeats every 30 minutes until it clears.

## Backups
```bash
.venv/Scripts/python.exe provision/backup_camp.py camps/<camp>/secrets.toml
```
Writes `backups/<camp>/<date>/<tab>.csv` (read-only on the Sheet), keeps 30 days. Run it nightly
for every camp (Windows Task Scheduler on the computer that holds `camps/`). `backups/` and
`camps/` are git-ignored: the staff tab contains codes (hashed once the camp has run the PIN
hashing step), so treat that folder as private.

## Restoring
Open the CSV in the dated backup, copy the rows back into the camp's Sheet tab. For a whole-tab
mistake, Google's own version history (File > Version history) in the camp's Sheet is faster for
the last few weeks; the backups are for everything older or if the Sheet itself is gone.
