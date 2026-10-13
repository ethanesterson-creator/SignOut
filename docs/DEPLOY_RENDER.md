# Deploying a camp on Render

One Render **web service per camp**, each with its own secrets. Nothing is shared
between camps. `start.sh` is the start command; it puts the camp's secrets where
Streamlit reads them and starts the app on Render's port.

## Trial deploy (test spreadsheet, never the live one)

In the Render dashboard: **New > Web Service**, connect the GitHub repo
`ethanesterson-creator/SignOut`, then:

| Field | Value |
|---|---|
| Name | `signout-trial` |
| Region | Virginia (US East), nearest to Massachusetts |
| Branch | `deploy/render` (use `main` once this is merged) |
| Runtime | Python 3 |
| Build Command | `pip install -r requirements.txt` |
| Start Command | `sh start.sh` |
| Instance Type | **Starter** ($7/mo, always on). Not Free: Free spins down when idle. |

Then, before creating it:
- **Environment variable** `PYTHON_VERSION` = `3.12.11`. Render's default is newer
  than the version this app is tested on.
- **Secret File**: filename `secrets.toml`, contents = the camp's `secrets.toml`
  (for the trial, your local `.streamlit/secrets.toml`, which points at the TEST sheet).
- **Advanced > Health Check Path**: `/_stcore/health`.

Open the `https://<name>.onrender.com` address when the deploy finishes.

## What to measure during the trial
- Render dashboard > Metrics: memory while the board is open, and during a burst of sign-outs.
  Starter has 512 MB. If peak is above ~400 MB, move up a size or move to Fly.io.
- First load after a deploy/restart (the kiosk must come back fast).
- Leave one browser on Who's Out for a day; confirm it stays up.

## A real camp
Same steps with the camp's own `secrets.toml` from `provision/new_camp.py`.
Updates: Bauercrest's service follows `main`; every other camp follows a `stable`
branch that is moved forward only after Bauercrest has run the change cleanly.
