# The Headcount website (headcountcamp.com)

A plain static site in `site/` (HTML, CSS, a little JavaScript, self-hosted fonts, screenshots of the demo camp).
No build step. Light and dark mode follow the visitor's device. Everything on it is fake demo data.

## Put it on Render (free static site)
1. Render > **New** > **Static Site**, connect the `SignOut` repo, branch `main`.
2. Name: `headcount-site`. Build command: leave empty (or `echo ok`). **Publish directory: `site`**.
3. Create it. It gets a `onrender.com` address; check it loads.
4. Settings > **Custom Domains** > add `headcountcamp.com` and `www.headcountcamp.com`. Render shows the DNS records
   to add at your registrar (usually an A/ALIAS record for the bare domain and a CNAME for `www`). Add them,
   then Verify. Static sites on Render are free; confirm on their pricing page.

## Change the words
Edit `site/index.html` (all the copy is there), commit, push to `main`; Render redeploys the site.

## Refresh the screenshots
Run the demo camp locally with a busy board, then capture:
```bash
.venv/Scripts/python.exe provision/stage_demo.py camps/camp-demo/secrets.toml          # busy board
.venv/Scripts/python.exe provision/stage_demo.py camps/camp-demo/secrets.toml --emergency   # for the emergency shot
.venv/Scripts/python.exe provision/reset_demo.py camps/camp-demo/secrets.toml          # clean up after
```
The screenshots in `site/img/` were captured at 1366x768 from the demo only. Never use a real camp's data.

## Things to confirm before it goes public
- "Built at a Massachusetts summer camp. Used every day by about 60 counselors." (Bauercrest is not named. Add the name only with the camp's OK.)
- "Camps usually pay thousands to have a system like this built." (Your claim from earlier research; keep a source.)
- "Up and running in days." (A promise about your setup time.)
- Contact email: ethan.esterson@gmail.com.
