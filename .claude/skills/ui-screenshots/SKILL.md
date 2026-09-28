---
name: ui-screenshots
description: See the multi-agent-search SPA the way a user does — start a seeded demo backend (the real FastAPI app on the in-memory store with realistic researches, a running research, users, admin data, a share link; no LLM, no database), the vite dev server, and take headless-Chrome screenshots of any routes in light/dark themes at desktop, laptop and phone sizes, as a signed-out visitor, a user or an admin, with console errors and failed requests logged per shot. Use it to check a frontend change visually, to reproduce a UI bug, to capture before/after images for a PR or a design review, or whenever the user asks to "посмотреть", "сделать скриншоты", "check how it looks" or review the UI.
---

# Screenshots of the SPA with seeded data

Three pieces, all in this skill's `scripts/`:

- **`demo_server.py`** builds the real app (`src.api.app.create_app`) on the in-memory store and seeds it through the real store and service methods. It serves on `127.0.0.1:8000`, the vite proxy's target, and writes the ids and credentials to `<tempdir>/veris-demo/seed_info.json` (change with `--out`). It seeds:
  - `ru_completed`: a completed Russian HARD research with a replan loop, conflicts, red team, stance, cross-language, chat follow-ups and a **public share link**;
  - `en_completed`: a completed English research with a comparison table;
  - `running`: an analyzing research with a partial report and live worker heartbeats;
  - a history of completed, failed and cancelled researches;
  - `admin_research`: one research owned by the admin;
  - a regular user, an admin and five more users with sessions, two weeks of LLM usage, events and audit rows;
  - a password-reset link.
  The content (`demo_content.py`) is invented; its quotes from real sites are not real quotes. Nothing in the repo is written.
- **The vite dev server** (`web/`, port 5173) proxies `/v1` and `/health` to 8000.
- **`shoot.cjs`** logs in through `/v1/auth/login`, sets theme and locale in localStorage, visits routes and saves PNGs plus `manifest.json`. Each manifest entry lists the console errors and warnings, page errors, and failed or 4xx/5xx requests to the app's own origin seen during that shot. Third-party requests (Google Fonts) are not listed, and a signed-out page's expected 401 on `/v1/auth/me` is filtered out.

## Run it

From the repo root, with relative paths. Windows interpreters driven from WSL cannot open `/mnt/...` script paths, but relative ones work.

```bash
S=.claude/skills/ui-screenshots/scripts
T="${TMPDIR:-/tmp}"
# 1. backend (keep running). Wait for its "SEED_OK" line before step 4: seed_info.json is
#    written only then, and a stale one from an earlier run is deleted at start.
$PY $S/demo_server.py > "$T/demo_server.log" 2>&1 &
until grep -q SEED_OK "$T/demo_server.log"; do sleep 1; done
# 2. frontend dev server (proxies /v1 and /health to :8000)
(cd web && node node_modules/vite/bin/vite.js --port 5173 --strictPort) &
# 3. one-time: puppeteer-core outside the repo (not a web/ dependency)
npm install --prefix "$T/veris-shot-tools" puppeteer-core
# 4. shots
PUPPETEER_DIR="$T/veris-shot-tools" node $S/shoot.cjs \
  --routes='/,/thread/{ru_completed},/settings?tab=security,/r/{share_token}' \
  --as=user --viewports=desktop,mobile --themes=light,dark --scroll=1
```

- **Python.** `$PY` is the repo venv's Python. Its dependencies are enough: the app runs with `TASK_STORE_BACKEND=memory`, no Redis and no LLM keys.
- **Port 8000 taken?** Run `demo_server.py --port N` and start vite with `VITE_API_PROXY=http://localhost:N`.
- **Browser.** `CHROME=<path>` overrides detection. Detection finds Chrome on Windows, macOS and Linux, and Edge on Windows. A Linux node under WSL cannot see a Windows browser, so it needs `CHROME=`.
- **On WSL with Windows tools:**
  - Use `.venv/Scripts/python.exe` and `"/mnt/c/Program Files/nodejs/node.exe"`, with relative script paths as above.
  - Environment variables must hold Windows paths and be listed in `WSLENV`, e.g. `PUPPETEER_DIR='C:\Users\<you>\AppData\Local\Temp\veris-shot-tools' WSLENV=PUPPETEER_DIR` (add `VITE_API_PROXY` there too if you set it).
  - Install from inside that directory: `node.exe "C:\Program Files\nodejs\node_modules\npm\bin\npm-cli.js" install --prefix . puppeteer-core`.
  - `seed_info.json` and the shots land in the Windows temp dir (`%TEMP%\veris-demo`); read the PNGs through `/mnt/c/...`.
### shoot.cjs options

| Option | Default | Meaning |
|---|---|---|
| `--routes=a,b` | `/` | Paths, optionally with a query. Placeholders from seed_info.json: `{ru_completed}` `{en_completed}` `{running}` `{admin_research}` `{share_token}` |
| `--as=` | `user` | `anon` (signed out), `user`, `admin` |
| `--viewports=` | `desktop,mobile` | `desktop` 1440×900, `laptop` 1024×768, `mobile` 390×844 @2x with touch and an iPhone UA |
| `--themes=` | `light,dark` | Any `ThemeId`: `light dark midnight emerald rose sand system` |
| `--locale=` | `ru` | `ru`, `en`, `es` |
| `--scroll=N` | `0` | Also capture N viewport-sized steps of the view's main scroller, plus its top if it opened scrolled |
| `--out=` / `--seed=` / `--base=` | temp dir / temp dir / `http://localhost:5173` | |

**Useful routes:**
- signed out: `/login`, `/forgot-password`, `/reset-password`, `/r/{share_token}`;
- user: `/`, `/thread/{ru_completed}`, `/thread/{en_completed}`, `/thread/{running}`, `/research/{ru_completed}` (split view), `/settings?tab=profile|research|appearance|analytics|security`;
- admin: `/admin` and its tabs.

For the password-reset form, open `seed_info.json`'s `reset_link` path and hash as a route.

## Looking at the result

- **Read the PNGs** with the Read tool, which shows images, and read `manifest.json` too: a clean-looking shot can hide a page error or a 4xx.
- **Compare like with like:** the same route, viewport and theme before and after a change. Take "before" shots on the base commit (for example via `git stash` or a worktree) before editing.
- **Check the parts a static capture misses:**
  - both themes;
  - the phone layout (the drawer, 44 px targets, horizontal overflow);
  - text in all three locales, where long Spanish or Russian strings overflow first;
  - reduced motion. Emulate it in a custom script with `page.emulateMediaFeatures([{ name: "prefers-reduced-motion", value: "reduce" }])`.
- **For interactions** (open a menu, hover, press, drag), copy `shoot.cjs` into your scratch directory and add steps after `page.goto`. Do not grow the generic script with page-specific clicks: labels change, and those scripts rot.
- **What headless Chrome cannot show:** real touch press feedback, trackpad and pinch gestures, and iOS rubber-banding. Say so when you report, instead of claiming they work.

## Clean up

Stop the demo server and vite when you are done (kill the background jobs or their PIDs; on Windows, check for leftover `python.exe` / `node.exe` / `chrome.exe`). Delete the shot directories if they are no longer needed. Never commit screenshots or `seed_info.json`: the seeded passwords are demo values, but the output belongs in a temp dir.
