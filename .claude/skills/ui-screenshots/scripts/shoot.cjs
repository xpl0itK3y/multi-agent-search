// Screenshots of the SPA served by the vite dev server (which proxies /v1 to demo_server.py).
//
//   node shoot.cjs --routes=/,/thread/{ru_completed} [--as=user|admin|anon] [--viewports=desktop,mobile]
//                  [--themes=light,dark] [--locale=ru] [--scroll=2] [--out=DIR] [--seed=FILE] [--base=URL]
//
// Route placeholders come from seed_info.json: {ru_completed} {en_completed} {running}
// {admin_research} {share_token}; a route may carry a query (/settings?tab=security).
// --scroll=N also captures N more viewport-sized steps of the view's main scroller: the app
// shell is h-screen with overflow hidden, so a full-page screenshot is only the viewport.
//
// Every PNG is listed in <out>/manifest.json with the console errors, page errors and failed
// requests seen while it was taken; read those too, a clean-looking shot can hide a crash.
//
// Needs puppeteer-core (not a web/ dependency: install it next to the output, e.g.
// `npm install --prefix <dir> puppeteer-core` and pass PUPPETEER_DIR=<dir>) and an installed
// Chrome or Edge (auto-detected, or CHROME=<path to the executable>).
const fs = require("fs");
const os = require("os");
const path = require("path");

const arg = (name, fallback) => {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
};
const list = (value) => value.split(",").map((s) => s.trim()).filter(Boolean);

const OUT = path.resolve(arg("out", path.join(os.tmpdir(), "veris-demo", "shots")));
const SEED = path.resolve(arg("seed", path.join(os.tmpdir(), "veris-demo", "seed_info.json")));
const BASE = arg("base", "http://localhost:5173");
const ROUTES = list(arg("routes", "/"));
const IDENTITY = arg("as", "user");
const VIEWPORT_NAMES = list(arg("viewports", "desktop,mobile"));
const THEMES = list(arg("themes", "light,dark"));
const LOCALE = arg("locale", "ru");
const SCROLL_STEPS = Number(arg("scroll", "0"));

const VIEWPORTS = {
  desktop: { width: 1440, height: 900, deviceScaleFactor: 1 },
  laptop: { width: 1024, height: 768, deviceScaleFactor: 1 },
  mobile: { width: 390, height: 844, deviceScaleFactor: 2, isMobile: true, hasTouch: true },
};
const MOBILE_UA =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1";

function loadPuppeteer() {
  const paths = [process.env.PUPPETEER_DIR, process.cwd(), __dirname].filter(Boolean);
  try {
    return require(require.resolve("puppeteer-core", { paths }));
  } catch {
    console.error("puppeteer-core not found: npm install --prefix <dir> puppeteer-core, then PUPPETEER_DIR=<dir>");
    process.exit(2);
  }
}

function findChrome() {
  if (process.env.CHROME) return process.env.CHROME;
  const candidates = [
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
    "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
    "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
  ];
  const found = candidates.find((p) => fs.existsSync(p));
  if (!found) {
    console.error("No Chrome/Edge found: set CHROME=<path to the browser executable>");
    process.exit(2);
  }
  return found;
}

const seed = fs.existsSync(SEED) ? JSON.parse(fs.readFileSync(SEED, "utf8")) : {};
const IDENTITIES = {
  anon: null,
  user: seed.user && { email: seed.user.email, password: seed.user.password },
  admin: seed.admin && { email: seed.admin.email, password: seed.admin.password },
};
if (!(IDENTITY in IDENTITIES)) throw new Error(`--as must be one of ${Object.keys(IDENTITIES).join(", ")}`);
if (IDENTITY !== "anon" && !IDENTITIES[IDENTITY]) throw new Error(`no seed_info.json at ${SEED}: start demo_server.py first`);

const fill = (route) =>
  route.replace(/\{(\w+)\}/g, (_, key) => {
    if (seed[key] === undefined) throw new Error(`seed_info.json has no "${key}"`);
    return seed[key];
  });
const slug = (route) => route.replace(/^\/|[{}]/g, "").replace(/[^\w-]+/g, "_").replace(/_+$/, "").slice(0, 60) || "home";
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
// A signed-out page asks /v1/auth/me once and gets the 401 that tells it nobody is signed in.
const expected = (issue) =>
  IDENTITY === "anon" && (issue === "HTTP 401 GET /v1/auth/me" || /status of 401 \(Unauthorized\)/.test(issue));

async function settle(page) {
  try {
    await page.waitForNetworkIdle({ idleTime: 500, timeout: 8000 });
  } catch {}
  try {
    await page.evaluate(() => document.fonts && document.fonts.ready);
  } catch {}
  await sleep(600);
}

// Pick the view's main scroller (the tallest scrollable box) and tag it for scrolling.
async function markScroller(page) {
  return page.evaluate(() => {
    const boxes = [...document.querySelectorAll("*")].filter((el) => {
      const style = getComputedStyle(el);
      return /(auto|scroll)/.test(style.overflowY) && el.scrollHeight - el.clientHeight > 40 && el.clientHeight > 150;
    });
    if (!boxes.length) return null;
    boxes.sort((a, b) => b.clientHeight - a.clientHeight || b.scrollHeight - a.scrollHeight);
    boxes[0].setAttribute("data-shot-scroller", "1");
    return { scrollHeight: boxes[0].scrollHeight, clientHeight: boxes[0].clientHeight, scrollTop: boxes[0].scrollTop };
  });
}

async function newPage(browser, viewport, theme, log) {
  const context = await browser.createBrowserContext();
  const page = await context.newPage();
  await page.setViewport(VIEWPORTS[viewport]);
  if (viewport === "mobile") await page.setUserAgent(MOBILE_UA);
  await page.evaluateOnNewDocument(
    (theme, locale) => {
      try {
        localStorage.setItem("theme", theme);
        localStorage.setItem("locale", locale);
      } catch {}
    },
    theme,
    LOCALE,
  );
  page.on("console", (m) => {
    if (["error", "warn", "warning"].includes(m.type())) log.push(`console.${m.type()}: ${m.text().slice(0, 300)}`);
  });
  page.on("pageerror", (e) => log.push(`pageerror: ${String(e.message || e).slice(0, 300)}`));
  page.on("response", (r) => {
    if (r.status() >= 400 && r.url().startsWith(BASE)) log.push(`HTTP ${r.status()} ${r.request().method()} ${r.url().slice(BASE.length)}`);
  });
  page.on("requestfailed", (r) => {
    const reason = (r.failure() && r.failure().errorText) || "";
    if (r.url().startsWith(BASE) && !/ERR_ABORTED/.test(reason)) log.push(`requestfailed ${r.url().slice(BASE.length)} ${reason}`);
  });
  const identity = IDENTITIES[IDENTITY];
  if (identity) {
    await page.goto(BASE + "/health", { waitUntil: "domcontentloaded" });
    const status = await page.evaluate(async ({ email, password }) => {
      const r = await fetch("/v1/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password }),
        credentials: "include",
      });
      const body = await r.json().catch(() => ({}));
      if (body.access_token) localStorage.setItem("access_token", body.access_token);
      return r.status;
    }, identity);
    if (status !== 200) throw new Error(`login as ${IDENTITY} failed with HTTP ${status}`);
    log.length = 0;
  }
  return { context, page };
}

(async () => {
  const puppeteer = loadPuppeteer();
  fs.mkdirSync(OUT, { recursive: true });
  const browser = await puppeteer.launch({
    executablePath: findChrome(),
    headless: true,
    args: ["--no-first-run", "--no-default-browser-check", "--disable-extensions", `--lang=${LOCALE}`, "--force-color-profile=srgb"],
  });
  const manifest = [];
  try {
    for (const viewport of VIEWPORT_NAMES) {
      if (!VIEWPORTS[viewport]) throw new Error(`unknown viewport ${viewport}: ${Object.keys(VIEWPORTS).join(", ")}`);
      for (const theme of THEMES) {
        const log = [];
        const { context, page } = await newPage(browser, viewport, theme, log);
        for (const raw of ROUTES) {
          const route = fill(raw);
          const name = `${IDENTITY}-${slug(raw)}__${viewport}-${theme}`;
          const shoot = async (suffix, note) => {
            const file = `${name}${suffix}.png`;
            await page.screenshot({ path: path.join(OUT, file) });
            manifest.push({ file, route, viewport, theme, identity: IDENTITY, note, issues: log.splice(0).filter((i) => !expected(i)) });
            console.log("shot", file);
          };
          await page.goto(BASE + route, { waitUntil: "domcontentloaded", timeout: 30000 });
          await settle(page);
          await shoot("", "as opened");
          if (SCROLL_STEPS > 0) {
            const box = await markScroller(page);
            if (box) {
              if (box.scrollTop > 10) {
                // The view opened scrolled (a thread jumps to its newest turn): show its top too.
                await page.evaluate(() => document.querySelector("[data-shot-scroller]").scrollTo({ top: 0, behavior: "instant" }));
                await sleep(400);
                await shoot("__top", `main scroller at the top (it opened at ${box.scrollTop}px)`);
              }
              const step = Math.floor(box.clientHeight * 0.85);
              for (let i = 1, top = step; i <= SCROLL_STEPS && top < box.scrollHeight - box.clientHeight + step * 0.3; i++, top += step) {
                await page.evaluate((top) => document.querySelector("[data-shot-scroller]").scrollTo({ top, behavior: "instant" }), top);
                await sleep(400);
                await shoot(`__s${i}`, `main scroller at ${top}px of ${box.scrollHeight}px`);
              }
            }
          }
        }
        await context.close();
      }
    }
  } finally {
    const file = path.join(OUT, "manifest.json");
    let previous = [];
    try {
      previous = JSON.parse(fs.readFileSync(file, "utf8"));
    } catch {}
    const fresh = new Set(manifest.map((m) => m.file));
    fs.writeFileSync(file, JSON.stringify([...previous.filter((m) => !fresh.has(m.file)), ...manifest], null, 2));
    await browser.close();
  }
  const noisy = manifest.filter((m) => m.issues.length);
  console.log(`${manifest.length} shots in ${OUT}; ${noisy.length} with console/network issues (see manifest.json)`);
})().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
