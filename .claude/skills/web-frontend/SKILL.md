---
name: web-frontend
description: Conventions for changing the Vue 3 + TypeScript + Tailwind SPA in web/ (product name Veris) — routes and views, the API client and its error-to-i18n mapping, SSE streams, i18n in ru/en/es, the colour/elevation/motion tokens and their contrast test, press feedback and focus rings, materials, motion and gesture helpers (no motion library), sheets, popovers and confirm dialogs, the reduced-motion/transparency/contrast/forced-colors layer, the nginx CSP hashes for index.html, and the vitest patterns. Use it for any change under web/, any new UI string, colour, animation, view or API call on the client side, and for UI reviews, even when the request is only "поправь фронт", "add a button" or "make this look better".
---

# Frontend (web/)

- **Stack.** Vue 3.5 with `<script setup lang="ts">`, vue-router 4, pinia, vue-i18n 11, Tailwind 3.4 with the typography plugin, markdown-it and KaTeX. There is no UI kit and no motion library, and that is deliberate: CSS transitions plus `src/lib/gesture.ts`.
- **TypeScript** runs `strict` with `noUnusedLocals`/`noUnusedParameters`, and it type-checks test files too. An unused import fails CI.
- **Checks.** `ci_local.sh frontend` for vue-tsc plus vitest, then `guards` for the backend tests that parse `api.ts`, `stream.ts` and `nginx.conf` (see the `run-checks` skill). For how it looks, use the `ui-screenshots` skill.
- **`web/README.md` is stale**, so trust this file and the code.

## Structure and data flow

- **Boot (`src/main.ts`).**
  - `import "./linkCapture"` must stay the **first** import: it removes `#token=` from `/reset-password` and `/verify-email` before the router exists.
  - A passive `touchstart` listener makes iOS apply `:active`.
  - Pinia and i18n are installed, then `auth.fetchMe()`, and only then the router is installed and the app mounted.
- **Router (`src/router/index.ts`).**
  - Routes are lazy-loaded.
  - `meta` flags: `bare` (no app shell), `public` (no session needed), `linkToken`, `requiresAdmin`, `titleKey` (an i18n key for the tab title, with ` — Veris` appended).
  - **`requiresAdmin` is not enforced by the guard**; `AdminView` checks `auth.user?.is_admin` itself.
  - `/r/:token` is public. App.vue draws it without the shell only for signed-out readers.
- **App.vue.** Keys the routed view by **`route.path`, not `fullPath`**, so a query-only change (`?tab=`) keeps the view mounted (App.test.ts asserts this). Tab UIs use `router.replace({ query })` and watch `route.query`. It also hosts the mobile drawer (`useDragDismiss`, `inert`) and the single global `<ConfirmDialog/>`.
- **Stores.**
  - `auth`: user, `fetchMe`, login, register, logout, profile.
  - `research`: models, history, threads.
  - `ui`: the theme (`THEMES`: system, light, dark, midnight, emerald, rose, sand), locale, sidebar width, drawer.
- **localStorage** keys in use:
  - auth and shell: `access_token`, `theme`, `locale`, `sidebar.width`;
  - research defaults: `research.default_depth`, `research.default_model`, `research.plan_first`, `research.auto_expand_console`;
  - views: `activity_console.open`, `verify.inline`, `multi-agent-search:admin-nodes-pos-v3`.
  - Storage can throw (private mode, blocked site data), so wrap new access in try/catch, as `storageGet`/`storageSet` in `stores/ui.ts` do. Several older call sites (api.ts, i18n, Composer, SettingsView, ArtifactPanel) do not yet.
- **API client (`src/lib/api.ts`).**
  - `request<T>(path, init, { sessionRecovery = true })` always sends: `credentials: "include"`, JSON, `Accept-Language` from `<html lang>`, `Authorization: Bearer` from localStorage, and `X-CSRF-Token` (copied from the `csrf_token` cookie) on unsafe methods.
  - A non-2xx response throws `ApiError(status, detail)`.
  - A 401 drops the token and redirects to `/login?redirect=…`, except on the `PUBLIC_PAGES` routes and `/r/*`. Pass `{ sessionRecovery: false }` when a 401 means "wrong credential", not "expired session".
  - Downloads go through `fetchFile` + `lib/download.ts`. There are two client objects, `api` and `adminApi`; types are in `lib/types.ts`.
- **SSE (`src/lib/stream.ts`).**
  - `openResearchStream(id, handlers)` is an EventSource: `status_change`, `trace_step`, `reasoning_delta`, `report`, `stream_error` (not terminal) and `done`. It returns a close function; call it in `onBeforeUnmount`.
  - `streamChatAnswer` POSTs and parses SSE by hand; exactly one of onDone/onError fires.
  - `lib/trace.ts` deduplicates replays (`createTraceDeduper`) and paces reconnects (`reconnectDelayMs`).
- **URLs.** Every untrusted `:href` goes through `safeHttpUrl` (`lib/url.ts`).

## Strings (i18n)

- **One file:** `src/i18n/index.ts`. It holds `const ru = {…}` (the source of truth), then `const en: typeof ru` and `const es: typeof ru`, so vue-tsc flags a missing or extra key.
- **Adding a key.** Add it to `ru`, then at the same path in `en` and `es`.
  - Params are named: `t("sidebar.confirmDeleteNamed", { title })`.
  - Plurals use `|`. **ru has three forms** (`"{n} пункт | {n} пункта | {n} пунктов"`), en and es have two. Call `$t("plan.items", count)`.
- **Usage.** `$t()` in templates, `const { t } = useI18n()` in script, `i18n.global.t` outside components. Guard dynamic keys with `te()`.
- **Guards:**
  - `src/i18n/index.test.ts`: key parity across the three locales. Its non-empty check is a no-op, and its plural check covers only `plan.items`. Check for empty strings and the three ru plural forms yourself.
  - `src/sourceGuards.test.ts` checks that every literal key used in `*.vue`, `stores/*.ts` and `lib/*.ts` exists, and rejects **Cyrillic inside `<template>`**. Every visible string goes through i18n.
  - sourceGuards skips `router/*.ts`. Instead, `router/index.test.ts` checks that every `titleKey` exists in ru.
- **Server errors.** `apiErrorMessage(e, t)` tries these in order:
  1. `API_DETAIL_KEYS[status]` regexes, which map to `errors.api.<key>`;
  2. `API_STATUS_KEYS`, a generic text per status;
  3. the raw detail.
  - A network `TypeError` becomes `errors.api.network`.
  - Render the result in `<p role="alert" class="text-sm text-danger">`.
  - **A new mapping must match a detail the backend really raises with that status.** The backend test `test_frontend_error_contract.py` parses `API_DETAIL_KEYS` and `SERVER_STREAM_ERRORS`, so keep their literal format (`NNN: [ [/re/, "key"], … ]`). Write predicates on one line (`err.status === 400 && err.detail.startsWith("code")`). See the `api-endpoint` skill.
- **Forbidden in UI code:** `e.message`/`err.message`, which show raw errors (`sourceGuards` enforces this), and `alert(`.

## Design tokens

- **Where.** Tokens live in `src/style.css` as space-separated RGB channels, so alpha modifiers work (`bg-surface/50`).
  - `:root` is the light theme. Each theme overrides in `html[data-theme="…"]`, and `html.dark` holds what all dark bases share.
  - Tokens: `--c-bg`, `-rail`, `-surface`, `-surface-hover`, `-bd`, `-ink`, `-muted`, `-accent`, `-accent-soft`, `-on-accent`, `-success`, `-warning`, `-danger`, `-info`, plus `--scrim`, `--elev-1..3` and the easing variables.
- **Tailwind names** (`tailwind.config.js`): `bg`, `rail`, `surface`, `surfaceHover`, `bd`, `ink`, `muted`, `accent`, `accentSoft`, `onAccent`, `success`, `warning`, `danger`, `info`. Also `shadow-e1/e2/e3`, `ease-emphasized`/`ease-sheet`, `duration-160/240/320`, `text-3xs/2xs` with size-tuned tracking, `rounded-card`, `font-serif` (Lora).
  - **`text-accent` resolves to `--c-accent-soft`**, the text-safe shade on tints. `bg-`, `border-` and `ring-accent` keep the brand colour. On a solid `bg-accent`, use `text-onAccent`.
  - `future.hoverOnlyWhenSupported` is on: `hover:` utilities apply only with a real hover. **Hand-written hover CSS needs `@media (hover: hover) and (pointer: fine)`.**
- **Patterns.**
  - Text: `text-ink` / `text-muted`.
  - Cards: `bg-surface border border-bd`, with `shadow-sm` or no shadow. `shadow-e3` is for dialogs and sheets.
  - Primary button: `press bg-accent text-onAccent`.
  - Status box: `border-success/30 bg-success/10 text-success`.
  - Use tokens rather than raw palette classes; a few `text-red-…`-style leftovers remain.
- **`src/themeContrast.test.ts`** parses the token rules. Each selector must start a line as `selector {`. It checks the six themes light, sand, dark, midnight, emerald and rose for:
  - accent-soft text at least 4.5:1 on accent tints over every base;
  - the accent focus ring at least 3:1;
  - the field focus border and halo.
- **Adding a colour:**
  1. Add `--c-x: r g b;` to `:root` and to each theme or `html.dark` rule where it differs.
  2. Add a `prefers-contrast: more` override if it is a border or muted-type token.
  3. Map it as `"rgb(var(--c-x) / <alpha-value>)"` in tailwind.config.js.
  4. Add contrast assertions if it is text on a tint.
- **Adding a theme:**
  - a style.css block;
  - a `THEMES` entry and the `ThemeId` union;
  - `themes.<id>` in three locales;
  - the test's theme list;
  - if it is dark, the list inside `index.html`'s inline script, **which changes its CSP hashes** (see Security).
  - the report-export palettes, which are copied from the web themes and checked by no test: `siteThemes` in ArtifactPanel.vue, the `site.<id>` i18n keys, and `_THEME_VARS` / `_DARK_THEMES` in `src/ui/report_export.py`.

## Interaction: press, focus, hit areas, materials

- **Press feedback.** `button`, `a[href]`, `[role=button]` and `summary` dim on `:active` instantly.
  - Add `.press` for a `scale .97` dip.
  - Cards use `press press-lg` (**both** classes) for `.985`; `press-lg` alone does nothing.
  - `.press-none` opts out.
- **Hit areas.** `.hit` adds an invisible 8 px `::after` on coarse pointers only. Give small controls `.hit`, so a 28 px icon button reaches 44 px on phones.
- **Focus.**
  - Controls get a 2 px accent `:focus-visible` outline with a 2 px offset.
  - Fields get an accent border plus a 3 px halo.
  - A borderless input inside a styled box uses `.field-bare` on the input and `.field-host` on the box.
  - Never remove a focus style without a replacement. Forced colors force a `CanvasText` outline.
- **Materials.** `backdrop-filter` only through `.material-float`, `.material-popover`, `.material-bar` and `.scrim`, and only on chrome that floats over content. Never on in-flow cards, and never animate the blur. The reduced-transparency, more-contrast and forced-colors fallbacks target these classes, so reuse them instead of ad-hoc `backdrop-blur`.
- **Other helpers:** `.edge-fade-y/-top/-x` scroll masks, `.scrollbar-none`, and `.live-dot`, the one looping "live" pulse. Keep looping animations rare.
- **Attribute and class rules.**
  - Bind `inert` / `aria-hidden` as `true` or `undefined`, never `false`.
  - Tabs need `role="tab"` and `aria-selected`.
  - Tailwind classes must be full literals; built-up class strings are purged.
- **iOS zoom.** Coarse-pointer `input[class]`, `textarea[class]` and `select[class]` are forced to at least 16 px so iOS does not zoom. An input without a class attribute is not covered.

## Motion and gestures

- **Split rule.** Anything a button drives is a **CSS transition**: interruptible, no bounce. **Springs, decay and rubber-banding** only follow a finger drag. Animate `transform` and `opacity` only.
- **Vue transition names** (style.css):
  - `fade` (0.3 s) and `fade-quick` (160 ms);
  - `swap` (a cross-fade in one cell);
  - `pop` (menus grow from the trigger; add an `origin-top-left/right` class);
  - `modal`;
  - `view` (rises on enter only).
- **`src/lib/gesture.ts`:**
  - `rubberband(overshoot, dimension, c = .55)`, `project(velocity, rate = .998)`, `createVelocityTracker()`.
  - `springTo({ from, to, velocity, response = .3, onUpdate, onComplete })` is critically damped. `decay(...)`.
  - Both return `{ stop(): number }`, so a new grab resumes from the on-screen value.
- **`src/lib/motion.ts`.**
  - `prefersReducedMotion()` is a one-shot check; `useReducedMotion()` and `useMediaQuery(q)` are reactive.
  - **Every smooth scroll goes through `smoothOrAuto()`.** sourceGuards forbids a literal `behavior: "smooth"` or `scroll-behavior: smooth` in `*.vue`, `stores/*.ts` and `lib/*.ts`. Keep the rule everywhere, style.css included, even where the guard does not look.
- **Reduced motion.**
  - The CSS layer turns slides, rises and scales into fades. **Register any new transform transition or keyframe class there.**
  - In JS, switch classes on `useReducedMotion()`: the drawer and SlideOver cross-fade instead of sliding.
  - Use `motion-safe:` for hover scale.
- **Popover or menu:**
  1. A `div.relative` root, and a trigger `button` with a ref, `aria-haspopup` and `:aria-expanded`.
  2. `<Transition name="pop">` around a `v-if` panel with `material-popover absolute … origin-top-right rounded-xl border border-bd` and a role.
  3. `useDismiss(rootRef, open, { trigger })`: closes on an outside pointerdown or Escape, and returns focus to the trigger on Escape.
  4. Focus the first or checked item when it opens. A `role="menu"` panel handles the arrow keys, Home and End, and closes on Tab (see `onThemeMenuKey` in AppSidebar.vue).
- **Sheet.** `<SlideOver :open :label @close side="right">`. It traps focus when modal, focuses `[data-autofocus]` first, and restores focus on close. The scrim closes only when a press both starts and ends on it. Touch or pen drag-to-dismiss comes from `useDragDismiss`. Mark nested gesture areas with `data-no-drag-dismiss`.
- **Confirm.** `await confirm({ title, message, confirmText, cancelText, danger })` from `lib/confirm.ts`, only for destructive or irreversible actions. With `danger`, focus starts on Cancel and a bare Enter never confirms. **Always pass translated texts**, because the defaults are English literals.
- **Streams.** `useStickToBottom(el)` returns `{ pinned, follow, jumpToLatest }`. Call `follow()` after each chunk; only a real user gesture unpins.

## Security

- **CSP.** It is set **only by `web/nginx.conf`** (`set $spa_csp`); index.html has no CSP meta tag, and the vite dev server sends none, so CSP problems show only behind nginx.
  - `script-src 'self'` plus two sha256 hashes of the single inline theme script in `index.html`, one for LF and one for CRLF line endings.
  - `style-src-elem` has no `'unsafe-inline'`, so `:style` bindings are fine but runtime `<style>` elements are blocked.
  - `connect-src 'self'`: a new API origin must be added there and to `CORS_ALLOW_ORIGINS`.
- **Avoid new inline scripts.** If you edit the existing one, recompute both hashes and replace them in nginx.conf. `src/securityHeaders.test.ts` fails on any drift and prints the expected values. Without node:

```bash
S=$(mktemp -d)
perl -0777 -ne 'while(/<script\b(?![^>]*\bsrc=)[^>]*>(.*?)<\/script>/sg){my $t=$1;$t=~s/\r\n/\n/g;print $t}' web/index.html > $S/lf.js
perl -pe 's/\n/\r\n/' $S/lf.js > $S/crlf.js
for f in lf crlf; do echo "'sha256-$(openssl dgst -sha256 -binary $S/$f.js | base64)'"; done
```

- **Token URLs.** Any new route that carries a token needs the nginx `$spa_referrer_policy` (no-referrer) and `$spa_cache_control` (no-store) maps, plus the matching securityHeaders test cases.
  - **A token in the path** also needs the access-log redaction maps (`$redacted_request_uri`, `$redacted_http_referer`; `tests/test_nginx_log_redaction.py`) and the API log filter (`redact_share_tokens` in `src/observability/logging.py`).
  - **A token in the fragment** (`#token=`, like the reset and verify links) also needs `LINK_PAGES` in `lib/linkToken.ts`, `meta.linkToken` on the route, the route list in `router/index.test.ts` and `RECOVERY_PAGES` in `securityHeaders.test.ts`.
- **`v-html` appears only in `MarkdownView.vue`.**
  - markdown-it runs with `html: false`. Links get `target=_blank rel="noopener noreferrer"`.
  - Hand-built markup (citations, claim marks) uses `escAttr` and `safeHref`.
  - Do not add another `v-html`. Extend MarkdownView and its tests.
- **Scoped CSS does not reach `v-html` content.** Style it with `:deep(.cls)`. Page-wide classes such as `.md-citation` live in style.css. Tests compile the scoped CSS (`compileStyle`) to assert the selectors.

## Tests (vitest)

- **Environment.** The default is `node`. Put `// @vitest-environment jsdom` on line 1 for DOM tests.
- **Mounting.** Use a real pinia, a `createMemoryHistory` router with stub routes, and `i18n`. Set `i18n.global.locale.value = "en"` and assert on `t(key)`, never on literal text.
- **Mocking.** Mock `@/lib/api` with `vi.hoisted` + `importOriginal`, keeping the real `ApiError`/`apiErrorMessage`.
- **Cleanup.** `enableAutoUnmount(afterEach)`, `flushPromises()`, `vi.clearAllMocks()`.
- **Focus and Teleport.** Mount with `attachTo: document.body`.
- **jsdom gaps.** There is no PointerEvent (define `class TestPointerEvent extends MouseEvent`) and no `matchMedia` (stub it as `ui.test.ts` does).

```ts
// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { enableAutoUnmount, flushPromises, mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { createMemoryHistory, createRouter } from "vue-router";
import { i18n } from "@/i18n";

const mocks = vi.hoisted(() => ({ getThing: vi.fn() }));
// Real ApiError/apiErrorMessage; only the network calls are stubbed.
vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return { ...actual, api: { ...actual.api, ...mocks } };
});
import { ApiError } from "@/lib/api";
import ThingView from "./ThingView.vue";

const t = (key: string, params: Record<string, unknown> = {}) => i18n.global.t(key, params);

async function mountView() {
  const pinia = createPinia();
  setActivePinia(pinia);
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [{ path: "/thing", component: ThingView }, { path: "/login", component: { render: () => null } }],
  });
  await router.push("/thing");
  const wrapper = mount(ThingView, { global: { plugins: [pinia, router, i18n] } });
  await flushPromises();
  return wrapper;
}

enableAutoUnmount(afterEach);

describe("ThingView", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
    mocks.getThing.mockResolvedValue({});
  });
  afterEach(() => vi.clearAllMocks());

  it("shows a translated error, never the raw detail", async () => {
    mocks.getThing.mockRejectedValue(new ApiError(429, "Too many"));
    const wrapper = await mountView();
    expect(wrapper.find('[role="alert"]').text()).toBe(t("errors.api.rateLimited"));
  });
});
```

## Recipes

- **A new view:**
  1. `src/views/XView.vue`.
  2. A lazy route with a `name`, `props: true` for params, and a `meta.titleKey` that exists in all three locales.
  3. `public`/`bare` if needed. If it calls the API while signed out, add its path to `PUBLIC_PAGES` in api.ts.
  4. `XView.test.ts`.
- **A new API call:**
  1. Add a method on `api`/`adminApi` via `request<T>()`, with `encodeURIComponent` on user-supplied path parts and a type in `lib/types.ts`.
  2. At the call site: `try { … } catch (e) { error.value = apiErrorMessage(e, t) }`.
- **Telemetry events.** A new event name goes into three places: `lib/telemetry.ts`, the server allowlist `CLIENT_TELEMETRY_EVENTS` in `src/domain/models.py` (without it the server rejects the event), and `tests/test_telemetry_input_bounds.py`.
- **Password rules.** `api.test.ts` compares `PASSWORD_MIN_LENGTH` in api.ts with every `*password: str = Field(..., min_length=N)` in `src/domain/models.py` (LoginRequest excepted), so change them together.
