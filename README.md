# Multi-Agent Search & Optimizer

FastAPI backend for a research workflow with persistent jobs, Postgres storage, and a separate worker process.

## LangGraph Finalize Loop

The post-search finalize orchestration lives in `src/graph/` as a single
LangGraph state machine (a pinned hard dependency):

- collect source and evidence summaries
- branch into a replan step when coverage looks weak
- run the analyzer
- retry analysis once when the generated draft still contains report-note style quality warnings

Fresh and resumed runs (after a worker crash) walk the SAME topology — a
checkpointed run re-enters the graph at the successor of its last completed
step, so a research's behavior no longer depends on whether a worker died
mid-finalize.

## Observability

The project now supports:

- structured JSON logging with `LOG_FORMAT=json`
- Prometheus metrics at `/metrics` (API) and one exporter per worker
  (`WORKER_METRICS_PORT`, internal network only)
- Docker Compose services for `prometheus` and `grafana`, with provisioned
  dashboards (Platform Overview, Research Operations) and alert rules
  (`WorkerDown`, `APIDown`, `DeadLetterQueueGrowth`, `APIHighErrorRate`,
  `WorkerJobFailureRate`, `FinalizeQueueBacklog`); test the rules with
  `promtool test rules ops/prometheus/alerts.test.yml`.
  `mas_worker_jobs_total` counts job attempts: every failed attempt is a
  `status="failure"`, including one that is retried and a finalize attempt
  whose lease was lost (stale recovery or a requeue took the job over while it
  ran, even if the job then completed), so `WorkerJobFailureRate`
  needs more than 20% failed attempts and at least 6 of them in 15 minutes (two
  jobs' worth of `JOB_MAX_ATTEMPTS=3`); a single dead-lettered job is left to
  `DeadLetterQueueGrowth`
- `loki` + `promtail` collecting every container's stdout. JSON log lines are
  stored whole, so the Research Operations dashboard's Logs panel shows the API
  and worker logs of one research (type its id into the Research ID box)

The API request metrics (`mas_api_requests_total`,
`mas_api_request_duration_seconds`) keep a bounded label set. `path` is the
matched route template (`unmatched` when no route matched), never the raw path
with its share tokens. `method` is one of `GET`, `POST`, `PUT`, `PATCH`,
`DELETE`, `HEAD`, `OPTIONS`, or `OTHER` for any other method, so a client
cannot add series by sending made-up methods.

Metrics caveat: the API runs `uvicorn --workers $API_WORKERS` (2 in Compose)
without `prometheus_client` multiprocess mode, so each API process keeps its own
counters and every scrape reads one of them at random. API request rates,
latency and `APIHighErrorRate` are therefore approximate (process switches look
like counter resets). Set `API_WORKERS: "1"` on the `api` service in
`docker-compose.yml` when you need exact API metrics. The workers are
single-process and unaffected, which is why the queue alerts, the queue panels
and the LLM cost panels read the worker series only.

The Grafana LLM cost panels therefore show worker-side spend (search and
finalize jobs) and leave out LLM calls made inside the API (decompose, optimize,
clarify, plan approval, chat, summary). On a counter the per-process problem is
worse than noise: every scrape that lands on the other process looks like a
reset worth that process's whole total, so `rate()`/`increase()` over the API
series report spend that never happened. Exact totals, API calls included, are
in the admin panel's Analytics tab, which reads `llm_usage_logs`.

Health endpoints: `GET /health` is the cheap readiness probe (status +
dependency pings) and needs no login. Everything else under `/health` is
admin-only: `GET /health/detail` (the full operational payload: queue metrics,
graph alerts and trends), `GET /health/queues` (queue metrics with the
maintenance summary, which lists every user's research ids, and the admins'
resolution notes), the worker heartbeat routes under `/health/workers` (each
worker's status and last error) and the queue maintenance and recommendation
routes under `/health/queues/`.

`/metrics` is blocked at the nginx edge and, optionally, protected by a shared
secret (`METRICS_TOKEN`; sent as `Authorization: Bearer …` or `X-Metrics-Token`).
When you set it, add the same token to the Prometheus scrape jobs in
`ops/prometheus/prometheus.yml`.

Note: with `LANGSMITH_TRACING=true`, prompts and generated content are sent to
the configured LangSmith project — keep it off for sensitive workloads.

SQL parameters are never logged: the database engine is created with
`hide_parameters=True`, so a failed statement in the logs (and in Loki) shows
the SQL but not its bound values, such as emails, password hashes or prompts.

Useful flags:

```env
LOG_FORMAT=json
PROMETHEUS_METRICS_ENABLED=true
# METRICS_TOKEN=change-me
# Retention for finished researches; 0 keeps everything (default).
# RESEARCH_RETENTION_SECONDS=0
# Retention for admin telemetry: user_events rows and telemetry sessions
# (user_sessions) older than this are deleted; default 7776000 (90 days).
# USER_EVENTS_RETENTION_SECONDS=7776000
# USER_SESSIONS_RETENTION_SECONDS=7776000
# Retention for the admin audit log; 0 keeps it forever (default).
# ADMIN_AUDIT_RETENTION_SECONDS=0
```

## Authentication and Admins

Auth is on by default (`AUTH_DISABLED=false`): people sign in and see only
their own researches. The API refuses to start until `AUTH_SECRET_KEY`, which
signs the session tokens, is a random value of at least 32 characters; the
built-in default and anything shorter are rejected. Generate one with
`openssl rand -hex 32` and put it in `.env`. Behind HTTPS also set
`AUTH_COOKIE_SECURE=true` (see `.env.example`).

`AUTH_DISABLED=true` is for a trusted local single-user setup only: nobody
signs in, every request runs as one shared local user, and while `ADMIN_EMAILS`
is empty that user also passes every admin check. Do not expose such an
instance beyond your own machine.

Users register with email and password or sign in with Google. Google sign-in
with the address of an existing password account links Google to that account
(the callback accepts only addresses Google reports as verified). If the account
had verified its email, its password keeps working. If not, the password is
cleared and every session of the account is revoked, so a password set by
someone who never proved they own the address stops working (see
[Account recovery](#account-recovery)). An account already linked to a
different Google identity is never taken over: the login page shows
`?error=oauth_conflict` for that case and `?error=oauth_failed` for any other
callback failure.

An account has admin rights only when both hold:

- its email is listed in `ADMIN_EMAILS` (comma-separated), and
- the address is verified: the account is linked to Google (Google verified the
  email) or the operator provisioned it with `scripts/create_admin.py` (which
  sets `users.admin_provisioned_at`).

Sign-up does not wait for the email to be verified, so the list alone would
make whoever registered an address first its admin. For the same reason
`ADMIN_EMAILS` addresses cannot self-register, and the login form never sets
their first password. Verifying the address through the emailed link does not
grant admin rights either: only the two routes above do. An admin either signs
in with Google and then sets a password in Settings, or is provisioned by the
operator:

```bash
# Prompts for the password twice; the email must be listed in ADMIN_EMAILS.
docker compose exec api python scripts/create_admin.py ops@example.com
# Non-interactive:
printf '%s\n' "$ADMIN_PASSWORD" | docker compose exec -T api python scripts/create_admin.py ops@example.com --password-stdin
```

Running it for an existing account replaces the password, revokes that
account's sessions and marks it as operator-provisioned. That is the only way a
self-registered password account becomes an admin when its address is added to
`ADMIN_EMAILS` later: until you run the script for it, it stays a normal user,
and running it locks out whoever registered it. The password is never taken as
a command-line argument, so it stays out of shell history and `ps`. Locally,
run `python scripts/create_admin.py <email>` against the Postgres configured in
`.env`.

Whenever `ADMIN_EMAILS` is set, `AUTH_SECRET_KEY` must be strong (at least 32
random characters, not the built-in default) even with `AUTH_DISABLED=true`:
admin routes then still require an admin's token, which is signed with that
key, and the API refuses to start with a weak one.

Upgrading from a release without `users.admin_provisioned_at`: the migration
adds the column without a backfill, because a backfill would also bless any
account that squatted an admin address. Password-only admins therefore lose
their admin rights after `alembic upgrade head` until you run
`scripts/create_admin.py` once for each of them (it sets a new password and
signs them out everywhere). Admins linked to Google (`users.google_subject`
set) keep their rights. An admin account created with Google before Google ids
were stored gets them back the next time its owner signs in with Google, which
links the account (see [Legacy Google accounts](#legacy-google-accounts)); any
other admin account needs the script too.

Usage telemetry (`POST /v1/telemetry/event`, which feeds the admin analytics)
is accepted only from signed-in users with a valid CSRF token; the web UI sends
it only while you are logged in, so anonymous visitors and public share-link
viewers are not tracked. Each user gets `TELEMETRY_RATE_LIMIT_PER_MINUTE`
events per minute (default 120); heartbeats refresh the session row instead of
adding event rows.

Every admin mutation (queue maintenance, requeue/recover/cleanup, user
deletion) and every CSV export writes an admin audit row and shares the
`ADMIN_RATE_LIMIT_PER_MINUTE` budget (default 10). Accounts with admin rights
cannot be deleted from the admin panel; an unverified account squatting an
`ADMIN_EMAILS` address can. Deleting it is one remedy. The other is the owner
signing in with Google, which links that account and clears the squatter's
password and sessions (see [Account recovery](#account-recovery)).

LLM spend is recorded per call in `llm_usage_logs` (actual model id, tokens,
cache hits, cost), including API-side calls such as decompose, optimize and
chat. `DEEPSEEK_REASONER_MODEL` and `DEEPSEEK_REPAIR_MODEL` are sent to the
provider as configured; they are operator settings, never user-selectable.
The price tier comes from an explicit model-id map (the model catalog and its
aliases, plus `deepseek-reasoner` and `deepseek-chat`, which are billed at the
flash tier because DeepSeek serves them as v4-flash in thinking and
non-thinking mode). An id that is not in the map is guessed from its name: one
containing `pro` is billed as pro, one containing `flash` or `chat` as flash.
An id whose name matches neither is billed at the tier of `DEEPSEEK_MODEL`.

## Account recovery

A forgotten password is reset through a one-time link sent by email, sign-up
asks people to confirm their address the same way, and Google sign-in links to
an existing account under rules that lock a squatter out rather than the
owner. Everything that sends mail needs an email backend.

### Email settings

Set these in `.env` (`.env.example` lists them all):

| Setting | Default | Meaning |
|---|---|---|
| `EMAIL_BACKEND` | `disabled` | `disabled`, `console` or `smtp` (below) |
| `SMTP_HOST`, `SMTP_PORT` | none, `587` | The mail server; `SMTP_HOST` is required for `smtp` |
| `SMTP_SECURITY` | `starttls` | `starttls`, `ssl` (TLS from the first byte, usually port 465) or `none` (clear text, for a local relay only) |
| `SMTP_USERNAME`, `SMTP_PASSWORD` | empty | Credentials, if the server needs them |
| `SMTP_FROM` | none | Sender address; required for `smtp` |
| `SMTP_TIMEOUT_SECONDS` | `10` | Seconds to wait for the mail server before giving up |
| `PUBLIC_APP_URL` | `http://localhost:8502` | The web UI's address as users open it, without a trailing slash. Every link in an email starts with it |
| `PASSWORD_RESET_TTL_SECONDS` | `3600` | How long a password reset link works |
| `EMAIL_VERIFICATION_TTL_SECONDS` | `86400` | How long a verification link works |

- `disabled` sends nothing, so password reset and email verification are off:
  `GET /v1/auth/config` reports `"password_reset": false` and
  `"email_verification": false` next to `google_oauth`. Both are `true` with
  any other backend. An operator can still issue reset links
  ([below](#without-email-operator-issued-reset-links)).
- `smtp` sends through `SMTP_HOST`. The API refuses to start with `smtp`
  unless `SMTP_HOST` and `SMTP_FROM` are set.
- `console` is for development only. It writes every message, links included,
  to the application log at WARNING level (in Compose that log also goes to
  Loki), so anyone who can read the logs can reset any account's password. The
  API logs a warning at startup while it is on.

Messages are plain text, in the language the request's `Accept-Language`
prefers among English, Russian and Spanish (English otherwise). Links are
built from `PUBLIC_APP_URL`, not from the request, so a forged `Host` header
cannot point a reset link at another site. Set it whenever users reach the web
UI anywhere but `http://localhost:8502`: another `WEB_PORT`, a domain, HTTPS.
With any backend but `disabled`, the API refuses to start unless it is an
`http://` or `https://` URL.

### Password reset

In the web UI, the sign-in form links to "Forgot password?" (`/forgot-password`)
while `password_reset` is on; otherwise it tells people to ask the
administrator. Behind it:

1. `POST /v1/auth/password/forgot` with `{"email": "..."}` always answers 202
   `{"status": "accepted"}`: for a known and an unknown address alike, and also
   while email is disabled. The lookup and the mail happen after the response,
   so neither the answer nor its timing tells anyone whether an account exists.
   If one has that address, it is sent a link to
   `{PUBLIC_APP_URL}/reset-password#token=<token>`. A new link makes the
   account's earlier unused ones stop working.
2. That page sends the token and a new password to
   `POST /v1/auth/password/reset` and gets 200 `{"status": "ok"}`. The password
   has the same minimum length as everywhere else (6 characters; a shorter one
   gets 422). A link that is invalid, expired or already used gets 400 with a
   detail starting with `reset_token_invalid`.

A successful reset:

- sets the new password and marks the email as verified: opening the link
  proved control of the mailbox;
- revokes every session of the account, on every device, so whoever knew or
  guessed the old password is signed out;
- invalidates the account's other reset links that are still outstanding;
- emails a "your password was reset" notice, so the owner learns about a reset
  they did not ask for;
- does not sign you in. Sign in with the new password.

The token is random, works once and expires after `PASSWORD_RESET_TTL_SECONDS`
(1 hour by default). The database stores only a hash of it, so a database dump
or backup holds no working link. It travels in the URL fragment (after `#`),
which browsers never send to a server, so it stays out of access logs, proxies
and `Referer` headers. The web UI takes it out of the address bar as soon as
the page opens. nginx also serves `/reset-password` and `/verify-email`
with `Referrer-Policy: no-referrer` and `Cache-Control: no-store` (see
[Browser security headers](#browser-security-headers)).

Throttles, per hour and per API process like the other rate limits (see
[API](#api)), and on even with `AUTH_DISABLED=true`:

- reset requests: 20 per client address and 5 per email address (compared
  case-insensitively). The per-email limit counts whether or not an account has
  the address, so hitting it gives nothing away either, and it keeps anyone from
  flooding a mailbox with reset mail;
- redeeming a reset or a verification link: 30 per client address for each;
- new verification links: 5 per signed-in user.

Past a limit the API answers 429 `Too many attempts, please slow down`. Like
sign-in, the endpoints that work without a session need no CSRF token.

### Email verification

With email enabled, sign-up sends a link to
`{PUBLIC_APP_URL}/verify-email#token=<token>`. It works once, expires after
`EMAIL_VERIFICATION_TTL_SECONDS` (24 hours by default) and is stored hashed
like a reset token. The new account can be used right away, and a failure to
send the mail never fails the sign-up. `email_verified` in the user returned by
`GET /v1/auth/me`, sign-in and sign-up shows the state.

- `POST /v1/auth/email/verification` (signed in, with the usual CSRF token)
  sends a new link and answers 202 `{"status": "sent"}`, or 200
  `{"status": "already_verified"}`. It is throttled per user, and the new link
  makes the earlier ones stop working.
- `POST /v1/auth/email/verify` with `{"token": "..."}` answers 200
  `{"status": "verified"}`. A link that is invalid, expired or already used
  gets 400 with a detail starting with `verification_token_invalid`. It needs no
  session, so the link also works in another browser.

A verified address keeps its password when Google sign-in is linked (next
section). It does not grant admin rights (see
[Authentication and Admins](#authentication-and-admins)).

### Google sign-in and existing accounts

Google sign-in accepts only addresses that Google reports as verified. When no
account is linked to that Google identity yet but an account with the same
email exists:

| Existing account | Result of the Google sign-in |
|---|---|
| Not linked to Google, email verified | Google is linked. The password keeps working. |
| Not linked to Google, email not verified: a squatter's sign-up, a Google account from before migration `000019`, any password account whose owner never verified the address | Google is linked, the password is cleared, every session of the account is revoked and the email is marked verified. |
| Linked to a different Google identity | Nothing changes: the sign-in is refused and the login page shows `?error=oauth_conflict`. |

Linking emails the account a "Google sign-in was linked" notice. After a cleared
password, the owner can set a new one in Settings within 10 minutes of the
Google sign-in (see [Sessions and passwords](#sessions-and-passwords)). If you
signed up with a password and want to keep it, verify your address before you
first sign in with Google.

Upgrading to this release (migration `20260925_000033`) counts accounts that
are linked to Google or were provisioned with `scripts/create_admin.py` as
verified, and the script marks every account it provisions from then on
verified too. Every other existing password account starts unverified.

This rule prevents account pre-hijacking. Sign-up alone proves nothing about
an address, so an attacker can register yours with a password of their own
before you ever use the service, and keep a session open. If Google sign-in
simply merged into that account, the attacker's password and session would
keep working in the account you then fill with your research. Clearing an
unverified password and revoking every session removes both: from the moment
Google vouches for you, only you can get in. A verified password was set by
someone who could read the mailbox, the same person Google vouches for, so it
stays. An account linked to another Google identity is never merged, because a
second identity already claims it. Researches the squatter created before stay
in the account; delete any you do not recognize.

### Legacy Google accounts

Accounts created with Google before migration `20260904_000019` have no stored
Google id (`users.google_subject` is NULL) and a random password nobody knows.
Strict matching turned their Google sign-in away with `oauth_conflict`, and no
password let their owners in. They count as unverified, so the owner simply
signs in with Google again: the account is linked, the unknown random password
is cleared, and the owner is back with all of their researches. With email
enabled, a password reset works too. An admin among them gets admin rights
back at that sign-in, because the account is then linked to Google.

### Without email: operator-issued reset links

With `EMAIL_BACKEND=disabled` nobody receives reset links. The operator can
issue one instead, and also for a user whose mail does not arrive:

```bash
docker compose exec api python scripts/issue_password_reset.py user@example.com
```

The script prints a one-time link to `/reset-password` with the usual lifetime
and effects: using it sets the password, marks the address verified and signs
the account out everywhere, and issuing it makes the account's earlier reset
links stop working. Whoever opens the link controls the account, so confirm who
owns the address first and hand the link over directly, out of band. The
script refuses the in-memory store, exits with status 1 and creates nothing
when no account has the email, and logs nothing secret: the link alone goes to
standard output, a note about it to standard error. Locally, run
`python scripts/issue_password_reset.py <email>` against the Postgres
configured in `.env`.

## Security

### Sessions and passwords

Signing out (`POST /v1/auth/logout`) revokes every session token of the
account, not only the one in use, so it signs you out on all devices. It also
clears the session cookies and always answers 200 with
`{"status": "ok", "revoked": true}`. `revoked` is `false` when the request had
no valid session or the revocation failed; this browser is signed out either
way, but after a failure other devices may still be signed in. Changing the
password revokes every session too, and so do a password reset by email and a
Google sign-in that clears an unverified password.

Two password changes need a fresh Google sign-in: setting the first password
of an account created with Google, and resetting the password of a
Google-linked account without the current one. The session must come from the
Google callback no more than 10 minutes earlier. Otherwise
`POST /v1/auth/set-password` answers 403 with a detail starting with
`reauth_required`, and the web UI offers to sign in with Google again and come
back. The `/set-password` page shown right after a Google sign-up is within
that window. A stolen session alone therefore cannot add a password login to
someone's Google account.

Somebody else's local (email and password) sign-up of your address no longer
locks you out. Signing in with Google links the account to your Google identity
and removes the squatter's password and sessions; a password reset by email
replaces their password and ends their sessions (see
[Account recovery](#account-recovery)). If neither is possible, an operator can
still delete the squatting account in the admin panel's Users tab (audited; it
also deletes that account's researches) after confirming who owns the address.

### API

- `PATCH /v1/tasks/{task_id}` is admin-only. It rewrites a search task's
  results and log, which feed a finished report's sources, verification and
  confidence, also on its public share page. The web UI never calls it.
- Admin CSV exports (`GET /v1/admin/users/export`, `/v1/admin/prompts/export`
  and `/v1/admin/tokens/export`) check CSRF like unsafe methods: with a cookie
  session, `X-CSRF-Token` must match the `csrf` cookie, so a cross-site link
  cannot start an export under an admin's name. The admin panel sends it.
- Rate limits use one-minute sliding windows kept in each API process, so each
  of the `API_WORKERS` processes (2 in Compose) counts separately. They cover
  sign-in and sign-up per client address, sign-in per account,
  current-password checks per user, the LLM routes, telemetry and admin actions.
  The account recovery endpoints have throttles of their own (see
  [Account recovery](#account-recovery)). Each limiter keeps at most 10,000
  keys (client addresses, emails or user ids) per process. Past that, the key
  idle longest is dropped first, and its budget starts over.
- A client's `X-Request-ID` is kept, and echoed back, only if it matches
  `^[A-Za-z0-9._-]{1,64}$`. Any other value is replaced by a generated id.
- Webhook URLs often carry a credential (Slack, Discord and Teams incoming
  webhooks, for example), so logs show them as `scheme://host` only.
- Server-side requests to user-supplied URLs (research webhooks, source page
  fetches) go through an SSRF guard. It resolves the host and rejects every
  address that is not globally routable: private, loopback, link-local,
  reserved, multicast and unspecified ranges, and also the shared address space
  `100.64.0.0/10` (carrier-grade NAT, also used by Tailscale). Webhooks to
  internal hosts are refused by design.

### Browser security headers

The web UI's nginx (`web/nginx.conf`) sends these with every page and asset:

- `Content-Security-Policy`:
  - `default-src 'self'`;
  - scripts only from the bundle, plus the inline theme script of
    `web/index.html`, allowed by its hash;
  - stylesheets from the bundle and Google Fonts. Inline `style` attributes are
    allowed, because KaTeX math and Vue's pre-rendered markup use them, but
    inline `<style>` elements are not;
  - fonts from the bundle, `data:` and Google Fonts;
  - images from the site, `data:` and any `https:` URL (avatars, source
    favicons, report images). Plain `http:` images do not load;
  - `connect-src 'self'`, `object-src 'none'`, `base-uri 'none'`,
    `form-action 'self'` and `frame-ancestors 'none'`.
- `X-Frame-Options: DENY` and `X-Content-Type-Options: nosniff`.
- `Referrer-Policy: strict-origin-when-cross-origin`. The public share page
  (`/r/<token>`) gets `no-referrer`, so its token never appears in a `Referer`
  header, not even one sent to this site. The account recovery pages
  (`/reset-password`, `/verify-email`) get it too. Their token is in the URL
  fragment, which browsers never send anywhere, and the policy backs that up.

The HTML of every route (`index.html`) also gets `Cache-Control: no-cache`:
browsers revalidate it on every load, so a copy from before a deploy never asks
for bundles that are gone. `/reset-password` and `/verify-email` get `no-store`
instead, so no cache keeps a page that handles a one-time credential. Hashed
assets keep nginx's default caching.

Responses from the API (`/v1/`, `/health`) get
`Content-Security-Policy: default-src 'none'; frame-ancestors 'none'`,
`X-Frame-Options: DENY`, `nosniff` and `Referrer-Policy: no-referrer`. nginx
replaces the API's own copies, so each header is sent once. HSTS comes from the
API when `AUTH_COOKIE_SECURE=true`.

When you change the policy:

- The config is copied into the web image, so rebuild it afterwards
  (`docker compose up -d --build web`).
- nginx drops inherited `add_header` directives in any `location` that has an
  `add_header` of its own. Such a location must repeat the four security
  headers, as `location = /index.html` does. `web/src/securityHeaders.test.ts`
  (part of `npm test` in `web/`) fails otherwise.
- The inline script in `web/index.html` is allowed by its SHA-256 hash, listed
  once for LF and once for CRLF line endings. After editing the script, put its
  new hashes into `script-src`. The test fails and shows the expected values.
- Google sign-in needs no entry: it is a top-level navigation to
  `/v1/auth/google/login` and on to `accounts.google.com`, which CSP does not
  restrict.
- A web build with `VITE_API_BASE` set to another origin (the API is then not
  proxied under this site's `/v1/`) fetches and streams (SSE) from that origin.
  Add the origin to `connect-src`, for example
  `connect-src 'self' https://api.example.com`, and list the web UI's origin in
  the API's `CORS_ALLOW_ORIGINS`. Any other origin the UI starts loading from
  needs the matching directive too, for example `http:` in `img-src` if you
  must show plain-HTTP images.

## Requirements

- Python 3.11+
- Docker + Docker Compose
- DeepSeek API key

Minimal `.env`:

```env
DEEPSEEK_API_KEY=your_api_key_here
DEEPSEEK_MODEL=deepseek-v4-pro
TASK_STORE_BACKEND=postgres

# Auth is on by default and the API will not start without this: paste the
# output of `openssl rand -hex 32` (at least 32 random characters).
AUTH_SECRET_KEY=
# Or, for a trusted local single-user setup only (no login at all):
# AUTH_DISABLED=true

POSTGRES_USER=app
POSTGRES_PASSWORD=app
POSTGRES_DB=multi_agent_search
POSTGRES_HOST=localhost
POSTGRES_PORT=5433

FINALIZE_WORKER_INTERVAL=2.0
QUEUE_MAINTENANCE_INTERVAL_SECONDS=60
```

If `AUTH_SECRET_KEY` is left empty, is shorter than 32 characters or is the
built-in default while auth is on, the API exits at startup with
`Insecure auth configuration: AUTH_SECRET_KEY must be overridden ...`, so
under Docker Compose the API never comes up and the web UI answers 502 on
`/v1`. See "Authentication and Admins" above.

See [.env.example](./.env.example) for a full example.

## Native Text Processing Module

The repository now includes an optional Rust-backed text-processing module in `native/text_processing`.

It is a development-only accelerator: the Docker image does not build or ship
it, so containers always run the pure-Python fallback in
`src/core/rust_accel.py`. The fallback is the reference behaviour; the Rust
code must match it (lengths are counted in characters, not UTF-8 bytes), and CI
runs its unit tests with `cargo test`.

To build the native module into the active virtualenv:

```bash
venv/bin/python -m pip install maturin
./scripts/build_native_module.sh
```

The script uses:

- `cargo`
- `maturin`
- `native/text_processing/Cargo.toml`

If the native extension is unavailable, the project automatically falls back to the pure-Python implementation.

## Run With Docker Compose

```bash
docker compose up --build
```

This starts:

- `db`
- `migrate`
- `api`
- `worker`
- `worker_2`
- `worker_3`
- `web`
- `redis`
- `prometheus`
- `loki`
- `promtail`
- `grafana`

The Vue web UI will be available at `http://localhost:8502` (`WEB_PORT`); its
nginx proxies `/v1/` and `/health` to the API.

The API will be available directly at `http://localhost:8001` (`API_PORT`,
loopback only).

Prometheus will be available at `http://localhost:9090`.

Loki will be available at `http://localhost:3100`.

Grafana will be available at `http://localhost:3001` (Prometheus and Loki are pre-provisioned as datasources).

### Exposed ports

Only the web UI listens on all interfaces. The API, Postgres (`5433`),
PgBouncer (`6432`), Redis (`6379`), Prometheus (`9090`), Loki (`3100`) and
Grafana (`3001`) are published on `127.0.0.1` only; the containers reach each
other over the Compose network.

- To reach them from another machine, use an SSH tunnel
  (`ssh -L 3001:127.0.0.1:3001 user@host`) or an authenticated reverse proxy.
  Do not rebind them to `0.0.0.0`: Redis, Prometheus and Loki have no
  authentication at all.
- Docker-published ports bypass host firewalls: Docker forwards them through
  its own iptables rules, which UFW/firewalld input rules never see. Firewalling
  the host does not replace the loopback binding.
- The credentials in the Compose file are local-development defaults. For any
  real deployment set a strong Grafana admin password (`GRAFANA_ADMIN_PASSWORD`,
  passed to `GF_SECURITY_ADMIN_PASSWORD`) and change the Postgres credentials
  (`POSTGRES_USER` / `POSTGRES_PASSWORD`, which PgBouncer uses too) in `.env`.

### Workers and the Redis broker

The API and all three workers run with `USE_REDIS_BROKER=true`; the workers
share one `x-worker-env` block in `docker-compose.yml`. The LLM concurrency
limit (`LLM_MAX_CONCURRENT`, default 16) is therefore enforced through Redis
across the API and every worker together, not per process; raise it if worker
throughput drops.

Older Compose files silently dropped the Redis settings from the workers, which
then polled Postgres while the API kept pushing job ids that nobody popped. When
upgrading such a deployment, clear the stale Redis lists before starting the
new workers:

```bash
docker compose stop worker worker_2 worker_3
docker compose exec redis redis-cli DEL mas:search_jobs mas:finalize_jobs
docker compose up -d worker worker_2 worker_3
```

Stop everything:

```bash
docker compose down
```

Stop and remove the database volume:

```bash
docker compose down -v
```

## Run Locally

1. Install dependencies:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then set DEEPSEEK_API_KEY and AUTH_SECRET_KEY
```

2. Start Postgres:

```bash
docker compose up -d db
```

3. Run migrations:

```bash
python -m alembic upgrade head
```

4. Start the API:

```bash
uvicorn src.api.app:app --host 0.0.0.0 --port 8000
```

5. In another terminal, start the worker:

```bash
python scripts/run_finalize_worker.py
```

6. Optional: start the Vue web UI (dev server with API proxy):

```bash
cd web
npm install
npm run dev   # http://localhost:5173
```

Run the worker once:

```bash
python scripts/run_finalize_worker.py --once
```

## Database Migrations

`alembic upgrade head` applies the schema. Under Docker Compose the `migrate`
service runs it before the API starts; locally you run it yourself (step 3
above). Notes on recent revisions:

- `20260924_000028` (session and prompt-event indexes) and `20260925_000031`
  build their indexes with `CREATE INDEX CONCURRENTLY`, which does not block
  writes. A build that fails (a lock or statement timeout, a killed deploy job,
  a duplicate row racing in) leaves an INVALID index, which PostgreSQL never
  uses. The next `alembic upgrade head` drops it and builds it again, so no
  manual `DROP INDEX` is needed. `000028` also fails, without recording the
  revision, while an index it built is still not valid (for example, when
  another session is still building it); run the upgrade again after that
  build finishes. It drops the old `ix_user_sessions_session_id` only once the
  new unique index is valid.
- `20260925_000031` adds `ix_researches_created_id` on
  `researches (created_at, id)`. The admin Tokens list and export and the
  prompts export page through researches in that order. On a large table the
  build takes a while.
- `20260925_000032` deletes orphaned prompt copies from `user_events`, once.
  These are `research_prompt` and `chat_prompt` rows whose research no longer
  exists (left by researches deleted before `000028`'s release, and still
  readable in the admin Prompt and Event logs), plus prompt rows that name no
  research. Other events are kept. The deletion cannot be undone: downgrade
  does nothing, so take a backup first if you may need those rows (see
  "PostgreSQL Backup and Restore"). It walks the prompt rows in id order,
  1,000 per transaction, so it never holds one long lock, but its runtime grows
  with the size of `user_events`.

## Infra Notes

Implemented in this repository:

- structured JSON logs to stdout
- Prometheus scraping from the API
- Grafana and Prometheus services in Docker Compose
- explicit `langgraph` dependency in `requirements.txt`

Still not implemented yet:

- live production profiling on a real deployment

## Quick Check

The examples below target the local API from "Run Locally" (port 8000).
Against Docker Compose use `http://localhost:8001` instead (the API's loopback
port on the Docker host).

With auth on (the default) the calls need a token. Sign in once and keep it:

```bash
TOKEN=$(curl -s -X POST "http://localhost:8000/v1/auth/login" \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com","password":"..."}' |
  python -c 'import json, sys; print(json.load(sys.stdin)["access_token"])')
```

Then add `-H "Authorization: Bearer $TOKEN"` to each call. `/health` needs no
token (nor does `/metrics`, unless `METRICS_TOKEN` is set); `/health/detail`,
`/health/queues`, the worker heartbeats and every route under "Queue Admin"
need an admin's token.

Health:

```bash
curl http://localhost:8000/health          # cheap readiness probe
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/health/detail   # full payload (admin)
```

Delete an account with all owned data (researches, results, public share
links — cascades in the database; requires the current password and
`confirm: true`):

```bash
curl -X DELETE "http://localhost:8000/v1/auth/account" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"confirm": true, "current_password": "..."}'
```

Prometheus metrics:

```bash
curl http://localhost:8000/metrics
```

Create a research:

```bash
curl -X POST "http://localhost:8000/v1/research" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"prompt":"research renewable energy trends 2024","depth":"easy"}'
```

Check queue health (admin):

```bash
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/health/queues
```

Check worker heartbeat (admin):

```bash
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/health/workers/job-worker
```

## Queue Admin

These are admin routes: add `-H "Authorization: Bearer $TOKEN"` with an
admin's token (see "Quick Check").

List running search jobs:

```bash
curl "http://localhost:8000/v1/search-jobs?status=running"
```

List dead-letter search jobs:

```bash
curl "http://localhost:8000/v1/search-jobs?status=dead_letter"
```

List running finalize jobs:

```bash
curl "http://localhost:8000/v1/research/finalize-jobs?status=running"
```

List dead-letter finalize jobs:

```bash
curl "http://localhost:8000/v1/research/finalize-jobs?status=dead_letter"
```

Requeue a dead-letter search job:

```bash
curl -X POST "http://localhost:8000/v1/search-jobs/<job_id>/requeue"
```

Requeue a dead-letter finalize job:

```bash
curl -X POST "http://localhost:8000/v1/research/finalize-jobs/<job_id>/requeue"
```

Recover stale running jobs:

```bash
curl -X POST "http://localhost:8000/v1/search-jobs/recover-stale"
curl -X POST "http://localhost:8000/v1/research/finalize-jobs/recover-stale"
```

Clean up old completed and dead-letter jobs:

```bash
curl -X POST "http://localhost:8000/v1/search-jobs/cleanup"
curl -X POST "http://localhost:8000/v1/research/finalize-jobs/cleanup"
```

Run full queue maintenance manually:

```bash
curl -X POST "http://localhost:8000/health/queues/maintenance"
```

## PostgreSQL Backup and Restore

Create a compressed custom-format backup through the running `db` service:

```bash
./scripts/backup_postgres.sh
# Optional explicit destination:
./scripts/backup_postgres.sh /srv/mas-backups/postgres-$(date -u +%F).dump
```

The script writes through a temporary file, validates the archive with
`pg_restore --list`, refuses to overwrite an existing backup, and creates a
`.sha256` checksum when `sha256sum` is available. Keep backups outside the
repository and copy them to storage independent from the Docker host.

Schedule at least one daily backup and retain a policy appropriate to the
deployment; a practical baseline is 7 daily, 4 weekly, and 6 monthly copies.
For example, after a successful daily backup, files older than the chosen
window can be removed by the host's backup job. Alert on both backup failure and
absence of a recent archive.

Restore is destructive and deliberately requires the application, workers, and
PgBouncer to be stopped plus an explicit `--confirm` argument:

```bash
docker compose stop api worker worker_2 worker_3 pgbouncer
./scripts/restore_postgres.sh /srv/mas-backups/postgres-2026-09-09.dump --confirm
docker compose run --rm migrate
docker compose up -d pgbouncer api worker worker_2 worker_3
curl http://localhost:8001/health   # API_PORT; the API binds to loopback on the Docker host
```

Run a restore drill at least quarterly on an isolated host or maintenance
window: restore the newest archive, apply migrations, verify `/health`, open a
known completed report, and confirm its tasks and sources are present. Record
the archive name, checksum result, recovery duration, and verifier. Never use
`docker compose down -v` as part of a drill against the production project.

## Tests

Fast tests:

```bash
venv/bin/python -m pytest \
  tests/test_api.py \
  tests/test_app_logic.py \
  tests/test_search_jobs_store.py \
  tests/test_finalize_jobs_store.py \
  tests/test_requeue_jobs.py \
  tests/test_recovery_jobs.py \
  tests/test_maintenance_worker.py \
  tests/test_search_worker.py \
  tests/test_finalize_worker.py \
  tests/test_job_worker.py \
  tests/test_task_store_factory.py \
  tests/test_repository_mappers.py \
  tests/test_config.py \
  tests/test_agent.py -q
```

Postgres integration tests:

```bash
venv/bin/python -m pytest -m postgres -q
```

If Postgres is not running, these tests will be `skipped`.

## Smoke Check

Full runtime smoke check against a live API and Postgres:

```bash
venv/bin/python scripts/smoke_postgres_runtime.py
```

The script will:

- run migrations
- start a local `uvicorn` process
- verify health endpoints
- verify search/finalize jobs
- verify worker heartbeat
- verify final state persisted in Postgres

## License

Released under the [MIT License](./LICENSE).
