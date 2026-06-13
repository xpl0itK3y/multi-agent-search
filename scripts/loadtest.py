"""Lightweight load test for the API request/connection layer.

Hammers two paths at rising concurrency and reports throughput + latency percentiles:
  * /v1/models   — no DB, no auth (raw uvicorn / request handling)
  * /v1/auth/me  — authed DB read (exercises pgbouncer + the connection pool)

It does NOT start researches (those make real LLM calls). Safe to run against a live stack.

Usage: python scripts/loadtest.py [--base http://localhost:8000] [--duration 4]
"""
from __future__ import annotations

import argparse
import asyncio
import time

import httpx

EMAIL = "loadtest@local.test"
PASSWORD = "loadtest-pw-1234"


async def get_token(base: str) -> str | None:
    async with httpx.AsyncClient(base_url=base, timeout=15) as c:
        body = {"email": EMAIL, "password": PASSWORD}
        r = await c.post("/v1/auth/register", json=body)
        if r.status_code < 300:
            return r.json().get("access_token")
        r = await c.post("/v1/auth/login", json=body)
        if r.status_code < 300:
            return r.json().get("access_token")
        print(f"  (auth failed: register/login -> {r.status_code}; skipping the DB path)")
        return None


def _pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    k = max(0, min(len(values) - 1, int(round(p / 100 * (len(values) - 1)))))
    return sorted(values)[k]


async def _worker(client, path, headers, stop_at, out):
    while time.monotonic() < stop_at:
        t0 = time.monotonic()
        try:
            r = await client.get(path, headers=headers, timeout=30)
            out.append(((time.monotonic() - t0) * 1000, r.status_code))
        except Exception:
            out.append(((time.monotonic() - t0) * 1000, 0))


async def run_level(base, path, headers, concurrency, duration):
    out: list[tuple[float, int]] = []
    limits = httpx.Limits(max_connections=concurrency + 20, max_keepalive_connections=concurrency + 20)
    async with httpx.AsyncClient(base_url=base, limits=limits) as client:
        stop_at = time.monotonic() + duration
        await asyncio.gather(*[_worker(client, path, headers, stop_at, out) for _ in range(concurrency)])
    total = len(out)
    lat = [l for l, _ in out]
    errs = sum(1 for _, s in out if s == 0 or s >= 500)
    rps = total / duration
    print(
        f"  c={concurrency:<4} {rps:8.0f} req/s   "
        f"p50={_pct(lat,50):6.0f}ms  p95={_pct(lat,95):6.0f}ms  p99={_pct(lat,99):6.0f}ms   "
        f"err={errs/total*100 if total else 0:.1f}%   n={total}"
    )


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8000")
    ap.add_argument("--duration", type=float, default=4.0)
    ap.add_argument("--levels", default="25,50,100,200")
    args = ap.parse_args()
    levels = [int(x) for x in args.levels.split(",")]

    token = await get_token(args.base)
    print(f"\n=== /v1/models (no DB, no auth) — raw request handling ===")
    for c in levels:
        await run_level(args.base, "/v1/models", {}, c, args.duration)

    if token:
        print(f"\n=== /v1/auth/me (authed DB read — pgbouncer + pool) ===")
        hdr = {"Authorization": f"Bearer {token}"}
        for c in levels:
            await run_level(args.base, "/v1/auth/me", hdr, c, args.duration)


if __name__ == "__main__":
    asyncio.run(main())
