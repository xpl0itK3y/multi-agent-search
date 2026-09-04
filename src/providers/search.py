import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from configparser import ConfigParser
from dataclasses import asdict, dataclass
from typing import List, Dict, Optional
from urllib.parse import urlparse

import trafilatura
from ddgs import DDGS

from src.config import settings
from src.core import rust_accel

logger = logging.getLogger(__name__)

# Suppress annoying "Impersonate does not exist" warnings from curl_cffi used by ddgs
logging.getLogger("curl_cffi").setLevel(logging.ERROR)

class SuppressStderrFD:
    """Context manager to suppress stderr at the OS level (for Rust binaries like primp)."""
    def __enter__(self):
        self.devnull = os.open(os.devnull, os.O_WRONLY)
        self.old_stderr = os.dup(sys.stderr.fileno())
        os.dup2(self.devnull, sys.stderr.fileno())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.dup2(self.old_stderr, sys.stderr.fileno())
        os.close(self.old_stderr)
        os.close(self.devnull)

class SearchBackend:
    """A pluggable web-search source.

    Returns result dicts with keys: url, title, snippet, and optionally `content`
    (full page text when the backend can supply it directly — e.g. Tavily's
    raw_content — letting the agent skip the separate fetch/extract step).
    """

    name = "base"

    def search(self, query: str, max_results: int) -> List[Dict[str, str]]:
        raise NotImplementedError


class DuckDuckGoBackend(SearchBackend):
    """Free DuckDuckGo backend — no content, results feed the fetch/extract step."""

    name = "duckduckgo"

    def search(self, query: str, max_results: int) -> List[Dict[str, str]]:
        """Prefer DuckDuckGo's API backend, then race the HTML fallbacks on failure.

        Note: SuppressStderrFD is intentionally omitted from the per-backend worker because
        it manipulates OS-level file descriptors via os.dup2 and is not thread-safe.
        The curl_cffi Python logger is already silenced at module level.
        """

        def _try_backend(backend: str) -> List[Dict[str, str]]:
            with DDGS(timeout=20) as ddgs:
                ddgs_gen = ddgs.text(query, max_results=max_results, backend=backend)
                if ddgs_gen:
                    return [
                        {
                            "title": r.get("title", ""),
                            "url": r.get("href", ""),
                            "snippet": r.get("body", ""),
                        }
                        for r in ddgs_gen
                    ]
            return []

        try:
            results = _try_backend("api")
            if results:
                logger.info(
                    "ddg_search_success backend=%s results=%d query_prefix=%r",
                    "api",
                    len(results),
                    query[:50],
                )
                return results
        except Exception as exc:
            logger.warning("ddg_backend_failed backend=%s error=%s", "api", exc)

        fallback_backends = ["html", "lite"]
        executor = ThreadPoolExecutor(max_workers=len(fallback_backends))
        futures = {executor.submit(_try_backend, backend): backend for backend in fallback_backends}
        try:
            for future in as_completed(futures):
                backend = futures[future]
                try:
                    results = future.result(timeout=30.0)  # backend HTTP timeout is ~20s
                    if results:
                        logger.info(
                            "ddg_search_success backend=%s results=%d query_prefix=%r",
                            backend,
                            len(results),
                            query[:50],
                        )
                        return results
                except Exception as exc:
                    logger.warning("ddg_backend_failed backend=%s error=%s", backend, exc)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

        logger.error("ddg_all_backends_failed query=%r", query[:80])
        return []


class TavilyBackend(SearchBackend):
    """Tavily search API — relevant results plus raw page content in one call."""

    name = "tavily"
    ENDPOINT = "https://api.tavily.com/search"

    def __init__(
        self,
        api_key: str,
        *,
        search_depth: str = "advanced",
        timeout: float = 20.0,
        include_raw_content: bool = True,
    ):
        self.api_key = api_key
        self.search_depth = search_depth
        self.timeout = timeout
        self.include_raw_content = include_raw_content

    def search(self, query: str, max_results: int) -> List[Dict[str, str]]:
        import httpx

        payload = {
            "api_key": self.api_key,
            "query": query,
            "max_results": max_results,
            "search_depth": self.search_depth,
            "include_raw_content": self.include_raw_content,
        }
        response = httpx.post(self.ENDPOINT, json=payload, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()

        results: List[Dict[str, str]] = []
        for item in data.get("results", []):
            url = item.get("url") or ""
            if not url:
                continue
            raw = item.get("raw_content") if self.include_raw_content else None
            results.append(
                {
                    "title": item.get("title", ""),
                    "url": url,
                    "snippet": item.get("content", ""),
                    "content": raw or None,  # full page text → lets the agent skip fetching
                }
            )
        logger.info("tavily_search_success results=%d query_prefix=%r", len(results), query[:50])
        return results


def build_search_backends() -> tuple[SearchBackend, Optional[SearchBackend]]:
    """Resolve (primary, fallback) backends from settings.

    "auto" uses Tavily when ``TAVILY_API_KEY`` is set (with DuckDuckGo as fallback),
    otherwise DuckDuckGo only. "tavily"/"duckduckgo" force a single backend.
    """
    choice = (settings.search_backend or "auto").lower()
    ddg = DuckDuckGoBackend()
    has_key = bool(settings.tavily_api_key)

    def _tavily() -> "TavilyBackend":
        return TavilyBackend(
            settings.tavily_api_key,
            search_depth=settings.tavily_search_depth,
            timeout=settings.tavily_timeout_seconds,
            include_raw_content=settings.tavily_include_raw_content,
        )

    if choice == "tavily":
        if not has_key:
            logger.warning("search_backend=tavily but no TAVILY_API_KEY set; using DuckDuckGo")
            return ddg, None
        return _tavily(), None
    if choice == "duckduckgo" or not has_key:
        return ddg, None
    # auto (or anything else) with a key → Tavily primary, DuckDuckGo fallback
    return _tavily(), ddg


class SearchProvider:
    def __init__(self, max_results: int = 5):
        self.max_results = max_results
        self.primary, self.fallback = build_search_backends()

    def search(self, query: str) -> List[Dict[str, str]]:
        """Search via the primary backend, falling back to DuckDuckGo on error/empty."""
        try:
            results = self.primary.search(query, self.max_results)
            if results:
                return results
            logger.warning("search_primary_empty backend=%s query=%r", self.primary.name, query[:60])
        except Exception as exc:
            logger.warning("search_primary_failed backend=%s error=%s", self.primary.name, exc)

        if self.fallback is not None:
            try:
                return self.fallback.search(query, self.max_results)
            except Exception as exc:
                logger.error("search_fallback_failed backend=%s error=%s", self.fallback.name, exc)
        return []


@dataclass
class ExtractionMetricsSnapshot:
    attempts: int = 0
    success_count: int = 0
    empty_count: int = 0
    failure_count: int = 0
    downloaded_bytes: int = 0
    content_chars: int = 0
    total_download_ms: float = 0.0
    total_extract_ms: float = 0.0
    total_post_process_ms: float = 0.0
    total_total_ms: float = 0.0


class ExtractionMetricsRegistry:
    def __init__(self):
        self._lock = threading.Lock()
        self._metrics = ExtractionMetricsSnapshot()

    def record(
        self,
        *,
        outcome: str,
        download_ms: float,
        extract_ms: float,
        post_process_ms: float,
        total_ms: float,
        downloaded_bytes: int,
        content_chars: int,
    ) -> None:
        with self._lock:
            self._metrics.attempts += 1
            if outcome == "success":
                self._metrics.success_count += 1
            elif outcome == "empty":
                self._metrics.empty_count += 1
            elif outcome == "failed":
                self._metrics.failure_count += 1
            self._metrics.downloaded_bytes += int(downloaded_bytes)
            self._metrics.content_chars += int(content_chars)
            self._metrics.total_download_ms += float(download_ms)
            self._metrics.total_extract_ms += float(extract_ms)
            self._metrics.total_post_process_ms += float(post_process_ms)
            self._metrics.total_total_ms += float(total_ms)

    def snapshot(self) -> dict:
        with self._lock:
            return asdict(self._metrics)

    def reset(self) -> None:
        with self._lock:
            self._metrics = ExtractionMetricsSnapshot()


_EXTRACTION_METRICS = ExtractionMetricsRegistry()


def get_extraction_metrics_snapshot() -> dict:
    return _EXTRACTION_METRICS.snapshot()


def reset_extraction_metrics() -> None:
    _EXTRACTION_METRICS.reset()


@dataclass
class ExtractionDomainSnapshot:
    consecutive_failures: int = 0
    timeout_count: int = 0
    cooldown_until: float = 0.0


class ExtractionDomainRegistry:
    BLOCKED_DOMAIN_SUFFIXES = (
        "youtube.com",
        "youtu.be",
        "passport.yandex.ru",
        "login.aliexpress.com",
    )

    def __init__(self):
        self._lock = threading.Lock()
        self._domains: dict[str, ExtractionDomainSnapshot] = {}

    def should_skip(self, url: str) -> str | None:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().removeprefix("www.")
        normalized_url = (url or "").lower()

        if not domain:
            return "missing-domain"
        if any(domain == suffix or domain.endswith(f".{suffix}") for suffix in self.BLOCKED_DOMAIN_SUFFIXES):
            return "blocked-domain"
        if any(
            token in normalized_url
            for token in (
                "youtube.com/watch",
                "youtu.be/",
                "/shorts/",
                "passport.yandex.ru/auth",
                "login.aliexpress.com",
            )
        ):
            return "blocked-url"

        with self._lock:
            snapshot = self._domains.get(domain)
            if snapshot and snapshot.cooldown_until > time.monotonic():
                return "domain-cooldown"
        return None

    def record(self, url: str, *, outcome: str, timed_out: bool) -> None:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().removeprefix("www.")
        if not domain:
            return

        with self._lock:
            snapshot = self._domains.setdefault(domain, ExtractionDomainSnapshot())
            if outcome == "success":
                snapshot.consecutive_failures = 0
                snapshot.timeout_count = 0
                snapshot.cooldown_until = 0.0
                return

            snapshot.consecutive_failures += 1
            if timed_out:
                snapshot.timeout_count += 1
            if (
                snapshot.timeout_count >= 1
                or snapshot.consecutive_failures >= settings.search_domain_fail_threshold
            ):
                snapshot.cooldown_until = time.monotonic() + settings.search_domain_cooldown_seconds


_EXTRACTION_DOMAINS = ExtractionDomainRegistry()

class ContentExtractor:
    @staticmethod
    def should_skip_url(url: str) -> str | None:
        return _EXTRACTION_DOMAINS.should_skip(url)

    @staticmethod
    def _build_trafilatura_config() -> ConfigParser:
        config = ConfigParser()
        config.read_dict({"DEFAULT": dict(trafilatura.settings.DEFAULT_CONFIG.defaults())})
        config["DEFAULT"]["DOWNLOAD_TIMEOUT"] = str(settings.search_extraction_timeout_seconds)
        config["DEFAULT"]["EXTRACTION_TIMEOUT"] = str(settings.search_extraction_timeout_seconds)
        config["DEFAULT"]["MAX_REDIRECTS"] = str(settings.search_extraction_max_redirects)
        return config

    @staticmethod
    def extract_content(url: str) -> Optional[str]:
        """
        Download and extract clean text from a URL.
        """
        skip_reason = ContentExtractor.should_skip_url(url)
        if skip_reason:
            logger.info("content_extraction_skipped url=%s reason=%s", url, skip_reason)
            return None

        from src.net_safety import is_safe_public_url, safe_fetch_html

        ok, reason = is_safe_public_url(url)
        if not ok:
            logger.warning("content_extraction_blocked_ssrf url=%s reason=%s", url, reason)
            return None

        start = time.perf_counter()
        download_ms = 0.0
        extract_ms = 0.0
        post_process_ms = 0.0
        downloaded_size = 0
        content_chars = 0
        timed_out = False
        config = ContentExtractor._build_trafilatura_config()
        try:
            download_start = time.perf_counter()
            # SSRF-guarded fetch: validates every redirect hop (replaces trafilatura.fetch_url,
            # which would follow redirects without re-checking the target). See net_safety.
            downloaded = safe_fetch_html(
                url,
                timeout=settings.search_extraction_timeout_seconds,
                max_redirects=settings.search_extraction_max_redirects,
            )
            download_ms = (time.perf_counter() - download_start) * 1000
            downloaded_size = len(downloaded or "")
            if downloaded:
                extract_start = time.perf_counter()
                result = trafilatura.extract(
                    downloaded,
                    include_comments=False,
                    include_tables=True,
                    config=config,
                )
                extract_ms = (time.perf_counter() - extract_start) * 1000
                post_process_start = time.perf_counter()
                cleaned = rust_accel.clean_extracted_content(result)
                post_process_ms = (time.perf_counter() - post_process_start) * 1000
                content_chars = len(cleaned or "")
                total_ms = (time.perf_counter() - start) * 1000
                timed_out = bool(
                    download_ms >= settings.search_extraction_timeout_seconds * 900
                    and not cleaned
                )
                logger.info(
                    "content_extraction_completed url=%s download_ms=%.2f extract_ms=%.2f post_process_ms=%.2f total_ms=%.2f downloaded_bytes=%s content_chars=%s success=%s",
                    url,
                    download_ms,
                    extract_ms,
                    post_process_ms,
                    total_ms,
                    downloaded_size,
                    content_chars,
                    bool(cleaned),
                )
                _EXTRACTION_METRICS.record(
                    outcome="success" if cleaned else "empty",
                    download_ms=download_ms,
                    extract_ms=extract_ms,
                    post_process_ms=post_process_ms,
                    total_ms=total_ms,
                    downloaded_bytes=downloaded_size,
                    content_chars=content_chars,
                )
                _EXTRACTION_DOMAINS.record(
                    url,
                    outcome="success" if cleaned else "empty",
                    timed_out=timed_out,
                )
                return cleaned or None
        except Exception as e:
            total_ms = (time.perf_counter() - start) * 1000
            timed_out = bool(
                download_ms >= settings.search_extraction_timeout_seconds * 900
                or total_ms >= settings.search_extraction_timeout_seconds * 900
            )
            _EXTRACTION_METRICS.record(
                outcome="failed",
                download_ms=download_ms,
                extract_ms=extract_ms,
                post_process_ms=post_process_ms,
                total_ms=total_ms,
                downloaded_bytes=downloaded_size,
                content_chars=content_chars,
            )
            _EXTRACTION_DOMAINS.record(url, outcome="failed", timed_out=timed_out)
            logger.error(
                "content_extraction_failed url=%s download_ms=%.2f extract_ms=%.2f post_process_ms=%.2f total_ms=%.2f downloaded_bytes=%s content_chars=%s error=%s",
                url,
                download_ms,
                extract_ms,
                post_process_ms,
                total_ms,
                downloaded_size,
                content_chars,
                e,
            )
            return None
        total_ms = (time.perf_counter() - start) * 1000
        timed_out = bool(
            download_ms >= settings.search_extraction_timeout_seconds * 900
            or total_ms >= settings.search_extraction_timeout_seconds * 900
        )
        _EXTRACTION_METRICS.record(
            outcome="empty",
            download_ms=download_ms,
            extract_ms=extract_ms,
            post_process_ms=post_process_ms,
            total_ms=total_ms,
            downloaded_bytes=downloaded_size,
            content_chars=content_chars,
        )
        _EXTRACTION_DOMAINS.record(url, outcome="empty", timed_out=timed_out)
        logger.info(
            "content_extraction_empty url=%s download_ms=%.2f extract_ms=%.2f post_process_ms=%.2f total_ms=%.2f downloaded_bytes=%s content_chars=%s",
            url,
            download_ms,
            extract_ms,
            post_process_ms,
            total_ms,
            downloaded_size,
            content_chars,
        )
        return None
