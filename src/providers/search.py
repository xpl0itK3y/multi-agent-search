import logging
import os
import sys
import threading
import time
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

class SearchProvider:
    def __init__(self, max_results: int = 5):
        self.max_results = max_results

    def search(self, query: str) -> List[Dict[str, str]]:
        """
        Perform a search and return a list of result dictionaries with 'title', 'href', and 'body'.
        """
        results = []
        backends = ["api", "html", "lite"]
        
        for backend in backends:
            try:
                with SuppressStderrFD():
                    with DDGS(timeout=20) as ddgs:
                        ddgs_gen = ddgs.text(query, max_results=self.max_results, backend=backend)
                        if ddgs_gen:
                            for r in ddgs_gen:
                                results.append({
                                    "title": r.get("title", ""),
                                    "url": r.get("href", ""),
                                    "snippet": r.get("body", "")
                                })
                            return results # Return early if successful
            except Exception as e:
                logger.warning(f"DuckDuckGo search failed with backend {backend}: {e}")
                
        logger.error(f"All DuckDuckGo backends failed for query: {query}")
        return results


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
    def __init__(self):
        self._lock = threading.Lock()
        self._domains: dict[str, ExtractionDomainSnapshot] = {}

    def should_skip(self, url: str) -> str | None:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().removeprefix("www.")
        normalized_url = (url or "").lower()

        if not domain:
            return "missing-domain"
        if domain in {"youtube.com", "m.youtube.com", "youtu.be", "passport.yandex.ru"}:
            return "blocked-domain"
        if any(
            token in normalized_url
            for token in ("youtube.com/watch", "youtu.be/", "/shorts/", "passport.yandex.ru/auth")
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
            downloaded = trafilatura.fetch_url(url, config=config)
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
