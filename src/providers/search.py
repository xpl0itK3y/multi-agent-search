import logging
import os
import sys
import threading
import time
from dataclasses import asdict, dataclass
from typing import List, Dict, Optional

import trafilatura
from ddgs import DDGS

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

class ContentExtractor:
    @staticmethod
    def extract_content(url: str) -> Optional[str]:
        """
        Download and extract clean text from a URL.
        """
        start = time.perf_counter()
        download_ms = 0.0
        extract_ms = 0.0
        post_process_ms = 0.0
        downloaded_size = 0
        content_chars = 0
        try:
            download_start = time.perf_counter()
            downloaded = trafilatura.fetch_url(url)
            download_ms = (time.perf_counter() - download_start) * 1000
            downloaded_size = len(downloaded or "")
            if downloaded:
                extract_start = time.perf_counter()
                result = trafilatura.extract(downloaded, include_comments=False, include_tables=True)
                extract_ms = (time.perf_counter() - extract_start) * 1000
                post_process_start = time.perf_counter()
                cleaned = rust_accel.clean_extracted_content(result)
                post_process_ms = (time.perf_counter() - post_process_start) * 1000
                content_chars = len(cleaned or "")
                total_ms = (time.perf_counter() - start) * 1000
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
                return cleaned or None
        except Exception as e:
            total_ms = (time.perf_counter() - start) * 1000
            _EXTRACTION_METRICS.record(
                outcome="failed",
                download_ms=download_ms,
                extract_ms=extract_ms,
                post_process_ms=post_process_ms,
                total_ms=total_ms,
                downloaded_bytes=downloaded_size,
                content_chars=content_chars,
            )
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
        _EXTRACTION_METRICS.record(
            outcome="empty",
            download_ms=download_ms,
            extract_ms=extract_ms,
            post_process_ms=post_process_ms,
            total_ms=total_ms,
            downloaded_bytes=downloaded_size,
            content_chars=content_chars,
        )
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
