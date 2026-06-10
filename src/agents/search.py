import hashlib
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

from src.config import settings
from src.providers.search import SearchProvider, ContentExtractor
from src.api.schemas import SearchTaskMetrics, TaskStatus, TaskUpdate
from src.core import rust_accel
from src.repositories.protocols import TaskStore
from src.repositories.mappers import enrich_search_result_dict
from src.source_quality_policy import TOPIC_POLICIES, combined_topics

logger = logging.getLogger(__name__)


class SearchAgent:
    TRUSTED_DOMAIN_EXACT_MATCHES = {
        "developer.mozilla.org",
        "docs.python.org",
        "openai.com",
        "platform.openai.com",
        "wikipedia.org",
    }
    TRUSTED_DOMAIN_SUFFIXES = (
        ".gov",
        ".edu",
        ".readthedocs.io",
    )
    LOW_VALUE_DOMAIN_EXACT_MATCHES = {
        "linkedin.com",
        "www.linkedin.com",
        "pinterest.com",
        "www.pinterest.com",
        "facebook.com",
        "www.facebook.com",
        "x.com",
        "twitter.com",
        "tiktok.com",
        "www.tiktok.com",
        "vk.com",
        "www.vk.com",
        "medium.com",
        "www.medium.com",
        "behance.net",
        "www.behance.net",
        "youtube.com",
        "www.youtube.com",
        "m.youtube.com",
        "youtu.be",
        "passport.yandex.ru",
    }
    LOW_VALUE_DOMAIN_SUBSTRINGS = (
        "bookmark",
        "trendhunter",
        "grokipedia",
        "outmaxshop",
    )
    LOW_SIGNAL_TITLE_TOKENS = (
        "discover",
        "gallery",
        "pinterest",
        "tiktok",
        "forum",
        "youth development forum",
        "travel trends",
        "fashion trends",
        "aesthetic clinics",
    )
    LOW_SIGNAL_URL_TOKENS = (
        "bing.com/aclick",
        "news.google.com",
        "amazon.com/s",
        "amazon.",
        "aliexpress.",
        "ubuy.",
        "login.aliexpress.com",
        "yandex.",
        "/shopping/",
        "/discover/",
        "/video/",
        "/gallery/",
        "/pin/",
        "/dp/",
        "/wholesale-",
    )
    LOW_SIGNAL_RESULT_TOKENS = (
        "best",
        "top 10",
        "top ten",
        "buying guide",
        "rankings guide",
        "most anticipated",
        "predictions",
        "upcoming",
        "expected",
    )
    STRONG_RESULT_TOKENS = (
        "review",
        "reviews",
        "benchmark",
        "benchmarks",
        "specs",
        "comparison",
        "tested",
        "official",
        "press release",
    )

    def __init__(
        self,
        task_store: TaskStore,
        max_sources: int = 5,
        search_results_per_query: int = 8,
        max_candidate_urls: int = 12,
        extraction_concurrency: int = 4,
        extraction_timeout_seconds: int = 12,
    ):
        self.task_store = task_store
        self.search_provider = SearchProvider(max_results=search_results_per_query)
        self.extractor = ContentExtractor()
        self.max_sources = max_sources
        self.search_results_per_query = search_results_per_query
        self.max_candidate_urls = max_candidate_urls
        self.extraction_concurrency = max(1, extraction_concurrency)
        self.extraction_timeout_seconds = max(1, extraction_timeout_seconds)

    def _normalize_text(self, value: str | None) -> str:
        return rust_accel.normalize_text(value)

    def _content_fingerprint(self, title: str, content: str) -> str:
        return rust_accel.content_fingerprint(title, content, 200)

    def _detect_topics(self, task) -> set[str]:
        haystack_parts = [
            self._normalize_text(getattr(task, "description", None)),
            " ".join(self._normalize_text(query) for query in getattr(task, "queries", []) or []),
        ]
        return combined_topics(*haystack_parts)

    def _trusted_domain_score(self, domain: str) -> int:
        if not domain:
            return 0

        normalized_domain = domain.removeprefix("www.")

        if normalized_domain in self.TRUSTED_DOMAIN_EXACT_MATCHES:
            return 200
        if any(normalized_domain.endswith(suffix) for suffix in self.TRUSTED_DOMAIN_SUFFIXES):
            return 150
        if normalized_domain.endswith(".github.io"):
            return 40
        return 0

    def _matches_low_value_domain(self, domain: str | None) -> bool:
        if not domain:
            return False
        normalized_domain = domain.removeprefix("www.")
        exact_domains = {
            item.removeprefix("www.") for item in self.LOW_VALUE_DOMAIN_EXACT_MATCHES
        }
        return any(
            normalized_domain == item or normalized_domain.endswith(f".{item}")
            for item in exact_domains
        )

    def _low_value_domain_penalty(self, domain: str) -> int:
        if not domain:
            return 0
        normalized_domain = domain.removeprefix("www.")
        if self._matches_low_value_domain(normalized_domain):
            return 120
        if any(token in normalized_domain for token in self.LOW_VALUE_DOMAIN_SUBSTRINGS):
            return 90
        return 0

    def _should_skip_search_result(self, url: str, title: str | None, topics: set[str] | None = None) -> bool:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        normalized_domain = domain.removeprefix("www.")
        normalized_title = self._normalize_text(title).lower()
        normalized_url = (url or "").lower()
        topics = topics or set()

        if not normalized_domain:
            return True
        if self._matches_low_value_domain(normalized_domain):
            return True
        if any(token in normalized_domain for token in self.LOW_VALUE_DOMAIN_SUBSTRINGS):
            return True
        if any(token in normalized_url for token in self.LOW_SIGNAL_URL_TOKENS) or "/wall-" in normalized_url:
            return True
        if normalized_title and any(token in normalized_title for token in self.LOW_SIGNAL_TITLE_TOKENS):
            return True
        for topic_name in topics:
            policy = TOPIC_POLICIES[topic_name]
            if normalized_domain in policy.weak_domains:
                return True
            if any(token in normalized_domain for token in policy.weak_domain_substrings):
                return True
        return False

    def _authority_hint_score(self, url: str, title: str, content: str, source_quality: str | None) -> int:
        normalized_title = self._normalize_text(title).lower()
        normalized_content = self._normalize_text(content).lower()
        normalized_url = (url or "").lower()
        score = 0

        if source_quality == "high":
            score += 180
        elif source_quality == "medium":
            score += 60

        if any(token in normalized_url for token in ("/docs", "/documentation", "/reference", "/api")):
            score += 120
        if any(token in normalized_title for token in ("documentation", "docs", "reference", "api", "guide", "manual")):
            score += 80
        if any(token in normalized_content[:600] for token in ("official documentation", "api reference", "reference guide")):
            score += 60
        return score

    def _score_result(self, url: str, title: str, content: str, source_quality: str | None = None) -> int:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        normalized_title = self._normalize_text(title)
        normalized_content = self._normalize_text(content)

        score = len(normalized_content)
        if normalized_title:
            score += 100
        if parsed.scheme == "https":
            score += 25
        if domain and domain.startswith("www."):
            score += 5
        score += self._trusted_domain_score(domain)
        score += self._authority_hint_score(url, title, content, source_quality)
        score -= self._low_value_domain_penalty(domain)
        if "failed to extract content" in normalized_content.lower():
            score -= 5000
        return score

    def _select_best_results(self, results: list[dict]) -> list[dict]:
        scored_results = []
        for result in results:
            title = self._normalize_text(result.get("title"))
            content = self._normalize_text(result.get("content"))
            url = result.get("url", "")
            source_quality = result.get("source_quality")
            scored_results.append(
                {
                    **result,
                    "url": url,
                    "title": title or None,
                    "content": content,
                    "score": self._score_result(url, title, content, source_quality),
                }
            )
        return rust_accel.select_best_results(scored_results, self.max_sources)

    def _early_stop_success_target(self, candidate_count: int) -> int:
        if candidate_count <= self.max_sources:
            return candidate_count
        return min(candidate_count, max(self.max_sources * 2, self.extraction_concurrency))

    def _has_enough_strong_results(self, successful_results: list[dict], candidate_count: int) -> bool:
        target = self._early_stop_success_target(candidate_count)
        if len(successful_results) < target:
            return False
        preview = self._select_best_results(successful_results)
        if len(preview) < self.max_sources:
            return False
        strong_preview_count = sum(
            1 for item in preview if item.get("source_quality") in {"high", "medium"}
        )
        return strong_preview_count >= self.max_sources

    def _search_cache_key(self, query: str) -> str:
        backend = getattr(getattr(self.search_provider, "primary", None), "name", "?")
        raw = f"{backend}:{self.search_provider.max_results}:{query.strip().lower()}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _search_with_cache(self, query: str) -> list[dict]:
        """Search via the provider, backed by the shared TTL cache when available."""
        if not settings.search_cache_enabled or not hasattr(self.task_store, "get_cached_search"):
            return self.search_provider.search(query)
        key = self._search_cache_key(query)
        cached = self.task_store.get_cached_search(key, settings.search_cache_ttl_seconds)
        if cached is not None:
            logger.info("search_cache_hit query_prefix=%r results=%d", query[:50], len(cached))
            return cached
        results = self.search_provider.search(query)
        if results:
            try:
                self.task_store.put_cached_search(key, results)
            except Exception as exc:  # cache writes must never break a search
                logger.warning("search_cache_put_failed error=%s", exc)
        return results

    def run_task(self, task_id: str):
        """
        Execute a search task: search for queries, extract content, and update the configured task store.
        """
        task = self.task_store.get_task(task_id)
        if not task:
            logger.error(f"Task {task_id} not found in task store")
            return

        self.task_store.update_task(
            task_id,
            TaskUpdate(status=TaskStatus.RUNNING, log="Agent started search process"),
        )

        all_results = []
        raw_candidates: list[dict] = []
        # url -> full content when the search backend already supplied it (Tavily
        # raw_content), so the extraction step can skip fetching that URL.
        prefetched_content: dict[str, str] = {}
        topics = self._detect_topics(task)

        try:
            for query in task.queries:
                self.task_store.update_task(task_id, TaskUpdate(log=f"Searching for: {query}"))
                search_results = self._search_with_cache(query)

                for res in search_results:
                    url = res.get("url")
                    if not url:
                        continue
                    content = res.get("content")
                    if content and url not in prefetched_content:
                        prefetched_content[url] = content
                    raw_candidates.append(
                        {
                            "url": url,
                            "title": res.get("title"),
                            "snippet": res.get("snippet"),
                        }
                    )

            scored_candidates = rust_accel.score_search_candidates(
                raw_candidates,
                topics=topics,
                limit=self.max_candidate_urls,
            )
            candidate_results = [
                candidate
                for candidate in scored_candidates
                if not self._should_skip_search_result(candidate["url"], candidate.get("title"), topics=topics)
            ]
            selected_urls = {candidate["url"] for candidate in candidate_results}
            logged_skipped_urls: set[str] = set()
            for candidate in raw_candidates:
                url = candidate["url"]
                if url in selected_urls or url in logged_skipped_urls:
                    continue
                if self._should_skip_search_result(url, candidate.get("title"), topics=topics):
                    logged_skipped_urls.add(url)
                    self.task_store.update_task(
                        task_id,
                        TaskUpdate(log=f"Skipped low-value result: {url}"),
                    )

            self.task_store.update_task(
                task_id,
                TaskUpdate(
                    log=(
                        f"Queued {len(candidate_results)} candidate URLs for extraction "
                        f"with concurrency {self.extraction_concurrency} "
                        f"and timeout {self.extraction_timeout_seconds}s"
                    )
                ),
            )

            successful_results: list[dict] = []
            remaining_candidates = list(candidate_results)
            with ThreadPoolExecutor(max_workers=self.extraction_concurrency) as executor:
                future_to_candidate = {}

                def submit_candidates() -> None:
                    while remaining_candidates and len(future_to_candidate) < self.extraction_concurrency:
                        if self._has_enough_strong_results(successful_results, len(candidate_results)):
                            break
                        candidate = remaining_candidates.pop(0)
                        url = candidate["url"]
                        prefetched = prefetched_content.get(url)
                        if prefetched:
                            # The search backend already returned this page's content
                            # (Tavily raw_content) — use it directly, no fetch needed.
                            enriched_result = enrich_search_result_dict(
                                {
                                    "url": url,
                                    "title": candidate.get("title"),
                                    "content": prefetched[:10000],
                                    "snippet": candidate.get("snippet"),
                                }
                            )
                            all_results.append(enriched_result)
                            successful_results.append(enriched_result)
                            self.task_store.update_task(
                                task_id,
                                TaskUpdate(log=f"Used search-provided content for: {url}"),
                            )
                            continue
                        skip_reason = self.extractor.should_skip_url(url)
                        if skip_reason:
                            self.task_store.update_task(
                                task_id,
                                TaskUpdate(log=f"Skipped extraction candidate: {url} ({skip_reason})"),
                            )
                            continue
                        self.task_store.update_task(
                            task_id,
                            TaskUpdate(log=f"Extracting content from: {url}"),
                        )
                        future_to_candidate[executor.submit(self.extractor.extract_content, url)] = candidate

                submit_candidates()

                while future_to_candidate:
                    future = next(as_completed(future_to_candidate))
                    candidate = future_to_candidate.pop(future)
                    url = candidate["url"]
                    title = candidate.get("title")
                    snippet = candidate.get("snippet")
                    try:
                        content = future.result()
                    except Exception as exc:
                        logger.error("Error extracting content from %s: %s", url, exc)
                        content = None

                    if content:
                        enriched_result = enrich_search_result_dict(
                            {
                                "url": url,
                                "title": title,
                                "content": content[:10000],  # Limit content size to avoid huge payloads.
                                "snippet": snippet,
                            }
                        )
                        all_results.append(enriched_result)
                        successful_results.append(enriched_result)
                    else:
                        all_results.append(
                            enrich_search_result_dict(
                                {
                                    "url": url,
                                    "title": title,
                                    "content": "Failed to extract content",
                                    "snippet": snippet,
                                    "extraction_status": "failed",
                                }
                            )
                        )

                    if remaining_candidates and self._has_enough_strong_results(successful_results, len(candidate_results)):
                        skipped_tail = len(remaining_candidates)
                        remaining_candidates.clear()
                        self.task_store.update_task(
                            task_id,
                            TaskUpdate(
                                log=(
                                    f"Stopped extraction early after {len(successful_results)} successful sources; "
                                    f"skipped {skipped_tail} weaker queued candidates."
                                )
                            )
                        )
                    submit_candidates()

            selected_results = self._select_best_results(all_results)
            success_count = sum(1 for item in all_results if item.get("extraction_status") == "success")
            failure_count = sum(1 for item in all_results if item.get("extraction_status") != "success")
            avg_content_chars = (
                round(
                    sum(len(item.get("content") or "") for item in selected_results) / len(selected_results),
                    1,
                )
                if selected_results
                else 0.0
            )

            self.task_store.update_task(
                task_id,
                TaskUpdate(
                    status=TaskStatus.COMPLETED,
                    result=selected_results,
                    search_metrics=SearchTaskMetrics(
                        candidate_count=len(candidate_results),
                        extraction_attempts=len(all_results),
                        extraction_success_count=success_count,
                        extraction_failure_count=failure_count,
                        selected_source_count=len(selected_results),
                        avg_content_chars=avg_content_chars,
                    ),
                    log=f"Search completed. Selected {len(selected_results)} sources from {len(all_results)} collected results.",
                ),
            )

        except Exception as exc:
            logger.error(f"Error executing task {task_id}: {exc}")
            self.task_store.update_task(
                task_id,
                TaskUpdate(
                    status=TaskStatus.FAILED,
                    log=f"Error: {str(exc)}",
                ),
            )
