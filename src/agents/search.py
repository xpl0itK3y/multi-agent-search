import logging
import re
from urllib.parse import urlparse

from src.providers.search import SearchProvider, ContentExtractor
from src.api.schemas import TaskStatus, TaskUpdate
from src.repositories.protocols import TaskStore
from src.repositories.mappers import enrich_search_result_dict

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

    def __init__(self, task_store: TaskStore, max_sources: int = 5):
        self.task_store = task_store
        self.search_provider = SearchProvider(max_results=max_sources)
        self.extractor = ContentExtractor()
        self.max_sources = max_sources

    def _normalize_text(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", value).strip()

    def _content_fingerprint(self, title: str, content: str) -> str:
        normalized_title = self._normalize_text(title).lower()
        normalized_content = self._normalize_text(content).lower()
        content_prefix = normalized_content[:200]
        return f"{normalized_title}|{content_prefix}"

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

    def _low_value_domain_penalty(self, domain: str) -> int:
        if not domain:
            return 0
        normalized_domain = domain.removeprefix("www.")
        if normalized_domain in self.LOW_VALUE_DOMAIN_EXACT_MATCHES:
            return 120
        if any(token in normalized_domain for token in self.LOW_VALUE_DOMAIN_SUBSTRINGS):
            return 90
        return 0

    def _should_skip_search_result(self, url: str, title: str | None) -> bool:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        normalized_domain = domain.removeprefix("www.")
        normalized_title = self._normalize_text(title).lower()
        normalized_url = (url or "").lower()

        if not normalized_domain:
            return True
        if normalized_domain in {item.removeprefix("www.") for item in self.LOW_VALUE_DOMAIN_EXACT_MATCHES}:
            return True
        if any(token in normalized_domain for token in self.LOW_VALUE_DOMAIN_SUBSTRINGS):
            return True
        if any(token in normalized_url for token in ("/discover/", "/video/", "/gallery/", "/wall-", "/pin/")):
            return True
        if normalized_title and any(token in normalized_title for token in self.LOW_SIGNAL_TITLE_TOKENS):
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
        deduped_by_fingerprint: dict[str, tuple[int, dict]] = {}

        for result in results:
            title = self._normalize_text(result.get("title"))
            content = self._normalize_text(result.get("content"))
            url = result.get("url", "")
            source_quality = result.get("source_quality")
            fingerprint = self._content_fingerprint(title, content)
            score = self._score_result(url, title, content, source_quality)

            normalized_result = {
                **result,
                "url": url,
                "title": title or None,
                "content": content,
            }

            existing = deduped_by_fingerprint.get(fingerprint)
            if existing is None or score > existing[0]:
                deduped_by_fingerprint[fingerprint] = (score, normalized_result)

        ranked_results = sorted(
            (item for _, item in deduped_by_fingerprint.values()),
            key=lambda item: self._score_result(
                item["url"],
                item.get("title") or "",
                item.get("content") or "",
                item.get("source_quality"),
            ),
            reverse=True,
        )
        return ranked_results[: self.max_sources]

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
        unique_urls = set()

        try:
            for query in task.queries:
                self.task_store.update_task(task_id, TaskUpdate(log=f"Searching for: {query}"))
                search_results = self.search_provider.search(query)

                for res in search_results:
                    url = res.get("url")
                    if url and url not in unique_urls:
                        if self._should_skip_search_result(url, res.get("title")):
                            self.task_store.update_task(
                                task_id,
                                TaskUpdate(log=f"Skipped low-value result: {url}"),
                            )
                            continue
                        unique_urls.add(url)

                        self.task_store.update_task(
                            task_id,
                            TaskUpdate(log=f"Extracting content from: {url}"),
                        )
                        content = self.extractor.extract_content(url)

                        if content:
                            all_results.append(
                                enrich_search_result_dict(
                                    {
                                        "url": url,
                                        "title": res.get("title"),
                                        "content": content[:10000],  # Limit content size to avoid huge payloads.
                                        "snippet": res.get("snippet"),
                                    }
                                )
                            )
                        else:
                            all_results.append(
                                enrich_search_result_dict(
                                    {
                                        "url": url,
                                        "title": res.get("title"),
                                        "content": "Failed to extract content",
                                        "snippet": res.get("snippet"),
                                        "extraction_status": "failed",
                                    }
                                )
                            )

            selected_results = self._select_best_results(all_results)

            self.task_store.update_task(
                task_id,
                TaskUpdate(
                    status=TaskStatus.COMPLETED,
                    result=selected_results,
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
