import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

from src.providers.search import SearchProvider, ContentExtractor
from src.api.schemas import TaskStatus, TaskUpdate
from src.repositories.protocols import TaskStore
from src.repositories.mappers import enrich_search_result_dict

logger = logging.getLogger(__name__)


class SearchAgent:
    MOBILE_TECH_QUERY_TOKENS = (
        "smartphone",
        "smartphones",
        "phone",
        "phones",
        "смартфон",
        "смартфоны",
        "смартфонов",
        "телефон",
        "телефоны",
        "телефонов",
        "айфон",
        "флагман",
        "флагманы",
        "android",
        "iphone",
        "flagship",
        "camera phone",
        "mobile",
        "galaxy",
        "pixel",
        "oneplus",
        "xiaomi",
        "oppo",
        "honor",
        "foldable",
        "chipset",
        "benchmark",
    )
    MOBILE_TECH_STRONG_DOMAIN_EXACT_MATCHES = {
        "gsmarena.com",
        "www.gsmarena.com",
        "dxomark.com",
        "www.dxomark.com",
        "notebookcheck.net",
        "www.notebookcheck.net",
        "androidauthority.com",
        "www.androidauthority.com",
        "tomsguide.com",
        "www.tomsguide.com",
        "theverge.com",
        "www.theverge.com",
        "apple.com",
        "www.apple.com",
        "news.samsung.com",
        "samsung.com",
        "www.samsung.com",
        "blog.google",
        "store.google.com",
        "google.com",
        "www.google.com",
        "oneplus.com",
        "www.oneplus.com",
        "mi.com",
        "www.mi.com",
        "xiaomi.com",
        "www.xiaomi.com",
        "oppo.com",
        "www.oppo.com",
        "honor.com",
        "www.honor.com",
    }
    MOBILE_TECH_WEAK_DOMAIN_EXACT_MATCHES = {
        "vertu.com",
        "www.vertu.com",
        "axis-intelligence.com",
        "www.axis-intelligence.com",
        "gadgetph.com",
        "www.gadgetph.com",
        "asumetech.com",
        "www.asumetech.com",
        "techspecs.info",
        "www.techspecs.info",
        "techindeep.com",
        "www.techindeep.com",
        "technicalforum.org",
        "www.technicalforum.org",
        "macprices.net",
        "www.macprices.net",
        "nyongesasande.com",
        "www.nyongesasande.com",
        "brandvm.com",
        "www.brandvm.com",
        "mobileradar.com",
        "www.mobileradar.com",
        "techtimes.com",
        "www.techtimes.com",
        "dialoguepakistan.com",
        "www.dialoguepakistan.com",
        "techarc.net",
        "www.techarc.net",
        "wirefly.com",
        "www.wirefly.com",
        "news.wirefly.com",
        "techrankup.com",
        "www.techrankup.com",
        "asymco.com",
        "www.asymco.com",
    }
    MOBILE_TECH_WEAK_DOMAIN_SUBSTRINGS = (
        "buyersguide",
        "buyers-guide",
        "rankings-guide",
        "top-phones",
        "best-phones",
        "best-smartphones",
        "smartphone-rankings",
    )
    MOBILE_TECH_GENERIC_LISTICLE_TOKENS = (
        "best phones",
        "best smartphones",
        "top phones",
        "top smartphones",
        "best phone",
        "best smartphone",
        "top 10 best",
        "for every budget",
        "buyers guide",
        "buying guide",
        "should you choose",
        "which should you choose",
        "smartphone rankings",
        "rankings revealed",
        "performance ranking",
    )
    MOBILE_TECH_STRONG_EDITORIAL_TOKENS = (
        "tested",
        "review",
        "reviews",
        "benchmark",
        "benchmarks",
        "camera test",
        "hands-on",
        "official",
        "launch",
        "vs",
        "comparison",
    )
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
    LOW_SIGNAL_URL_TOKENS = (
        "bing.com/aclick",
        "news.google.com",
        "amazon.com/s",
        "yandex.",
        "/shopping/",
        "/discover/",
        "/video/",
        "/gallery/",
        "/pin/",
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
    ):
        self.task_store = task_store
        self.search_provider = SearchProvider(max_results=search_results_per_query)
        self.extractor = ContentExtractor()
        self.max_sources = max_sources
        self.search_results_per_query = search_results_per_query
        self.max_candidate_urls = max_candidate_urls
        self.extraction_concurrency = max(1, extraction_concurrency)

    def _normalize_text(self, value: str | None) -> str:
        if not value:
            return ""
        return re.sub(r"\s+", " ", value).strip()

    def _content_fingerprint(self, title: str, content: str) -> str:
        normalized_title = self._normalize_text(title).lower()
        normalized_content = self._normalize_text(content).lower()
        content_prefix = normalized_content[:200]
        return f"{normalized_title}|{content_prefix}"

    def _is_mobile_tech_query(self, task) -> bool:
        haystack_parts = [
            self._normalize_text(getattr(task, "description", None)).lower(),
            " ".join(self._normalize_text(query).lower() for query in getattr(task, "queries", []) or []),
        ]
        haystack = " ".join(part for part in haystack_parts if part)
        return any(token in haystack for token in self.MOBILE_TECH_QUERY_TOKENS)

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

    def _should_skip_search_result(self, url: str, title: str | None, is_mobile_tech_query: bool = False) -> bool:
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
        if any(token in normalized_url for token in self.LOW_SIGNAL_URL_TOKENS) or "/wall-" in normalized_url:
            return True
        if normalized_title and any(token in normalized_title for token in self.LOW_SIGNAL_TITLE_TOKENS):
            return True
        if is_mobile_tech_query:
            if normalized_domain in {item.removeprefix("www.") for item in self.MOBILE_TECH_WEAK_DOMAIN_EXACT_MATCHES}:
                return True
            if any(token in normalized_domain for token in self.MOBILE_TECH_WEAK_DOMAIN_SUBSTRINGS):
                return True
        return False

    def _score_search_candidate(self, url: str, title: str | None, snippet: str | None) -> int:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        normalized_title = self._normalize_text(title).lower()
        normalized_snippet = self._normalize_text(snippet).lower()
        normalized_url = (url or "").lower()

        score = 0
        if parsed.scheme == "https":
            score += 25
        score += self._trusted_domain_score(domain)
        score -= self._low_value_domain_penalty(domain)

        if normalized_title:
            score += 40
        if normalized_snippet:
            score += min(len(normalized_snippet), 240)

        if any(token in normalized_title for token in self.STRONG_RESULT_TOKENS):
            score += 60
        if any(token in normalized_snippet for token in self.STRONG_RESULT_TOKENS):
            score += 40
        if any(token in normalized_title for token in self.LOW_SIGNAL_RESULT_TOKENS):
            score -= 45
        if any(token in normalized_snippet for token in self.LOW_SIGNAL_RESULT_TOKENS):
            score -= 25
        if any(token in normalized_url for token in ("benchmark", "benchmarks", "review", "reviews", "compare")):
            score += 25
        if any(token in normalized_url for token in ("best-", "top-", "upcoming", "predictions")):
            score -= 20
        return score

    def _mobile_tech_domain_adjustment(self, url: str, title: str | None, snippet: str | None) -> int:
        parsed = urlparse(url)
        normalized_domain = parsed.netloc.lower().removeprefix("www.")
        normalized_title = self._normalize_text(title).lower()
        normalized_snippet = self._normalize_text(snippet).lower()
        normalized_url = (url or "").lower()
        score = 0
        has_strong_editorial_signal = any(
            token in normalized_title or token in normalized_snippet or token in normalized_url
            for token in self.MOBILE_TECH_STRONG_EDITORIAL_TOKENS
        )

        if normalized_domain in {item.removeprefix("www.") for item in self.MOBILE_TECH_STRONG_DOMAIN_EXACT_MATCHES}:
            score += 220
        if normalized_domain in {item.removeprefix("www.") for item in self.MOBILE_TECH_WEAK_DOMAIN_EXACT_MATCHES}:
            score -= 220
        if any(token in normalized_domain for token in self.MOBILE_TECH_WEAK_DOMAIN_SUBSTRINGS):
            score -= 120

        if any(token in normalized_url for token in ("/newsroom/", "/press/", "/launch", "/events/", "/smartphones/")):
            score += 50
        if any(token in normalized_title for token in ("hands-on", "review", "camera test", "benchmark", "official", "launch")):
            score += 70
        if any(token in normalized_title for token in ("buyers guide", "buying guide", "top phones", "best smartphones", "most anticipated", "smartphone rankings", "performance ranking")):
            score -= 80
        if any(token in normalized_snippet for token in ("rumored", "predicted", "expected to launch", "what to expect")):
            score -= 55
        if any(token in normalized_title for token in self.MOBILE_TECH_GENERIC_LISTICLE_TOKENS) and not has_strong_editorial_signal:
            score -= 140
        if any(token in normalized_url for token in ("best-phones", "best-smartphones", "top-smartphones", "top-phones", "rankings", "ranking")) and not has_strong_editorial_signal:
            score -= 110
        if "for every budget" in normalized_snippet and not has_strong_editorial_signal:
            score -= 70
        return score

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
        candidate_results: list[dict] = []
        is_mobile_tech_query = self._is_mobile_tech_query(task)

        try:
            for query in task.queries:
                self.task_store.update_task(task_id, TaskUpdate(log=f"Searching for: {query}"))
                search_results = self.search_provider.search(query)

                for res in search_results:
                    url = res.get("url")
                    if url and url not in unique_urls:
                        if self._should_skip_search_result(url, res.get("title"), is_mobile_tech_query=is_mobile_tech_query):
                            self.task_store.update_task(
                                task_id,
                                TaskUpdate(log=f"Skipped low-value result: {url}"),
                            )
                            continue
                        unique_urls.add(url)
                        candidate_results.append(
                            {
                                "url": url,
                                "title": res.get("title"),
                                "snippet": res.get("snippet"),
                                "score": self._score_search_candidate(
                                    url,
                                    res.get("title"),
                                    res.get("snippet"),
                                )
                                + (
                                    self._mobile_tech_domain_adjustment(
                                        url,
                                        res.get("title"),
                                        res.get("snippet"),
                                    )
                                    if is_mobile_tech_query
                                    else 0
                                ),
                            }
                        )

            candidate_results.sort(key=lambda item: item["score"], reverse=True)
            candidate_results = candidate_results[: self.max_candidate_urls]

            self.task_store.update_task(
                task_id,
                TaskUpdate(
                    log=(
                        f"Queued {len(candidate_results)} candidate URLs for extraction "
                        f"with concurrency {self.extraction_concurrency}"
                    )
                ),
            )

            with ThreadPoolExecutor(max_workers=self.extraction_concurrency) as executor:
                future_to_candidate = {}
                for candidate in candidate_results:
                    url = candidate["url"]
                    self.task_store.update_task(
                        task_id,
                        TaskUpdate(log=f"Extracting content from: {url}"),
                    )
                    future_to_candidate[executor.submit(self.extractor.extract_content, url)] = candidate

                for future in as_completed(future_to_candidate):
                    candidate = future_to_candidate[future]
                    url = candidate["url"]
                    title = candidate.get("title")
                    snippet = candidate.get("snippet")
                    try:
                        content = future.result()
                    except Exception as exc:
                        logger.error("Error extracting content from %s: %s", url, exc)
                        content = None

                    if content:
                        all_results.append(
                            enrich_search_result_dict(
                                {
                                    "url": url,
                                    "title": title,
                                    "content": content[:10000],  # Limit content size to avoid huge payloads.
                                    "snippet": snippet,
                                }
                            )
                        )
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
