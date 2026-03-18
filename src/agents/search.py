import logging
import re
from urllib.parse import urlparse

from src.providers.search import SearchProvider, ContentExtractor
from src.api.schemas import TaskStatus, TaskUpdate
from src.repositories.protocols import TaskStore

logger = logging.getLogger(__name__)


class SearchAgent:
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

    def _score_result(self, url: str, title: str, content: str) -> int:
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
        if "failed to extract content" in normalized_content.lower():
            score -= 5000
        return score

    def _select_best_results(self, results: list[dict]) -> list[dict]:
        deduped_by_fingerprint: dict[str, tuple[int, dict]] = {}

        for result in results:
            title = self._normalize_text(result.get("title"))
            content = self._normalize_text(result.get("content"))
            url = result.get("url", "")
            fingerprint = self._content_fingerprint(title, content)
            score = self._score_result(url, title, content)

            normalized_result = {
                "url": url,
                "title": title or None,
                "content": content,
            }

            existing = deduped_by_fingerprint.get(fingerprint)
            if existing is None or score > existing[0]:
                deduped_by_fingerprint[fingerprint] = (score, normalized_result)

        ranked_results = sorted(
            (item for _, item in deduped_by_fingerprint.values()),
            key=lambda item: self._score_result(item["url"], item.get("title") or "", item.get("content") or ""),
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
                        unique_urls.add(url)

                        self.task_store.update_task(
                            task_id,
                            TaskUpdate(log=f"Extracting content from: {url}"),
                        )
                        content = self.extractor.extract_content(url)

                        if content:
                            all_results.append({
                                "url": url,
                                "title": res.get("title"),
                                "content": content[:10000],  # Limit content size to avoid huge payloads.
                            })
                        else:
                            all_results.append({
                                "url": url,
                                "title": res.get("title"),
                                "content": "Failed to extract content",
                            })

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
