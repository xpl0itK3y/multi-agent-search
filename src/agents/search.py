import logging
import time
from typing import List
from src.core.agent import BaseAgent
from src.providers.search import SearchProvider, ContentExtractor
from src.api.schemas import SearchTask, TaskStatus, TaskUpdate
from src.core.task_manager import task_manager

logger = logging.getLogger(__name__)

class SearchAgent:
    def __init__(self, max_sources: int = 5):
        self.search_provider = SearchProvider(max_results=max_sources)
        self.extractor = ContentExtractor()
        self.max_sources = max_sources

    def run_task(self, task_id: str):
        """
        Execute a search task: search for queries, extract content, and update TaskManager.
        """
        task = task_manager.get_task(task_id)
        if not task:
            logger.error(f"Task {task_id} not found in TaskManager")
            return

        task_manager.update_task(task_id, TaskUpdate(status=TaskStatus.RUNNING, log="Agent started search process"))
        
        all_results = []
        unique_urls = set()
        
        try:
            for query in task.queries:
                task_manager.update_task(task_id, TaskUpdate(log=f"Searching for: {query}"))
                search_results = self.search_provider.search(query)
                
                for res in search_results:
                    url = res.get("url")
                    if url and url not in unique_urls and len(unique_urls) < self.max_sources:
                        unique_urls.add(url)
                        
                        task_manager.update_task(task_id, TaskUpdate(log=f"Extracting content from: {url}"))
                        content = self.extractor.extract_content(url)
                        
                        if content:
                            all_results.append({
                                "url": url,
                                "title": res.get("title"),
                                "content": content[:10000] # Limit content size to avoid OOM or huge JSON
                            })
                        else:
                            all_results.append({
                                "url": url,
                                "title": res.get("title"),
                                "content": "Failed to extract content"
                            })

            task_manager.update_task(task_id, TaskUpdate(
                status=TaskStatus.COMPLETED, 
                result=all_results,
                log=f"Search completed. Found and processed {len(all_results)} sources."
            ))
            
        except Exception as e:
            logger.error(f"Error executing task {task_id}: {e}")
            task_manager.update_task(task_id, TaskUpdate(
                status=TaskStatus.FAILED, 
                log=f"Error: {str(e)}"
            ))
