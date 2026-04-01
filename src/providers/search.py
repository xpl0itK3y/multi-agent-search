import logging
import trafilatura
from ddgs import DDGS
from typing import List, Dict, Optional
import os
import sys

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

class ContentExtractor:
    @staticmethod
    def extract_content(url: str) -> Optional[str]:
        """
        Download and extract clean text from a URL.
        """
        try:
            downloaded = trafilatura.fetch_url(url)
            if downloaded:
                result = trafilatura.extract(downloaded, include_comments=False, include_tables=True)
                cleaned = rust_accel.clean_extracted_content(result)
                return cleaned or None
        except Exception as e:
            logger.error(f"Error extracting content from {url}: {e}")
        return None
