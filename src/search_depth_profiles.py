from src.api.schemas import SearchDepth


SEARCH_DEPTH_PROFILES = {
    # Supply (task_count × source_limit) is sized to fill the analyzer's source
    # pool of 30 / 60 / 120 (EASY / MEDIUM / HARD) with margin for dedup loss.
    SearchDepth.EASY: {
        "label": "Quick Scan",
        "description": "Fast pass for a compact answer.",
        "task_count": 3,
        "source_limit": 12,
        "search_results_per_query": 16,
        "max_candidate_urls": 24,
    },
    SearchDepth.MEDIUM: {
        "label": "Balanced",
        "description": "Broader coverage with more cross-checking.",
        "task_count": 4,
        "source_limit": 16,
        "search_results_per_query": 18,
        "max_candidate_urls": 32,
    },
    SearchDepth.HARD: {
        "label": "Deep Dive",
        "description": "Wider decomposition and the highest source coverage.",
        "task_count": 6,
        "source_limit": 24,
        "search_results_per_query": 24,
        "max_candidate_urls": 48,
    },
}


def get_depth_profile(depth: SearchDepth) -> dict:
    return SEARCH_DEPTH_PROFILES.get(depth, SEARCH_DEPTH_PROFILES[SearchDepth.EASY])
