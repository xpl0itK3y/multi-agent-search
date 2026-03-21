from src.api.schemas import SearchDepth


SEARCH_DEPTH_PROFILES = {
    SearchDepth.EASY: {
        "label": "Quick Scan",
        "description": "Fast pass for a compact answer.",
        "task_count": 2,
        "source_limit": 5,
        "search_results_per_query": 8,
        "max_candidate_urls": 12,
    },
    SearchDepth.MEDIUM: {
        "label": "Balanced",
        "description": "Broader coverage with more cross-checking.",
        "task_count": 4,
        "source_limit": 12,
        "search_results_per_query": 12,
        "max_candidate_urls": 24,
    },
    SearchDepth.HARD: {
        "label": "Deep Dive",
        "description": "Wider decomposition and the highest source coverage.",
        "task_count": 6,
        "source_limit": 20,
        "search_results_per_query": 16,
        "max_candidate_urls": 36,
    },
}


def get_depth_profile(depth: SearchDepth) -> dict:
    return SEARCH_DEPTH_PROFILES.get(depth, SEARCH_DEPTH_PROFILES[SearchDepth.EASY])
