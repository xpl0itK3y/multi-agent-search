import importlib
import json
import re
from functools import lru_cache
from urllib.parse import urlparse

from src.source_quality_policy import TOPIC_POLICIES


_MODULE = None
_MODULE_LOAD_ATTEMPTED = False


def _load_module():
    global _MODULE, _MODULE_LOAD_ATTEMPTED
    if not _MODULE_LOAD_ATTEMPTED:
        _MODULE_LOAD_ATTEMPTED = True
        try:
            _MODULE = importlib.import_module("multi_agent_search_native")
        except ImportError:
            _MODULE = None
    return _MODULE


def rust_acceleration_available() -> bool:
    return _load_module() is not None


def _normalized_domain_from_url(url: str) -> str:
    return urlparse(url or "").netloc.lower().removeprefix("www.")


def _contains_any(haystack: str, needles: tuple[str, ...] | list[str]) -> bool:
    return any(token in haystack for token in needles)


@lru_cache(maxsize=1)
def _topic_policy_payload() -> list[dict]:
    return [
        {
            "name": name,
            "premium_domains": sorted(policy.premium_domains),
            "secondary_domains": sorted(policy.secondary_domains),
            "weak_domains": sorted(policy.weak_domains),
            "weak_domain_substrings": list(policy.weak_domain_substrings),
            "strong_editorial_tokens": list(policy.strong_editorial_tokens),
            "weak_signal_tokens": list(policy.weak_signal_tokens),
            "generic_listicle_tokens": list(policy.generic_listicle_tokens),
        }
        for name, policy in TOPIC_POLICIES.items()
    ]


@lru_cache(maxsize=1)
def _search_config() -> dict:
    return {
        "trusted_domain_exact_matches": [
            "developer.mozilla.org",
            "docs.python.org",
            "openai.com",
            "platform.openai.com",
            "wikipedia.org",
        ],
        "trusted_domain_suffixes": [".gov", ".edu", ".readthedocs.io"],
        "low_value_domain_exact_matches": [
            "linkedin.com",
            "pinterest.com",
            "facebook.com",
            "x.com",
            "twitter.com",
            "tiktok.com",
            "vk.com",
            "medium.com",
            "behance.net",
        ],
        "low_value_domain_substrings": ["bookmark", "trendhunter", "grokipedia", "outmaxshop"],
        "low_signal_title_tokens": [
            "discover",
            "gallery",
            "pinterest",
            "tiktok",
            "forum",
            "youth development forum",
            "travel trends",
            "fashion trends",
            "aesthetic clinics",
        ],
        "low_signal_url_tokens": [
            "bing.com/aclick",
            "news.google.com",
            "amazon.com/s",
            "yandex.",
            "/shopping/",
            "/discover/",
            "/video/",
            "/gallery/",
            "/pin/",
        ],
        "low_signal_result_tokens": [
            "best",
            "top 10",
            "top ten",
            "buying guide",
            "rankings guide",
            "most anticipated",
            "predictions",
            "upcoming",
            "expected",
        ],
        "strong_result_tokens": [
            "review",
            "reviews",
            "benchmark",
            "benchmarks",
            "specs",
            "comparison",
            "tested",
            "official",
            "press release",
        ],
        "topic_policies": _topic_policy_payload(),
        "docs_url_positive_tokens": [
            "/docs",
            "/documentation",
            "/reference",
            "/manual",
            "/api",
            "/extensions",
            "/async",
            "/tutorial/",
        ],
        "docs_title_positive_tokens": [
            "documentation",
            "reference",
            "api",
            "extensions",
            "async / await",
            "user guide",
        ],
        "docs_title_negative_tokens": [
            "comparison",
            "versus",
            "vs",
            "showdown",
            "which framework is best",
            "in-depth comparison",
        ],
        "docs_snippet_negative_tokens": [
            "use cases",
            "pros and cons",
            "which one to choose",
            "key differences",
        ],
        "global_url_positive_tokens": [
            "/newsroom/",
            "/press/",
            "/launch",
            "/events/",
            "/docs",
            "/documentation",
            "/reference",
            "/api",
        ],
        "global_url_negative_tokens": [
            "rumor",
            "rumours",
            "rumors",
            "launch-date",
            "price-in",
            "upcoming",
            "ranking",
        ],
        "global_snippet_negative_phrases": ["for every budget"],
    }


@lru_cache(maxsize=1)
def _analyzer_config() -> dict:
    return {
        "trusted_domain_exact_matches": [
            "developer.mozilla.org",
            "docs.python.org",
            "openai.com",
            "platform.openai.com",
            "wikipedia.org",
        ],
        "trusted_domain_suffixes": [".gov", ".edu", ".readthedocs.io"],
        "low_value_domain_exact_matches": [
            "linkedin.com",
            "pinterest.com",
            "facebook.com",
            "x.com",
            "twitter.com",
            "eventify.io",
        ],
        "low_value_domain_substrings": [
            "bookmark",
            "newsnviews",
            "techandgadgetreviews",
            "techspymagazine",
        ],
        "speculative_title_tokens": [
            "predictions",
            "prediction",
            "coming soon",
            "coming in",
            "coming to",
            "future of",
            "trends to watch",
            "what to expect",
            "gadgets coming",
            "best gadgets",
            "breakthrough technologies",
        ],
        "speculative_content_tokens": [
            "expected to",
            "may",
            "might",
            "could",
            "rumored",
            "rumoured",
            "is likely to",
            "are likely to",
            "what to expect",
            "predictions for",
        ],
        "topic_policies": _topic_policy_payload(),
        "docs_url_positive_tokens": [
            "/docs",
            "/documentation",
            "/reference",
            "/manual",
            "/api",
            "/extensions",
            "/async",
            "/tutorial/",
        ],
        "docs_title_positive_tokens": [
            "documentation",
            "reference",
            "api",
            "extensions",
            "async / await",
            "user guide",
        ],
        "docs_title_negative_tokens": [
            "comparison",
            "versus",
            "vs",
            "showdown",
            "which framework is best",
            "in-depth comparison",
        ],
        "docs_content_negative_tokens": [
            "use cases",
            "pros and cons",
            "which one to choose",
            "key differences",
        ],
        "global_url_negative_tokens": [
            "rumor",
            "rumours",
            "rumors",
            "launch-date",
            "price-in",
            "upcoming",
        ],
    }


def normalize_text(value: str | None) -> str:
    text = value or ""
    module = _load_module()
    if module is not None:
        return module.normalize_text(text)
    return re.sub(r"\s+", " ", text).strip()


def content_fingerprint(title: str, content: str, prefix_len: int) -> str:
    module = _load_module()
    if module is not None:
        return module.content_fingerprint(title, content, prefix_len)
    normalized_title = normalize_text(title).lower()
    normalized_content = normalize_text(content).lower()
    return f"{normalized_title}|{normalized_content[:prefix_len]}"


def compact_source_content(content: str, max_chars: int) -> str:
    module = _load_module()
    if module is not None:
        return module.compact_source_content(content, max_chars)

    normalized = normalize_text(content)
    if len(normalized) <= max_chars:
        return normalized

    sentences = re.split(r"(?<=[.!?])\s+", normalized)
    compact_parts: list[str] = []
    current_length = 0
    for sentence in sentences:
        cleaned = normalize_text(sentence)
        if not cleaned:
            continue
        next_length = current_length + len(cleaned) + (1 if compact_parts else 0)
        if next_length > max_chars:
            break
        compact_parts.append(cleaned)
        current_length = next_length

    if compact_parts:
        compact = " ".join(compact_parts)
        if len(compact) < len(normalized):
            return compact.rstrip() + " ..."
        return compact

    return normalized[:max_chars].rstrip() + " ..."


def clean_extracted_content(content: str | None) -> str:
    text = content or ""
    module = _load_module()
    if module is not None:
        native_fn = getattr(module, "clean_extracted_content", None)
        if native_fn is not None:
            return native_fn(text)

    lines = []
    seen: set[str] = set()
    for raw_line in re.split(r"[\r\n]+", text):
        cleaned = normalize_text(raw_line)
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if cleaned in seen:
            continue
        if lowered in {
            "cookie policy",
            "privacy policy",
            "terms of service",
            "all rights reserved",
            "subscribe",
            "sign up",
            "share this article",
        }:
            continue
        seen.add(cleaned)
        lines.append(cleaned)
    return "\n".join(lines)


def build_snippet(content: str | None, max_chars: int) -> str | None:
    text = content or ""
    module = _load_module()
    if module is not None:
        native_fn = getattr(module, "build_snippet", None)
        if native_fn is not None:
            return native_fn(text, max_chars) or None

    normalized = clean_extracted_content(text)
    if not normalized:
        return None
    sentences = re.split(r"(?<=[.!?])\s+", normalized)
    snippet_parts: list[str] = []
    current_length = 0
    for sentence in sentences:
        cleaned = normalize_text(sentence)
        if not cleaned:
            continue
        next_length = current_length + len(cleaned) + (1 if snippet_parts else 0)
        if next_length > max_chars:
            break
        snippet_parts.append(cleaned)
        current_length = next_length
    if snippet_parts:
        snippet = " ".join(snippet_parts)
        if len(snippet) < len(normalized):
            return snippet.rstrip() + " ..."
        return snippet
    return normalized[:max_chars].rstrip() + (" ..." if len(normalized) > max_chars else "")


def extract_used_source_ids(report_body: str) -> list[str]:
    module = _load_module()
    if module is not None:
        return list(module.extract_used_source_ids(report_body))

    ordered_ids: list[str] = []
    for match in re.finditer(r"\[S(\d+)\]", report_body):
        source_id = f"S{match.group(1)}"
        if source_id not in ordered_ids:
            ordered_ids.append(source_id)
    return ordered_ids


def sanitize_citations(report: str, valid_source_ids: set[str]) -> str:
    module = _load_module()
    if module is not None:
        return module.sanitize_citations(report, sorted(valid_source_ids))

    def replace(match: re.Match[str]) -> str:
        source_id = f"S{match.group(1)}"
        return match.group(0) if source_id in valid_source_ids else ""

    sanitized = re.sub(r"\[S(\d+)\]", replace, report)
    sanitized = re.sub(r"\[(?:,\s*)+\]", "", sanitized)
    sanitized = re.sub(r"[ \t]{2,}", " ", sanitized)
    sanitized = re.sub(r"\s+([,.;:])", r"\1", sanitized)
    return sanitized


def detect_conflicts(
    aggregated_data: list[dict],
    stopwords: set[str],
    generic_tokens: set[str],
    negation_tokens: set[str],
    max_conflicts: int,
) -> list[dict]:
    module = _load_module()
    if module is not None:
        payload = [
            {
                "source_id": item.get("source_id"),
                "content": item.get("content", ""),
            }
            for item in aggregated_data
        ]
        return json.loads(
            module.detect_conflicts(
                json.dumps(payload, ensure_ascii=False),
                sorted(stopwords),
                sorted(generic_tokens),
                sorted(negation_tokens),
                max_conflicts,
            )
        )

    sentence_pattern = re.compile(r"(?<=[.!?])\s+")

    def is_likely_year(value: str) -> bool:
        if "." in value:
            return False
        try:
            number = int(value)
        except ValueError:
            return False
        return 1900 <= number <= 2100

    claims: list[dict] = []
    for source in aggregated_data:
        content = source.get("content") or ""
        for sentence in sentence_pattern.split(content):
            normalized_sentence = normalize_text(sentence)
            if len(normalized_sentence) < 50 or len(normalized_sentence) > 260:
                continue
            lowered = normalized_sentence.lower()
            tokens = [
                token
                for token in re.findall(r"[a-zа-я0-9]+", lowered)
                if len(token) >= 4 and token not in stopwords
            ]
            unique_tokens: list[str] = []
            for token in tokens:
                if token not in unique_tokens:
                    unique_tokens.append(token)
            if len(unique_tokens) < 2:
                continue
            numbers = tuple(
                number
                for number in re.findall(r"\b\d+(?:\.\d+)?\b", lowered)
                if not is_likely_year(number)
            )
            claims.append(
                {
                    "source_id": source["source_id"],
                    "sentence": normalized_sentence,
                    "tokens": unique_tokens[:6],
                    "numbers": numbers,
                    "has_negation": any(token in lowered for token in negation_tokens),
                }
            )

    def informative_shared_tokens(left: dict, right: dict) -> set[str]:
        return {
            token
            for token in set(left["tokens"]) & set(right["tokens"])
            if token not in generic_tokens
        }

    conflicts: list[dict] = []
    seen_pairs: set[tuple[str, str]] = set()
    for index, left in enumerate(claims):
        for right in claims[index + 1:]:
            if left["source_id"] == right["source_id"]:
                continue
            shared = sorted(informative_shared_tokens(left, right))
            if len(shared) < 2:
                continue
            left_numbers = set(left["numbers"])
            right_numbers = set(right["numbers"])
            is_conflict = left["has_negation"] != right["has_negation"] or (
                left_numbers and right_numbers and left_numbers != right_numbers and len(shared) >= 3
            )
            if not is_conflict:
                continue
            pair_key = tuple(sorted((left["source_id"], right["source_id"])))
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            reason = "one source affirms the claim while the other negates it"
            if left["has_negation"] == right["has_negation"]:
                reason = "the sources report different concrete figures"
            conflicts.append(
                {
                    "topic": ", ".join(shared[:3]) or "source disagreement",
                    "source_ids": [left["source_id"], right["source_id"]],
                    "sentences": [left["sentence"], right["sentence"]],
                    "reason": reason,
                }
            )
            if len(conflicts) >= max_conflicts:
                return conflicts
    return conflicts


def score_search_candidates(candidates: list[dict], topics: set[str], limit: int) -> list[dict]:
    module = _load_module()
    payload = {
        "candidates": candidates,
        "topics": sorted(topics),
        "config": _search_config(),
        "limit": limit,
    }
    if module is not None:
        native_fn = getattr(module, "score_search_candidates", None)
        if native_fn is not None:
            return json.loads(native_fn(json.dumps(payload, ensure_ascii=False)))

    config = _search_config()
    topic_policies = {item["name"]: item for item in config["topic_policies"]}

    def trusted_domain_score(domain: str) -> int:
        if not domain:
            return 0
        if domain in config["trusted_domain_exact_matches"]:
            return 200
        if any(domain.endswith(suffix) for suffix in config["trusted_domain_suffixes"]):
            return 150
        if domain.endswith(".github.io"):
            return 40
        return 0

    def low_value_domain_penalty(domain: str) -> int:
        if not domain:
            return 0
        if domain in config["low_value_domain_exact_matches"]:
            return 120
        if _contains_any(domain, config["low_value_domain_substrings"]):
            return 90
        return 0

    best_by_url: dict[str, dict] = {}
    for candidate in candidates:
        url = candidate.get("url") or ""
        if not url:
            continue
        normalized_title = normalize_text(candidate.get("title")).lower()
        normalized_snippet = normalize_text(candidate.get("snippet")).lower()
        normalized_url = url.lower()
        normalized_domain = _normalized_domain_from_url(url)
        if not normalized_domain:
            continue
        if normalized_domain in config["low_value_domain_exact_matches"]:
            continue
        if _contains_any(normalized_domain, config["low_value_domain_substrings"]):
            continue
        if _contains_any(normalized_url, config["low_signal_url_tokens"]) or "/wall-" in normalized_url:
            continue
        if normalized_title and _contains_any(normalized_title, config["low_signal_title_tokens"]):
            continue

        topic_adjustment = 0
        should_skip = False
        for topic_name in topics:
            policy = topic_policies[topic_name]
            has_strong_editorial_signal = any(
                token in normalized_title or token in normalized_snippet or token in normalized_url
                for token in policy["strong_editorial_tokens"]
            )
            if normalized_domain in policy["weak_domains"]:
                should_skip = True
                break
            if _contains_any(normalized_domain, policy["weak_domain_substrings"]):
                should_skip = True
                break
            if normalized_domain in policy["premium_domains"]:
                topic_adjustment += 220
            if normalized_domain in policy["secondary_domains"]:
                topic_adjustment += 70
            if _contains_any(normalized_title, policy["strong_editorial_tokens"]):
                topic_adjustment += 70
            if _contains_any(normalized_snippet, policy["strong_editorial_tokens"]):
                topic_adjustment += 35
            if _contains_any(normalized_title, policy["weak_signal_tokens"]):
                topic_adjustment -= 90
            if _contains_any(normalized_snippet, policy["weak_signal_tokens"]):
                topic_adjustment -= 60
            if _contains_any(normalized_title, policy["generic_listicle_tokens"]) and not has_strong_editorial_signal:
                topic_adjustment -= 140
            if _contains_any(normalized_snippet, policy["generic_listicle_tokens"]) and not has_strong_editorial_signal:
                topic_adjustment -= 70
            if topic_name == "docs_programming":
                if _contains_any(normalized_url, config["docs_url_positive_tokens"]):
                    topic_adjustment += 110
                if _contains_any(normalized_title, config["docs_title_positive_tokens"]):
                    topic_adjustment += 90
                if _contains_any(normalized_title, config["docs_title_negative_tokens"]) and not has_strong_editorial_signal:
                    topic_adjustment -= 120
                if _contains_any(normalized_snippet, config["docs_snippet_negative_tokens"]) and not has_strong_editorial_signal:
                    topic_adjustment -= 70
        if should_skip:
            continue
        if _contains_any(normalized_url, config["global_url_positive_tokens"]):
            topic_adjustment += 50
        if _contains_any(normalized_url, config["global_url_negative_tokens"]):
            topic_adjustment -= 70
        if _contains_any(normalized_snippet, config["global_snippet_negative_phrases"]):
            topic_adjustment -= 70

        score = 0
        if urlparse(url).scheme == "https":
            score += 25
        score += trusted_domain_score(normalized_domain)
        score -= low_value_domain_penalty(normalized_domain)
        if normalized_title:
            score += 40
        if normalized_snippet:
            score += min(len(normalized_snippet), 240)
        if _contains_any(normalized_title, config["strong_result_tokens"]):
            score += 60
        if _contains_any(normalized_snippet, config["strong_result_tokens"]):
            score += 40
        if _contains_any(normalized_title, config["low_signal_result_tokens"]):
            score -= 45
        if _contains_any(normalized_snippet, config["low_signal_result_tokens"]):
            score -= 25
        if _contains_any(normalized_url, ["benchmark", "benchmarks", "review", "reviews", "compare"]):
            score += 25
        if _contains_any(normalized_url, ["best-", "top-", "upcoming", "predictions"]):
            score -= 20
        score += topic_adjustment
        normalized_candidate = {
            "url": url,
            "title": normalize_text(candidate.get("title")) or None,
            "snippet": normalize_text(candidate.get("snippet")) or None,
            "score": score,
        }
        existing = best_by_url.get(url)
        if existing is None or score > int(existing.get("score", 0)):
            best_by_url[url] = normalized_candidate

    return sorted(best_by_url.values(), key=lambda item: item.get("score", 0), reverse=True)[:limit]


def select_analyzer_sources(
    candidates: list[dict],
    topics: set[str],
    max_sources: int,
    max_sources_per_domain: int,
    max_sources_per_task: int,
) -> list[dict]:
    module = _load_module()
    payload = {
        "candidates": candidates,
        "topics": sorted(topics),
        "config": _analyzer_config(),
        "max_sources": max_sources,
        "max_sources_per_domain": max_sources_per_domain,
        "max_sources_per_task": max_sources_per_task,
    }
    if module is not None:
        native_fn = getattr(module, "select_analyzer_sources", None)
        if native_fn is not None:
            return json.loads(native_fn(json.dumps(payload, ensure_ascii=False)))

    config = _analyzer_config()
    topic_policies = {item["name"]: item for item in config["topic_policies"]}

    def trusted_domain_score(url: str) -> int:
        domain = _normalized_domain_from_url(url)
        if not domain:
            return 0
        if domain in config["trusted_domain_exact_matches"]:
            return 200
        if any(domain.endswith(suffix) for suffix in config["trusted_domain_suffixes"]):
            return 150
        if domain.endswith(".github.io"):
            return 40
        return 0

    def low_value_domain_penalty(url: str) -> int:
        domain = _normalized_domain_from_url(url)
        if not domain:
            return 0
        if domain in config["low_value_domain_exact_matches"]:
            return 120
        if _contains_any(domain, config["low_value_domain_substrings"]):
            return 90
        return 0

    def source_quality_score(source_quality: str | None) -> int:
        if source_quality == "high":
            return 180
        if source_quality == "medium":
            return 60
        return 0

    def authority_hint_score(url: str, title: str, content: str) -> int:
        normalized_title = normalize_text(title).lower()
        normalized_content = normalize_text(content).lower()
        normalized_url = (url or "").lower()
        score = 0
        if _contains_any(normalized_url, ["/docs", "/documentation", "/reference", "/api"]):
            score += 120
        if _contains_any(normalized_title, ["documentation", "docs", "reference", "api", "guide", "manual"]):
            score += 80
        if _contains_any(normalized_content[:600], ["official documentation", "api reference", "reference guide"]):
            score += 60
        return score

    def speculative_penalty(url: str, title: str, content: str, source_quality: str | None) -> int:
        normalized_title = normalize_text(title).lower()
        normalized_content = normalize_text(content).lower()
        normalized_url = (url or "").lower()
        score = 0
        if _contains_any(normalized_title, config["speculative_title_tokens"]):
            score += 130
        if _contains_any(normalized_url, ["prediction", "predictions", "gadgets", "coming", "future"]):
            score += 35
        content_window = normalized_content[:700]
        speculative_hits = sum(1 for token in config["speculative_content_tokens"] if token in content_window)
        score += speculative_hits * 15
        if source_quality == "low":
            score += 45
        return score

    def topic_domain_adjustment(url: str, title: str, content: str, source_quality: str | None) -> int:
        normalized_domain = _normalized_domain_from_url(url)
        normalized_title = normalize_text(title).lower()
        normalized_content = normalize_text(content).lower()
        normalized_url = (url or "").lower()
        score = 0
        for topic_name in topics:
            policy = topic_policies[topic_name]
            has_strong_editorial_signal = any(
                token in normalized_title or token in normalized_content[:500] or token in normalized_url
                for token in policy["strong_editorial_tokens"]
            )
            if normalized_domain in policy["premium_domains"]:
                score += 220
            if normalized_domain in policy["secondary_domains"]:
                score += 60
            if normalized_domain in policy["weak_domains"]:
                score -= 180
            if _contains_any(normalized_domain, policy["weak_domain_substrings"]):
                score -= 120
            if _contains_any(normalized_title, policy["generic_listicle_tokens"]) and not has_strong_editorial_signal:
                score -= 140
            if _contains_any(normalized_content[:450], policy["generic_listicle_tokens"]) and not has_strong_editorial_signal:
                score -= 80
            if _contains_any(normalized_title, policy["weak_signal_tokens"]):
                score -= 95
            if _contains_any(normalized_content[:500], policy["weak_signal_tokens"]):
                score -= 70
            if topic_name == "docs_programming":
                if _contains_any(normalized_url, config["docs_url_positive_tokens"]):
                    score += 120
                if _contains_any(normalized_title, config["docs_title_positive_tokens"]):
                    score += 90
                if _contains_any(normalized_title, config["docs_title_negative_tokens"]) and not has_strong_editorial_signal:
                    score -= 120
                if _contains_any(normalized_content[:500], config["docs_content_negative_tokens"]) and not has_strong_editorial_signal:
                    score -= 75
        if _contains_any(normalized_url, config["global_url_negative_tokens"]):
            score -= 75
        if source_quality == "low":
            score -= 40
        return score

    def should_exclude(url: str, title: str, content: str, source_quality: str | None) -> bool:
        penalty = speculative_penalty(url, title, content, source_quality)
        trusted_score = trusted_domain_score(url)
        normalized_content = normalize_text(content).lower()
        if source_quality == "low" and penalty >= 160 and trusted_score <= 0:
            return True
        if source_quality == "low" and len(normalized_content) < 220 and penalty >= 80:
            return True
        if topics:
            topic_score = topic_domain_adjustment(url, title, content, source_quality)
            if topic_score <= -180 and trusted_score <= 0 and source_quality != "high":
                return True
            if topic_score <= -120 and source_quality == "low" and len(normalized_content) < 1200:
                return True
        return False

    def score_candidate(candidate: dict) -> int:
        url = candidate.get("url") or ""
        title = candidate.get("title") or ""
        content = candidate.get("content") or ""
        source_quality = candidate.get("source_quality")
        normalized_title = normalize_text(title)
        normalized_content = normalize_text(content)
        score = len(normalized_content)
        if normalized_title:
            score += 100
        score += trusted_domain_score(url)
        score += source_quality_score(source_quality)
        score += authority_hint_score(url, title, content)
        score -= low_value_domain_penalty(url)
        if "failed to extract content" in normalized_content.lower():
            score -= 5000
        score -= speculative_penalty(url, title, content, source_quality)
        score += topic_domain_adjustment(url, title, content, source_quality)
        return score

    filtered = []
    for candidate in candidates:
        url = candidate.get("url") or ""
        title = normalize_text(candidate.get("title")) or ""
        content = normalize_text(candidate.get("content")) or ""
        if not url or not content or "failed to extract content" in content.lower():
            continue
        source_quality = candidate.get("source_quality") or "low"
        if should_exclude(url, title, content, source_quality):
            continue
        filtered.append(
            {
                **candidate,
                "title": title or None,
                "content": content,
                "source_quality": source_quality,
                "domain": candidate.get("domain") or _normalized_domain_from_url(url) or None,
            }
        )

    best_by_url: dict[str, tuple[int, dict]] = {}
    for candidate in filtered:
        score = score_candidate(candidate)
        existing = best_by_url.get(candidate["url"])
        if existing is None or score > existing[0]:
            best_by_url[candidate["url"]] = (score, candidate)

    best_by_fingerprint: dict[str, tuple[int, dict]] = {}
    for score, candidate in best_by_url.values():
        fingerprint = content_fingerprint(candidate.get("title") or "", candidate.get("content") or "", 250)
        existing = best_by_fingerprint.get(fingerprint)
        if existing is None or score > existing[0]:
            best_by_fingerprint[fingerprint] = (score, candidate)

    ranked_candidates = sorted(
        ({**candidate, "_score": score} for score, candidate in best_by_fingerprint.values()),
        key=lambda item: item["_score"],
        reverse=True,
    )

    selected_candidates: list[dict] = []
    domain_counts: dict[str, int] = {}
    task_counts: dict[str, int] = {}
    for candidate in ranked_candidates:
        domain = (candidate.get("domain") or "").lower()
        task_description = candidate.get("task_description") or ""
        is_strong_source = candidate.get("source_quality") == "high"
        if domain and domain_counts.get(domain, 0) >= max_sources_per_domain and not is_strong_source:
            continue
        if task_description and task_counts.get(task_description, 0) >= max_sources_per_task and not is_strong_source:
            continue
        selected_candidates.append(candidate)
        if domain:
            domain_counts[domain] = domain_counts.get(domain, 0) + 1
        if task_description:
            task_counts[task_description] = task_counts.get(task_description, 0) + 1
        if len(selected_candidates) >= max_sources:
            break

    unique_domains = {
        (candidate.get("domain") or "").lower()
        for candidate in ranked_candidates
        if candidate.get("domain")
    }
    if len(unique_domains) <= 1 and len(selected_candidates) < max_sources:
        selected_urls = {candidate["url"] for candidate in selected_candidates}
        for candidate in ranked_candidates:
            if candidate["url"] in selected_urls:
                continue
            selected_candidates.append(candidate)
            if len(selected_candidates) >= max_sources:
                break

    return [
        {key: value for key, value in candidate.items() if key != "_score"}
        for candidate in selected_candidates
    ]


def extract_evidence_groups(
    aggregated_data: list[dict],
    stopwords: set[str],
    generic_tokens: set[str],
    negation_tokens: set[str],
    max_groups: int,
) -> list[dict]:
    module = _load_module()
    payload = [
        {
            "source_id": item.get("source_id"),
            "content": item.get("content", ""),
        }
        for item in aggregated_data
    ]
    if module is not None:
        native_fn = getattr(module, "extract_evidence_groups", None)
        if native_fn is not None:
            return json.loads(
                native_fn(
                    json.dumps(payload, ensure_ascii=False),
                    sorted(stopwords),
                    sorted(generic_tokens),
                    sorted(negation_tokens),
                    max_groups,
                )
            )

    sentence_pattern = re.compile(r"(?<=[.!?])\s+")
    groups: dict[tuple[str, ...], dict] = {}
    for source in aggregated_data:
        content = source.get("content") or ""
        for sentence in sentence_pattern.split(content):
            normalized_sentence = normalize_text(sentence)
            if len(normalized_sentence) < 50 or len(normalized_sentence) > 260:
                continue
            lowered = normalized_sentence.lower()
            tokens = []
            for token in re.findall(r"[a-zа-я0-9]+", lowered):
                if len(token) < 4 or token in stopwords or token in generic_tokens or token in tokens:
                    continue
                tokens.append(token)
            if len(tokens) < 2:
                continue
            key = tuple(tokens[:3])
            group = groups.setdefault(
                key,
                {
                    "topic": ", ".join(key),
                    "source_ids": [],
                    "evidence": [],
                    "has_conflict_signal": False,
                },
            )
            source_id = source.get("source_id")
            if source_id not in group["source_ids"]:
                group["source_ids"].append(source_id)
            if len(group["evidence"]) < 4:
                group["evidence"].append(
                    {
                        "source_id": source_id,
                        "sentence": normalized_sentence,
                    }
                )
            has_negation = any(token in lowered for token in negation_tokens)
            if has_negation:
                group["has_conflict_signal"] = True

    ranked_groups = sorted(
        (item for item in groups.values() if len(item["source_ids"]) >= 2),
        key=lambda item: (len(item["source_ids"]), len(item["evidence"])),
        reverse=True,
    )
    return ranked_groups[:max_groups]


def select_top_candidates(candidates: list[dict], limit: int) -> list[dict]:
    module = _load_module()
    if module is not None:
        return json.loads(module.select_top_candidates(json.dumps(candidates, ensure_ascii=False), limit))

    return sorted(candidates, key=lambda item: item.get("score", 0), reverse=True)[:limit]


def select_best_results(results: list[dict], limit: int) -> list[dict]:
    module = _load_module()
    if module is not None:
        return json.loads(module.select_best_results(json.dumps(results, ensure_ascii=False), limit))

    deduped_by_fingerprint: dict[str, tuple[int, dict]] = {}
    for result in results:
        title = normalize_text(result.get("title") or "")
        content = normalize_text(result.get("content") or "")
        fingerprint = content_fingerprint(title, content, 200)
        score = int(result.get("score", 0))
        normalized_result = {
            **result,
            "title": title or None,
            "content": content,
        }
        existing = deduped_by_fingerprint.get(fingerprint)
        if existing is None or score > existing[0]:
            deduped_by_fingerprint[fingerprint] = (score, normalized_result)

    ranked = sorted(
        (item for _, item in deduped_by_fingerprint.values()),
        key=lambda item: item.get("score", 0),
        reverse=True,
    )
    return ranked[:limit]
