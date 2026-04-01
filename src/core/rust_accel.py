import importlib
import json
import re


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
