use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Debug, Deserialize)]
struct AggregatedSource {
    source_id: String,
    content: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct ScoredCandidate {
    url: String,
    title: Option<String>,
    snippet: Option<String>,
    score: i64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct ScoredResult {
    url: String,
    title: Option<String>,
    content: String,
    source_quality: Option<String>,
    score: i64,
    #[serde(flatten)]
    extra: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ConflictRecord {
    topic: String,
    source_ids: Vec<String>,
    sentences: Vec<String>,
    reason: String,
}

#[derive(Debug)]
struct ClaimRecord {
    source_id: String,
    sentence: String,
    tokens: Vec<String>,
    numbers: Vec<String>,
    has_negation: bool,
}

fn normalize_text_impl(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn content_fingerprint_impl(title: &str, content: &str, prefix_len: usize) -> String {
    let normalized_title = normalize_text_impl(title).to_lowercase();
    let normalized_content = normalize_text_impl(content).to_lowercase();
    let prefix: String = normalized_content.chars().take(prefix_len).collect();
    format!("{}|{}", normalized_title, prefix)
}

fn split_sentences(content: &str) -> Vec<String> {
    let sentence_re = Regex::new(r"(?<=[.!?])\s+").unwrap();
    sentence_re
        .split(content)
        .map(normalize_text_impl)
        .filter(|item| !item.is_empty())
        .collect()
}

fn is_likely_year(value: &str) -> bool {
    if value.contains('.') {
        return false;
    }
    match value.parse::<i32>() {
        Ok(number) => (1900..=2100).contains(&number),
        Err(_) => false,
    }
}

fn unique_tokens(
    sentence: &str,
    stopwords: &HashSet<String>,
    token_re: &Regex,
) -> Vec<String> {
    let mut unique = Vec::new();
    for token in token_re.find_iter(sentence) {
        let normalized = token.as_str().to_lowercase();
        if normalized.len() < 4 || stopwords.contains(&normalized) {
            continue;
        }
        if !unique.contains(&normalized) {
            unique.push(normalized);
        }
    }
    unique
}

fn extract_claims(
    aggregated_data: &[AggregatedSource],
    stopwords: &HashSet<String>,
    negation_tokens: &HashSet<String>,
) -> Vec<ClaimRecord> {
    let token_re = Regex::new(r"[a-zA-Zа-яА-Я0-9]+").unwrap();
    let number_re = Regex::new(r"\b\d+(?:\.\d+)?\b").unwrap();

    let mut claims = Vec::new();
    for source in aggregated_data {
        for sentence in split_sentences(&source.content) {
            if sentence.len() < 50 || sentence.len() > 260 {
                continue;
            }
            let lowered = sentence.to_lowercase();
            let tokens = unique_tokens(&lowered, stopwords, &token_re);
            if tokens.len() < 2 {
                continue;
            }

            let numbers = number_re
                .find_iter(&lowered)
                .map(|m| m.as_str().to_string())
                .filter(|value| !is_likely_year(value))
                .collect::<Vec<_>>();

            let has_negation = negation_tokens.iter().any(|token| lowered.contains(token));
            claims.push(ClaimRecord {
                source_id: source.source_id.clone(),
                sentence,
                tokens: tokens.into_iter().take(6).collect(),
                numbers,
                has_negation,
            });
        }
    }
    claims
}

fn informative_shared_tokens(
    left: &ClaimRecord,
    right: &ClaimRecord,
    generic_tokens: &HashSet<String>,
) -> Vec<String> {
    let mut shared = left
        .tokens
        .iter()
        .filter(|token| right.tokens.contains(token) && !generic_tokens.contains(*token))
        .cloned()
        .collect::<Vec<_>>();
    shared.sort();
    shared.dedup();
    shared
}

fn detect_conflicts_impl(
    aggregated_data_json: &str,
    stopwords: Vec<String>,
    generic_tokens: Vec<String>,
    negation_tokens: Vec<String>,
    max_conflicts: usize,
) -> Result<String, String> {
    let aggregated_data: Vec<AggregatedSource> =
        serde_json::from_str(aggregated_data_json).map_err(|err| err.to_string())?;
    let stopwords_set = stopwords.into_iter().collect::<HashSet<_>>();
    let generic_tokens_set = generic_tokens.into_iter().collect::<HashSet<_>>();
    let negation_tokens_set = negation_tokens.into_iter().collect::<HashSet<_>>();

    let claims = extract_claims(&aggregated_data, &stopwords_set, &negation_tokens_set);
    let mut conflicts = Vec::new();
    let mut seen_pairs = HashSet::new();

    for (index, left) in claims.iter().enumerate() {
        for right in claims.iter().skip(index + 1) {
            if left.source_id == right.source_id {
                continue;
            }
            let shared_tokens = informative_shared_tokens(left, right, &generic_tokens_set);
            if shared_tokens.len() < 2 {
                continue;
            }

            let left_numbers = left.numbers.iter().cloned().collect::<HashSet<_>>();
            let right_numbers = right.numbers.iter().cloned().collect::<HashSet<_>>();
            let is_conflict = left.has_negation != right.has_negation
                || (!left_numbers.is_empty()
                    && !right_numbers.is_empty()
                    && left_numbers != right_numbers
                    && shared_tokens.len() >= 3);

            if !is_conflict {
                continue;
            }

            let mut pair = vec![left.source_id.clone(), right.source_id.clone()];
            pair.sort();
            if !seen_pairs.insert(pair.clone()) {
                continue;
            }

            let reason = if left.has_negation != right.has_negation {
                "one source affirms the claim while the other negates it".to_string()
            } else {
                "the sources report different concrete figures".to_string()
            };

            conflicts.push(ConflictRecord {
                topic: shared_tokens
                    .iter()
                    .take(3)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", "),
                source_ids: vec![left.source_id.clone(), right.source_id.clone()],
                sentences: vec![left.sentence.clone(), right.sentence.clone()],
                reason,
            });

            if conflicts.len() >= max_conflicts {
                return serde_json::to_string(&conflicts).map_err(|err| err.to_string());
            }
        }
    }

    serde_json::to_string(&conflicts).map_err(|err| err.to_string())
}

fn select_top_candidates_impl(candidates_json: &str, limit: usize) -> Result<String, String> {
    let mut candidates: Vec<ScoredCandidate> =
        serde_json::from_str(candidates_json).map_err(|err| err.to_string())?;
    candidates.sort_by(|left, right| right.score.cmp(&left.score));
    let selected = candidates.into_iter().take(limit).collect::<Vec<_>>();
    serde_json::to_string(&selected).map_err(|err| err.to_string())
}

fn select_best_results_impl(results_json: &str, limit: usize) -> Result<String, String> {
    let results: Vec<ScoredResult> =
        serde_json::from_str(results_json).map_err(|err| err.to_string())?;
    let mut best_by_fingerprint: Vec<(String, ScoredResult)> = Vec::new();

    for result in results {
        let title = result.title.clone().unwrap_or_default();
        let fingerprint = content_fingerprint_impl(&title, &result.content, 200);
        if let Some((_, existing)) = best_by_fingerprint.iter_mut().find(|(key, _)| key == &fingerprint) {
            if result.score > existing.score {
                *existing = result;
            }
        } else {
            best_by_fingerprint.push((fingerprint, result));
        }
    }

    let mut deduped = best_by_fingerprint
        .into_iter()
        .map(|(_, result)| result)
        .collect::<Vec<_>>();
    deduped.sort_by(|left, right| right.score.cmp(&left.score));
    let selected = deduped.into_iter().take(limit).collect::<Vec<_>>();
    serde_json::to_string(&selected).map_err(|err| err.to_string())
}

#[pyfunction]
fn normalize_text(value: &str) -> String {
    normalize_text_impl(value)
}

#[pyfunction]
fn content_fingerprint(title: &str, content: &str, prefix_len: usize) -> String {
    content_fingerprint_impl(title, content, prefix_len)
}

#[pyfunction]
fn compact_source_content(content: &str, max_chars: usize) -> String {
    let normalized = normalize_text_impl(content);
    if normalized.chars().count() <= max_chars {
        return normalized;
    }

    let mut compact_parts = Vec::new();
    let mut current_length = 0usize;
    for sentence in split_sentences(&normalized) {
        let sentence_len = sentence.chars().count();
        let next_length = current_length + sentence_len + if compact_parts.is_empty() { 0 } else { 1 };
        if next_length > max_chars {
            break;
        }
        compact_parts.push(sentence);
        current_length = next_length;
    }

    if !compact_parts.is_empty() {
        let compact = compact_parts.join(" ");
        if compact.chars().count() < normalized.chars().count() {
            return format!("{} ...", compact.trim_end());
        }
        return compact;
    }

    let prefix = normalized.chars().take(max_chars).collect::<String>();
    format!("{} ...", prefix.trim_end())
}

#[pyfunction]
fn extract_used_source_ids(report_body: &str) -> Vec<String> {
    let citation_re = Regex::new(r"\[S(\d+)\]").unwrap();
    let mut ordered = Vec::new();
    for caps in citation_re.captures_iter(report_body) {
        let source_id = format!("S{}", &caps[1]);
        if !ordered.contains(&source_id) {
            ordered.push(source_id);
        }
    }
    ordered
}

#[pyfunction]
fn sanitize_citations(report: &str, valid_source_ids: Vec<String>) -> String {
    let citation_re = Regex::new(r"\[S(\d+)\]").unwrap();
    let valid = valid_source_ids.into_iter().collect::<HashSet<_>>();
    let replaced = citation_re.replace_all(report, |caps: &regex::Captures| {
        let source_id = format!("S{}", &caps[1]);
        if valid.contains(&source_id) {
            caps[0].to_string()
        } else {
            String::new()
        }
    });

    let empty_brackets_re = Regex::new(r"\[(?:,\s*)+\]").unwrap();
    let spaces_re = Regex::new(r"[ \t]{2,}").unwrap();
    let punctuation_re = Regex::new(r"\s+([,.;:])").unwrap();

    let step1 = empty_brackets_re.replace_all(&replaced, "");
    let step2 = spaces_re.replace_all(&step1, " ");
    punctuation_re.replace_all(&step2, "$1").to_string()
}

#[pyfunction]
fn detect_conflicts(
    aggregated_data_json: &str,
    stopwords: Vec<String>,
    generic_tokens: Vec<String>,
    negation_tokens: Vec<String>,
    max_conflicts: usize,
) -> PyResult<String> {
    detect_conflicts_impl(
        aggregated_data_json,
        stopwords,
        generic_tokens,
        negation_tokens,
        max_conflicts,
    )
    .map_err(PyValueError::new_err)
}

#[pyfunction]
fn select_top_candidates(candidates_json: &str, limit: usize) -> PyResult<String> {
    select_top_candidates_impl(candidates_json, limit).map_err(PyValueError::new_err)
}

#[pyfunction]
fn select_best_results(results_json: &str, limit: usize) -> PyResult<String> {
    select_best_results_impl(results_json, limit).map_err(PyValueError::new_err)
}

#[pymodule]
fn multi_agent_search_native(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(normalize_text, module)?)?;
    module.add_function(wrap_pyfunction!(content_fingerprint, module)?)?;
    module.add_function(wrap_pyfunction!(compact_source_content, module)?)?;
    module.add_function(wrap_pyfunction!(extract_used_source_ids, module)?)?;
    module.add_function(wrap_pyfunction!(sanitize_citations, module)?)?;
    module.add_function(wrap_pyfunction!(detect_conflicts, module)?)?;
    module.add_function(wrap_pyfunction!(select_top_candidates, module)?)?;
    module.add_function(wrap_pyfunction!(select_best_results, module)?)?;
    Ok(())
}
