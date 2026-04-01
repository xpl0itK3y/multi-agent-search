use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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

#[derive(Debug, Deserialize)]
struct TopicPolicyPayload {
    name: String,
    premium_domains: Vec<String>,
    secondary_domains: Vec<String>,
    weak_domains: Vec<String>,
    weak_domain_substrings: Vec<String>,
    strong_editorial_tokens: Vec<String>,
    weak_signal_tokens: Vec<String>,
    generic_listicle_tokens: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SearchConfigPayload {
    trusted_domain_exact_matches: Vec<String>,
    trusted_domain_suffixes: Vec<String>,
    low_value_domain_exact_matches: Vec<String>,
    low_value_domain_substrings: Vec<String>,
    low_signal_title_tokens: Vec<String>,
    low_signal_url_tokens: Vec<String>,
    low_signal_result_tokens: Vec<String>,
    strong_result_tokens: Vec<String>,
    topic_policies: Vec<TopicPolicyPayload>,
    docs_url_positive_tokens: Vec<String>,
    docs_title_positive_tokens: Vec<String>,
    docs_title_negative_tokens: Vec<String>,
    docs_snippet_negative_tokens: Vec<String>,
    global_url_positive_tokens: Vec<String>,
    global_url_negative_tokens: Vec<String>,
    global_snippet_negative_phrases: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SearchCandidateBatchPayload {
    candidates: Vec<ScoredCandidate>,
    topics: Vec<String>,
    config: SearchConfigPayload,
    limit: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct AnalyzerCandidate {
    task_description: Option<String>,
    url: String,
    domain: Option<String>,
    source_quality: Option<String>,
    title: Option<String>,
    content: String,
    #[serde(flatten)]
    extra: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct AnalyzerConfigPayload {
    trusted_domain_exact_matches: Vec<String>,
    trusted_domain_suffixes: Vec<String>,
    low_value_domain_exact_matches: Vec<String>,
    low_value_domain_substrings: Vec<String>,
    speculative_title_tokens: Vec<String>,
    speculative_content_tokens: Vec<String>,
    topic_policies: Vec<TopicPolicyPayload>,
    docs_url_positive_tokens: Vec<String>,
    docs_title_positive_tokens: Vec<String>,
    docs_title_negative_tokens: Vec<String>,
    docs_content_negative_tokens: Vec<String>,
    global_url_negative_tokens: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AnalyzerSelectionPayload {
    candidates: Vec<AnalyzerCandidate>,
    topics: Vec<String>,
    config: AnalyzerConfigPayload,
    max_sources: usize,
    max_sources_per_domain: usize,
    max_sources_per_task: usize,
}

#[derive(Debug, Serialize)]
struct EvidenceItem {
    source_id: String,
    sentence: String,
}

#[derive(Debug, Serialize)]
struct EvidenceGroup {
    topic: String,
    source_ids: Vec<String>,
    evidence: Vec<EvidenceItem>,
    has_conflict_signal: bool,
}

fn normalize_text_impl(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn normalized_domain(url: &str) -> String {
    let without_scheme = url
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(url);
    without_scheme
        .split('/')
        .next()
        .unwrap_or("")
        .to_lowercase()
        .trim_start_matches("www.")
        .to_string()
}

fn contains_any(haystack: &str, needles: &[String]) -> bool {
    needles.iter().any(|token| haystack.contains(token))
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

fn trusted_domain_score(domain: &str, exact_matches: &[String], suffixes: &[String]) -> i64 {
    if domain.is_empty() {
        return 0;
    }
    if exact_matches.iter().any(|item| item == domain) {
        return 200;
    }
    if suffixes.iter().any(|suffix| domain.ends_with(suffix)) {
        return 150;
    }
    if domain.ends_with(".github.io") {
        return 40;
    }
    0
}

fn low_value_domain_penalty(domain: &str, exact_matches: &[String], substrings: &[String]) -> i64 {
    if domain.is_empty() {
        return 0;
    }
    if exact_matches.iter().any(|item| item == domain) {
        return 120;
    }
    if contains_any(domain, substrings) {
        return 90;
    }
    0
}

fn score_search_candidates_impl(payload_json: &str) -> Result<String, String> {
    let payload: SearchCandidateBatchPayload =
        serde_json::from_str(payload_json).map_err(|err| err.to_string())?;
    let topic_policies = payload
        .config
        .topic_policies
        .iter()
        .map(|policy| (policy.name.clone(), policy))
        .collect::<HashMap<_, _>>();
    let mut best_by_url = HashMap::<String, ScoredCandidate>::new();

    for candidate in payload.candidates {
        if candidate.url.is_empty() {
            continue;
        }
        let normalized_title = normalize_text_impl(candidate.title.as_deref().unwrap_or("")).to_lowercase();
        let normalized_snippet = normalize_text_impl(candidate.snippet.as_deref().unwrap_or("")).to_lowercase();
        let normalized_url = candidate.url.to_lowercase();
        let domain = normalized_domain(&candidate.url);

        if domain.is_empty() {
            continue;
        }
        if payload
            .config
            .low_value_domain_exact_matches
            .iter()
            .any(|item| item == &domain)
        {
            continue;
        }
        if contains_any(&domain, &payload.config.low_value_domain_substrings)
            || contains_any(&normalized_url, &payload.config.low_signal_url_tokens)
            || normalized_url.contains("/wall-")
            || (!normalized_title.is_empty() && contains_any(&normalized_title, &payload.config.low_signal_title_tokens))
        {
            continue;
        }

        let mut topic_adjustment = 0i64;
        let mut should_skip = false;
        for topic_name in &payload.topics {
            let Some(policy) = topic_policies.get(topic_name) else {
                continue;
            };
            let has_strong_editorial_signal = policy.strong_editorial_tokens.iter().any(|token| {
                normalized_title.contains(token) || normalized_snippet.contains(token) || normalized_url.contains(token)
            });
            if policy.weak_domains.iter().any(|item| item == &domain)
                || contains_any(&domain, &policy.weak_domain_substrings)
            {
                should_skip = true;
                break;
            }
            if policy.premium_domains.iter().any(|item| item == &domain) {
                topic_adjustment += 220;
            }
            if policy.secondary_domains.iter().any(|item| item == &domain) {
                topic_adjustment += 70;
            }
            if contains_any(&normalized_title, &policy.strong_editorial_tokens) {
                topic_adjustment += 70;
            }
            if contains_any(&normalized_snippet, &policy.strong_editorial_tokens) {
                topic_adjustment += 35;
            }
            if contains_any(&normalized_title, &policy.weak_signal_tokens) {
                topic_adjustment -= 90;
            }
            if contains_any(&normalized_snippet, &policy.weak_signal_tokens) {
                topic_adjustment -= 60;
            }
            if contains_any(&normalized_title, &policy.generic_listicle_tokens) && !has_strong_editorial_signal {
                topic_adjustment -= 140;
            }
            if contains_any(&normalized_snippet, &policy.generic_listicle_tokens) && !has_strong_editorial_signal {
                topic_adjustment -= 70;
            }
            if topic_name == "docs_programming" {
                if contains_any(&normalized_url, &payload.config.docs_url_positive_tokens) {
                    topic_adjustment += 110;
                }
                if contains_any(&normalized_title, &payload.config.docs_title_positive_tokens) {
                    topic_adjustment += 90;
                }
                if contains_any(&normalized_title, &payload.config.docs_title_negative_tokens) && !has_strong_editorial_signal {
                    topic_adjustment -= 120;
                }
                if contains_any(&normalized_snippet, &payload.config.docs_snippet_negative_tokens) && !has_strong_editorial_signal {
                    topic_adjustment -= 70;
                }
            }
        }

        if should_skip {
            continue;
        }
        if contains_any(&normalized_url, &payload.config.global_url_positive_tokens) {
            topic_adjustment += 50;
        }
        if contains_any(&normalized_url, &payload.config.global_url_negative_tokens) {
            topic_adjustment -= 70;
        }
        if contains_any(&normalized_snippet, &payload.config.global_snippet_negative_phrases) {
            topic_adjustment -= 70;
        }

        let mut score = 0i64;
        if normalized_url.starts_with("https://") {
            score += 25;
        }
        score += trusted_domain_score(
            &domain,
            &payload.config.trusted_domain_exact_matches,
            &payload.config.trusted_domain_suffixes,
        );
        score -= low_value_domain_penalty(
            &domain,
            &payload.config.low_value_domain_exact_matches,
            &payload.config.low_value_domain_substrings,
        );
        if !normalized_title.is_empty() {
            score += 40;
        }
        if !normalized_snippet.is_empty() {
            score += std::cmp::min(normalized_snippet.len(), 240) as i64;
        }
        if contains_any(&normalized_title, &payload.config.strong_result_tokens) {
            score += 60;
        }
        if contains_any(&normalized_snippet, &payload.config.strong_result_tokens) {
            score += 40;
        }
        if contains_any(&normalized_title, &payload.config.low_signal_result_tokens) {
            score -= 45;
        }
        if contains_any(&normalized_snippet, &payload.config.low_signal_result_tokens) {
            score -= 25;
        }
        if ["benchmark", "benchmarks", "review", "reviews", "compare"]
            .iter()
            .any(|token| normalized_url.contains(token))
        {
            score += 25;
        }
        if ["best-", "top-", "upcoming", "predictions"]
            .iter()
            .any(|token| normalized_url.contains(token))
        {
            score -= 20;
        }
        score += topic_adjustment;

        let normalized_candidate = ScoredCandidate {
            url: candidate.url,
            title: {
                let value = normalize_text_impl(candidate.title.as_deref().unwrap_or(""));
                if value.is_empty() { None } else { Some(value) }
            },
            snippet: {
                let value = normalize_text_impl(candidate.snippet.as_deref().unwrap_or(""));
                if value.is_empty() { None } else { Some(value) }
            },
            score,
        };

        let should_replace = best_by_url
            .get(&normalized_candidate.url)
            .map(|existing| normalized_candidate.score > existing.score)
            .unwrap_or(true);
        if should_replace {
            best_by_url.insert(normalized_candidate.url.clone(), normalized_candidate);
        }
    }

    let mut ranked = best_by_url.into_values().collect::<Vec<_>>();
    ranked.sort_by(|left, right| right.score.cmp(&left.score));
    ranked.truncate(payload.limit);
    serde_json::to_string(&ranked).map_err(|err| err.to_string())
}

fn select_analyzer_sources_impl(payload_json: &str) -> Result<String, String> {
    let payload: AnalyzerSelectionPayload =
        serde_json::from_str(payload_json).map_err(|err| err.to_string())?;
    let topic_policies = payload
        .config
        .topic_policies
        .iter()
        .map(|policy| (policy.name.clone(), policy))
        .collect::<HashMap<_, _>>();

    let source_quality_score = |value: Option<&str>| match value {
        Some("high") => 180,
        Some("medium") => 60,
        _ => 0,
    };

    let speculative_penalty = |url: &str, title: &str, content: &str, source_quality: Option<&str>| {
        let normalized_title = normalize_text_impl(title).to_lowercase();
        let normalized_content = normalize_text_impl(content).to_lowercase();
        let normalized_url = url.to_lowercase();
        let mut score = 0i64;
        if contains_any(&normalized_title, &payload.config.speculative_title_tokens) {
            score += 130;
        }
        if ["prediction", "predictions", "gadgets", "coming", "future"]
            .iter()
            .any(|token| normalized_url.contains(token))
        {
            score += 35;
        }
        let content_window = normalized_content.chars().take(700).collect::<String>();
        let speculative_hits = payload
            .config
            .speculative_content_tokens
            .iter()
            .filter(|token| content_window.contains(token.as_str()))
            .count() as i64;
        score += speculative_hits * 15;
        if source_quality == Some("low") {
            score += 45;
        }
        score
    };

    let authority_hint_score = |url: &str, title: &str, content: &str| {
        let normalized_title = normalize_text_impl(title).to_lowercase();
        let normalized_content = normalize_text_impl(content).to_lowercase();
        let normalized_url = url.to_lowercase();
        let mut score = 0i64;
        if ["/docs", "/documentation", "/reference", "/api"]
            .iter()
            .any(|token| normalized_url.contains(token))
        {
            score += 120;
        }
        if ["documentation", "docs", "reference", "api", "guide", "manual"]
            .iter()
            .any(|token| normalized_title.contains(token))
        {
            score += 80;
        }
        if ["official documentation", "api reference", "reference guide"]
            .iter()
            .any(|token| normalized_content.chars().take(600).collect::<String>().contains(token))
        {
            score += 60;
        }
        score
    };

    let topic_domain_adjustment = |url: &str, title: &str, content: &str, source_quality: Option<&str>| {
        let domain = normalized_domain(url);
        let normalized_title = normalize_text_impl(title).to_lowercase();
        let normalized_content = normalize_text_impl(content).to_lowercase();
        let normalized_url = url.to_lowercase();
        let mut score = 0i64;
        for topic_name in &payload.topics {
            let Some(policy) = topic_policies.get(topic_name) else {
                continue;
            };
            let content_head = normalized_content.chars().take(500).collect::<String>();
            let has_strong_editorial_signal = policy.strong_editorial_tokens.iter().any(|token| {
                normalized_title.contains(token) || content_head.contains(token) || normalized_url.contains(token)
            });
            if policy.premium_domains.iter().any(|item| item == &domain) {
                score += 220;
            }
            if policy.secondary_domains.iter().any(|item| item == &domain) {
                score += 60;
            }
            if policy.weak_domains.iter().any(|item| item == &domain) {
                score -= 180;
            }
            if contains_any(&domain, &policy.weak_domain_substrings) {
                score -= 120;
            }
            if contains_any(&normalized_title, &policy.generic_listicle_tokens) && !has_strong_editorial_signal {
                score -= 140;
            }
            if contains_any(&normalized_content.chars().take(450).collect::<String>(), &policy.generic_listicle_tokens)
                && !has_strong_editorial_signal
            {
                score -= 80;
            }
            if contains_any(&normalized_title, &policy.weak_signal_tokens) {
                score -= 95;
            }
            if contains_any(&content_head, &policy.weak_signal_tokens) {
                score -= 70;
            }
            if topic_name == "docs_programming" {
                if contains_any(&normalized_url, &payload.config.docs_url_positive_tokens) {
                    score += 120;
                }
                if contains_any(&normalized_title, &payload.config.docs_title_positive_tokens) {
                    score += 90;
                }
                if contains_any(&normalized_title, &payload.config.docs_title_negative_tokens) && !has_strong_editorial_signal {
                    score -= 120;
                }
                if contains_any(&content_head, &payload.config.docs_content_negative_tokens) && !has_strong_editorial_signal {
                    score -= 75;
                }
            }
        }
        if contains_any(&normalized_url, &payload.config.global_url_negative_tokens) {
            score -= 75;
        }
        if source_quality == Some("low") {
            score -= 40;
        }
        score
    };

    let should_exclude = |url: &str, title: &str, content: &str, source_quality: Option<&str>| {
        let penalty = speculative_penalty(url, title, content, source_quality);
        let trusted_score = trusted_domain_score(
            &normalized_domain(url),
            &payload.config.trusted_domain_exact_matches,
            &payload.config.trusted_domain_suffixes,
        );
        let normalized_content = normalize_text_impl(content).to_lowercase();
        if source_quality == Some("low") && penalty >= 160 && trusted_score <= 0 {
            return true;
        }
        if source_quality == Some("low") && normalized_content.len() < 220 && penalty >= 80 {
            return true;
        }
        if !payload.topics.is_empty() {
            let topic_score = topic_domain_adjustment(url, title, content, source_quality);
            if topic_score <= -180 && trusted_score <= 0 && source_quality != Some("high") {
                return true;
            }
            if topic_score <= -120 && source_quality == Some("low") && normalized_content.len() < 1200 {
                return true;
            }
        }
        false
    };

    let score_candidate = |candidate: &AnalyzerCandidate| {
        let title = candidate.title.as_deref().unwrap_or("");
        let content = candidate.content.as_str();
        let domain = normalized_domain(&candidate.url);
        let mut score = normalize_text_impl(content).len() as i64;
        if !normalize_text_impl(title).is_empty() {
            score += 100;
        }
        score += trusted_domain_score(
            &domain,
            &payload.config.trusted_domain_exact_matches,
            &payload.config.trusted_domain_suffixes,
        );
        score += source_quality_score(candidate.source_quality.as_deref());
        score += authority_hint_score(&candidate.url, title, content);
        score -= low_value_domain_penalty(
            &domain,
            &payload.config.low_value_domain_exact_matches,
            &payload.config.low_value_domain_substrings,
        );
        if normalize_text_impl(content)
            .to_lowercase()
            .contains("failed to extract content")
        {
            score -= 5000;
        }
        score -= speculative_penalty(&candidate.url, title, content, candidate.source_quality.as_deref());
        score += topic_domain_adjustment(&candidate.url, title, content, candidate.source_quality.as_deref());
        score
    };

    let mut filtered = Vec::<AnalyzerCandidate>::new();
    for mut candidate in payload.candidates {
        let title = normalize_text_impl(candidate.title.as_deref().unwrap_or(""));
        let content = normalize_text_impl(&candidate.content);
        if candidate.url.is_empty() || content.is_empty() || content.to_lowercase().contains("failed to extract content")
        {
            continue;
        }
        let source_quality = candidate.source_quality.clone().unwrap_or_else(|| "low".to_string());
        if should_exclude(&candidate.url, &title, &content, Some(source_quality.as_str())) {
            continue;
        }
        candidate.title = if title.is_empty() { None } else { Some(title) };
        candidate.content = content;
        candidate.source_quality = Some(source_quality);
        if candidate.domain.as_deref().unwrap_or("").is_empty() {
            let domain = normalized_domain(&candidate.url);
            candidate.domain = if domain.is_empty() { None } else { Some(domain) };
        }
        filtered.push(candidate);
    }

    let mut best_by_url = HashMap::<String, (i64, AnalyzerCandidate)>::new();
    for candidate in filtered {
        let score = score_candidate(&candidate);
        let should_replace = best_by_url
            .get(&candidate.url)
            .map(|existing| score > existing.0)
            .unwrap_or(true);
        if should_replace {
            best_by_url.insert(candidate.url.clone(), (score, candidate));
        }
    }

    let mut best_by_fingerprint = HashMap::<String, (i64, AnalyzerCandidate)>::new();
    for (score, candidate) in best_by_url.into_values() {
        let fingerprint = content_fingerprint_impl(
            candidate.title.as_deref().unwrap_or(""),
            &candidate.content,
            250,
        );
        let should_replace = best_by_fingerprint
            .get(&fingerprint)
            .map(|existing| score > existing.0)
            .unwrap_or(true);
        if should_replace {
            best_by_fingerprint.insert(fingerprint, (score, candidate));
        }
    }

    let mut ranked = best_by_fingerprint
        .into_values()
        .map(|(score, candidate)| (score, candidate))
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| right.0.cmp(&left.0));

    let mut selected = Vec::<AnalyzerCandidate>::new();
    let mut selected_urls = HashSet::<String>::new();
    let mut domain_counts = HashMap::<String, usize>::new();
    let mut task_counts = HashMap::<String, usize>::new();
    for (_, candidate) in &ranked {
        let domain = candidate.domain.clone().unwrap_or_default().to_lowercase();
        let task_description = candidate.task_description.clone().unwrap_or_default();
        let is_strong_source = candidate.source_quality.as_deref() == Some("high");
        if !domain.is_empty()
            && *domain_counts.get(&domain).unwrap_or(&0) >= payload.max_sources_per_domain
            && !is_strong_source
        {
            continue;
        }
        if !task_description.is_empty()
            && *task_counts.get(&task_description).unwrap_or(&0) >= payload.max_sources_per_task
            && !is_strong_source
        {
            continue;
        }
        selected.push(candidate.clone());
        selected_urls.insert(candidate.url.clone());
        if !domain.is_empty() {
            *domain_counts.entry(domain).or_insert(0) += 1;
        }
        if !task_description.is_empty() {
            *task_counts.entry(task_description).or_insert(0) += 1;
        }
        if selected.len() >= payload.max_sources {
            break;
        }
    }

    let unique_domains = ranked
        .iter()
        .filter_map(|(_, candidate)| candidate.domain.clone())
        .map(|value| value.to_lowercase())
        .collect::<HashSet<_>>();
    if unique_domains.len() <= 1 && selected.len() < payload.max_sources {
        for (_, candidate) in ranked {
            if selected_urls.contains(&candidate.url) {
                continue;
            }
            selected.push(candidate.clone());
            selected_urls.insert(candidate.url.clone());
            if selected.len() >= payload.max_sources {
                break;
            }
        }
    }

    serde_json::to_string(&selected).map_err(|err| err.to_string())
}

fn extract_evidence_groups_impl(
    aggregated_data_json: &str,
    stopwords: Vec<String>,
    generic_tokens: Vec<String>,
    negation_tokens: Vec<String>,
    max_groups: usize,
) -> Result<String, String> {
    let aggregated_data: Vec<AggregatedSource> =
        serde_json::from_str(aggregated_data_json).map_err(|err| err.to_string())?;
    let stopwords_set = stopwords.into_iter().collect::<HashSet<_>>();
    let generic_tokens_set = generic_tokens.into_iter().collect::<HashSet<_>>();
    let negation_tokens_set = negation_tokens.into_iter().collect::<HashSet<_>>();

    let token_re = Regex::new(r"[a-zA-Zа-яА-Я0-9]+").unwrap();
    let mut groups = HashMap::<Vec<String>, EvidenceGroup>::new();

    for source in aggregated_data {
        for sentence in split_sentences(&source.content) {
            if sentence.len() < 50 || sentence.len() > 260 {
                continue;
            }
            let lowered = sentence.to_lowercase();
            let tokens = unique_tokens(&lowered, &stopwords_set, &token_re)
                .into_iter()
                .filter(|token| !generic_tokens_set.contains(token))
                .take(3)
                .collect::<Vec<_>>();
            if tokens.len() < 2 {
                continue;
            }
            let has_negation = negation_tokens_set.iter().any(|token| lowered.contains(token));
            let group = groups.entry(tokens.clone()).or_insert(EvidenceGroup {
                topic: tokens.join(", "),
                source_ids: Vec::new(),
                evidence: Vec::new(),
                has_conflict_signal: false,
            });
            if !group.source_ids.contains(&source.source_id) {
                group.source_ids.push(source.source_id.clone());
            }
            if group.evidence.len() < 4 {
                group.evidence.push(EvidenceItem {
                    source_id: source.source_id.clone(),
                    sentence: sentence.clone(),
                });
            }
            if has_negation {
                group.has_conflict_signal = true;
            }
        }
    }

    let mut ranked = groups
        .into_values()
        .filter(|item| item.source_ids.len() >= 2)
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .source_ids
            .len()
            .cmp(&left.source_ids.len())
            .then(right.evidence.len().cmp(&left.evidence.len()))
    });
    ranked.truncate(max_groups);
    serde_json::to_string(&ranked).map_err(|err| err.to_string())
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
fn clean_extracted_content(content: &str) -> String {
    let mut lines = Vec::new();
    let mut seen = HashSet::new();
    let drop_exact = [
        "cookie policy",
        "privacy policy",
        "terms of service",
        "all rights reserved",
        "subscribe",
        "sign up",
        "share this article",
    ]
    .into_iter()
    .collect::<HashSet<_>>();

    for raw_line in content.split(['\r', '\n']) {
        let cleaned = normalize_text_impl(raw_line);
        if cleaned.is_empty() {
            continue;
        }
        let lowered = cleaned.to_lowercase();
        if seen.contains(&cleaned) || drop_exact.contains(lowered.as_str()) {
            continue;
        }
        seen.insert(cleaned.clone());
        lines.push(cleaned);
    }
    lines.join("\n")
}

#[pyfunction]
fn build_snippet(content: &str, max_chars: usize) -> String {
    let normalized = clean_extracted_content(content);
    if normalized.is_empty() {
        return String::new();
    }

    let mut snippet_parts = Vec::new();
    let mut current_length = 0usize;
    for sentence in split_sentences(&normalized) {
        let cleaned = normalize_text_impl(&sentence);
        if cleaned.is_empty() {
            continue;
        }
        let next_length = current_length + cleaned.chars().count() + if snippet_parts.is_empty() { 0 } else { 1 };
        if next_length > max_chars {
            break;
        }
        snippet_parts.push(cleaned);
        current_length = next_length;
    }

    if !snippet_parts.is_empty() {
        let snippet = snippet_parts.join(" ");
        if snippet.chars().count() < normalized.chars().count() {
            return format!("{} ...", snippet.trim_end());
        }
        return snippet;
    }

    let prefix = normalized.chars().take(max_chars).collect::<String>();
    if normalized.chars().count() > max_chars {
        return format!("{} ...", prefix.trim_end());
    }
    prefix
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

#[pyfunction]
fn score_search_candidates(payload_json: &str) -> PyResult<String> {
    score_search_candidates_impl(payload_json).map_err(PyValueError::new_err)
}

#[pyfunction]
fn select_analyzer_sources(payload_json: &str) -> PyResult<String> {
    select_analyzer_sources_impl(payload_json).map_err(PyValueError::new_err)
}

#[pyfunction]
fn extract_evidence_groups(
    aggregated_data_json: &str,
    stopwords: Vec<String>,
    generic_tokens: Vec<String>,
    negation_tokens: Vec<String>,
    max_groups: usize,
) -> PyResult<String> {
    extract_evidence_groups_impl(
        aggregated_data_json,
        stopwords,
        generic_tokens,
        negation_tokens,
        max_groups,
    )
    .map_err(PyValueError::new_err)
}

#[pymodule]
fn multi_agent_search_native(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(normalize_text, module)?)?;
    module.add_function(wrap_pyfunction!(content_fingerprint, module)?)?;
    module.add_function(wrap_pyfunction!(clean_extracted_content, module)?)?;
    module.add_function(wrap_pyfunction!(build_snippet, module)?)?;
    module.add_function(wrap_pyfunction!(compact_source_content, module)?)?;
    module.add_function(wrap_pyfunction!(extract_used_source_ids, module)?)?;
    module.add_function(wrap_pyfunction!(sanitize_citations, module)?)?;
    module.add_function(wrap_pyfunction!(detect_conflicts, module)?)?;
    module.add_function(wrap_pyfunction!(select_top_candidates, module)?)?;
    module.add_function(wrap_pyfunction!(select_best_results, module)?)?;
    module.add_function(wrap_pyfunction!(score_search_candidates, module)?)?;
    module.add_function(wrap_pyfunction!(select_analyzer_sources, module)?)?;
    module.add_function(wrap_pyfunction!(extract_evidence_groups, module)?)?;
    Ok(())
}
