"""Declarative catalog and metadata for all multi-agent search agents."""

from src.domain import AgentMetadataItem

AGENTS_CATALOG: list[AgentMetadataItem] = [
    # ── Stage 1: Planning ─────────────────────────────────────────────────────
    AgentMetadataItem(
        id="clarifier",
        name="ClarifierAgent",
        stage="planning",
        role="Query Clarification & Ambiguity Resolver",
        trigger="Incoming user research request with ambiguous or short prompts",
        llm_model="deepseek-chat",
        inputs=["prompt", "user_context"],
        outputs=["clarification_needed", "suggested_followups"],
        source_file="src/agents/clarifier.py",
        line_number=15,
        description="Detects underspecified or ambiguous prompts, suggesting targeted clarifying questions to sharpen research focus before dispatch.",
        dependencies=[],
        system_prompt="""You are a research intake assistant. Given a research request, decide whether you need clarification to produce a focused plan.

Return ONLY a JSON array of 0 to 3 short clarifying questions (strings).
- If the request is already specific and clear, return [].
- Ask only high-value questions (scope, time period, geography, audience, desired depth) that would MATERIALLY change the research direction.
- Keep questions short and concrete. Write them in the user's language.
- No preamble, no markdown, no extra keys — just the JSON array.""",
        temperature=0.3,
        max_tokens=1024,
        context_window="64k tokens",
        response_format="JSON Array of Strings (`[\"...\"]`)",
        tools=["Pydantic JSON Validator", "Prompt Ambiguity Evaluator"],
        timeout_seconds=15,
        retry_policy="2 retries with exponential backoff (1s, 3s)",
        cache_ttl="6 hours (Redis)",
        example_input={
            "prompt": "Quantum computing advances",
            "user_context": {"locale": "ru", "depth": "medium"}
        },
        example_output={
            "clarification_needed": True,
            "suggested_followups": [
                "Вас интересуют достижения за 2024-2025 год или общий исторический срез?",
                "Фокусироваться на аппаратных реализациях (сверхпроводники, ионы) или алгоритмах?"
            ]
        }
    ),
    AgentMetadataItem(
        id="optimizer",
        name="PromptOptimizerAgent",
        stage="planning",
        role="Prompt Expansion & Perspective Synthesis",
        trigger="Initial research dispatch or post-clarification intake",
        llm_model="deepseek-chat",
        inputs=["prompt", "depth", "language"],
        outputs=["optimized_prompt", "angles", "hypotheses"],
        source_file="src/agents/optimizer.py",
        line_number=4,
        description="Enriches user prompts with academic, industry, and multi-angle perspectives, resolving implicit assumptions.",
        dependencies=["clarifier"],
        system_prompt="""You are an expert prompt engineer specializing in transforming raw, unclear user inputs into precise, well-structured prompts for AI systems.

## CRITICAL LANGUAGE RULE
- Detect the language of the user's input automatically.
- Your entire output MUST be in that same language — no exceptions.
- Do NOT translate. Do NOT switch languages. Mirror the input language exactly.

## YOUR TASK
Transform the given raw prompt by applying:
1. Error Correction — Fix grammar, spelling, and punctuation.
2. Clarity — Eliminate ambiguity, vague phrasing, and redundancy.
3. Structure — Organize into Role, Context, Task, Constraints, Output Format.
4. Completeness — Infer reasonable defaults for missing dimensions.

## OUTPUT RULES
- Output ONLY the improved prompt brief without meta-commentary.""",
        temperature=0.6,
        max_tokens=2048,
        context_window="64k tokens",
        response_format="Structured Analytical Prompt Brief",
        tools=["Language Detection (langdetect)", "Style Normalizer"],
        timeout_seconds=20,
        retry_policy="3 retries with backoff",
        cache_ttl="24 hours (Redis)",
        example_input={
            "prompt": "квантовые компьютеры 2025",
            "depth": "hard",
            "language": "ru"
        },
        example_output={
            "optimized_prompt": "Комплексный аналитический отчёт о коммерческих и научных прорывах в квантовых вычислениях за 2024-2025 годы...",
            "angles": [
                "Квантовое превосходство и квантовая коррекция ошибок (QEC)",
                "Коммерческие дорожные карты IBM, Google, Quantinuum",
                "Криптографические риски и постквантовая стандартизация NIST"
            ],
            "hypotheses": [
                "Логические кубиты с подавлением ошибок стали коммерчески воспроизводимыми в 2024-2025 гг."
            ]
        }
    ),
    AgentMetadataItem(
        id="orchestrator",
        name="OrchestratorAgent",
        stage="planning",
        role="Task Decomposition & Execution Planning",
        trigger="Post prompt optimization",
        llm_model="deepseek-chat",
        inputs=["optimized_prompt", "depth"],
        outputs=["subtasks", "queries", "execution_graph"],
        source_file="src/agents/orchestrator.py",
        line_number=11,
        description="Decomposes research goal into structured search tasks with specific search queries, domains, and parallel execution groups.",
        dependencies=["optimizer"],
        system_prompt="""You are the Lead Research Orchestrator. Given an optimized analytical research brief, decompose it into a directed acyclic graph (DAG) of distinct search subtasks with targeted keyword queries, domain constraints, and temporal filters. Balance breadth across subtasks and depth within technical queries.""",
        temperature=0.3,
        max_tokens=4096,
        context_window="128k tokens",
        response_format="JSON DAG (`SearchTask` graph with parallel dependencies)",
        tools=["Graphlib Topological Sorter", "Docs Framework Heuristics", "Search Depth Profiles"],
        timeout_seconds=30,
        retry_policy="3 retries with fallback model",
        cache_ttl="12 hours (Redis)",
        example_input={
            "optimized_prompt": "Квантовые вычисления 2025: логические кубиты и QEC",
            "depth": "hard"
        },
        example_output={
            "subtasks": [
                {"id": "st-1", "title": "Hardware QEC Architectures", "queries": ["neutral atom quantum error correction 2024 2025", "superconducting logical qubits surface code"]},
                {"id": "st-2", "title": "NIST Post-Quantum Cryptography", "queries": ["NIST post-quantum standards FIPS 203 204 205 implementation 2025"]}
            ],
            "execution_graph": {"parallel_groups": [["st-1", "st-2"]]}
        }
    ),
    AgentMetadataItem(
        id="cross_language",
        name="CrossLanguageAgent",
        stage="planning",
        role="Multilingual Query Expansion",
        trigger="Orchestrator planning pass if cross_language_enabled",
        llm_model="deepseek-chat",
        inputs=["queries", "target_languages"],
        outputs=["translated_queries", "language_specific_subtasks"],
        source_file="src/agents/cross_language.py",
        line_number=1,
        description="Generates native-language search queries in non-query languages (Russian, Chinese, German, Japanese) to tap regional sources.",
        dependencies=["orchestrator"],
        system_prompt="""You are a Multilingual Research Expansion Specialist. Identify key non-English terminology, technical acronyms, and regional keywords (Chinese, Russian, German, Japanese) to retrieve native documents from primary geographic sources and academic portals.""",
        temperature=0.4,
        max_tokens=2048,
        context_window="64k tokens",
        response_format="JSON Object with language mappings",
        tools=["FastText LangID", "Regional Corpus Dictionaries"],
        timeout_seconds=15,
        retry_policy="2 retries",
        cache_ttl="24 hours (Redis)",
        example_input={
            "queries": ["optical quantum computing roadmap 2025"],
            "target_languages": ["zh", "ru", "de"]
        },
        example_output={
            "translated_queries": {
                "zh": ["光量子计算 路线图 2025", "玻色采样 实用化进展"],
                "de": ["Photonisches Quantencomputing Entwicklung 2025"],
                "ru": ["фотонные квантовые процессоры дорожная карта"]
            }
        }
    ),

    # ── Stage 2: Search & Extraction ──────────────────────────────────────────
    AgentMetadataItem(
        id="search",
        name="SearchAgent",
        stage="search",
        role="Web Search & Native Content Extraction",
        trigger="Worker picks up search_task_job from Redis Queue",
        llm_model=None,
        inputs=["query", "depth", "backend"],
        outputs=["raw_snippets", "scraped_pages", "extracted_text"],
        source_file="src/agents/search.py",
        line_number=1,
        description="Executes parallel web searches against DuckDuckGo/SearXNG/Tavily with Rust-accelerated scraping and PDF binary parsing.",
        dependencies=["orchestrator", "cross_language"],
        system_prompt="Native Rust & Python parallel crawler executing high-concurrency HTTP/2 async search requests across multiple providers with automatic failover, domain rate-limiting, and binary DOM extraction.",
        temperature=None,
        max_tokens=None,
        context_window="Unlimited Streaming",
        response_format="Raw JSON Search Snippets & Full Cleaned Markdown",
        tools=["Tavily Search API", "SearXNG Self-Hosted Cluster", "DuckDuckGo Fallback", "Trafilatura DOM Scraper", "PyMuPDF Rust PDF Parser"],
        timeout_seconds=45,
        retry_policy="Per-provider circuit breaker (SearXNG -> Tavily -> DuckDuckGo)",
        cache_ttl="72 hours (Redis: search_snippet:{hash})",
        example_input={
            "query": "neutral atom quantum error correction 2025",
            "depth": "hard",
            "backend": "tavily"
        },
        example_output={
            "raw_snippets": 18,
            "scraped_pages": 12,
            "extracted_text_bytes": 142800
        }
    ),
    AgentMetadataItem(
        id="source_critic",
        name="SourceCriticAgent",
        stage="search",
        role="Low-Value Domain & SEO Spam Gatekeeper",
        trigger="Search result parsing pipeline",
        llm_model=None,
        inputs=["search_results", "urls"],
        outputs=["filtered_results", "rejection_reasons"],
        source_file="src/agents/source_critic.py",
        line_number=1,
        description="Rejects clickbait, content farms, parked domains, scraper sites, and automated AI spam pages before extraction.",
        dependencies=["search"],
        system_prompt="Heuristic and rule-based filter evaluating URL structures, domain suffixes, TLDs, and HTML metadata to purge spam, parked domains, SEO link-farms, and clickbait aggregator sites.",
        temperature=None,
        max_tokens=None,
        context_window="Batch In-Memory",
        response_format="Boolean Gate & Discard Log",
        tools=["Public Suffix List (psl)", "AdGuard DNS Blocklist", "Domain Age WHOIS Cache"],
        timeout_seconds=10,
        retry_policy="Fail-open with warning log",
        cache_ttl="7 days (Redis: domain_reputation)",
        example_input={
            "search_results": [
                {"url": "https://spam-tech-news.biz/quantum-fake", "title": "OMG Quantum!"},
                {"url": "https://nature.com/articles/s41586-024-qec", "title": "Logical quantum processor"}
            ]
        },
        example_output={
            "filtered_results": 1,
            "rejected_count": 1,
            "rejection_reasons": {"spam-tech-news.biz": "SEO farm keyword density > 18%"}
        }
    ),
    AgentMetadataItem(
        id="source_reputation",
        name="SourceReputationAgent",
        stage="search",
        role="Authority & Reliability Scorer",
        trigger="Post source-critic candidate evaluation",
        llm_model=None,
        inputs=["domain", "url", "tls_info"],
        outputs=["reputation_score", "authority_tier", "is_academic"],
        source_file="src/agents/source_reputation.py",
        line_number=1,
        description="Assigns trust weights to scientific repositories (.edu/.gov, PubMed, IEEE, arXiv) and vetted news outlets.",
        dependencies=["source_critic"],
        system_prompt="Mathematical authority scoring engine assigning credibility weights based on TLD (.edu, .gov), peer-reviewed indexing (PubMed, Crossref, IEEE, arXiv), TLS certificate transparency, and domain citations.",
        temperature=None,
        max_tokens=None,
        context_window="Deterministic Scorer",
        response_format="Float Score (0.0 to 1.0) & Tier Enum",
        tools=["OpenAlex Registry", "Crossref API", "Mozilla TLS Inspector"],
        timeout_seconds=12,
        retry_policy="Cached lookup fallback",
        cache_ttl="30 days (Redis)",
        example_input={
            "domain": "nature.com",
            "url": "https://nature.com/articles/s41586-024-qec"
        },
        example_output={
            "reputation_score": 0.98,
            "authority_tier": "TIER_1_PEER_REVIEWED",
            "is_academic": True
        }
    ),
    AgentMetadataItem(
        id="source_independence",
        name="SourceIndependenceAgent",
        stage="search",
        role="Syndication & Source Clustering",
        trigger="Result aggregation across subtasks",
        llm_model=None,
        inputs=["all_sources", "text_signatures"],
        outputs=["independent_clusters", "canonical_sources"],
        source_file="src/agents/source_independence.py",
        line_number=1,
        description="Deduplicates syndicated press releases (e.g. AP/Reuters re-publishers) using SimHash to prevent false consensus.",
        dependencies=["source_reputation"],
        system_prompt="MinHash / SimHash locality-sensitive clustering algorithm that groups identical syndicated press releases (AP, Reuters, PR Newswire) to prevent consensus distortion from multiple copycats.",
        temperature=None,
        max_tokens=None,
        context_window="Vectorized Corpus",
        response_format="Cluster Graph with Canonical Leader",
        tools=["SimHash Tokenizer", "Jaccard Distance Matrix"],
        timeout_seconds=15,
        retry_policy="Fast fallback to URL origin",
        cache_ttl="Session scoped",
        example_input={"all_sources_count": 28},
        example_output={
            "independent_clusters": 14,
            "canonical_sources": 14,
            "deduplicated_syndications": 14
        }
    ),
    AgentMetadataItem(
        id="evidence_mapper",
        name="EvidenceMapperAgent",
        stage="search",
        role="Evidence Chunking & Subtask Attribution",
        trigger="End of search stage",
        llm_model=None,
        inputs=["extracted_text", "subtasks"],
        outputs=["attributed_evidence_blocks", "coverage_matrix"],
        source_file="src/agents/evidence_mapper.py",
        line_number=1,
        description="Aligns extracted text paragraphs with specific subtask goals and hypotheses using dense embeddings.",
        dependencies=["source_independence"],
        system_prompt="Semantic segmenter splitting extracted full-texts into atomic claims (200-400 tokens) and computing cosine embedding distance against task objectives to build a clean attribution matrix.",
        temperature=None,
        max_tokens=None,
        context_window="Batch Embeddings",
        response_format="Indexed Attribution Matrix",
        tools=["FastEmbed (bge-small-en-v1.5)", "Memory HNSW Index"],
        timeout_seconds=25,
        retry_policy="Chunked batch retry",
        cache_ttl="Session scoped",
        example_input={"extracted_documents": 14, "subtasks": 4},
        example_output={
            "evidence_blocks_count": 92,
            "coverage_matrix": {"st-1": 28, "st-2": 31, "st-3": 19, "st-4": 14}
        }
    ),

    # ── Stage 3: Synthesis & Verification (LangGraph) ────────────────────────
    AgentMetadataItem(
        id="replan",
        name="ReplanAgent",
        stage="synthesis",
        role="Evidence Gap Analysis & Dynamic Re-Querying",
        trigger="LangGraph conditional edge on evidence evaluation",
        llm_model="deepseek-chat",
        inputs=["subtasks", "evidence_blocks", "gap_threshold"],
        outputs=["gap_detected", "new_search_queries"],
        source_file="src/agents/replan.py",
        line_number=1,
        description="Detects blind spots or unanswered subtasks; spawns targeted secondary search passes if coverage is below threshold.",
        dependencies=["evidence_mapper"],
        system_prompt="""You are the LangGraph Research Reflexion Agent. Inspect the collected evidence coverage across all research questions. If significant knowledge gaps or contradictory evidence exist, formulate 1 to 3 targeted rescue queries.""",
        temperature=0.2,
        max_tokens=2048,
        context_window="64k tokens",
        response_format="JSON Decision (`{ \"gap_detected\": bool, \"queries\": [...] }`)",
        tools=["LangGraph Conditional State Machine", "Coverage Threshold Engine"],
        timeout_seconds=20,
        retry_policy="2 retries, fallback: proceed to synthesis",
        cache_ttl="No cache (dynamic per-state)",
        example_input={
            "subtasks_count": 4,
            "coverage_scores": {"st-1": 0.92, "st-2": 0.41},
            "gap_threshold": 0.6
        },
        example_output={
            "gap_detected": True,
            "unanswered_aspects": ["FIPS 203 commercial deployment timeline"],
            "new_search_queries": ["FIPS 203 module lattice cryptography deployment deadline 2025"]
        }
    ),
    AgentMetadataItem(
        id="analyzer",
        name="AnalyzerAgent",
        stage="synthesis",
        role="Deep Multi-Perspective Synthesis",
        trigger="Finalize graph execution",
        llm_model="deepseek-reasoner",
        inputs=["attributed_evidence_blocks", "prompt", "depth"],
        outputs=["draft_report", "reasoning_steps", "key_findings"],
        source_file="src/agents/analyzer.py",
        line_number=1,
        description="Core reasoning engine synthesizing hundreds of citations into a coherent, structured report using chain-of-thought.",
        dependencies=["replan"],
        system_prompt="""You are DeepSeek-Reasoner, an elite research analyst. You synthesize hundreds of verified evidence snippets into an exhaustive, highly structured, evidence-grounded research report. You use chain-of-thought to cross-verify claims, detect nuanced trends, and attribute all statements using rigorous bracketed citations [1], [2]. Never extrapolate beyond cited facts.""",
        temperature=0.3,
        max_tokens=8192,
        context_window="128k tokens",
        response_format="Long-form Markdown with Evidence Footnotes",
        tools=["DeepSeek R1 Chain-of-Thought", "Evidence Context Builder", "Citation Injector"],
        timeout_seconds=180,
        retry_policy="3 retries with progressive context pruning",
        cache_ttl="No cache (generative synthesis)",
        example_input={
            "evidence_blocks": 92,
            "prompt": "Квантовые вычисления 2025",
            "depth": "hard"
        },
        example_output={
            "report_length_words": 3450,
            "citations_count": 42,
            "sections": [
                "Executive Summary",
                "Hardware Milestones",
                "Error Correction Benchmarks",
                "Commercial Implications"
            ]
        }
    ),
    AgentMetadataItem(
        id="numeric_check",
        name="NumericCheckAgent",
        stage="synthesis",
        role="Statistical & Quantitative Cross-Verification",
        trigger="Draft report generation",
        llm_model=None,
        inputs=["draft_report", "evidence_tables"],
        outputs=["numeric_discrepancies", "reconciled_metrics"],
        source_file="src/agents/numeric_check.py",
        line_number=1,
        description="Validates financial figures, percentages, dates, and units across multilingual sources against primary tables.",
        dependencies=["analyzer"],
        system_prompt="Regex and AST parser extracting all numeric metrics (dates, percentages, financial amounts, qubit counts) and cross-checking them against evidence source text to catch subtle LLM transposition errors.",
        temperature=None,
        max_tokens=None,
        context_window="In-Memory AST",
        response_format="Discrepancy Audit Log",
        tools=["Python AST Evaluator", "Units & Currency Normalizer", "Levenshtein Metric Matcher"],
        timeout_seconds=15,
        retry_policy="Fail-safe pass through with warning",
        cache_ttl="Session scoped",
        example_input={"draft_claims_with_numbers": 34},
        example_output={
            "numeric_discrepancies": [],
            "verified_figures_count": 34,
            "accuracy_rate": 1.0
        }
    ),
    AgentMetadataItem(
        id="claim_verifier",
        name="ClaimVerifierAgent",
        stage="synthesis",
        role="Fact-Checking & Hallucination Elimination",
        trigger="Post numerical validation",
        llm_model="deepseek-chat",
        inputs=["key_claims", "primary_sources"],
        outputs=["verified_claims", "unsupported_claims_flagged"],
        source_file="src/agents/claim_verifier.py",
        line_number=1,
        description="Verifies every factual assertion against primary cited sentences to eliminate LLM hallucinations.",
        dependencies=["numeric_check"],
        system_prompt="""You are a rigorous Fact-Checking Agent. For every key claim in the synthesized report, locate the exact quoted sentence in the primary scraped document. If a claim lacks direct textual substantiation, flag it as unverified.""",
        temperature=0.1,
        max_tokens=4096,
        context_window="64k tokens",
        response_format="JSON Claim Verification Matrix",
        tools=["Sentence Boundary Disambiguator", "NLI Entailment Classifier"],
        timeout_seconds=40,
        retry_policy="2 retries",
        cache_ttl="Session scoped",
        example_input={"claims_to_audit": 42},
        example_output={
            "verified_claims": 40,
            "flagged_unsupported": 2,
            "entailment_confidence": 0.952
        }
    ),
    AgentMetadataItem(
        id="citation_audit",
        name="CitationAuditAgent",
        stage="synthesis",
        role="Inline Citation Audit & URL Integrity",
        trigger="Verification stage",
        llm_model=None,
        inputs=["draft_report", "cited_urls"],
        outputs=["valid_citations", "broken_or_misleading_citations"],
        source_file="src/agents/citation_audit.py",
        line_number=1,
        description="Audits bracketed citations [1], [2] to ensure URL endpoints are live (HTTP 200) and match quoted text.",
        dependencies=["claim_verifier"],
        system_prompt="Audits inline bracketed citations [1], [2] to ensure URL endpoints are live (HTTP 200), do not point to error 404 pages, and match the referenced publication title.",
        temperature=None,
        max_tokens=None,
        context_window="Async HTTP Inspector",
        response_format="Citation Validation Log",
        tools=["Async HTTP Head Checker", "Canonical URL Resolver"],
        timeout_seconds=20,
        retry_policy="2 retries on network timeout",
        cache_ttl="24 hours",
        example_input={"cited_urls_count": 26},
        example_output={
            "valid_citations": 26,
            "broken_links": 0,
            "redirects_resolved": 3
        }
    ),
    AgentMetadataItem(
        id="retraction",
        name="RetractionAgent",
        stage="synthesis",
        role="Academic Retraction Watchdog",
        trigger="Citation audit step if retraction_check_enabled",
        llm_model=None,
        inputs=["dois", "academic_urls"],
        outputs=["retracted_sources", "disclaimer_notices"],
        source_file="src/agents/retraction.py",
        line_number=1,
        description="Queries Crossref and Retraction Watch databases to flag retracted papers or Expressions of Concern.",
        dependencies=["citation_audit"],
        system_prompt="Academic integrity verification against Crossref Retraction Watch database and PubMed retraction notices. Checks DOI metadata to ensure no cited research paper has been retracted or placed under Expression of Concern.",
        temperature=None,
        max_tokens=None,
        context_window="API Client",
        response_format="Retraction Alert Report",
        tools=["Crossref Retraction Watch API", "OpenAlex Retraction Index"],
        timeout_seconds=15,
        retry_policy="Cache fallback",
        cache_ttl="30 days",
        example_input={
            "dois": ["10.1038/s41586-024-07100-3", "10.1103/PhysRevLett.132.050601"]
        },
        example_output={
            "retracted_sources": [],
            "clean_dois": 2,
            "notices_count": 0
        }
    ),
    AgentMetadataItem(
        id="red_team",
        name="RedTeamAgent",
        stage="synthesis",
        role="Adversarial Stress Testing & Counter-Arguments",
        trigger="HARD / deep research depth",
        llm_model="deepseek-chat",
        inputs=["draft_report", "main_thesis"],
        outputs=["counter_arguments", "stress_test_critique"],
        source_file="src/agents/red_team.py",
        line_number=1,
        description="Attacks the draft report's assumptions with devil's advocate arguments to identify blind spots and cognitive biases.",
        dependencies=["claim_verifier"],
        system_prompt="""You are an Adversarial Red Team Analyst. Your job is to aggressively challenge the report's core theses: find unstated assumptions, spotlight conflicting research, identify corporate marketing exaggeration, and articulate strong counter-theses.""",
        temperature=0.7,
        max_tokens=3072,
        context_window="64k tokens",
        response_format="Structured Counter-Argument Section",
        tools=["Cognitive Bias Heuristics", "Contrarian Angle Generator"],
        timeout_seconds=35,
        retry_policy="2 retries",
        cache_ttl="No cache",
        example_input={
            "main_thesis": "Квантовые компьютеры на нейтральных атомах превзойдут сверхпроводники к 2026 году"
        },
        example_output={
            "counter_arguments": [
                "Медленное время двухкубитовых гейтов (микросекунды против наносекунд)",
                "Сложность лазерной адресации тысяч отдельных оптических пинцетов"
            ]
        }
    ),
    AgentMetadataItem(
        id="stance",
        name="StanceAgent",
        stage="synthesis",
        role="Consensus & Divergent Viewpoint Taxonomy",
        trigger="Controversial or multi-faceted topic synthesis",
        llm_model="deepseek-chat",
        inputs=["evidence_blocks", "source_stances"],
        outputs=["consensus_level", "minority_perspectives", "conflicts"],
        source_file="src/agents/stance.py",
        line_number=1,
        description="Categorizes findings into scientific consensus, disputed areas, and emerging minority views.",
        dependencies=["red_team"],
        system_prompt="""You are a Consensus & Controversy Classifier. You classify expert opinions into Consensus, Debated Frontier, or Emerging Minority Stances to prevent false balance and accurately represent the state of scientific debate.""",
        temperature=0.2,
        max_tokens=2048,
        context_window="64k tokens",
        response_format="JSON Consensus Matrix",
        tools=["Stance Detection Classifier", "Viewpoint Diversity Scorer"],
        timeout_seconds=25,
        retry_policy="2 retries",
        cache_ttl="Session scoped",
        example_input={"topic": "Timeline to commercial fault-tolerant quantum advantage"},
        example_output={
            "consensus_level": "DEBATED",
            "majority_view": "2029-2032 (58% of surveyed experts)",
            "minority_view": "2026-2027 (18% industry labs), >2035 (24% academic sceptics)"
        }
    ),
    AgentMetadataItem(
        id="report_critic",
        name="ReportCriticAgent",
        stage="synthesis",
        role="Editorial Polish, Tone & Brevity Refinement",
        trigger="Final editorial pass if report_editor_enabled",
        llm_model="deepseek-chat",
        inputs=["draft_report", "style_guide"],
        outputs=["polished_report", "executive_summary"],
        source_file="src/agents/report_critic.py",
        line_number=1,
        description="Streamlines prose, applies inverted-pyramid structure, and eliminates redundant filler and buzzwords.",
        dependencies=["stance", "retraction"],
        system_prompt="""You are the Executive Managing Editor. You polish the draft report according to the Inverted Pyramid principle: crisp executive summary first, followed by key evidence sections, with fluff, buzzwords, and redundant phrases aggressively removed.""",
        temperature=0.3,
        max_tokens=8192,
        context_window="128k tokens",
        response_format="Final Polished Publication Markdown",
        tools=["Flesch-Kincaid Readability Analyzer", "Style Guide Enforcer"],
        timeout_seconds=60,
        retry_policy="2 retries",
        cache_ttl="No cache",
        example_input={
            "draft_report_length": 3450,
            "style_guide": "executive_analytical"
        },
        example_output={
            "polished_report_length": 3180,
            "readability_grade": 11.2,
            "compression_ratio": "0.92"
        }
    ),
    AgentMetadataItem(
        id="confidence",
        name="ConfidenceAgent",
        stage="synthesis",
        role="Calibrated Confidence Scoring & Risk Matrix",
        trigger="Pre-finalization",
        llm_model=None,
        inputs=["source_scores", "verification_results", "stance_diversity"],
        outputs=["confidence_score", "trust_indicators", "audit_report"],
        source_file="src/agents/confidence.py",
        line_number=1,
        description="Computes deterministic confidence score (0-100%) and Trust & Transparency audit badge.",
        dependencies=["report_critic"],
        system_prompt="Deterministic Trust & Confidence Scoring Engine calculating an objective score (0-100%) based on source reputation weights, citation verification ratio, consensus alignment, and data freshness.",
        temperature=None,
        max_tokens=None,
        context_window="Pure Math Calculation",
        response_format="Confidence DTO (`score: int, level: str, breakdown: dict`)",
        tools=["Bayesian Evidence Weighting", "Trust & Transparency Badge Generator"],
        timeout_seconds=5,
        retry_policy="Deterministic (instant)",
        cache_ttl="Permanent for report ID",
        example_input={
            "verified_sources": 26,
            "unsupported_claims": 0,
            "avg_source_score": 0.94
        },
        example_output={
            "confidence_score": 96,
            "confidence_level": "HIGH",
            "trust_indicators": {
                "source_authority": "98%",
                "fact_verification": "100%",
                "freshness": "92%"
            }
        }
    ),

    # ── Stage 4: Delivery & Follow-Up ─────────────────────────────────────────
    AgentMetadataItem(
        id="chat",
        name="ChatAgent",
        stage="delivery",
        role="Interactive Follow-Up Q&A & Deep-Dive Assistant",
        trigger="User chat message on finalized research",
        llm_model="deepseek-chat",
        inputs=["final_report", "evidence_cache", "user_question"],
        outputs=["grounded_response", "referenced_citations"],
        source_file="src/agents/chat.py",
        line_number=1,
        description="Answers interactive user questions grounded strictly in the report's gathered evidence pool.",
        dependencies=["confidence"],
        system_prompt="""You are the Interactive Research Assistant. You answer user questions strictly adhering to the verified findings and evidence pool in the research report. If the evidence does not contain the answer, state that clearly instead of speculating.""",
        temperature=0.4,
        max_tokens=4096,
        context_window="64k tokens",
        response_format="Interactive Markdown with Clickable Citations",
        tools=["Report Evidence Vector Index", "Grounding Guardrail"],
        timeout_seconds=30,
        retry_policy="Stream reconnect on dropped socket",
        cache_ttl="Session scoped",
        example_input={
            "user_question": "Какие именно алгоритмы коррекции ошибок использует Quantinuum?",
            "report_id": "res-921"
        },
        example_output={
            "grounded_response": "Согласно разделу 2 отчёта, Quantinuum в сотрудничестве с Microsoft использует цветовые коды (color codes) поверх кубитов на ионных ловушках...",
            "referenced_citations": ["[12]", "[15]"]
        }
    ),
]


def get_agents_catalog() -> list[AgentMetadataItem]:
    """Return the complete metadata catalog for all multi-agent search agents."""
    return AGENTS_CATALOG
