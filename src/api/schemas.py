from pydantic import BaseModel, Field, model_validator
from enum import Enum
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

class SearchDepth(str, Enum):
    EASY = "easy"
    MEDIUM = "medium"
    HARD = "hard"

class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class ResearchStatus(str, Enum):
    QUEUED = "queued"
    CLARIFYING = "clarifying"
    PLAN_REVIEW = "plan_review"
    PROCESSING = "processing"
    ANALYZING = "analyzing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class FinalizeJobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


class SearchJobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"

class OptimizeRequest(BaseModel):
    prompt: str = Field(..., description="The original user prompt to optimize", min_length=1)

class OptimizeResponse(BaseModel):
    optimized_prompt: str
    status: str = "success"

class DecomposeRequest(BaseModel):
    prompt: str = Field(..., description="The complex query to decompose")
    depth: SearchDepth = Field(default=SearchDepth.EASY, description="Depth of the search (easy, medium, hard)")

class SearchTaskMetrics(BaseModel):
    candidate_count: int = 0
    extraction_attempts: int = 0
    extraction_success_count: int = 0
    extraction_failure_count: int = 0
    selected_source_count: int = 0
    avg_content_chars: float = 0.0

class SearchTask(BaseModel):
    id: str
    research_id: Optional[str] = None
    description: str
    queries: List[str]
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    result: Optional[List[Dict[str, Any]]] = None
    logs: List[str] = Field(default_factory=list)
    search_metrics: SearchTaskMetrics = Field(default_factory=SearchTaskMetrics)


class SearchSourcePreview(BaseModel):
    url: str
    title: Optional[str] = None
    domain: Optional[str] = None
    source_quality: Optional[str] = None
    extraction_status: Optional[str] = None
    snippet: Optional[str] = None


class SearchTaskSummary(BaseModel):
    id: str
    research_id: Optional[str] = None
    description: str
    queries: List[str]
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime
    updated_at: datetime
    result_count: int = 0
    log_count: int = 0
    recent_logs: List[str] = Field(default_factory=list)
    source_preview: List[SearchSourcePreview] = Field(default_factory=list)
    search_metrics: SearchTaskMetrics = Field(default_factory=SearchTaskMetrics)
    latest_search_job: Optional["SearchTaskJob"] = None


class SourceCriticSummary(BaseModel):
    total_sources: int = 0
    high_confidence_sources: int = 0
    medium_confidence_sources: int = 0
    low_confidence_sources: int = 0
    primary_sources: int = 0
    editorial_sources: int = 0
    community_sources: int = 0
    speculative_sources: int = 0
    flagged_sources: int = 0
    dominant_domains: List[str] = Field(default_factory=list)


class EvidenceCoverageSummary(BaseModel):
    evidence_group_count: int = 0
    multi_source_group_count: int = 0
    weak_group_count: int = 0
    avg_sources_per_group: float = 0.0


class ClaimVerificationSummary(BaseModel):
    uncited_lines: int = 0
    unsupported_lines: int = 0
    insufficient_evidence_lines: int = 0
    downgraded_lines: int = 0
    verification_notes: List[str] = Field(default_factory=list)


class ConfidenceFinding(BaseModel):
    """A corroborated finding with its source support level (per-claim confidence)."""
    statement: str = ""
    support_level: str = "weak"  # strong | medium | weak
    source_count: int = 0
    source_ids: List[str] = Field(default_factory=list)


class PlanCoverageItem(BaseModel):
    """One plan sub-question and whether the final report addresses it."""
    question: str = ""
    covered: bool = False
    match_ratio: float = 0.0


class VerificationReport(BaseModel):
    """P3 verifier output: per-claim confidence, plan coverage, and open gaps.

    Recomputed cheaply (no LLM) from the finalized report, the task plan and the
    source pool — mirrors how conflicts are derived on demand.
    """
    research_id: str
    findings: List[ConfidenceFinding] = Field(default_factory=list)
    plan_coverage: List[PlanCoverageItem] = Field(default_factory=list)
    uncovered_questions: List[str] = Field(default_factory=list)
    coverage_ratio: float = 0.0
    claim_verification: ClaimVerificationSummary = Field(default_factory=ClaimVerificationSummary)


class RedTeamFinding(BaseModel):
    """One load-bearing claim that was stress-tested against counter-evidence."""
    claim: str = ""
    verdict: str = "holds"  # refuted | contested | qualified | holds
    challenge: str = ""     # the counter-argument / what the report under-weights (or why it held)
    source_urls: List[str] = Field(default_factory=list)


class RedTeamReport(BaseModel):
    """Adversarial pass: the report's key claims searched for refutation, then judged."""
    research_id: str = ""
    findings: List[RedTeamFinding] = Field(default_factory=list)
    challenged: int = 0  # refuted + contested + qualified
    held: int = 0        # claims that survived the attack


class CitationGround(BaseModel):
    """Per-source grounding for inline hover: the best passage in the cited source."""
    source_id: str = ""
    url: str = ""
    title: str = ""
    quote: str = ""
    supported: bool = True


class CitationAudit(BaseModel):
    """Deterministic citation check: does each [Sn] source's text actually match the claim?

    Lexical overlap between the cited sentence and the source content — flags citations
    whose source doesn't even mention the claim's terms (the big Gemini/Perplexity failure).
    """
    research_id: str = ""
    total: int = 0          # inline citations checked
    supported: int = 0      # citations whose source text matches the claim
    integrity: float = 0.0  # supported / total
    unsupported_claims: List[str] = Field(default_factory=list)
    grounding: List[CitationGround] = Field(default_factory=list)


class OriginCluster(BaseModel):
    """A set of sources that are NOT independent of each other.

    Either the same outlet (multiple articles from one domain) or the same reprinted
    text spread across several domains (a wire story / syndication).
    """
    label: str = ""               # representative domain or title
    kind: str = "unique"          # unique | single-domain | syndicated
    size: int = 0
    domains: List[str] = Field(default_factory=list)
    source_ids: List[str] = Field(default_factory=list)


class SourceIndependence(BaseModel):
    """How many INDEPENDENT origins the cited sources really represent (echo-chamber detector).

    Clusters sources that are the same outlet or the same reprinted text, so a report with
    "12 sources" that are really 3 origins is exposed — the circular-sourcing failure that
    Perplexity/Gemini hide by presenting every citation as if it were independent corroboration.
    """
    research_id: str = ""
    total_sources: int = 0
    independent_origins: int = 0
    independence_score: float = 0.0     # origins / total (1.0 = every source independent)
    dominant_origin_share: float = 0.0  # largest cluster / total (1.0 = one origin behind everything)
    clusters: List[OriginCluster] = Field(default_factory=list)  # only the non-trivial (size>1) echo groups
    echo_warnings: List[str] = Field(default_factory=list)


class ConfidenceComponent(BaseModel):
    """One transparent input to the overall confidence score (so the number isn't a black box)."""
    key: str = ""        # citations | corroboration | resilience | independence
    score: float = 0.0   # 0..1
    weight: float = 0.0  # 0..1 (weights of present components sum to 1)
    detail: str = ""     # short human note, e.g. "8/10 citations matched source"


class ConfidenceClaim(BaseModel):
    """A load-bearing finding with its fused confidence band."""
    statement: str = ""
    band: str = "solid"           # solid | contested | speculative
    support_level: str = "weak"   # original verification level (strong|medium|weak)
    source_ids: List[str] = Field(default_factory=list)
    note: str = ""                # why it was downgraded (red-team / weak grounding / single origin)


class ConfidenceReport(BaseModel):
    """Honesty meter: fuses citation grounding, claim verification, red-team and source
    independence into one calibrated confidence — the single trust number Perplexity/Gemini
    never show. Transparent: every component that feeds the score is listed.
    """
    research_id: str = ""
    overall: float = 0.0          # 0..1 weighted blend of the components
    grade: str = "low"            # high | medium | low
    total_claims: int = 0
    solid: int = 0                # claim counts by band (the "71/20/9" headline)
    contested: int = 0
    speculative: int = 0
    components: List[ConfidenceComponent] = Field(default_factory=list)
    claims: List[ConfidenceClaim] = Field(default_factory=list)


class NumericClaim(BaseModel):
    """One quantitative claim and whether its cited source actually contains the figure."""
    value: str = ""        # the figure as written, e.g. "40%" / "$2.3B" / "2019"
    subject: str = ""      # short context — what the number measures
    source_id: str = ""    # the [Sn] it was cited against
    sentence: str = ""     # the claim sentence (trimmed)


class NumericContradiction(BaseModel):
    """Two figures in the report that describe the same quantity but disagree."""
    subject: str = ""
    values: List[str] = Field(default_factory=list)
    sentences: List[str] = Field(default_factory=list)


class NumericCheck(BaseModel):
    """Deterministic figure check: is every number traceable to its cited source, and is the
    report internally consistent? Catches the classic LLM failure of mangling/​inventing a
    statistic — which neither Perplexity nor Gemini verifies against the source.
    """
    research_id: str = ""
    total: int = 0          # figures checked against a cited source
    supported: int = 0      # figures found in the cited source
    integrity: float = 0.0  # supported / total
    unsupported: List[NumericClaim] = Field(default_factory=list)         # figure not in source
    contradictions: List[NumericContradiction] = Field(default_factory=list)


class StanceSource(BaseModel):
    """One source's stance toward the question's central proposition."""
    source_id: str = ""
    stance: str = "neutral"   # supports | opposes | neutral


class StanceBalance(BaseModel):
    """Viewpoint balance: how the cited evidence splits for vs against the question's central
    claim — surfaces a one-sided report even when it's well-cited. Complements source
    independence (origin diversity) with *viewpoint* diversity.
    """
    research_id: str = ""
    applicable: bool = False        # only contestable/opinion-shaped questions are assessed
    proposition: str = ""           # the central claim stances are measured against
    supports: int = 0
    opposes: int = 0
    neutral: int = 0
    dominant_side: str = ""         # supports | opposes | balanced
    skew: float = 0.0               # max(supports,opposes)/(supports+opposes); 0.5 = balanced
    sources: List[StanceSource] = Field(default_factory=list)


class LanguageCount(BaseModel):
    lang: str = ""
    count: int = 0


class CrossLanguageFinding(BaseModel):
    lang: str = ""
    finding: str = ""


class CrossLanguageReport(BaseModel):
    """Language diversity of the evidence: which languages the sources span, whether the
    research is stuck in a single-language bubble, and what non-query-language sources uniquely
    add — coverage no monolingual tool surfaces.
    """
    research_id: str = ""
    query_language: str = ""
    languages: List[LanguageCount] = Field(default_factory=list)
    target_languages: List[str] = Field(default_factory=list)  # languages we deliberately searched
    foreign_source_count: int = 0
    monolingual: bool = True
    unique_findings: List[CrossLanguageFinding] = Field(default_factory=list)


class IntegrityFlag(BaseModel):
    """A cited source backed by a paper Crossref/Retraction Watch records as retracted."""
    source_id: str = ""
    doi: str = ""
    kind: str = ""     # retraction | concern
    detail: str = ""


class SourceIntegrity(BaseModel):
    """Retraction check: cited DOIs looked up against Crossref/Retraction Watch — flags a claim
    resting on a retracted paper (or one under an expression of concern). The single most
    damning source problem, and one no competitor surfaces inline.
    """
    research_id: str = ""
    checked_dois: int = 0
    retracted_count: int = 0
    flagged: List[IntegrityFlag] = Field(default_factory=list)


class ReputationFlag(BaseModel):
    """A cited source whose domain is on the transparent low-credibility / bias list."""
    source_id: str = ""
    domain: str = ""
    category: str = ""   # satire | fabricated | conspiracy | state_media
    reason: str = ""


class SourceReputation(BaseModel):
    """Domain-credibility check: flags cited sources from satire, fabricated/fake-news,
    conspiracy or state-controlled domains against a transparent bundled list — the kind of
    'your source is a known hoax site' signal Perplexity/Gemini never surface inline.
    """
    research_id: str = ""
    total_sources: int = 0
    flagged_count: int = 0
    categories: List[str] = Field(default_factory=list)   # distinct categories present
    flagged: List[ReputationFlag] = Field(default_factory=list)


class AuditQuery(BaseModel):
    """One planned sub-question and the search queries actually issued for it."""
    task: str = ""
    queries: List[str] = Field(default_factory=list)
    status: str = ""
    result_count: int = 0


class AuditSource(BaseModel):
    """A source the report could cite, with the [Sn] id the report numbered it by."""
    source_id: str = ""
    url: str = ""
    domain: str = ""
    title: str = ""
    source_quality: str = ""
    extraction_status: str = ""


class AuditStep(BaseModel):
    """One finalize-graph execution step (collect_context / replan / analyze / verify / …)."""
    step: str = ""
    detail: str = ""
    timestamp: str = ""


class AuditTrail(BaseModel):
    """Reproducible provenance — every sub-question, search query, fetched source and graph
    decision behind the report. A 'show your work' artifact a fact-checker can audit or
    reproduce; assembled deterministically from what the pipeline already records.
    """
    research_id: str = ""
    prompt: str = ""
    model: str = ""
    depth: str = ""
    status: str = ""
    created_at: str = ""
    completed_at: str = ""
    plan: List[str] = Field(default_factory=list)                # the sub-questions
    queries: List[AuditQuery] = Field(default_factory=list)      # queries issued per task
    sources: List[AuditSource] = Field(default_factory=list)
    steps: List[AuditStep] = Field(default_factory=list)         # finalize-graph execution trail
    decisions: List[str] = Field(default_factory=list)           # replan/tie-break/analyze/red-team etc.
    token_usage: Dict[str, Any] = Field(default_factory=dict)
    source_count: int = 0
    query_count: int = 0


class ShareInfo(BaseModel):
    """Public-share state for a research: whether a link exists and its (opaque) token."""
    shared: bool = False
    token: str = ""


class PublicReport(BaseModel):
    """Read-only, strictly-scoped payload for a public share link. Carries the report and its
    trust layer (the point of sharing) — never the owner, internal graph_state, or the token.
    """
    prompt: str = ""
    final_report: str = ""
    depth: str = ""
    model: str = ""
    created_at: str = ""
    sources: List[SearchSourcePreview] = Field(default_factory=list)
    citations: CitationAudit = Field(default_factory=CitationAudit)
    confidence: ConfidenceReport = Field(default_factory=ConfidenceReport)
    source_independence: SourceIndependence = Field(default_factory=SourceIndependence)
    source_reputation: SourceReputation = Field(default_factory=SourceReputation)
    numeric_check: NumericCheck = Field(default_factory=NumericCheck)
    stance: StanceBalance = Field(default_factory=StanceBalance)
    red_team: RedTeamReport = Field(default_factory=RedTeamReport)
    source_integrity: SourceIntegrity = Field(default_factory=SourceIntegrity)
    cross_language: CrossLanguageReport = Field(default_factory=CrossLanguageReport)


class ResearchWatch(BaseModel):
    """A standing 'watch this question': periodic auto re-runs + an alert when the answer
    materially changes. The watch follows the thread head — each scheduled re-run creates a
    new run and the watch moves to it. Turns one-shot research into a monitor.
    """
    research_id: str = ""        # current head of the watched thread
    enabled: bool = False
    interval_seconds: int = 0
    next_run_at: str = ""        # ISO; when the next re-run is due
    last_run_at: str = ""        # ISO; when a scheduled re-run last fired
    last_change_at: str = ""     # ISO; when a re-run last produced a material change
    acknowledged_at: str = ""    # ISO; user dismissed the change badge
    runs: int = 0                # scheduled re-runs fired so far
    has_unseen_change: bool = False  # derived: last_change_at later than acknowledged_at


class WatchRequest(BaseModel):
    """Enable/disable a watch and (optionally) set its cadence."""
    enabled: bool = True
    interval_seconds: Optional[int] = None


class AppExportRequest(BaseModel):
    """Free-text design brief for the AI-generated custom HTML export."""
    prompt: str = Field(default="", max_length=4000)


class ComparisonCell(BaseModel):
    """One (option × criterion) value with the source ids that back it."""
    option: str = ""
    value: str = ""
    source_ids: List[str] = Field(default_factory=list)


class ComparisonRow(BaseModel):
    criterion: str = ""
    cells: List[ComparisonCell] = Field(default_factory=list)


class ComparisonTable(BaseModel):
    """Structured side-by-side comparison extracted from the report (for 'compare X vs Y')."""
    research_id: str = ""
    options: List[str] = Field(default_factory=list)   # column headers — the things compared
    rows: List[ComparisonRow] = Field(default_factory=list)  # criteria
    recommendation: str = ""

    @property
    def has_table(self) -> bool:
        return len(self.options) >= 2 and len(self.rows) >= 1


class DiffClaim(BaseModel):
    """A claim whose confidence shifted between two runs of the same research."""
    statement: str = ""
    old_level: str = ""
    new_level: str = ""


class ResearchDiff(BaseModel):
    """What changed vs the previous run of this research (living-research diff)."""
    research_id: str = ""
    compared_to: str = ""   # parent research id
    compared_at: str = ""   # parent run timestamp (ISO)
    new_claims: List[str] = Field(default_factory=list)
    dropped_claims: List[str] = Field(default_factory=list)
    shifted_claims: List[DiffClaim] = Field(default_factory=list)
    new_sources: int = 0
    new_domains: List[str] = Field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(self.new_claims or self.dropped_claims or self.shifted_claims or self.new_sources)


class ReplanRecommendation(BaseModel):
    reason: str
    suggested_queries: List[str] = Field(default_factory=list)


class GraphExecutionSummary(BaseModel):
    branching_active: bool = False
    follow_up_task_count: int = 0
    replan_task_count: int = 0
    tie_break_task_count: int = 0
    follow_up_query_count: int = 0
    follow_up_queries: List[str] = Field(default_factory=list)


class TaskUpdate(BaseModel):
    status: Optional[TaskStatus] = None
    result: Optional[List[Dict[str, Any]]] = None
    log: Optional[str] = None
    search_metrics: Optional[SearchTaskMetrics] = None

class DecomposeResponse(BaseModel):
    tasks: List[SearchTask]
    depth: SearchDepth

class ResearchRequest(BaseModel):
    prompt: str = Field(..., description="The goal or topic of the research", min_length=5, max_length=100000)
    depth: SearchDepth = Field(default=SearchDepth.EASY)
    webhook_url: Optional[str] = Field(
        default=None,
        description="Optional URL to POST when research completes (JSON: {research_id, status})",
    )
    model: Optional[str] = Field(
        default=None,
        description="Optional model id from the catalog (GET /v1/models); validated server-side, falls back to default",
    )
    plan_first: bool = Field(
        default=False,
        description="If true, generate an editable research plan and wait for approval before searching",
    )
    thread_id: Optional[str] = Field(
        default=None,
        description="Optional conversation thread to attach this research to; a new one is created when omitted",
    )

class RegisterRequest(BaseModel):
    email: str = Field(..., min_length=3, max_length=200)
    password: str = Field(..., min_length=6, max_length=200)


class SetPasswordRequest(BaseModel):
    password: str = Field(..., min_length=6, max_length=200)


class LoginRequest(BaseModel):
    email: str = Field(..., min_length=3, max_length=200)
    password: str = Field(..., min_length=1, max_length=200)


class AuthUser(BaseModel):
    id: str
    email: str
    name: Optional[str] = None
    avatar_url: Optional[str] = None


class AuthSession(BaseModel):
    """Login/register response: a Bearer JWT plus the authenticated user.

    The token is also set as an httpOnly cookie; API clients can use ``access_token``
    as ``Authorization: Bearer <token>``.
    """
    access_token: str
    token_type: str = "bearer"
    user: AuthUser


class UserRecord(BaseModel):
    id: str
    email: str
    password_hash: str
    name: Optional[str] = None
    avatar_url: Optional[str] = None


class ResearchRecord(BaseModel):
    id: str
    prompt: str
    user_id: Optional[str] = None
    depth: SearchDepth
    status: ResearchStatus = ResearchStatus.PROCESSING
    task_ids: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    final_report: Optional[str] = None
    graph_state: Dict[str, Any] = Field(default_factory=dict)
    graph_trail: List[Dict[str, Any]] = Field(default_factory=list)


class ResearchSummary(BaseModel):
    id: str
    prompt: str
    depth: SearchDepth
    status: ResearchStatus = ResearchStatus.PROCESSING
    task_ids: List[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    has_final_report: bool = False
    partial_report: Optional[str] = None
    task_count: int = 0
    completed_tasks: int = 0
    pending_tasks: int = 0
    running_tasks: int = 0
    failed_tasks: int = 0
    collected_sources: int = 0
    avg_sources_per_task: float = 0.0
    total_candidates: int = 0
    total_extraction_attempts: int = 0
    total_extraction_success_count: int = 0
    total_extraction_failure_count: int = 0
    total_selected_source_count: int = 0
    finalize_ready: bool = False
    source_critic_summary: SourceCriticSummary = Field(default_factory=SourceCriticSummary)
    evidence_coverage_summary: EvidenceCoverageSummary = Field(default_factory=EvidenceCoverageSummary)
    claim_verification_summary: ClaimVerificationSummary = Field(default_factory=ClaimVerificationSummary)
    replan_recommendations: List[ReplanRecommendation] = Field(default_factory=list)
    graph_execution_summary: GraphExecutionSummary = Field(default_factory=GraphExecutionSummary)
    latest_finalize_job: Optional["ResearchFinalizeJob"] = None
    tasks: List[SearchTaskSummary] = Field(default_factory=list)
    llm_token_usage: Dict[str, Any] = Field(default_factory=dict)


class ResearchStatusSummary(BaseModel):
    """Cheap status snapshot — no source-critic/evidence/claim/replan/LLM work.

    Use this for polling; the full ResearchSummary runs heavy analysis on demand.
    """
    id: str
    prompt: str
    depth: SearchDepth
    status: ResearchStatus = ResearchStatus.PROCESSING
    created_at: datetime
    updated_at: datetime
    has_final_report: bool = False
    partial_report: Optional[str] = None
    task_count: int = 0
    completed_tasks: int = 0
    pending_tasks: int = 0
    running_tasks: int = 0
    failed_tasks: int = 0
    collected_sources: int = 0
    avg_sources_per_task: float = 0.0
    finalize_ready: bool = False
    latest_finalize_job: Optional["ResearchFinalizeJob"] = None
    llm_token_usage: Dict[str, Any] = Field(default_factory=dict)
    queue_position: Optional[int] = None  # 1-based position when status == queued


class ResearchConflict(BaseModel):
    topic: str = ""
    reason: Optional[str] = None
    source_ids: List[str] = Field(default_factory=list)
    sentences: List[str] = Field(default_factory=list)


class ResearchPlanItem(BaseModel):
    id: str
    description: str = ""
    queries: List[str] = Field(default_factory=list)


class ResearchPlan(BaseModel):
    research_id: str
    status: ResearchStatus
    items: List[ResearchPlanItem] = Field(default_factory=list)


class ResearchPlanUpdate(BaseModel):
    items: List[ResearchPlanItem]


class ChatMessage(BaseModel):
    role: str  # "user" | "assistant"
    content: str


class ChatAsk(BaseModel):
    question: str = Field(..., min_length=1, max_length=8000)


class Clarification(BaseModel):
    research_id: str
    status: ResearchStatus
    questions: List[str] = Field(default_factory=list)
    answers: List[str] = Field(default_factory=list)


class ClarifyAnswers(BaseModel):
    answers: List[str] = Field(default_factory=list)


class ResearchReportResponse(BaseModel):
    research_id: str
    status: ResearchStatus
    final_report: Optional[str] = None


class ResearchGraphResponse(BaseModel):
    research_id: str
    status: ResearchStatus
    graph_state: Dict[str, Any] = Field(default_factory=dict)
    graph_trail: List[Dict[str, Any]] = Field(default_factory=list)

class ResearchHistoryItem(BaseModel):
    id: str
    prompt: str
    title: Optional[str] = None
    thread_id: Optional[str] = None
    depth: SearchDepth
    status: ResearchStatus
    created_at: datetime
    updated_at: datetime
    has_final_report: bool = False


class ResearchRename(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)


class ResearchResponse(BaseModel):
    research_id: str
    status: str
    message: str
    thread_id: Optional[str] = None


class ResearchFinalizeJob(BaseModel):
    id: str
    research_id: str
    status: FinalizeJobStatus = FinalizeJobStatus.PENDING
    attempt_count: int = 0
    max_attempts: int = 3
    error: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class SearchTaskJob(BaseModel):
    id: str
    task_id: str
    depth: SearchDepth
    status: SearchJobStatus = SearchJobStatus.PENDING
    attempt_count: int = 0
    max_attempts: int = 3
    error: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ResearchFinalizeResponse(BaseModel):
    research: ResearchRecord
    finalize_job_id: Optional[str] = None


class ExtractionMetrics(BaseModel):
    attempts: int = 0
    success_count: int = 0
    empty_count: int = 0
    failure_count: int = 0
    downloaded_bytes: int = 0
    content_chars: int = 0
    total_download_ms: float = 0.0
    total_extract_ms: float = 0.0
    total_post_process_ms: float = 0.0
    total_total_ms: float = 0.0
    success_rate_percent: float = 0.0
    avg_download_ms: float = 0.0
    avg_extract_ms: float = 0.0
    avg_post_process_ms: float = 0.0
    avg_total_ms: float = 0.0

    @model_validator(mode="after")
    def populate_derived_fields(self):
        attempts = max(self.attempts, 0)
        if attempts > 0:
            self.success_rate_percent = round((self.success_count / attempts) * 100, 1)
            self.avg_download_ms = round(self.total_download_ms / attempts, 2)
            self.avg_extract_ms = round(self.total_extract_ms / attempts, 2)
            self.avg_post_process_ms = round(self.total_post_process_ms / attempts, 2)
            self.avg_total_ms = round(self.total_total_ms / attempts, 2)
        else:
            self.success_rate_percent = 0.0
            self.avg_download_ms = 0.0
            self.avg_extract_ms = 0.0
            self.avg_post_process_ms = 0.0
            self.avg_total_ms = 0.0
        return self


class GraphStepMetrics(BaseModel):
    run_count: int = 0
    failure_count: int = 0
    total_ms: float = 0.0
    avg_ms: float = 0.0

    @model_validator(mode="after")
    def _derive_averages(self) -> "GraphStepMetrics":
        runs = max(self.run_count, 0)
        self.avg_ms = round(self.total_ms / runs, 2) if runs > 0 else 0.0
        return self


def _default_graph_steps() -> Dict[str, GraphStepMetrics]:
    return {
        "collect_context": GraphStepMetrics(),
        "replan": GraphStepMetrics(),
        "analyze": GraphStepMetrics(),
        "verify": GraphStepMetrics(),
        "tie_break": GraphStepMetrics(),
    }


class GraphMetrics(BaseModel):

    resume_count: int = 0
    replan_pass_count: int = 0
    tie_break_pass_count: int = 0
    analyze_pass_count: int = 0
    completed_run_count: int = 0
    steps: Dict[str, GraphStepMetrics] = Field(default_factory=_default_graph_steps)

    @model_validator(mode="after")
    def _normalize_steps(self) -> "GraphMetrics":
        merged_steps = _default_graph_steps()
        for step_name, payload in (self.steps or {}).items():
            if step_name not in merged_steps:
                continue
            merged_steps[step_name] = GraphStepMetrics.model_validate(payload)
        self.steps = merged_steps
        return self


class GraphAlert(BaseModel):
    code: str
    severity: str = "warning"
    step: Optional[str] = None
    current_value: float = 0.0
    threshold: float = 0.0
    hint: Optional[str] = None


class GraphAlertHistoryEntry(BaseModel):
    timestamp: datetime
    code: str
    severity: str = "warning"
    step: Optional[str] = None
    current_value: float = 0.0
    threshold: float = 0.0
    research_id: Optional[str] = None
    worker_name: Optional[str] = None


class GraphAlertTrend(BaseModel):
    worsening_steps: List[str] = Field(default_factory=list)
    improving_steps: List[str] = Field(default_factory=list)
    repeated_alerts: Dict[str, int] = Field(default_factory=dict)
    top_research_ids: List[str] = Field(default_factory=list)
    top_worker_names: List[str] = Field(default_factory=list)
    recent_alerts: List[GraphAlertHistoryEntry] = Field(default_factory=list)


class MaintenanceSummary(BaseModel):
    class MaintenanceAlert(BaseModel):
        code: str
        severity: str = "warning"
        current_value: float = 0.0
        threshold: float = 0.0
        hint: Optional[str] = None

    class MaintenanceRunEntry(BaseModel):
        recovered_count: int = 0
        deleted_count: int = 0
        compacted_count: int = 0
        total_count: int = 0
        last_run_at: Optional[datetime] = None

    class MaintenanceTrend(BaseModel):
        cleanup_volume_direction: str = "stable"
        average_compacted_count: float = 0.0
        recent_total_counts: List[int] = Field(default_factory=list)
        recent_compacted_counts: List[int] = Field(default_factory=list)

    class RecommendationAnalytics(BaseModel):
        average_time_to_ack_hours: float = 0.0
        average_time_to_resolve_hours: float = 0.0
        oldest_unresolved_hours: float = 0.0
        unresolved_count: int = 0
        repeated_reappeared_count: int = 0
        top_recurring_codes: List[str] = Field(default_factory=list)

    class RecommendationEvent(BaseModel):
        code: str
        event_type: str
        message: str
        timestamp: Optional[datetime] = None
        note: Optional[str] = None

    recovered_count: int = 0
    deleted_count: int = 0
    compacted_count: int = 0
    total_count: int = 0
    compacted_graph_event_worker_names: List[str] = Field(default_factory=list)
    compacted_graph_trail_research_ids: List[str] = Field(default_factory=list)
    last_run_at: Optional[datetime] = None
    recent_runs: List["MaintenanceSummary.MaintenanceRunEntry"] = Field(default_factory=list)
    trend: "MaintenanceSummary.MaintenanceTrend" = Field(default_factory=MaintenanceTrend)
    alerts: List["MaintenanceSummary.MaintenanceAlert"] = Field(default_factory=list)
    recommendation_analytics: "MaintenanceSummary.RecommendationAnalytics" = Field(default_factory=RecommendationAnalytics)
    recent_operational_health: List["OperationalHealth.OperationalHealthEntry"] = Field(default_factory=list)
    recent_operational_recommendations: List["OperationalHealth.RecommendationEntry"] = Field(default_factory=list)
    recent_operational_recommendation_events: List["MaintenanceSummary.RecommendationEvent"] = Field(default_factory=list)


class OperationalHealth(BaseModel):
    class OperationalHealthAlert(BaseModel):
        code: str
        severity: str = "warning"
        current_value: float = 0.0
        threshold: float = 0.0
        hint: Optional[str] = None

    class OperationalHealthEntry(BaseModel):
        status: str = "healthy"
        score: int = 100
        reasons: List[str] = Field(default_factory=list)
        timestamp: Optional[datetime] = None

    class OperationalHealthTrend(BaseModel):
        score_direction: str = "stable"
        average_score: float = 100.0
        recent_scores: List[int] = Field(default_factory=list)
        recent_statuses: List[str] = Field(default_factory=list)

    class RecommendationEntry(BaseModel):
        code: str
        message: str
        shown_count: int = 1
        active: bool = True
        first_shown_at: Optional[datetime] = None
        last_shown_at: Optional[datetime] = None
        acknowledged: bool = False
        acknowledged_at: Optional[datetime] = None
        resolved: bool = False
        resolved_at: Optional[datetime] = None
        resolution_note: Optional[str] = None

    status: str = "healthy"
    score: int = 100
    reasons: List[str] = Field(default_factory=list)
    alerts: List["OperationalHealth.OperationalHealthAlert"] = Field(default_factory=list)
    recommendations: List["OperationalHealth.RecommendationEntry"] = Field(default_factory=list)
    history: List["OperationalHealth.OperationalHealthEntry"] = Field(default_factory=list)
    trend: "OperationalHealth.OperationalHealthTrend" = Field(default_factory=OperationalHealthTrend)


class WorkerHeartbeat(BaseModel):
    worker_name: str
    processed_jobs: int = 0
    status: str = "idle"
    last_error: Optional[str] = None
    extraction_metrics: ExtractionMetrics = Field(default_factory=ExtractionMetrics)
    graph_metrics: GraphMetrics = Field(default_factory=GraphMetrics)
    graph_alerts: List[GraphAlert] = Field(default_factory=list)
    graph_alert_trend: GraphAlertTrend = Field(default_factory=GraphAlertTrend)
    maintenance_summary: MaintenanceSummary = Field(default_factory=MaintenanceSummary)
    operational_health: OperationalHealth = Field(default_factory=OperationalHealth)
    last_seen_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class OperationalRecommendationResolveRequest(BaseModel):
    note: Optional[str] = None


class QueueMetrics(BaseModel):
    pending_search_jobs: int = 0
    running_search_jobs: int = 0
    dead_letter_search_jobs: int = 0
    pending_finalize_jobs: int = 0
    running_finalize_jobs: int = 0
    dead_letter_finalize_jobs: int = 0
    extraction_metrics: ExtractionMetrics = Field(default_factory=ExtractionMetrics)
    graph_metrics: GraphMetrics = Field(default_factory=GraphMetrics)
    graph_alerts: List[GraphAlert] = Field(default_factory=list)
    graph_alert_trend: GraphAlertTrend = Field(default_factory=GraphAlertTrend)
    maintenance_summary: MaintenanceSummary = Field(default_factory=MaintenanceSummary)
    operational_health: OperationalHealth = Field(default_factory=OperationalHealth)


class JobRecoveryResponse(BaseModel):
    recovered_job_ids: List[str] = Field(default_factory=list)
    recovered_count: int = 0


class JobCleanupResponse(BaseModel):
    deleted_job_ids: List[str] = Field(default_factory=list)
    deleted_count: int = 0


class QueueMaintenanceResponse(BaseModel):
    recovered_search_job_ids: List[str] = Field(default_factory=list)
    recovered_finalize_job_ids: List[str] = Field(default_factory=list)
    deleted_search_job_ids: List[str] = Field(default_factory=list)
    deleted_finalize_job_ids: List[str] = Field(default_factory=list)
    compacted_graph_event_worker_names: List[str] = Field(default_factory=list)
    compacted_graph_trail_research_ids: List[str] = Field(default_factory=list)
    recovered_count: int = 0
    deleted_count: int = 0
    compacted_count: int = 0
    total_count: int = 0


SearchTaskSummary.model_rebuild()
ResearchSummary.model_rebuild()
