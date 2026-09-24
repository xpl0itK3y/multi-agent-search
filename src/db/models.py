from datetime import datetime, timezone

from sqlalchemy import CheckConstraint, DateTime, Float, ForeignKey, Index, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.session import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class UserORM(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    email: Mapped[str] = mapped_column(String(200), nullable=False, unique=True, index=True)
    password_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    google_subject: Mapped[str | None] = mapped_column(
        String(255), nullable=True, unique=True, index=True
    )
    token_version: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    avatar_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )
    last_seen_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    last_ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_device: Mapped[str | None] = mapped_column(String(64), nullable=True)


class ResearchORM(Base):
    __tablename__ = "researches"
    # Indexes also created by migrations; declared here so the models are the source of truth
    # and create_all() / drift checks match the migrated schema (AUD-037).
    __table_args__ = (
        Index("ix_researches_status", "status"),
        CheckConstraint(
            "status IN ('queued','clarifying','plan_review','processing','analyzing',"
            "'completed','failed','cancelled')",
            name="ck_researches_status",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    language: Mapped[str] = mapped_column(String(8), nullable=False, default="unknown")
    # Real FK: deleting a user cascades their researches (and via existing FKs, all
    # tasks/results/jobs) — previously a manual user delete left data publicly served
    # through surviving share tokens (DATA-LIFECYCLE).
    user_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=True, index=True
    )
    depth: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="processing")
    final_report: Mapped[str | None] = mapped_column(Text, nullable=True)
    partial_report: Mapped[str | None] = mapped_column(Text, nullable=True)
    partial_reasoning: Mapped[str | None] = mapped_column(Text, nullable=True)
    graph_state: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    graph_trail: Mapped[list[dict]] = mapped_column(JSONB, nullable=False, default=list)
    task_ids: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )

    tasks: Mapped[list["SearchTaskORM"]] = relationship(
        back_populates="research",
        cascade="all, delete-orphan",
        passive_deletes=True,  # rely on the DB ON DELETE CASCADE (AUD-025) instead of loading children
    )


Index(
    "ix_researches_thread_id",
    ResearchORM.graph_state["thread_id"].astext,
)
Index(
    "ix_researches_share_token",
    ResearchORM.graph_state["share_token"].astext,
)
Index(
    "ix_researches_user_created",
    ResearchORM.user_id,
    ResearchORM.created_at.desc(),
)


class SearchTaskORM(Base):
    __tablename__ = "search_tasks"
    __table_args__ = (
        Index("ix_search_tasks_status", "status"),
        CheckConstraint(
            "status IN ('pending','running','completed','failed')",
            name="ck_search_tasks_status",
        ),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    research_id: Mapped[str | None] = mapped_column(
        ForeignKey("researches.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    description: Mapped[str] = mapped_column(Text, nullable=False)
    queries: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    logs: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    search_metrics: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )

    research: Mapped[ResearchORM | None] = relationship(back_populates="tasks")
    results: Mapped[list["SearchResultORM"]] = relationship(
        back_populates="task",
        cascade="all, delete-orphan",
        passive_deletes=True,  # rely on the DB ON DELETE CASCADE (AUD-025)
    )


class SearchResultORM(Base):
    __tablename__ = "search_results"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(
        ForeignKey("search_tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    content: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )

    task: Mapped[SearchTaskORM] = relationship(back_populates="results")


class ResearchFinalizeJobORM(Base):
    __tablename__ = "research_finalize_jobs"
    __table_args__ = (
        Index("ix_research_finalize_jobs_status", "status"),
        CheckConstraint(
            "status IN ('pending','running','completed','failed','dead_letter')",
            name="ck_research_finalize_jobs_status",
        ),
        Index(
            "ix_research_finalize_jobs_pending",
            "created_at",
            postgresql_where=text("status = 'pending'"),
        ),
        Index("ix_research_finalize_jobs_status_updated", "status", "updated_at"),
        Index("ix_research_finalize_jobs_status_created", "status", "created_at"),
        # DB-level backstop for AUD-005: at most one RUNNING finalize job per research.
        Index(
            "uq_running_finalize_job_per_research",
            "research_id",
            unique=True,
            postgresql_where=text("status = 'running'"),
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    research_id: Mapped[str] = mapped_column(
        ForeignKey("researches.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    lease_epoch: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )


class SearchTaskJobORM(Base):
    __tablename__ = "search_task_jobs"
    __table_args__ = (
        Index("ix_search_task_jobs_status", "status"),
        CheckConstraint(
            "status IN ('pending','running','completed','failed','dead_letter')",
            name="ck_search_task_jobs_status",
        ),
        Index(
            "ix_search_task_jobs_pending",
            "created_at",
            postgresql_where=text("status = 'pending'"),
        ),
        Index("ix_search_task_jobs_status_updated", "status", "updated_at"),
        Index("ix_search_task_jobs_status_created", "status", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    task_id: Mapped[str] = mapped_column(
        ForeignKey("search_tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    depth: Mapped[str] = mapped_column(String(16), nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )


class WorkerHeartbeatORM(Base):
    __tablename__ = "worker_heartbeats"
    __table_args__ = (Index("ix_worker_heartbeats_status", "status"),)

    worker_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    processed_jobs: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="idle")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    extraction_metrics: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    graph_metrics: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    graph_step_events: Mapped[list[dict]] = mapped_column(JSONB, nullable=False, default=list)
    maintenance_summary: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )


class SearchCacheORM(Base):
    """Cached web-search results keyed by backend+query, with a TTL on read.

    Shared across workers so a repeated query doesn't re-hit the (paid) search API
    or re-fetch pages. Entries are looked up by age; stale rows are ignored.
    """

    __tablename__ = "search_cache"

    cache_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    payload: Mapped[list[dict]] = mapped_column(JSONB, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
        index=True,
    )


class LLMUsageLogORM(Base):
    __tablename__ = "llm_usage_logs"
    __table_args__ = (
        Index("ix_llm_usage_logs_created_model", "created_at", "model"),
        Index("ix_llm_usage_logs_research_created", "research_id", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    research_id: Mapped[str | None] = mapped_column(
        ForeignKey("researches.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    user_id: Mapped[str | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    model: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    prompt_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    completion_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    total_tokens: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
    estimated_cost_usd: Mapped[float] = mapped_column(Float, nullable=False, default=0.0, server_default=text("0.0"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
        index=True,
    )


class AdminAuditLogORM(Base):
    __tablename__ = "admin_audit_logs"
    __table_args__ = (
        Index("ix_admin_audit_logs_actor_created", "actor_email", "created_at"),
        Index("ix_admin_audit_logs_action_created", "action", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    actor_email: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    action: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    target_type: Mapped[str] = mapped_column(String(64), nullable=False)
    target_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    details: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    ip_address: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
        index=True,
    )


class UserSessionORM(Base):
    __tablename__ = "user_sessions"
    __table_args__ = (
        Index("ix_user_sessions_started_at", "started_at"),
        Index("ix_user_sessions_last_active", "last_active_at"),
        # One row per client session id per account: the upsert's ON CONFLICT arbiter.
        Index("uq_user_sessions_session_user", "session_id", "user_id", unique=True),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    user_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    session_id: Mapped[str] = mapped_column(String(64), nullable=False)
    ip_address: Mapped[str | None] = mapped_column(String(64), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)
    device_type: Mapped[str] = mapped_column(
        String(32), nullable=False, default="desktop", server_default=text("'desktop'")
    )
    browser: Mapped[str | None] = mapped_column(String(64), nullable=True)
    os: Mapped[str | None] = mapped_column(String(64), nullable=True)
    screen_res: Mapped[str | None] = mapped_column(String(32), nullable=True)
    viewport: Mapped[str | None] = mapped_column(String(32), nullable=True)
    language: Mapped[str | None] = mapped_column(String(16), nullable=True)
    timezone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    country: Mapped[str | None] = mapped_column(String(64), nullable=True)
    city: Mapped[str | None] = mapped_column(String(64), nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )
    last_active_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )


class UserEventORM(Base):
    __tablename__ = "user_events"
    __table_args__ = (
        Index("ix_user_events_user_created", "user_id", "created_at"),
        Index("ix_user_events_category", "event_category"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    user_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("users.id", ondelete="CASCADE"), nullable=True, index=True
    )
    session_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    event_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_category: Mapped[str] = mapped_column(
        String(32), nullable=False, default="general", server_default=text("'general'")
    )
    details: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    ip_address: Mapped[str | None] = mapped_column(String(64), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
        index=True,
    )
