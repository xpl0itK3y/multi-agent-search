from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.session import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ResearchORM(Base):
    __tablename__ = "researches"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    depth: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="processing")
    final_report: Mapped[str | None] = mapped_column(Text, nullable=True)
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
    )


class SearchTaskORM(Base):
    __tablename__ = "search_tasks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    research_id: Mapped[str | None] = mapped_column(
        ForeignKey("researches.id", ondelete="CASCADE"),
        nullable=True,
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

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    research_id: Mapped[str] = mapped_column(
        ForeignKey("researches.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
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

    worker_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    processed_jobs: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="idle")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    extraction_metrics: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    graph_metrics: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    graph_step_events: Mapped[list[dict]] = mapped_column(JSONB, nullable=False, default=list)
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
    )
