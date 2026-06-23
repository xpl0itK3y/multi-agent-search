"""Operational-health, maintenance-summary & recommendation reporting of ResearchService,
extracted as a mixin (AUD-030). Composed into ResearchService; the builders call helpers that
remain on ResearchService (self._build_graph_alerts, self._filter_graph_step_events, etc.) via
normal composition.
"""
import json
import time
from datetime import datetime, timezone

from src.config import settings
from src.domain import *  # noqa: F401,F403 (op-health builders reference many domain models)


class OperationalHealthMixin:
    def _build_graph_alert_trend(self, step_events: list[dict]) -> GraphAlertTrend:
        step_windows: dict[str, list[float]] = {}
        repeated_alerts: dict[str, int] = {}
        research_counts: dict[str, int] = {}
        worker_counts: dict[str, int] = {}
        recent_alerts: list[GraphAlertHistoryEntry] = []

        for event in step_events:
            step_name = str(event.get("step") or "").strip()
            if not step_name:
                continue
            elapsed_ms = float(event.get("elapsed_ms") or 0.0)
            failed = bool(event.get("failed"))
            step_windows.setdefault(step_name, []).append(elapsed_ms)

            alert_code = None
            severity = "warning"
            threshold = 0.0
            if failed:
                alert_code = "step_failures"
                threshold = float(self.GRAPH_STEP_FAILURE_WARNING_COUNT)
            elif elapsed_ms >= self.GRAPH_STEP_WARNING_MS:
                alert_code = "high_avg_ms"
                severity = "critical" if elapsed_ms >= self.GRAPH_STEP_CRITICAL_MS else "warning"
                threshold = self.GRAPH_STEP_WARNING_MS if severity == "warning" else self.GRAPH_STEP_CRITICAL_MS

            if not alert_code:
                continue

            repeated_alerts[alert_code] = repeated_alerts.get(alert_code, 0) + 1
            if event.get("research_id"):
                research_id_value = str(event["research_id"])
                research_counts[research_id_value] = research_counts.get(research_id_value, 0) + 1
            if event.get("worker_name"):
                worker_name_value = str(event["worker_name"])
                worker_counts[worker_name_value] = worker_counts.get(worker_name_value, 0) + 1
            recent_alerts.append(
                GraphAlertHistoryEntry(
                    timestamp=datetime.fromisoformat(event["timestamp"]),
                    code=alert_code,
                    severity=severity,
                    step=step_name,
                    current_value=elapsed_ms if alert_code == "high_avg_ms" else 1.0,
                    threshold=threshold,
                    research_id=event.get("research_id"),
                    worker_name=event.get("worker_name"),
                )
            )

        worsening_steps: list[str] = []
        improving_steps: list[str] = []
        for step_name, values in step_windows.items():
            if len(values) < 4:
                continue
            window_size = min(5, len(values) // 2)
            if window_size <= 0:
                continue
            previous = values[-(window_size * 2):-window_size]
            recent = values[-window_size:]
            if not previous or not recent:
                continue
            previous_avg = sum(previous) / len(previous)
            recent_avg = sum(recent) / len(recent)
            if recent_avg > previous_avg * 1.25 and recent_avg - previous_avg >= 100:
                worsening_steps.append(step_name)
            elif previous_avg > 0 and recent_avg < previous_avg * 0.8 and previous_avg - recent_avg >= 100:
                improving_steps.append(step_name)

        top_research_ids = [
            research_id_value
            for research_id_value, _ in sorted(research_counts.items(), key=lambda item: item[1], reverse=True)[:3]
        ]
        top_worker_names = [
            worker_name_value
            for worker_name_value, _ in sorted(worker_counts.items(), key=lambda item: item[1], reverse=True)[:3]
        ]
        return GraphAlertTrend(
            worsening_steps=sorted(set(worsening_steps)),
            improving_steps=sorted(set(improving_steps)),
            repeated_alerts=dict(sorted(repeated_alerts.items(), key=lambda item: item[1], reverse=True)),
            top_research_ids=top_research_ids,
            top_worker_names=top_worker_names,
            recent_alerts=recent_alerts[-10:],
        )

    def _build_maintenance_summary(self, summary: MaintenanceSummary) -> MaintenanceSummary:
        recent_runs = list(summary.recent_runs or [])
        recommendation_events = list(summary.recent_operational_recommendation_events or [])
        recommendations = list(summary.recent_operational_recommendations or [])
        total_counts = [int(item.total_count or 0) for item in recent_runs[-8:]]
        compacted_counts = [int(item.compacted_count or 0) for item in recent_runs[-8:]]
        average_compacted = round(sum(compacted_counts) / len(compacted_counts), 2) if compacted_counts else 0.0
        direction = "stable"
        if len(total_counts) >= 4:
            half = len(total_counts) // 2
            previous = total_counts[:half]
            recent = total_counts[half:]
            previous_avg = sum(previous) / len(previous) if previous else 0.0
            recent_avg = sum(recent) / len(recent) if recent else 0.0
            if recent_avg > previous_avg * 1.2 and recent_avg - previous_avg >= 1:
                direction = "growing"
            elif previous_avg > 0 and recent_avg < previous_avg * 0.8 and previous_avg - recent_avg >= 1:
                direction = "shrinking"
        trend = MaintenanceSummary.MaintenanceTrend(
            cleanup_volume_direction=direction,
            average_compacted_count=average_compacted,
            recent_total_counts=total_counts,
            recent_compacted_counts=compacted_counts,
        )
        alerts: list[MaintenanceSummary.MaintenanceAlert] = []
        recent_avg_total = round(sum(total_counts[-4:]) / len(total_counts[-4:]), 2) if total_counts[-4:] else 0.0
        if direction == "growing" and recent_avg_total >= self.MAINTENANCE_GROWING_CRITICAL_RECENT_AVG:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="cleanup_volume_growing",
                    severity="critical",
                    current_value=recent_avg_total,
                    threshold=self.MAINTENANCE_GROWING_CRITICAL_RECENT_AVG,
                    hint="Maintenance cleanup volume is rising; inspect backlog growth, retry churn, and graph operational noise.",
                )
            )
        elif direction == "growing" and recent_avg_total >= self.MAINTENANCE_GROWING_WARNING_RECENT_AVG:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="cleanup_volume_growing",
                    severity="warning",
                    current_value=recent_avg_total,
                    threshold=self.MAINTENANCE_GROWING_WARNING_RECENT_AVG,
                    hint="Cleanup volume is trending upward; check whether queue churn or graph retries are increasing.",
                )
            )

        if average_compacted >= self.MAINTENANCE_COMPACTED_CRITICAL_AVG:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="high_compacted_average",
                    severity="critical",
                    current_value=average_compacted,
                    threshold=self.MAINTENANCE_COMPACTED_CRITICAL_AVG,
                    hint="Persisted graph operational data is being compacted heavily; review event/trail volume and retention settings.",
                )
            )
        elif average_compacted >= self.MAINTENANCE_COMPACTED_WARNING_AVG:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="high_compacted_average",
                    severity="warning",
                    current_value=average_compacted,
                    threshold=self.MAINTENANCE_COMPACTED_WARNING_AVG,
                    hint="Compacted graph data per run is elevated; check whether operational history is growing too quickly.",
                )
            )

        if summary.last_run_at is not None:
            age_seconds = max((datetime.now(timezone.utc) - summary.last_run_at).total_seconds(), 0.0)
            if age_seconds >= self.MAINTENANCE_STALE_CRITICAL_SECONDS:
                alerts.append(
                    MaintenanceSummary.MaintenanceAlert(
                        code="maintenance_stale",
                        severity="critical",
                        current_value=round(age_seconds, 2),
                        threshold=float(self.MAINTENANCE_STALE_CRITICAL_SECONDS),
                        hint="Maintenance has not run recently; verify the maintenance worker heartbeat and queue maintenance path.",
                    )
                )
            elif age_seconds >= self.MAINTENANCE_STALE_WARNING_SECONDS:
                alerts.append(
                    MaintenanceSummary.MaintenanceAlert(
                        code="maintenance_stale",
                        severity="warning",
                        current_value=round(age_seconds, 2),
                        threshold=float(self.MAINTENANCE_STALE_WARNING_SECONDS),
                        hint="Maintenance cadence is getting stale; verify periodic cleanup is still running.",
                    )
                )

        recommendation_events_by_code: dict[str, list[MaintenanceSummary.RecommendationEvent]] = {}
        for event in recommendation_events:
            recommendation_events_by_code.setdefault(event.code, []).append(event)

        ack_durations_hours: list[float] = []
        resolve_durations_hours: list[float] = []
        reappeared_count = 0
        top_recurring_codes = sorted(
            (
                (item.code, max(int(item.shown_count or 1) - 1, 0))
                for item in recommendations
            ),
            key=lambda item: (-item[1], item[0]),
        )

        for code, events in recommendation_events_by_code.items():
            shown_timestamp: datetime | None = None
            for event in events:
                if event.event_type in {"shown", "reappeared"}:
                    if event.event_type == "reappeared":
                        reappeared_count += 1
                    shown_timestamp = event.timestamp
                elif event.event_type == "acknowledged" and shown_timestamp and event.timestamp:
                    ack_durations_hours.append(max((event.timestamp - shown_timestamp).total_seconds(), 0.0) / 3600.0)
                elif event.event_type == "resolved" and shown_timestamp and event.timestamp:
                    resolve_durations_hours.append(max((event.timestamp - shown_timestamp).total_seconds(), 0.0) / 3600.0)
                    shown_timestamp = None

        unresolved_items = [item for item in recommendations if not item.resolved]
        now_utc = datetime.now(timezone.utc)
        unresolved_ages_hours = [
            max((now_utc - (item.first_shown_at or item.last_shown_at)).total_seconds(), 0.0) / 3600.0
            for item in unresolved_items
            if item.first_shown_at or item.last_shown_at
        ]
        average_time_to_ack_hours = round(sum(ack_durations_hours) / len(ack_durations_hours), 2) if ack_durations_hours else 0.0
        average_time_to_resolve_hours = round(sum(resolve_durations_hours) / len(resolve_durations_hours), 2) if resolve_durations_hours else 0.0
        oldest_unresolved_hours = round(max(unresolved_ages_hours), 2) if unresolved_ages_hours else 0.0
        recommendation_analytics = MaintenanceSummary.RecommendationAnalytics(
            average_time_to_ack_hours=average_time_to_ack_hours,
            average_time_to_resolve_hours=average_time_to_resolve_hours,
            oldest_unresolved_hours=oldest_unresolved_hours,
            unresolved_count=len(unresolved_items),
            repeated_reappeared_count=reappeared_count,
            top_recurring_codes=[code for code, count in top_recurring_codes[:3] if count > 0],
        )

        if recommendation_analytics.unresolved_count >= self.RUNBOOK_UNRESOLVED_CRITICAL_COUNT:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="runbook_unresolved_pressure",
                    severity="critical",
                    current_value=float(recommendation_analytics.unresolved_count),
                    threshold=float(self.RUNBOOK_UNRESOLVED_CRITICAL_COUNT),
                    hint="Too many unresolved runbook items remain active; clear the operator queue before retries and backlog compound.",
                )
            )
        elif recommendation_analytics.unresolved_count >= self.RUNBOOK_UNRESOLVED_WARNING_COUNT:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="runbook_unresolved_pressure",
                    severity="warning",
                    current_value=float(recommendation_analytics.unresolved_count),
                    threshold=float(self.RUNBOOK_UNRESOLVED_WARNING_COUNT),
                    hint="Runbook unresolved items are accumulating; review whether recommendations are being acknowledged but not completed.",
                )
            )

        if recommendation_analytics.average_time_to_resolve_hours >= self.RUNBOOK_RESOLUTION_CRITICAL_HOURS:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="runbook_slow_resolution",
                    severity="critical",
                    current_value=recommendation_analytics.average_time_to_resolve_hours,
                    threshold=self.RUNBOOK_RESOLUTION_CRITICAL_HOURS,
                    hint="Recommendations are taking too long to close; prioritize recurring issues and reduce reappearing operational debt.",
                )
            )
        elif recommendation_analytics.average_time_to_resolve_hours >= self.RUNBOOK_RESOLUTION_WARNING_HOURS:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="runbook_slow_resolution",
                    severity="warning",
                    current_value=recommendation_analytics.average_time_to_resolve_hours,
                    threshold=self.RUNBOOK_RESOLUTION_WARNING_HOURS,
                    hint="Resolution time is drifting upward; confirm operators are not just acknowledging items without completing the fix.",
                )
            )

        if recommendation_analytics.repeated_reappeared_count >= self.RUNBOOK_REAPPEARED_CRITICAL_COUNT:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="runbook_reappeared_items",
                    severity="critical",
                    current_value=float(recommendation_analytics.repeated_reappeared_count),
                    threshold=float(self.RUNBOOK_REAPPEARED_CRITICAL_COUNT),
                    hint="Runbook items are repeatedly reappearing; fixes are likely not addressing the root cause.",
                )
            )
        elif recommendation_analytics.repeated_reappeared_count >= self.RUNBOOK_REAPPEARED_WARNING_COUNT:
            alerts.append(
                MaintenanceSummary.MaintenanceAlert(
                    code="runbook_reappeared_items",
                    severity="warning",
                    current_value=float(recommendation_analytics.repeated_reappeared_count),
                    threshold=float(self.RUNBOOK_REAPPEARED_WARNING_COUNT),
                    hint="Some recommendations keep returning; audit the last fixes and operator notes for incomplete remediation.",
                )
            )

        return summary.model_copy(update={"trend": trend, "alerts": alerts, "recommendation_analytics": recommendation_analytics})

    def _build_operational_health(
        self,
        metrics: QueueMetrics,
        graph_alerts: list[GraphAlert],
        maintenance_summary: MaintenanceSummary,
    ) -> OperationalHealth:
        score = 100
        reasons: list[str] = []

        for alert in graph_alerts:
            if alert.severity == "critical":
                score -= 25
            else:
                score -= 10
            reasons.append(f"graph:{alert.code}")

        for alert in maintenance_summary.alerts:
            if alert.severity == "critical":
                score -= 20
            else:
                score -= 8
            reasons.append(f"maintenance:{alert.code}")

        backlog = (
            metrics.pending_search_jobs
            + metrics.running_search_jobs
            + metrics.dead_letter_search_jobs
            + metrics.pending_finalize_jobs
            + metrics.running_finalize_jobs
            + metrics.dead_letter_finalize_jobs
        )
        if backlog >= 20:
            score -= 20
            reasons.append("queue:high_backlog")
        elif backlog >= 8:
            score -= 10
            reasons.append("queue:elevated_backlog")

        score = max(score, 0)
        status = "healthy"
        if any(
            (alert.severity == "critical" for alert in graph_alerts)
        ) or any((alert.severity == "critical" for alert in maintenance_summary.alerts)) or score <= 50:
            status = "critical"
        elif graph_alerts or maintenance_summary.alerts or score < 90:
            status = "warning"

        deduped_reasons = list(dict.fromkeys(reasons))
        current_health = OperationalHealth(status=status, score=score, reasons=deduped_reasons[:8])
        history = list(maintenance_summary.recent_operational_health or [])
        current_timestamp = maintenance_summary.last_run_at
        history.append(
            OperationalHealth.OperationalHealthEntry(
                status=status,
                score=score,
                reasons=deduped_reasons[:8],
                timestamp=current_timestamp,
            )
        )
        history = history[-20:]
        score_values = [int(item.score or 100) for item in history[-8:]]
        statuses = [str(item.status or "healthy") for item in history[-8:]]
        average_score = round(sum(score_values) / len(score_values), 2) if score_values else 100.0
        score_direction = "stable"
        if len(score_values) >= 4:
            half = len(score_values) // 2
            previous = score_values[:half]
            recent = score_values[half:]
            previous_avg = sum(previous) / len(previous) if previous else 0.0
            recent_avg = sum(recent) / len(recent) if recent else 0.0
            if recent_avg < previous_avg - 8:
                score_direction = "worsening"
            elif recent_avg > previous_avg + 8:
                score_direction = "improving"
        trend = OperationalHealth.OperationalHealthTrend(
            score_direction=score_direction,
            average_score=average_score,
            recent_scores=score_values,
            recent_statuses=statuses,
        )
        alerts: list[OperationalHealth.OperationalHealthAlert] = []
        if len(score_values) >= 4:
            half = len(score_values) // 2
            previous = score_values[:half]
            recent = score_values[half:]
            previous_avg = sum(previous) / len(previous) if previous else 0.0
            recent_avg = sum(recent) / len(recent) if recent else 0.0
            if recent_avg <= previous_avg - self.OPERATIONAL_WORSENING_CRITICAL_DELTA:
                alerts.append(
                    OperationalHealth.OperationalHealthAlert(
                        code="score_worsening",
                        severity="critical",
                        current_value=round(recent_avg, 2),
                        threshold=round(previous_avg, 2),
                        hint="Operational score is falling quickly; inspect graph and maintenance alerts before backlog compounds.",
                    )
                )
            elif recent_avg <= previous_avg - self.OPERATIONAL_WORSENING_WARNING_DELTA:
                alerts.append(
                    OperationalHealth.OperationalHealthAlert(
                        code="score_worsening",
                        severity="warning",
                        current_value=round(recent_avg, 2),
                        threshold=round(previous_avg, 2),
                        hint="Operational score is trending downward; check recent alert growth and cleanup pressure.",
                    )
                )

        recent_critical_count = sum(1 for item in history[-5:] if str(item.status or "").lower() == "critical")
        if recent_critical_count >= self.OPERATIONAL_CRITICAL_STATE_CRITICAL_COUNT:
            alerts.append(
                OperationalHealth.OperationalHealthAlert(
                    code="repeated_critical_states",
                    severity="critical",
                    current_value=float(recent_critical_count),
                    threshold=float(self.OPERATIONAL_CRITICAL_STATE_CRITICAL_COUNT),
                    hint="Too many recent critical states; investigate persistent graph failures, backlog pressure, or stale maintenance.",
                )
            )
        elif recent_critical_count >= self.OPERATIONAL_CRITICAL_STATE_WARNING_COUNT:
            alerts.append(
                OperationalHealth.OperationalHealthAlert(
                    code="repeated_critical_states",
                    severity="warning",
                    current_value=float(recent_critical_count),
                    threshold=float(self.OPERATIONAL_CRITICAL_STATE_WARNING_COUNT),
                    hint="Critical states are recurring; confirm the system is actually recovering between maintenance cycles.",
                )
            )

        if len(history) >= 2:
            previous_status = str(history[-2].status or "healthy").lower()
            if previous_status in {"critical", "warning"} and status == "healthy" and score >= 90:
                alerts.append(
                    OperationalHealth.OperationalHealthAlert(
                        code="score_recovered",
                        severity="warning",
                        current_value=float(score),
                        threshold=90.0,
                        hint="Operational health has recovered; verify the underlying cause was actually resolved and not just transient.",
                    )
                )

        recommendations = self._build_operational_recommendations(
            metrics=metrics,
            graph_alerts=graph_alerts,
            maintenance_summary=maintenance_summary,
            operational_alerts=alerts,
            reasons=deduped_reasons[:8],
        )
        return current_health.model_copy(
            update={
                "alerts": alerts,
                "recommendations": recommendations,
                "history": history,
                "trend": trend,
            }
        )

    def _build_operational_recommendations(
        self,
        metrics: QueueMetrics,
        graph_alerts: list[GraphAlert],
        maintenance_summary: MaintenanceSummary,
        operational_alerts: list[OperationalHealth.OperationalHealthAlert],
        reasons: list[str],
    ) -> list[OperationalHealth.RecommendationEntry]:
        recommendation_specs: list[tuple[str, str]] = []
        alert_codes = {alert.code for alert in operational_alerts}
        reason_set = set(reasons)
        current_timestamp = maintenance_summary.last_run_at or datetime.now(timezone.utc)

        if "repeated_critical_states" in alert_codes:
            recommendation_specs.append(
                (
                    "increase_worker_parallelism",
                    "Repeated critical states detected: consider increasing worker parallelism and checking whether one worker is saturating the queue.",
                )
            )
        if "score_worsening" in alert_codes:
            recommendation_specs.append(
                (
                    "inspect_backlog_latency_and_retries",
                    "Operational score is worsening: inspect queue backlog, extraction latency, and graph retries before the next maintenance cycle.",
                )
            )
        if "score_recovered" in alert_codes:
            recommendation_specs.append(
                (
                    "verify_recovered_score_root_cause",
                    "Score recovered after degradation: verify the underlying issue is resolved and not just temporarily masked.",
                )
            )

        maintenance_alert_codes = {alert.code for alert in maintenance_summary.alerts}
        if "maintenance_stale" in maintenance_alert_codes:
            recommendation_specs.append(
                (
                    "restart_or_verify_maintenance_path",
                    "Maintenance appears stale: verify the maintenance worker is running and trigger the maintenance path if needed.",
                )
            )
        if "high_compacted_average" in maintenance_alert_codes:
            recommendation_specs.append(
                (
                    "review_graph_retention_pressure",
                    "High graph compaction volume: review graph event/trail retention and whether operational data is growing too quickly.",
                )
            )

        graph_alert_codes = {alert.code for alert in graph_alerts}
        if "analyze_retries" in graph_alert_codes:
            recommendation_specs.append(
                (
                    "tighten_source_selection_and_claim_verification",
                    "Frequent analyze retries: tighten source selection or claim verification to reduce repeated finalize passes.",
                )
            )
        if "step_failures" in graph_alert_codes:
            recommendation_specs.append(
                (
                    "inspect_graph_failures_and_search_quality",
                    "Graph step failures detected: inspect failing steps, search quality, and blocked domains before rerunning jobs.",
                )
            )

        if "queue:high_backlog" in reason_set or "queue:elevated_backlog" in reason_set:
            recommendation_specs.append(
                (
                    "reduce_queue_backlog",
                    "Queue backlog is elevated: consider adding more workers and review long-running search/finalize jobs.",
                )
            )

        if metrics.extraction_metrics.avg_total_ms >= 3000:
            recommendation_specs.append(
                (
                    "reduce_extraction_latency",
                    "Extraction latency is elevated: review slow domains, timeout settings, and extraction concurrency.",
                )
            )

        previous_entries = {
            item.code: item
            for item in (maintenance_summary.recent_operational_recommendations or [])
        }
        recommendation_events = list(maintenance_summary.recent_operational_recommendation_events or [])
        active_codes: list[str] = []
        merged_entries: list[OperationalHealth.RecommendationEntry] = []
        seen_codes: set[str] = set()

        for code, message in recommendation_specs:
            if code in seen_codes:
                continue
            seen_codes.add(code)
            active_codes.append(code)
            previous = previous_entries.get(code)
            event_type = None
            if previous is None:
                event_type = "shown"
            elif not previous.active or previous.resolved:
                event_type = "reappeared"
            if event_type is not None:
                recommendation_events = self._append_operational_recommendation_event(
                    recommendation_events,
                    code=code,
                    event_type=event_type,
                    message=message,
                    timestamp=current_timestamp,
                )
            merged_entries.append(
                OperationalHealth.RecommendationEntry(
                    code=code,
                    message=message,
                    shown_count=(previous.shown_count + 1) if previous else 1,
                    active=True,
                    first_shown_at=(previous.first_shown_at if previous else current_timestamp),
                    last_shown_at=current_timestamp,
                    acknowledged=(previous.acknowledged if previous and not previous.resolved else False),
                    acknowledged_at=(previous.acknowledged_at if previous and not previous.resolved else None),
                    resolved=False,
                    resolved_at=None,
                    resolution_note=None,
                )
            )

        for code, previous in previous_entries.items():
            if code in active_codes:
                continue
            merged_entries.append(
                previous.model_copy(
                    update={
                        "active": False,
                    }
                )
            )

        merged_entries.sort(
            key=lambda item: (
                0 if item.active else 1,
                0 if not item.resolved else 1,
                0 if not item.acknowledged else 1,
                -(item.shown_count or 0),
                item.code,
            )
        )
        maintenance_summary.recent_operational_recommendation_events = recommendation_events[-self.OPERATIONAL_RECOMMENDATION_EVENT_LIMIT :]
        return merged_entries[:8]

    def _append_operational_recommendation_event(
        self,
        existing_events: list[dict] | list[MaintenanceSummary.RecommendationEvent],
        *,
        code: str,
        event_type: str,
        message: str,
        timestamp: str | datetime,
        note: str | None = None,
    ) -> list[MaintenanceSummary.RecommendationEvent]:
        normalized_timestamp = (
            timestamp.isoformat()
            if isinstance(timestamp, datetime)
            else str(timestamp)
        )
        events = [
            item.model_dump(mode="json") if isinstance(item, MaintenanceSummary.RecommendationEvent) else dict(item)
            for item in existing_events
        ]
        if events:
            latest = events[-1]
            if (
                str(latest.get("code") or "") == code
                and str(latest.get("event_type") or "") == event_type
                and str(latest.get("message") or "") == message
            ):
                return [
                    MaintenanceSummary.RecommendationEvent.model_validate(item)
                    for item in events[-self.OPERATIONAL_RECOMMENDATION_EVENT_LIMIT :]
                ]
        events.append(
            {
                "code": code,
                "event_type": event_type,
                "message": message,
                "timestamp": normalized_timestamp,
                "note": note,
            }
        )
        return [
            MaintenanceSummary.RecommendationEvent.model_validate(item)
            for item in events[-self.OPERATIONAL_RECOMMENDATION_EVENT_LIMIT :]
        ]

    def _graph_alert_hint(self, code: str, step: str | None) -> str:
        step_name = (step or "").strip().lower()
        if code == "high_avg_ms":
            if step_name == "analyze":
                return "Check LLM latency and consider reducing analyzer payload budget for large reports."
            if step_name in {"replan", "tie_break"}:
                return "Inspect follow-up query volume and search pass depth; trim weak branches before re-running."
            if step_name == "collect_context":
                return "Review source pool size and pre-filtering; excessive source aggregation is slowing graph preparation."
            if step_name == "verify":
                return "Check claim-verification heuristics and conflict volume; verification may be over-processing weak evidence."
            return "Review the slow graph step and reduce unnecessary work before the next finalize pass."
        if code == "step_failures":
            if step_name == "tie_break":
                return "Check search quality, blocked domains, and domain filters for tie-break follow-up queries."
            if step_name == "analyze":
                return "Inspect analyzer prompt size, model stability, and citation-repair loops."
            if step_name == "replan":
                return "Review replan recommendations and whether follow-up queries are too broad or malformed."
            if step_name == "collect_context":
                return "Inspect source critic/evidence mapping inputs; malformed source data may be breaking context collection."
            if step_name == "verify":
                return "Review claim-verifier assumptions and conflict payload quality before verification."
            return "Inspect logs for the failing graph step and tighten the corresponding inputs."
        if code == "analyze_retries":
            return "Strengthen source selection or claim verification so finalize produces a cleaner draft in fewer analyze passes."
        return "Inspect the related graph step and recent worker logs for the underlying cause."

