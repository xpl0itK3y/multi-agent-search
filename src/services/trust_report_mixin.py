"""Trust-pipeline reporting concern of ResearchService (red-team/citation/source-independence/
reputation/retraction/cross-language/stance/numeric builders + their get_research_* getters +
the audit trail), extracted as a mixin (AUD-030). Composed into ResearchService; the builders
are driven from complete_research_finalization and use self.<agent> collaborators + helpers
that remain on ResearchService, via normal composition.
"""
import json
import logging
import uuid

from fastapi import HTTPException

from src.agents.cross_language import detect_language
from src.config import settings
from src.domain import *  # noqa: F401,F403

logger = logging.getLogger(__name__)


class TrustReportMixin:
    def _store_red_team(self, research_id: str, red_team: RedTeamReport) -> None:
        research = self.task_store.get_research(research_id)
        state = dict((research.graph_state if research else None) or {})
        state["red_team"] = red_team.model_dump()
        self.task_store.update_research_graph_state(research_id, state)

    def get_research_red_team(self, research_id: str) -> RedTeamReport:
        """Stored adversarial findings for the artifact panel (empty if the pass didn't run)."""
        research = self.task_store.get_research(research_id)
        data = ((research.graph_state if research else None) or {}).get("red_team")
        if not data:
            return RedTeamReport(research_id=research_id)
        return RedTeamReport.model_validate(data)

    # ── citation audit (deterministic claim↔source grounding) ───────────────────

    def _aggregated_sources(self, research, tasks: list) -> list | None:
        """Reconstruct the analyzer's exact [Sn] source numbering once, to share across the
        finalize trust steps (each would otherwise recompute it — AUD-013). None for a minimal
        analyzer; defensive so a prep failure can't break finalization."""
        prepare = getattr(self.analyzer, "_prepare_aggregated_data", None)
        if not callable(prepare):
            return None
        try:
            aggregated, _ = prepare(research.prompt, tasks, research.depth)
            return aggregated
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("aggregate_sources_failed research_id=%s error=%s", research.id, exc)
            return None

    def _audit_citations(self, report: str, research, tasks: list, aggregated: list | None = None) -> None:
        """Check each [Sn] citation against its source text; store grounding + integrity.

        Reconstructs the analyzer's exact source numbering so [Sn] line up, then matches
        lexically. Never raises — an audit failure must not break finalization.
        """
        if not (report or "").strip():
            return
        prepare = getattr(self.analyzer, "_prepare_aggregated_data", None)
        if not callable(prepare):
            return  # minimal analyzer — no [Sn] numbering to reconstruct
        try:
            if aggregated is None:
                aggregated, _ = prepare(research.prompt, tasks, research.depth)
            sources_by_id = {
                source["source_id"]: {
                    "content": source.get("content"),
                    "url": source.get("url"),
                    "title": source.get("title"),
                }
                for source in aggregated
                if source.get("source_id")
            }
            audit = self.citation_auditor.audit(report, sources_by_id)
            audit.research_id = research.id
            self.task_store.merge_research_graph_state(research.id, {"citation_audit": audit.model_dump()})
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("citation_audit_failed research_id=%s error=%s", research.id, exc)

    def get_research_citation_audit(self, research_id: str) -> CitationAudit:
        """Stored citation grounding/integrity for inline hover + the trust scorecard."""
        research = self.task_store.get_research(research_id)
        data = ((research.graph_state if research else None) or {}).get("citation_audit")
        if not data:
            return CitationAudit(research_id=research_id)
        return CitationAudit.model_validate(data)

    # ── source independence (echo-chamber / circular-sourcing detector) ─────────

    def _analyze_source_independence(self, research, tasks: list, aggregated: list | None = None) -> None:
        """Cluster the cited sources into independent origins; store the result.

        Reuses the analyzer's exact source numbering so cluster source_ids line up with
        the report's [Sn]. Deterministic and defensive — never breaks finalization.
        """
        prepare = getattr(self.analyzer, "_prepare_aggregated_data", None)
        if not callable(prepare):
            return
        try:
            if aggregated is None:
                aggregated, _ = prepare(research.prompt, tasks, research.depth)
            sources_by_id = {
                source["source_id"]: {
                    "content": source.get("content"),
                    "url": source.get("url"),
                    "title": source.get("title"),
                }
                for source in aggregated
                if source.get("source_id")
            }
            independence = self.independence_auditor.analyze(sources_by_id)
            independence.research_id = research.id
            self.task_store.merge_research_graph_state(research.id, {"source_independence": independence.model_dump()})
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("source_independence_failed research_id=%s error=%s", research.id, exc)

    def get_research_source_independence(self, research_id: str) -> SourceIndependence:
        """Stored echo-chamber analysis: how many independent origins the sources really are."""
        research = self.task_store.get_research(research_id)
        data = ((research.graph_state if research else None) or {}).get("source_independence")
        if not data:
            return SourceIndependence(research_id=research_id)
        return SourceIndependence.model_validate(data)

    # ── source reputation (low-credibility / state-media domain flags) ───────────

    def _assess_source_reputation(self, research, tasks: list, aggregated: list | None = None) -> None:
        """Flag cited sources from satire/fabricated/conspiracy/state-controlled domains.

        Reuses the analyzer's [Sn] numbering so flags line up with the report. Deterministic
        and defensive — never breaks finalization.
        """
        prepare = getattr(self.analyzer, "_prepare_aggregated_data", None)
        if not callable(prepare):
            return
        try:
            if aggregated is None:
                aggregated, _ = prepare(research.prompt, tasks, research.depth)
            sources_by_id = {
                s["source_id"]: {"url": s.get("url"), "domain": s.get("domain")}
                for s in aggregated
                if s.get("source_id")
            }
            reputation = self.reputation_auditor.assess(sources_by_id)
            reputation.research_id = research.id
            self.task_store.merge_research_graph_state(research.id, {"source_reputation": reputation.model_dump()})
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("source_reputation_failed research_id=%s error=%s", research.id, exc)

    def get_research_source_reputation(self, research_id: str) -> SourceReputation:
        """Stored domain-credibility flags (satire / fabricated / conspiracy / state media)."""
        research = self.task_store.get_research(research_id)
        data = ((research.graph_state if research else None) or {}).get("source_reputation")
        if not data:
            return SourceReputation(research_id=research_id)
        return SourceReputation.model_validate(data)

    # ── retraction check (cited DOIs vs Crossref / Retraction Watch) ─────────────

    def _crossref_fetch(self, doi: str) -> dict | None:
        """Fetch a Crossref work message for a DOI (cached, short timeout, never raises)."""
        if doi in self._crossref_cache:
            return self._crossref_cache[doi]
        message: dict | None = None
        try:
            import urllib.parse
            import urllib.request

            url = "https://api.crossref.org/works/" + urllib.parse.quote(doi, safe="")
            req = urllib.request.Request(url, headers={"User-Agent": "Veris/1.0 (research integrity check)"})
            with urllib.request.urlopen(req, timeout=settings.retraction_check_timeout) as resp:
                message = json.loads(resp.read().decode("utf-8")).get("message")
        except Exception as exc:  # pragma: no cover - network/parse failures degrade to "no info"
            logger.info("crossref_fetch_failed doi=%s error=%s", doi, exc)
            message = None
        if len(self._crossref_cache) > 5000:
            self._crossref_cache.clear()
        self._crossref_cache[doi] = message
        return message

    def _check_retractions(self, research, tasks: list, aggregated: list | None = None) -> None:
        """Flag cited sources backed by a retracted paper (Crossref/Retraction Watch).

        Network step — gated by a flag, capped, and fully defensive: any failure degrades to
        no flags and never breaks finalization. Stored only when DOIs were actually checked.
        """
        if not settings.retraction_check_enabled:
            return
        prepare = getattr(self.analyzer, "_prepare_aggregated_data", None)
        if not callable(prepare):
            return
        try:
            if aggregated is None:
                aggregated, _ = prepare(research.prompt, tasks, research.depth)
            sources_by_id = {
                s["source_id"]: {"url": s.get("url"), "content": s.get("content")}
                for s in aggregated
                if s.get("source_id")
            }
            integrity = self.retraction_agent.check(sources_by_id, self._crossref_fetch)
            if integrity.checked_dois == 0:
                return  # no academic DOIs to verify — nothing to record
            integrity.research_id = research.id
            self.task_store.merge_research_graph_state(research.id, {"source_integrity": integrity.model_dump()})
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("retraction_check_failed research_id=%s error=%s", research.id, exc)

    def get_research_source_integrity(self, research_id: str) -> SourceIntegrity:
        """Stored retraction check: cited DOIs flagged as retracted / under concern."""
        research = self.task_store.get_research(research_id)
        data = ((research.graph_state if research else None) or {}).get("source_integrity")
        if not data:
            return SourceIntegrity(research_id=research_id)
        return SourceIntegrity.model_validate(data)

    # ── cross-language coverage (search other languages, surface what they add) ───

    def _maybe_add_cross_language_task(self, research_id: str, prompt: str, tasks_raw: list) -> None:
        """Append a task that searches the question in other relevant languages. Additive +
        defensive — a failure just leaves the plan monolingual."""
        if not settings.cross_language_enabled or self.cross_language_agent is None:
            return
        try:
            query_lang = detect_language(prompt)
            langs, queries = self.cross_language_agent.plan(prompt, query_lang, settings.cross_language_max_targets)
            if not queries:
                return
            tasks_raw.append({
                "id": str(uuid.uuid4()),
                "description": "Coverage in other languages",
                "queries": queries,
                "status": TaskStatus.PENDING,
            })
            state = dict((self.task_store.get_research(research_id).graph_state) or {})
            state["cross_language_targets"] = langs
            state.setdefault("query_language", query_lang)
            self.task_store.update_research_graph_state(research_id, state)
            logger.info("cross_language_task_added research_id=%s langs=%s", research_id, langs)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("cross_language_plan_failed research_id=%s error=%s", research_id, exc)

    def _analyze_cross_language(self, research, tasks: list, aggregated: list | None = None) -> None:
        """Tag each source's language, build the distribution, and surface findings unique to
        non-query-language sources. Deterministic distribution + one gated LLM call. Never raises."""
        if not settings.cross_language_enabled:
            return
        prepare = getattr(self.analyzer, "_prepare_aggregated_data", None)
        if not callable(prepare):
            return
        try:
            if aggregated is None:
                aggregated, _ = prepare(research.prompt, tasks, research.depth)
            state = research.graph_state or {}
            query_lang = state.get("query_language") or detect_language(research.prompt)
            by_lang: dict[str, int] = {}
            foreign_snippets: dict[str, list[str]] = {}
            for s in aggregated:
                content = (s.get("content") or s.get("title") or "").strip()
                if not content:
                    continue
                lang = detect_language(content)
                by_lang[lang] = by_lang.get(lang, 0) + 1
                if lang not in ("unknown", query_lang):
                    foreign_snippets.setdefault(lang, []).append(content)
            if not by_lang:
                return
            foreign_count = sum(c for lang, c in by_lang.items() if lang not in ("unknown", query_lang))
            unique_findings = []
            if foreign_snippets and self.cross_language_agent is not None:
                unique_findings = self.cross_language_agent.surface(research.prompt, query_lang, foreign_snippets)
            report = CrossLanguageReport(
                research_id=research.id,
                query_language=query_lang,
                languages=[LanguageCount(lang=lang, count=c) for lang, c in sorted(by_lang.items(), key=lambda kv: -kv[1])],
                target_languages=state.get("cross_language_targets", []),
                foreign_source_count=foreign_count,
                monolingual=foreign_count == 0,
                unique_findings=unique_findings,
            )
            self.task_store.merge_research_graph_state(research.id, {"cross_language": report.model_dump()})
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("cross_language_analysis_failed research_id=%s error=%s", research.id, exc)

    def get_research_cross_language(self, research_id: str) -> CrossLanguageReport:
        """Stored language-diversity report: distribution + what non-query-language sources add."""
        research = self.task_store.get_research(research_id)
        data = ((research.graph_state if research else None) or {}).get("cross_language")
        if not data:
            return CrossLanguageReport(research_id=research_id)
        return CrossLanguageReport.model_validate(data)

    # ── stance / viewpoint balance (one LLM call, contestable questions only) ─────

    _STANCE_SIGNALS = (
        "should", "better", "worth it", "worth the", "pros and cons", "good or bad", "safe",
        "overrated", "underrated", "is it worth", "does it work", "harmful", "beneficial",
        "right or wrong", "ethical", "vs", "versus", " or ", " better than ",
        "стоит ли", "лучше ли", "нужно ли", "вреден", "вредно", "полезен", "полезно", "опасен",
        "опасно", "этично", "переоценён", "стоит того", "лучше чем", "против",
        "vale la pena", "es mejor", "deberí", "es seguro", "es ético",
    )

    def _looks_contestable(self, prompt: str) -> bool:
        lowered = f" {(prompt or '').lower()} "
        return any(signal in lowered for signal in self._STANCE_SIGNALS)

    def _maybe_assess_stance(self, research, tasks: list, aggregated: list | None = None) -> None:
        """For opinion/debate-shaped questions, label each source's stance and store the balance.

        Heuristic-gated (one LLM call only when the question has sides). Never raises.
        """
        if self.stance_agent is None:
            return
        if not self._looks_contestable(research.prompt):
            return
        prepare = getattr(self.analyzer, "_prepare_aggregated_data", None)
        if not callable(prepare):
            return
        try:
            if aggregated is None:
                aggregated, _ = prepare(research.prompt, tasks, research.depth)
            sources_by_id = {
                s["source_id"]: {"content": s.get("content"), "title": s.get("title")}
                for s in aggregated[:14]
                if s.get("source_id")
            }
            language = self._detect_report_language(research.prompt, research.final_report or "")
            balance = self.stance_agent.assess(
                research.prompt, sources_by_id, language=language, model=settings.red_team_model
            )
            if not balance.applicable:
                return
            balance.research_id = research.id
            self.task_store.merge_research_graph_state(research.id, {"stance_balance": balance.model_dump()})
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("stance_assess_failed research_id=%s error=%s", research.id, exc)

    def get_research_stance(self, research_id: str) -> StanceBalance:
        """Stored viewpoint balance: how evidence splits for/against the central claim."""
        research = self.task_store.get_research(research_id)
        data = ((research.graph_state if research else None) or {}).get("stance_balance")
        if not data:
            return StanceBalance(research_id=research_id)
        return StanceBalance.model_validate(data)

    # ── numeric & contradiction check (every figure traced to its source) ────────

    def _check_numbers(self, report: str, research, tasks: list, aggregated: list | None = None) -> None:
        """Verify each cited figure against its source text and flag internal contradictions.

        Reconstructs the analyzer's [Sn] numbering so figures line up with their sources.
        Deterministic; never breaks finalization.
        """
        if not (report or "").strip():
            return
        prepare = getattr(self.analyzer, "_prepare_aggregated_data", None)
        if not callable(prepare):
            return
        try:
            if aggregated is None:
                aggregated, _ = prepare(research.prompt, tasks, research.depth)
            sources_by_id = {
                source["source_id"]: {"content": source.get("content")}
                for source in aggregated
                if source.get("source_id")
            }
            check = self.numeric_checker.check(report, sources_by_id)
            check.research_id = research.id
            self.task_store.merge_research_graph_state(research.id, {"numeric_check": check.model_dump()})
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("numeric_check_failed research_id=%s error=%s", research.id, exc)

    def get_research_numeric_check(self, research_id: str) -> NumericCheck:
        """Stored figure check: figures traced to source + internal contradictions."""
        research = self.task_store.get_research(research_id)
        data = ((research.graph_state if research else None) or {}).get("numeric_check")
        if not data:
            return NumericCheck(research_id=research_id)
        return NumericCheck.model_validate(data)

    # ── audit trail (reproducible 'show your work' provenance) ───────────────────

    def get_research_audit_trail(self, research_id: str) -> AuditTrail:
        """Assemble the full provenance behind a report — sub-questions, queries, sources,
        graph steps and decisions — deterministically from what the pipeline already records."""
        research = self.task_store.get_research(research_id)
        if not research:
            raise HTTPException(status_code=404, detail="Research not found")
        tasks = self.task_store.get_tasks_by_research(research_id)
        state = research.graph_state or {}

        plan = [t.description for t in tasks if (t.description or "").strip()]
        queries = [
            AuditQuery(
                task=t.description or "",
                queries=list(t.queries or []),
                status=getattr(t.status, "value", str(t.status)),
                result_count=len(t.result or []),
            )
            for t in tasks
        ]

        sources = self._audit_sources(research, tasks)

        steps = [
            AuditStep(
                step=str(e.get("step") or "unknown"),
                detail=str(e.get("detail") or ""),
                timestamp=str(e.get("timestamp") or ""),
            )
            for e in (research.graph_trail or [])
        ][:120]

        terminal = research.status in (ResearchStatus.COMPLETED, ResearchStatus.FAILED)
        return AuditTrail(
            research_id=research.id,
            prompt=research.prompt,
            model=state.get("model", ""),
            depth=getattr(research.depth, "value", str(research.depth)),
            status=getattr(research.status, "value", str(research.status)),
            created_at=research.created_at.isoformat() if research.created_at else "",
            completed_at=research.updated_at.isoformat() if (terminal and research.updated_at) else "",
            plan=plan,
            queries=queries,
            sources=sources,
            steps=steps,
            decisions=self._audit_decisions(state),
            token_usage=state.get("llm_token_usage", {}) or {},
            source_count=len(sources),
            query_count=sum(len(q.queries) for q in queries),
        )

    def _audit_sources(self, research, tasks: list) -> list[AuditSource]:
        """Sources numbered by the same [Sn] scheme the report used, when reconstructable."""
        prepare = getattr(self.analyzer, "_prepare_aggregated_data", None)
        if callable(prepare):
            try:
                aggregated, _ = prepare(research.prompt, tasks, research.depth)
                return [
                    AuditSource(
                        source_id=s.get("source_id", ""),
                        url=s.get("url", "") or "",
                        domain=s.get("domain", "") or "",
                        title=s.get("title", "") or "",
                        source_quality=s.get("source_quality", "") or "",
                        extraction_status=s.get("extraction_status", "") or "",
                    )
                    for s in aggregated
                    if s.get("source_id")
                ]
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("audit_trail_sources_failed research_id=%s error=%s", research.id, exc)
        # Fallback: the deduped preview list (no [Sn] numbering).
        return [
            AuditSource(url=s.url or "", domain=s.domain or "", title=s.title or "", source_quality=s.source_quality or "")
            for s in self.get_research_sources(research.id)
        ]

    @staticmethod
    def _audit_decisions(state: dict) -> list[str]:
        """Human-readable summary of the finalize graph's decisions from its counters."""
        out: list[str] = []
        replan = int(state.get("replan_attempts", 0) or 0)
        tie = int(state.get("tie_break_attempts", 0) or 0)
        analyze = int(state.get("analyze_attempts", 0) or 0)
        if analyze:
            out.append(f"analyze passes: {analyze}")
        if replan:
            out.append(f"replan triggered: {replan}x (coverage looked weak)")
        if tie:
            out.append(f"tie-break passes: {tie}")
        rt = state.get("red_team") or {}
        if rt.get("challenged") or rt.get("held"):
            out.append(f"red-team: {rt.get('challenged', 0)} challenged, {rt.get('held', 0)} held")
        ind = state.get("source_independence") or {}
        if ind.get("total_sources"):
            out.append(f"source independence: {ind.get('independent_origins', 0)}/{ind.get('total_sources', 0)} origins")
        rep = state.get("source_reputation") or {}
        if rep.get("flagged_count"):
            out.append(f"reputation flags: {rep.get('flagged_count')} source(s) ({', '.join(rep.get('categories', []))})")
        st = state.get("stance_balance") or {}
        if st.get("applicable"):
            out.append(f"viewpoint balance: {st.get('supports', 0)} for / {st.get('opposes', 0)} against / {st.get('neutral', 0)} neutral")
        integ = state.get("source_integrity") or {}
        if integ.get("checked_dois"):
            out.append(f"retraction check: {integ.get('checked_dois')} DOI(s) checked, {integ.get('retracted_count', 0)} retracted")
        xl = state.get("cross_language") or {}
        if xl.get("languages"):
            langs = ", ".join(f"{lc.get('lang')}×{lc.get('count')}" for lc in xl.get("languages", [])[:6])
            out.append(f"cross-language: {langs}" + (f" ({xl.get('foreign_source_count')} non-{xl.get('query_language')})" if xl.get("foreign_source_count") else ""))
        return out

    def _render_audit_trail_md(self, trail: AuditTrail) -> str:
        """Human-readable Markdown rendering of the audit trail for export."""
        L: list[str] = [f"# Audit trail — {trail.prompt}", ""]
        meta = [
            f"- **Research ID:** {trail.research_id}",
            f"- **Model:** {trail.model or '—'}   **Depth:** {trail.depth}   **Status:** {trail.status}",
            f"- **Started:** {trail.created_at or '—'}   **Completed:** {trail.completed_at or '—'}",
            f"- **Sources:** {trail.source_count}   **Search queries:** {trail.query_count}",
        ]
        if trail.token_usage:
            u = trail.token_usage
            meta.append(
                f"- **Tokens:** {u.get('prompt_tokens', 0)} in / {u.get('completion_tokens', 0)} out"
                + (f"   **Est. cost:** ${u.get('estimated_cost_usd', 0):.4f}" if u.get("estimated_cost_usd") else "")
            )
        L += meta + [""]

        if trail.plan:
            L += ["## Plan (sub-questions)", ""] + [f"{i}. {q}" for i, q in enumerate(trail.plan, 1)] + [""]
        if trail.decisions:
            L += ["## Graph decisions", ""] + [f"- {d}" for d in trail.decisions] + [""]
        if trail.queries:
            L += ["## Search queries", ""]
            for q in trail.queries:
                L.append(f"- **{q.task}** _({q.status}, {q.result_count} results)_")
                L += [f"    - `{query}`" for query in q.queries]
            L.append("")
        if trail.steps:
            L += ["## Execution trail", ""]
            for s in trail.steps:
                ts = f" _{s.timestamp}_" if s.timestamp else ""
                L.append(f"- **{s.step}**{ts}: {s.detail}".rstrip(": "))
            L.append("")
        if trail.sources:
            L += ["## Sources fetched", ""]
            for s in trail.sources:
                sid = f"[{s.source_id}] " if s.source_id else ""
                qual = f" _(quality: {s.source_quality})_" if s.source_quality else ""
                L.append(f"- {sid}[{s.title or s.domain or s.url}]({s.url}){qual}")
            L.append("")
        return "\n".join(L).strip() + "\n"
