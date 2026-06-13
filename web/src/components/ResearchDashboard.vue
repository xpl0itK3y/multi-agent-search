<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { api } from "@/lib/api";
import type { CitationAudit, ComparisonTable, RedTeamReport, ResearchDiff, SourcePreview, VerificationReport } from "@/lib/types";

const props = defineProps<{ id: string }>();
const emit = defineEmits<{ navigate: [string] }>();

const verification = ref<VerificationReport | null>(null);
const citations = ref<CitationAudit | null>(null);
const redTeam = ref<RedTeamReport | null>(null);
const comparison = ref<ComparisonTable | null>(null);
const diff = ref<ResearchDiff | null>(null);
const sources = ref<SourcePreview[]>([]);
const loading = ref(true);

onMounted(async () => {
  const [v, c, r, cmp, d, s] = await Promise.allSettled([
    api.getVerification(props.id),
    api.getCitations(props.id),
    api.getRedTeam(props.id),
    api.getComparison(props.id),
    api.getDiff(props.id),
    api.getSources(props.id),
  ]);
  if (v.status === "fulfilled") verification.value = v.value;
  if (c.status === "fulfilled") citations.value = c.value;
  if (r.status === "fulfilled") redTeam.value = r.value;
  if (cmp.status === "fulfilled") comparison.value = cmp.value;
  if (d.status === "fulfilled") diff.value = d.value;
  if (s.status === "fulfilled") sources.value = s.value;
  loading.value = false;
});

const levelClass: Record<string, string> = {
  strong: "text-emerald-500 border-emerald-500/40",
  medium: "text-amber-500 border-amber-500/40",
  weak: "text-red-400 border-red-400/40",
};

const totalSources = computed(() => sources.value.length);
const quality = computed(() => {
  const c = { high: 0, medium: 0, low: 0 };
  for (const s of sources.value) {
    const q = (s.source_quality || "low") as keyof typeof c;
    if (q in c) c[q]++;
    else c.low++;
  }
  return c;
});
const topFindings = computed(() => {
  const order: Record<string, number> = { strong: 0, medium: 1, weak: 2 };
  return [...(verification.value?.findings || [])]
    .sort((a, b) => (order[a.support_level] ?? 3) - (order[b.support_level] ?? 3))
    .slice(0, 6);
});
const integrityPct = computed(() =>
  citations.value && citations.value.total ? Math.round(citations.value.integrity * 100) : null,
);
const coveragePct = computed(() =>
  verification.value ? Math.round(verification.value.coverage_ratio * 100) : null,
);
const diffChanged = computed(() => {
  const d = diff.value;
  return d ? d.new_claims.length + d.dropped_claims.length + d.shifted_claims.length + d.new_sources : 0;
});
const integrityColor = computed(() => {
  const p = integrityPct.value ?? 0;
  return p >= 80 ? "text-emerald-500" : p >= 50 ? "text-amber-500" : "text-red-400";
});
function pct(n: number): string {
  return totalSources.value ? `${(n / totalSources.value) * 100}%` : "0%";
}
</script>

<template>
  <div v-if="loading" class="text-muted">{{ $t("common.loading") }}</div>
  <div v-else class="space-y-6">
    <!-- Trust scorecard -->
    <div class="grid grid-cols-2 gap-3 sm:grid-cols-4">
      <div class="rounded-card border border-bd bg-surface/40 p-3">
        <div class="text-xs text-muted">{{ $t("dashboard.coverage") }}</div>
        <div class="mt-1 text-2xl font-semibold text-ink">{{ coveragePct !== null ? coveragePct + "%" : "—" }}</div>
      </div>
      <div class="rounded-card border border-bd bg-surface/40 p-3">
        <div class="text-xs text-muted">{{ $t("dashboard.citations") }}</div>
        <div class="mt-1 text-2xl font-semibold" :class="integrityColor">{{ integrityPct !== null ? integrityPct + "%" : "—" }}</div>
        <div v-if="citations && citations.total" class="text-xs text-muted">{{ citations.supported }}/{{ citations.total }}</div>
      </div>
      <div class="rounded-card border border-bd bg-surface/40 p-3">
        <div class="text-xs text-muted">{{ $t("dashboard.sources") }}</div>
        <div class="mt-1 text-2xl font-semibold text-ink">{{ totalSources }}</div>
        <div v-if="quality.high" class="text-xs text-muted">{{ quality.high }} {{ $t("dashboard.highQuality") }}</div>
      </div>
      <div class="rounded-card border border-bd bg-surface/40 p-3">
        <div class="text-xs text-muted">{{ $t("dashboard.redteam") }}</div>
        <div v-if="redTeam && (redTeam.challenged || redTeam.held)" class="mt-1 text-sm font-medium text-ink">
          <span class="text-amber-500">{{ redTeam.challenged }}</span> / <span class="text-emerald-500">{{ redTeam.held }}</span>
        </div>
        <div v-else class="mt-1 text-2xl font-semibold text-muted">—</div>
        <div v-if="redTeam && (redTeam.challenged || redTeam.held)" class="text-xs text-muted">{{ $t("dashboard.challengedHeld") }}</div>
      </div>
    </div>

    <!-- Quick links to richer artifacts -->
    <div class="flex flex-wrap gap-2">
      <button v-if="comparison && comparison.options.length >= 2" class="rounded-full border border-accent/40 bg-accent/5 px-3 py-1 text-xs text-ink hover:bg-accent/10" @click="emit('navigate', 'comparison')">
        ⊞ {{ $t("dashboard.openComparison") }}
      </button>
      <button v-if="diffChanged" class="rounded-full border border-accent/40 bg-accent/5 px-3 py-1 text-xs text-ink hover:bg-accent/10" @click="emit('navigate', 'report')">
        ↻ {{ $t("dashboard.changes", { n: diffChanged }) }}
      </button>
      <button v-if="redTeam && redTeam.findings.length" class="rounded-full border border-bd px-3 py-1 text-xs text-muted hover:text-ink" @click="emit('navigate', 'redteam')">
        ⚔ {{ $t("dashboard.openRedteam") }}
      </button>
    </div>

    <!-- Source quality bar -->
    <div v-if="totalSources">
      <div class="mb-1 text-sm font-medium text-ink">{{ $t("dashboard.sourceQuality") }}</div>
      <div class="flex h-2.5 w-full overflow-hidden rounded-full bg-surface">
        <div class="h-full bg-emerald-500" :style="{ width: pct(quality.high) }" />
        <div class="h-full bg-amber-500" :style="{ width: pct(quality.medium) }" />
        <div class="h-full bg-red-400/70" :style="{ width: pct(quality.low) }" />
      </div>
      <div class="mt-1 flex gap-4 text-xs text-muted">
        <span><span class="text-emerald-500">●</span> {{ quality.high }} {{ $t("dashboard.qHigh") }}</span>
        <span><span class="text-amber-500">●</span> {{ quality.medium }} {{ $t("dashboard.qMedium") }}</span>
        <span><span class="text-red-400">●</span> {{ quality.low }} {{ $t("dashboard.qLow") }}</span>
      </div>
    </div>

    <!-- Key findings -->
    <div v-if="topFindings.length">
      <div class="mb-2 text-sm font-medium text-ink">{{ $t("dashboard.keyFindings") }}</div>
      <div class="space-y-2">
        <div v-for="(f, i) in topFindings" :key="i" class="rounded-lg border border-bd bg-surface/40 p-3">
          <div class="mb-1 flex items-center gap-2">
            <span class="rounded border px-1.5 py-0.5 text-[10px] font-semibold uppercase" :class="levelClass[f.support_level] || 'text-muted border-bd'">
              {{ $t("confidence." + f.support_level) }}
            </span>
            <span class="text-xs text-muted">{{ f.source_ids.map((s) => "[" + s + "]").join("") }}</span>
          </div>
          <div class="text-sm text-ink">{{ f.statement }}</div>
        </div>
      </div>
    </div>

    <p v-if="!topFindings.length && !totalSources" class="text-muted">{{ $t("dashboard.empty") }}</p>
  </div>
</template>
