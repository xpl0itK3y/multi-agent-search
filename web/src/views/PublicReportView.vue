<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { api } from "@/lib/api";
import type { PublicReport } from "@/lib/types";
import MarkdownView from "@/components/MarkdownView.vue";
import SparkLogo from "@/components/SparkLogo.vue";

const props = defineProps<{ token: string }>();

const report = ref<PublicReport | null>(null);
const loading = ref(true);
const notFound = ref(false);

onMounted(async () => {
  try {
    report.value = await api.getPublicReport(props.token);
  } catch {
    notFound.value = true;
  } finally {
    loading.value = false;
  }
});

const confidencePct = computed(() =>
  report.value?.confidence?.components?.length ? Math.round(report.value.confidence.overall * 100) : null,
);
const integrityPct = computed(() =>
  report.value?.citations?.total ? Math.round(report.value.citations.integrity * 100) : null,
);
const independencePct = computed(() =>
  (report.value?.source_independence?.total_sources ?? 0) > 1
    ? Math.round(report.value!.source_independence.independence_score * 100)
    : null,
);
const stanceText = computed(() => {
  const s = report.value?.stance;
  if (!s?.applicable) return null;
  const total = s.supports + s.opposes + s.neutral || 1;
  return `${Math.round((s.supports / total) * 100)}% / ${Math.round((s.opposes / total) * 100)}%`;
});
</script>

<template>
  <div class="mx-auto min-h-full max-w-3xl px-5 py-8">
    <header class="mb-6 flex items-center justify-between border-b border-bd pb-4">
      <a href="/" class="flex items-center gap-2">
        <SparkLogo :size="24" />
        <span class="veris-wordmark text-lg font-semibold tracking-tight">{{ $t("sidebar.brand") }}</span>
      </a>
      <span class="rounded-full border border-bd px-2.5 py-1 text-[11px] uppercase tracking-wide text-muted">
        {{ $t("share.publicBadge") }}
      </span>
    </header>

    <p v-if="loading" class="text-muted">{{ $t("common.loading") }}</p>

    <div v-else-if="notFound" class="rounded-xl border border-bd bg-surface/40 p-8 text-center">
      <div class="text-2xl">🔗</div>
      <div class="mt-2 font-medium text-ink">{{ $t("share.notFoundTitle") }}</div>
      <div class="mt-1 text-sm text-muted">{{ $t("share.notFoundHint") }}</div>
    </div>

    <article v-else-if="report" class="animate-rise">
      <!-- Trust scorecard: the point of a shared Veris link -->
      <div class="mb-6 flex flex-wrap gap-2 text-xs">
        <span v-if="confidencePct !== null" class="rounded-lg border border-bd bg-surface/40 px-2.5 py-1">
          {{ $t("confidence.meter") }}: <b class="text-ink">{{ confidencePct }}%</b>
        </span>
        <span v-if="integrityPct !== null" class="rounded-lg border border-bd bg-surface/40 px-2.5 py-1">
          {{ $t("citations.integrity") }}: <b class="text-ink">{{ integrityPct }}%</b>
          <span class="text-muted"> ({{ report.citations.supported }}/{{ report.citations.total }})</span>
        </span>
        <span v-if="independencePct !== null" class="rounded-lg border border-bd bg-surface/40 px-2.5 py-1">
          {{ $t("independence.title") }}: <b class="text-ink">{{ independencePct }}%</b>
        </span>
        <span v-if="stanceText" class="rounded-lg border border-bd bg-surface/40 px-2.5 py-1">
          {{ $t("stance.title") }}: <b class="text-ink">{{ stanceText }}</b>
        </span>
        <span v-if="report.numeric_check.total" class="rounded-lg border border-bd bg-surface/40 px-2.5 py-1">
          {{ $t("numbers.title") }}: <b class="text-ink">{{ report.numeric_check.supported }}/{{ report.numeric_check.total }}</b>
        </span>
        <span v-if="report.source_reputation.flagged_count" class="rounded-lg border border-red-400/40 px-2.5 py-1 text-red-400">
          ⚑ {{ report.source_reputation.flagged_count }} {{ $t("reputation.flagged") }}
        </span>
      </div>

      <MarkdownView :source="report.final_report" :grounding="report.citations.grounding" />

      <footer class="mt-10 border-t border-bd pt-4 text-xs text-muted">
        {{ $t("share.footer") }} · <a href="/" class="text-accent hover:underline">{{ $t("sidebar.brand") }}</a>
      </footer>
    </article>
  </div>
</template>
