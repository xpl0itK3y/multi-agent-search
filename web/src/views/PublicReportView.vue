<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { useI18n } from "vue-i18n";
import { api } from "@/lib/api";
import type { PublicReport } from "@/lib/types";
import { useUiStore } from "@/stores/ui";
import MarkdownView from "@/components/MarkdownView.vue";
import SparkLogo from "@/components/SparkLogo.vue";

const props = defineProps<{ token: string }>();
const ui = useUiStore();
const { t, te } = useI18n();

const report = ref<PublicReport | null>(null);
const loading = ref(true);
const notFound = ref(false);
// The page's own scroller. The frames around it clip at the viewport, so the document
// cannot scroll: with focus left on <body>, PageDown, Space, the arrows and End went nowhere
// until the reader clicked into the report. Focused on arrival, the keys scroll it at once.
const scroller = ref<HTMLElement | null>(null);

onMounted(async () => {
  scroller.value?.focus({ preventScroll: true });
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

// What was asked, when, how deep and on how many sources: the reader of a shared link
// has none of the context the author had (apple-design §16 wayfinding).
const dateLabel = computed(() => {
  const at = report.value?.created_at;
  if (!at) return "";
  const date = new Date(at);
  if (Number.isNaN(date.getTime())) return "";
  try {
    return new Intl.DateTimeFormat(ui.locale, { dateStyle: "long" }).format(date);
  } catch {
    return "";
  }
});
const metaParts = computed(() => {
  const r = report.value;
  if (!r) return [];
  const parts: string[] = [];
  if (dateLabel.value) parts.push(dateLabel.value);
  if (r.depth && te(`depth.${r.depth}`)) parts.push(t(`depth.${r.depth}`));
  if (r.sources?.length) parts.push(t("share.sourcesCount", r.sources.length));
  return parts;
});
</script>

<template>
  <div ref="scroller" tabindex="-1" data-scroll-root class="h-full scroll-pt-14 overflow-y-auto">
    <!-- The page scrolls itself: the shell and the bare frame both clip at the viewport.
         tabindex="-1": focusable from script (keyboard scrolling on arrival), not a Tab stop.
         scroll-pt-14: a link reached with (Shift+)Tab stops below the sticky header, not under it. -->
    <div class="mx-auto max-w-[44rem] px-4 pb-10 sm:px-5">
      <!-- Floating chrome: the report scrolls beneath a translucent bar, no hard divider. -->
      <header
        class="material-bar sticky top-0 z-10 -mx-4 mb-6 flex items-center justify-between px-4 py-3 shadow-[0_1px_0_rgb(var(--c-bd)/0.6)] sm:-mx-5 sm:px-5"
      >
        <a href="/" class="flex items-center gap-2">
          <SparkLogo :size="24" />
          <span class="veris-wordmark text-lg font-semibold tracking-tight">{{ $t("sidebar.brand") }}</span>
        </a>
        <span class="rounded-full border border-bd px-2.5 py-1 text-2xs font-medium uppercase tracking-wide text-muted">
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
        <p v-if="report.prompt" class="line-clamp-3 text-pretty font-serif text-lg leading-snug text-ink" :title="report.prompt">
          {{ report.prompt }}
        </p>
        <p v-if="metaParts.length" class="mb-5 mt-1 text-xs tabular-nums text-muted">{{ metaParts.join(" · ") }}</p>

        <!-- Trust scorecard: the point of a shared Veris link -->
        <div class="mb-6 flex flex-wrap gap-2 text-xs">
          <span v-if="confidencePct !== null" class="rounded-lg border border-bd bg-surface/40 px-2.5 py-1">
            {{ $t("confidence.meter") }}: <b class="tabular-nums text-ink">{{ confidencePct }}%</b>
          </span>
          <span v-if="integrityPct !== null" class="rounded-lg border border-bd bg-surface/40 px-2.5 py-1">
            {{ $t("citations.integrity") }}: <b class="tabular-nums text-ink">{{ integrityPct }}%</b>
            <span class="tabular-nums text-muted"> ({{ report.citations.supported }}/{{ report.citations.total }})</span>
          </span>
          <span v-if="independencePct !== null" class="rounded-lg border border-bd bg-surface/40 px-2.5 py-1">
            {{ $t("independence.title") }}: <b class="tabular-nums text-ink">{{ independencePct }}%</b>
          </span>
          <span v-if="stanceText" class="rounded-lg border border-bd bg-surface/40 px-2.5 py-1">
            {{ $t("stance.title") }}: <b class="tabular-nums text-ink">{{ stanceText }}</b>
          </span>
          <span v-if="report.numeric_check.total" class="rounded-lg border border-bd bg-surface/40 px-2.5 py-1">
            {{ $t("numbers.title") }}: <b class="tabular-nums text-ink">{{ report.numeric_check.supported }}/{{ report.numeric_check.total }}</b>
          </span>
          <span v-if="report.source_reputation.flagged_count" class="rounded-lg border border-danger/40 px-2.5 py-1 text-danger">
            ⚑ {{ report.source_reputation.flagged_count }} {{ $t("reputation.flagged") }}
          </span>
          <span v-if="report.source_integrity.flagged.length" class="rounded-lg border border-danger/50 px-2.5 py-1 text-danger">
            ⛔ {{ report.source_integrity.retracted_count }} {{ $t("integrity.retracted") }}
          </span>
          <span v-if="report.cross_language.languages.length > 1" class="rounded-lg border border-bd bg-surface/40 px-2.5 py-1">
            🌐 {{ report.cross_language.languages.length }} {{ $t("crosslang.langs") }}
          </span>
        </div>

        <MarkdownView
          :source="report.final_report"
          :sources="report.sources"
          :grounding="report.citations.grounding"
        />

        <footer class="mt-10 border-t border-bd pt-4 text-xs text-muted">
          {{ $t("share.footer") }} · <a href="/" class="text-accent hover:underline">{{ $t("sidebar.brand") }}</a>
        </footer>
      </article>
    </div>
  </div>
</template>
