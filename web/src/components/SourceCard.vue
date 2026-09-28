<script setup lang="ts">
import type { SourcePreview } from "@/lib/types";
import { safeHttpUrl } from "@/lib/url";

defineProps<{ source: SourcePreview; index: number }>();

// Quality chips in the theme's status tokens (readable in light and dark), labelled in
// the reader's language.
const QUALITY: Record<string, { key: string; cls: string }> = {
  high: { key: "dashboard.qHigh", cls: "bg-success/15 text-success" },
  medium: { key: "dashboard.qMedium", cls: "bg-warning/15 text-warning" },
  low: { key: "dashboard.qLow", cls: "bg-muted/10 text-muted" },
};
function quality(q?: string | null) {
  return QUALITY[q || "low"] ?? QUALITY.low;
}
</script>

<template>
  <a
    :href="safeHttpUrl(source.url) ?? undefined"
    target="_blank"
    rel="noopener noreferrer"
    class="block rounded-lg border border-bd bg-surface/50 p-3 transition-[transform,border-color,background-color,opacity] duration-200 hover:border-accentSoft/40 hover:bg-surface motion-safe:hover:-translate-y-0.5"
  >
    <div class="flex items-center gap-2">
      <span class="shrink-0 text-xs tabular-nums text-muted">[{{ source.source_id || `S${index}` }}]</span>
      <span class="truncate text-sm text-ink">{{ source.title || source.domain || source.url }}</span>
      <span
        class="ml-auto shrink-0 rounded-full px-2 py-0.5 text-2xs"
        :class="quality(source.source_quality).cls"
      >
        {{ $t(quality(source.source_quality).key) }}
      </span>
    </div>
    <div class="mt-1 truncate text-xs text-muted">{{ source.domain || source.url }}</div>
    <p v-if="source.snippet" class="mt-1.5 line-clamp-2 text-xs text-muted">{{ source.snippet }}</p>
  </a>
</template>
