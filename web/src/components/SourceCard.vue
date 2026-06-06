<script setup lang="ts">
import type { SourcePreview } from "@/lib/types";

defineProps<{ source: SourcePreview; index: number }>();

const QUALITY: Record<string, { label: string; cls: string }> = {
  high: { label: "high", cls: "bg-emerald-500/15 text-emerald-300" },
  medium: { label: "medium", cls: "bg-amber-500/15 text-amber-300" },
  low: { label: "low", cls: "bg-white/10 text-muted" },
};
function quality(q?: string | null) {
  return QUALITY[q || "low"] ?? QUALITY.low;
}
</script>

<template>
  <a
    :href="source.url"
    target="_blank"
    rel="noopener noreferrer"
    class="block rounded-lg border border-bd bg-surface/50 p-3 hover:bg-surface"
  >
    <div class="flex items-center gap-2">
      <span class="shrink-0 text-xs text-muted">[S{{ index }}]</span>
      <span class="truncate text-sm text-ink">{{ source.title || source.domain || source.url }}</span>
      <span
        class="ml-auto shrink-0 rounded-full px-2 py-0.5 text-[11px]"
        :class="quality(source.source_quality).cls"
      >
        {{ quality(source.source_quality).label }}
      </span>
    </div>
    <div class="mt-1 truncate text-xs text-muted">{{ source.domain || source.url }}</div>
    <p v-if="source.snippet" class="mt-1.5 line-clamp-2 text-xs text-muted">{{ source.snippet }}</p>
  </a>
</template>
