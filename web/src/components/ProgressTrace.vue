<script setup lang="ts">
import { ref } from "vue";

export interface TraceSource {
  domain: string;
  title?: string;
}
export interface TraceEntry {
  step: string;
  detail: string;
  sources?: TraceSource[];
}

defineProps<{ entries: TraceEntry[]; reasoning?: string; live?: boolean }>();

const open = ref(true);
const reasoningOpen = ref(false);

import { useI18n } from "vue-i18n";
const { t, te } = useI18n();

function label(step: string): string {
  return te(`trace.${step}`) ? t(`trace.${step}`) : step;
}
</script>

<template>
  <div class="rounded-card border border-bd bg-surface/50">
    <button
      class="flex w-full items-center justify-between px-4 py-3 text-sm text-ink"
      @click="open = !open"
    >
      <span class="flex items-center gap-2">
        <span
          v-if="live"
          class="h-1.5 w-1.5 animate-pulse rounded-full bg-accent"
        />
        {{ $t("trace.title") }}
        <span class="text-muted">· {{ entries.length }}</span>
      </span>
      <span class="text-muted">{{ open ? "▾" : "▸" }}</span>
    </button>

    <div v-if="open" class="border-t border-bd px-4 py-3">
      <!-- Raw model reasoning (extended-thinking style), collapsed by default -->
      <div v-if="reasoning" class="mb-3 rounded-lg border border-bd bg-bg/40">
        <button
          class="flex w-full items-center gap-2 px-3 py-2 text-sm text-muted hover:text-ink"
          @click="reasoningOpen = !reasoningOpen"
        >
          <span class="text-accentSoft">✶</span>
          {{ $t("trace.reasoning") }}
          <span class="ml-auto">{{ reasoningOpen ? "▾" : "▸" }}</span>
        </button>
        <div
          v-if="reasoningOpen"
          class="max-h-72 overflow-y-auto whitespace-pre-wrap border-t border-bd px-3 py-2 font-sans text-[13px] leading-relaxed text-muted"
        >{{ reasoning }}</div>
      </div>

      <transition-group name="trace" tag="ol" class="relative space-y-3">
        <li
          v-for="(entry, i) in entries"
          :key="i"
          class="flex gap-3 text-sm"
        >
          <span
            class="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full"
            :class="live && i === entries.length - 1 ? 'animate-pulse bg-accent' : 'bg-accentSoft'"
          />
          <div class="min-w-0 flex-1">
            <div class="text-ink">{{ label(entry.step) }}</div>
            <div v-if="entry.detail" class="line-clamp-2 break-words text-muted">{{ entry.detail }}</div>
            <!-- live research map: the actual sites the agent is looking at -->
            <div v-if="entry.sources && entry.sources.length" class="mt-1.5 flex flex-wrap gap-1">
              <span
                v-for="(s, j) in entry.sources"
                :key="j"
                class="inline-flex max-w-[220px] items-center gap-1 rounded-md border border-bd bg-surface/60 px-1.5 py-0.5 text-[11px] text-muted"
                :title="s.title || s.domain"
              >
                <img
                  :src="`https://www.google.com/s2/favicons?domain=${s.domain}&sz=32`"
                  alt=""
                  class="h-3 w-3 shrink-0 rounded-sm"
                  loading="lazy"
                  @error="($event.target as HTMLImageElement).style.display = 'none'"
                />
                <span class="truncate">{{ s.domain }}</span>
              </span>
            </div>
          </div>
        </li>
      </transition-group>
    </div>
  </div>
</template>
