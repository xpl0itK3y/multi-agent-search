<script setup lang="ts">
defineProps<{
  phase?: string;
  sourceCount?: number;
}>();
</script>

<template>
  <div class="relative w-full h-full p-8 max-w-3xl mx-auto space-y-8 animate-fade-in overflow-hidden">
    <!-- Floating status chip -->
    <div class="flex items-center justify-between border-b border-bd/70 pb-4">
      <div class="flex items-center gap-2.5">
        <!-- The one live signal here: a slow opacity breath (live-dot), no scaling halo. -->
        <span class="live-dot h-2 w-2 shrink-0 rounded-full bg-accent" />
        <span class="text-xs font-medium text-ink">
          {{ $t("artifact.generating") }}
        </span>
        <span class="text-xs text-muted">
          · {{ $t("artifact.reportForming") }}
        </span>
      </div>

      <span v-if="sourceCount" class="text-xs text-muted font-mono bg-surface px-2 py-0.5 rounded border border-bd">
        {{ $t("artifact.sourcesInPool", sourceCount) }}
      </span>
    </div>

    <!-- One pulse for the whole skeleton instead of one per bar: the same look, a
         single animation. -->
    <div class="animate-pulse space-y-8" aria-hidden="true">
      <!-- Title Skeleton -->
      <div class="space-y-3">
        <div class="h-7 w-3/4 rounded-lg bg-surface/70" />
        <div class="h-4 w-1/3 rounded-md bg-surface/40" />
      </div>

      <!-- Executive Summary Skeleton -->
      <div class="space-y-3 rounded-xl border border-bd/60 bg-surface/30 p-5">
        <div class="h-4 w-1/4 rounded bg-surface/80" />
        <div class="space-y-2">
          <div class="h-3.5 w-full rounded bg-surface/60" />
          <div class="h-3.5 w-11/12 rounded bg-surface/60" />
          <div class="h-3.5 w-4/5 rounded bg-surface/60" />
        </div>
      </div>

      <!-- Section 1 Skeleton -->
      <div class="space-y-3">
        <div class="h-5 w-1/3 rounded-md bg-surface/70" />
        <div class="space-y-2">
          <div class="h-3.5 w-full rounded bg-surface/50" />
          <div class="h-3.5 w-5/6 rounded bg-surface/50" />
          <div class="h-3.5 w-3/4 rounded bg-surface/50" />
        </div>
      </div>

      <!-- Section 2 Skeleton (Bullet Points) -->
      <div class="space-y-3">
        <div class="h-5 w-2/5 rounded-md bg-surface/70" />
        <div class="space-y-2 pl-4">
          <div class="flex items-center gap-2">
            <div class="h-2 w-2 rounded-full bg-accent/60" />
            <div class="h-3.5 w-10/12 rounded bg-surface/50" />
          </div>
          <div class="flex items-center gap-2">
            <div class="h-2 w-2 rounded-full bg-accent/60" />
            <div class="h-3.5 w-9/12 rounded bg-surface/50" />
          </div>
          <div class="flex items-center gap-2">
            <div class="h-2 w-2 rounded-full bg-accent/60" />
            <div class="h-3.5 w-11/12 rounded bg-surface/50" />
          </div>
        </div>
      </div>

      <!-- Citations / Verification Grid Skeleton -->
      <div class="grid grid-cols-1 sm:grid-cols-2 gap-3 pt-2">
        <div class="h-16 rounded-lg border border-bd/40 bg-surface/20 p-3 flex flex-col justify-between">
          <div class="h-3 w-1/2 rounded bg-surface/60" />
          <div class="h-4 w-1/3 rounded bg-accent/30" />
        </div>
        <div class="h-16 rounded-lg border border-bd/40 bg-surface/20 p-3 flex flex-col justify-between">
          <div class="h-3 w-1/2 rounded bg-surface/60" />
          <div class="h-4 w-1/3 rounded bg-emerald-500/30" />
        </div>
      </div>
    </div>
  </div>
</template>
