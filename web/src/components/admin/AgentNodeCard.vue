<script setup lang="ts">
import type { AgentMetadataItem } from "@/lib/types";

defineProps<{
  agent: AgentMetadataItem;
  selected: boolean;
  highlighted: boolean;
  dimmed: boolean;
}>();

const emit = defineEmits<{
  (e: "select", agent: AgentMetadataItem): void;
}>();

function stageColor(stage: string): { badge: string; border: string; glow: string } {
  switch (stage) {
    case "planning":
      return {
        badge: "bg-blue-500/15 text-blue-400 border-blue-500/30",
        border: "hover:border-blue-500/60",
        glow: "border-blue-500 shadow-lg shadow-blue-500/20",
      };
    case "search":
      return {
        badge: "bg-emerald-500/15 text-emerald-400 border-emerald-500/30",
        border: "hover:border-emerald-500/60",
        glow: "border-emerald-500 shadow-lg shadow-emerald-500/20",
      };
    case "synthesis":
      return {
        badge: "bg-purple-500/15 text-purple-400 border-purple-500/30",
        border: "hover:border-purple-500/60",
        glow: "border-purple-500 shadow-lg shadow-purple-500/20",
      };
    case "delivery":
      return {
        badge: "bg-amber-500/15 text-amber-400 border-amber-500/30",
        border: "hover:border-amber-500/60",
        glow: "border-amber-500 shadow-lg shadow-amber-500/20",
      };
    default:
      return {
        badge: "bg-slate-500/15 text-slate-400 border-slate-500/30",
        border: "hover:border-slate-500/60",
        glow: "border-accent",
      };
  }
}
</script>

<template>
  <div
    class="group relative flex w-64 cursor-pointer flex-col rounded-xl border bg-surface/80 p-3.5 shadow-md backdrop-blur transition-all duration-200"
    :class="[
      stageColor(agent.stage).border,
      selected ? stageColor(agent.stage).glow + ' scale-[1.02]' : 'border-bd/80',
      dimmed ? 'opacity-30' : 'opacity-100',
      highlighted && !selected ? 'ring-2 ring-accent/60' : '',
    ]"
    @click="emit('select', agent)"
  >
    <!-- Left Input Port (Handle) -->
    <div
      v-if="agent.dependencies.length > 0"
      class="absolute -left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 rounded-full border-2 border-surface bg-muted shadow transition-all group-hover:scale-125 group-hover:bg-accent"
      title="Input connection"
    />

    <!-- Right Output Port (Handle) -->
    <div
      class="absolute -right-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 rounded-full border-2 border-surface bg-muted shadow transition-all group-hover:scale-125 group-hover:bg-accent"
      title="Output connection"
    />

    <!-- Header: Stage Tag & Model -->
    <div class="flex items-center justify-between gap-1">
      <span
        class="rounded border px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider"
        :class="stageColor(agent.stage).badge"
      >
        {{ agent.stage }}
      </span>

      <span
        v-if="agent.llm_model"
        class="rounded bg-bg/80 px-1.5 py-0.5 font-mono text-[9px] font-medium text-ink/70"
      >
        {{ agent.llm_model }}
      </span>
      <span
        v-else
        class="rounded bg-emerald-500/10 px-1.5 py-0.5 text-[9px] font-medium text-emerald-400"
      >
        Rust / Native
      </span>
    </div>

    <!-- Agent Name -->
    <div class="mt-2 text-sm font-bold text-ink transition-colors group-hover:text-accent">
      {{ agent.name }}
    </div>

    <!-- Agent Role Subtitle -->
    <div class="mt-1 line-clamp-2 text-xs text-muted leading-relaxed">
      {{ agent.role }}
    </div>

    <!-- Footer: Contract Badges -->
    <div class="mt-3 flex items-center justify-between border-t border-bd/40 pt-2 text-[10px] text-muted">
      <span>{{ agent.inputs.length }} In / {{ agent.outputs.length }} Out</span>
      <span class="font-mono text-accent group-hover:underline">Inspect →</span>
    </div>
  </div>
</template>
