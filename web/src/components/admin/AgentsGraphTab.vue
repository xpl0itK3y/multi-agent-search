<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { useI18n } from "vue-i18n";
import { adminApi } from "@/lib/api";
import type { AgentMetadataItem } from "@/lib/types";
import AgentNodeCard from "./AgentNodeCard.vue";
import AgentInspectorDrawer from "./AgentInspectorDrawer.vue";

const { t } = useI18n();

const agents = ref<AgentMetadataItem[]>([]);
const loading = ref(true);
const error = ref<string | null>(null);

const searchQuery = ref("");
const selectedStage = ref<string>("all");
const selectedAgent = ref<AgentMetadataItem | null>(null);
const drawerOpen = ref(false);
const zoom = ref(1);

const stageOrder = ["planning", "search", "synthesis", "delivery"] as const;

async function fetchAgents() {
  try {
    loading.value = true;
    error.value = null;
    agents.value = await adminApi.getAgents();
  } catch (err: any) {
    error.value = err.message || "Failed to load agent catalog";
  } finally {
    loading.value = false;
  }
}

onMounted(() => {
  fetchAgents();
});

const filteredAgents = computed(() => {
  const q = searchQuery.value.trim().toLowerCase();
  return agents.value.filter((agent) => {
    const matchesStage =
      selectedStage.value === "all" || agent.stage === selectedStage.value;
    if (!matchesStage) return false;
    if (!q) return true;
    return (
      agent.name.toLowerCase().includes(q) ||
      agent.role.toLowerCase().includes(q) ||
      agent.description.toLowerCase().includes(q) ||
      agent.trigger.toLowerCase().includes(q) ||
      agent.inputs.some((i) => i.toLowerCase().includes(q)) ||
      agent.outputs.some((o) => o.toLowerCase().includes(q))
    );
  });
});

const agentsByStage = computed(() => {
  const map: Record<string, AgentMetadataItem[]> = {
    planning: [],
    search: [],
    synthesis: [],
    delivery: [],
  };
  for (const agent of filteredAgents.value) {
    if (map[agent.stage]) {
      map[agent.stage].push(agent);
    }
  }
  return map;
});

function openInspector(agent: AgentMetadataItem) {
  selectedAgent.value = agent;
  drawerOpen.value = true;
}

function selectAgentById(id: string) {
  const target = agents.value.find((a) => a.id === id);
  if (target) {
    openInspector(target);
  }
}

function isHighlighted(agent: AgentMetadataItem): boolean {
  if (!selectedAgent.value) return false;
  return (
    agent.id === selectedAgent.value.id ||
    selectedAgent.value.dependencies.includes(agent.id) ||
    agent.dependencies.includes(selectedAgent.value.id)
  );
}

function isDimmed(agent: AgentMetadataItem): boolean {
  if (searchQuery.value.trim() && !filteredAgents.value.some((a) => a.id === agent.id)) {
    return true;
  }
  if (!selectedAgent.value) return false;
  return !isHighlighted(agent);
}

function stageTitle(stage: string): string {
  switch (stage) {
    case "planning":
      return t("admin.agents.stagePlanning");
    case "search":
      return t("admin.agents.stageSearch");
    case "synthesis":
      return t("admin.agents.stageSynthesis");
    case "delivery":
      return t("admin.agents.stageDelivery");
    default:
      return stage;
  }
}

function zoomIn() {
  zoom.value = Math.min(1.4, Math.round((zoom.value + 0.1) * 10) / 10);
}
function zoomOut() {
  zoom.value = Math.max(0.6, Math.round((zoom.value - 0.1) * 10) / 10);
}
function resetZoom() {
  zoom.value = 1.0;
}
</script>

<template>
  <div class="relative flex h-full flex-col space-y-4">
    <!-- Graph Controls Bar -->
    <div class="flex flex-wrap items-center justify-between gap-3 border-b border-bd pb-4">
      <!-- Search Filter -->
      <div class="relative w-72">
        <span class="absolute left-3 top-1/2 -translate-y-1/2 text-muted">🔍</span>
        <input
          v-model="searchQuery"
          type="text"
          :placeholder="t('admin.agents.searchPlaceholder')"
          class="w-full rounded-xl border border-bd bg-surface/60 py-2 pl-9 pr-3 text-xs text-ink placeholder:text-muted focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
        />
        <button
          v-if="searchQuery"
          class="absolute right-2.5 top-1/2 -translate-y-1/2 text-xs text-muted hover:text-ink"
          @click="searchQuery = ''"
        >
          ✕
        </button>
      </div>

      <!-- Stage Filters -->
      <div class="flex flex-wrap items-center gap-1.5">
        <button
          class="rounded-lg px-2.5 py-1 text-xs font-medium transition"
          :class="selectedStage === 'all' ? 'bg-accent text-white font-semibold' : 'bg-surface text-muted hover:text-ink'"
          @click="selectedStage = 'all'"
        >
          All ({{ agents.length }})
        </button>
        <button
          v-for="stg in stageOrder"
          :key="stg"
          class="rounded-lg px-2.5 py-1 text-xs font-medium capitalize transition"
          :class="selectedStage === stg ? 'bg-accent text-white font-semibold' : 'bg-surface text-muted hover:text-ink'"
          @click="selectedStage = stg"
        >
          {{ stg }} ({{ agents.filter((a) => a.stage === stg).length }})
        </button>
      </div>

      <!-- Zoom Controls -->
      <div class="flex items-center gap-1 rounded-lg border border-bd bg-surface/60 p-1 text-xs text-muted">
        <button class="rounded px-2 py-1 hover:bg-surface hover:text-ink" :title="'Zoom In'" @click="zoomIn">
          +
        </button>
        <button class="px-1.5 py-1 font-mono text-[11px] hover:text-ink" @click="resetZoom">
          {{ Math.round(zoom * 100) }}%
        </button>
        <button class="rounded px-2 py-1 hover:bg-surface hover:text-ink" :title="'Zoom Out'" @click="zoomOut">
          −
        </button>
      </div>
    </div>

    <!-- Error State -->
    <div v-if="error" class="rounded-lg border border-red-500/30 bg-red-500/10 p-4 text-xs text-red-400">
      {{ error }}
    </div>

    <!-- Loading State -->
    <div v-if="loading" class="flex h-64 items-center justify-center text-sm text-muted">
      {{ t("common.loading") }}
    </div>

    <!-- n8n Canvas Viewport -->
    <div
      v-else
      class="relative flex-1 overflow-auto rounded-2xl border border-bd/80 bg-canvas p-8 shadow-inner"
      style="min-height: 600px; background-image: radial-gradient(circle, rgba(255, 255, 255, 0.08) 1px, transparent 1px); background-size: 20px 20px;"
    >
      <!-- Zoom Container -->
      <div
        class="flex gap-10 transition-transform duration-150 origin-top-left"
        :style="{ transform: `scale(${zoom})` }"
      >
        <!-- Columns by Stage -->
        <div
          v-for="stg in stageOrder"
          :key="stg"
          class="flex flex-col space-y-4"
          :class="{ hidden: selectedStage !== 'all' && selectedStage !== stg }"
        >
          <!-- Stage Column Header -->
          <div class="flex items-center justify-between border-b border-bd/60 pb-2 px-1">
            <h3 class="text-xs font-bold uppercase tracking-wider text-muted">
              {{ stageTitle(stg) }}
            </h3>
            <span
              class="rounded-full px-2 py-0.5 text-[10px] font-bold"
              :class="{
                'bg-blue-500/15 text-blue-400': stg === 'planning',
                'bg-emerald-500/15 text-emerald-400': stg === 'search',
                'bg-purple-500/15 text-purple-400': stg === 'synthesis',
                'bg-amber-500/15 text-amber-400': stg === 'delivery',
              }"
            >
              {{ agentsByStage[stg].length }}
            </span>
          </div>

          <!-- Stack of Node Cards -->
          <div class="flex flex-col space-y-4">
            <AgentNodeCard
              v-for="agent in agentsByStage[stg]"
              :key="agent.id"
              :agent="agent"
              :selected="selectedAgent?.id === agent.id"
              :highlighted="isHighlighted(agent)"
              :dimmed="isDimmed(agent)"
              @select="openInspector"
            />
          </div>
        </div>
      </div>
    </div>

    <!-- Slide-over Inspector Drawer -->
    <AgentInspectorDrawer
      :agent="selectedAgent"
      :open="drawerOpen"
      @close="drawerOpen = false"
      @select-agent="selectAgentById"
    />
  </div>
</template>

<style scoped>
.bg-canvas {
  background-color: rgb(var(--c-bg));
}
</style>
