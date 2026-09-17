<script setup lang="ts">
import { computed, ref } from "vue";
import { useI18n } from "vue-i18n";
import type { AgentMetadataItem } from "@/lib/types";

const props = defineProps<{
  agent: AgentMetadataItem | null;
  allAgents?: AgentMetadataItem[];
  open: boolean;
}>();

const emit = defineEmits<{
  (e: "close"): void;
  (e: "select-agent", agentId: string): void;
}>();

const { t } = useI18n();
const copied = ref(false);

const downstreamAgents = computed(() => {
  if (!props.agent || !props.allAgents) return [];
  return props.allAgents.filter((a) => a.dependencies.includes(props.agent!.id));
});

function copyPath() {
  if (!props.agent) return;
  navigator.clipboard.writeText(props.agent.source_file);
  copied.value = true;
  setTimeout(() => {
    copied.value = false;
  }, 2000);
}
</script>

<template>
  <div>
    <!-- Backdrop Overlay -->
    <div
      v-if="open"
      class="fixed inset-0 z-40 bg-black/40 backdrop-blur-sm transition-opacity"
      @click="emit('close')"
    />

    <!-- Slide-over Drawer -->
    <div
      class="fixed inset-y-0 right-0 z-50 flex w-full max-w-md flex-col border-l border-bd bg-bg p-6 shadow-2xl transition-transform duration-300 ease-in-out"
      :class="open ? 'translate-x-0' : 'translate-x-full'"
    >
      <div v-if="agent" class="flex h-full flex-col">
        <!-- Header -->
        <div class="flex items-start justify-between border-b border-bd pb-4">
          <div>
            <div class="flex items-center gap-2">
              <span
                class="rounded border px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider"
                :class="{
                  'bg-blue-500/15 text-blue-400 border-blue-500/30': agent.stage === 'planning',
                  'bg-emerald-500/15 text-emerald-400 border-emerald-500/30': agent.stage === 'search',
                  'bg-purple-500/15 text-purple-400 border-purple-500/30': agent.stage === 'synthesis',
                  'bg-amber-500/15 text-amber-400 border-amber-500/30': agent.stage === 'delivery',
                }"
              >
                {{ agent.stage }}
              </span>
              <span v-if="agent.llm_model" class="rounded bg-surface px-2 py-0.5 font-mono text-[10px] text-muted">
                {{ agent.llm_model }}
              </span>
              <span v-else class="rounded bg-emerald-500/10 px-2 py-0.5 text-[10px] text-emerald-400 font-medium">
                Native Rust
              </span>
            </div>

            <h2 class="mt-2 text-xl font-bold text-ink">{{ agent.name }}</h2>
            <p class="mt-1 text-xs text-accent font-medium">{{ agent.role }}</p>
          </div>

          <button
            class="rounded-lg p-1.5 text-muted hover:bg-surface hover:text-ink transition"
            :title="t('admin.agents.close')"
            @click="emit('close')"
          >
            ✕
          </button>
        </div>

        <!-- Body content -->
        <div class="flex-1 space-y-5 overflow-y-auto py-4 text-xs">
          <!-- Description -->
          <div>
            <h4 class="font-semibold uppercase tracking-wider text-muted text-[10px]">Overview</h4>
            <p class="mt-1 text-ink leading-relaxed">{{ agent.description }}</p>
          </div>

          <!-- Trigger -->
          <div>
            <h4 class="font-semibold uppercase tracking-wider text-muted text-[10px]">
              {{ t("admin.agents.trigger") }}
            </h4>
            <div class="mt-1.5 rounded-lg border border-bd bg-surface/50 p-2.5 text-ink font-medium">
              ⚡ {{ agent.trigger }}
            </div>
          </div>

          <!-- Input Data Contracts -->
          <div>
            <h4 class="font-semibold uppercase tracking-wider text-muted text-[10px]">
              {{ t("admin.agents.inputs") }}
            </h4>
            <div class="mt-1.5 flex flex-wrap gap-1.5">
              <span
                v-for="inp in agent.inputs"
                :key="inp"
                class="rounded-md border border-bd/80 bg-surface px-2 py-1 font-mono text-[11px] text-ink"
              >
                📥 {{ inp }}
              </span>
            </div>
          </div>

          <!-- Output Data Contracts -->
          <div>
            <h4 class="font-semibold uppercase tracking-wider text-muted text-[10px]">
              {{ t("admin.agents.outputs") }}
            </h4>
            <div class="mt-1.5 flex flex-wrap gap-1.5">
              <span
                v-for="out in agent.outputs"
                :key="out"
                class="rounded-md border border-accent/30 bg-accent/10 px-2 py-1 font-mono text-[11px] text-accent"
              >
                📤 {{ out }}
              </span>
            </div>
          </div>

          <!-- Dependencies -->
          <div>
            <h4 class="font-semibold uppercase tracking-wider text-muted text-[10px]">
              {{ t("admin.agents.dependencies") }}
            </h4>
            <div v-if="agent.dependencies.length === 0" class="mt-1 text-muted italic text-[11px]">
              None (Root entry agent)
            </div>
            <div v-else class="mt-1.5 flex flex-wrap gap-1.5">
              <button
                v-for="dep in agent.dependencies"
                :key="dep"
                class="rounded-md border border-bd bg-surface px-2.5 py-1 text-[11px] font-medium text-ink transition hover:border-accent hover:text-accent"
                @click="emit('select-agent', dep)"
              >
                🔗 {{ dep }}
              </button>
            </div>
          </div>

          <!-- Downstream Consumers -->
          <div>
            <h4 class="font-semibold uppercase tracking-wider text-muted text-[10px]">
              {{ t("admin.agents.downstream") }}
            </h4>
            <div v-if="downstreamAgents.length === 0" class="mt-1 text-muted italic text-[11px]">
              None (Terminal delivery agent)
            </div>
            <div v-else class="mt-1.5 flex flex-wrap gap-1.5">
              <button
                v-for="down in downstreamAgents"
                :key="down.id"
                class="rounded-md border border-bd bg-surface px-2.5 py-1 text-[11px] font-medium text-ink transition hover:border-accent hover:text-accent"
                @click="emit('select-agent', down.id)"
              >
                ➔ {{ down.name }}
              </button>
            </div>
          </div>

          <!-- Source File Reference -->
          <div>
            <h4 class="font-semibold uppercase tracking-wider text-muted text-[10px]">
              {{ t("admin.agents.sourceCode") }}
            </h4>
            <div class="mt-1.5 flex items-center justify-between rounded-lg border border-bd bg-surface/70 p-2 font-mono text-[11px]">
              <span class="truncate text-ink/80">{{ agent.source_file }}</span>
              <button
                class="ml-2 rounded px-2 py-0.5 text-[10px] font-sans font-medium text-accent hover:bg-surface"
                @click="copyPath"
              >
                {{ copied ? "Copied!" : "Copy" }}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
