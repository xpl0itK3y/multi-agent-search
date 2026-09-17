<script setup lang="ts">
import { computed, ref, watch } from "vue";
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

type TabKey = "overview" | "prompt" | "model" | "schemas" | "code";
const activeTab = ref<TabKey>("overview");

// Reset to overview whenever a new agent is opened
watch(
  () => props.agent?.id,
  () => {
    activeTab.value = "overview";
  }
);

const copiedPath = ref(false);
const copiedPrompt = ref(false);
const copiedInput = ref(false);
const copiedOutput = ref(false);

const downstreamAgents = computed(() => {
  if (!props.agent || !props.allAgents) return [];
  return props.allAgents.filter((a) => a.dependencies.includes(props.agent!.id));
});

const upstreamAgents = computed(() => {
  if (!props.agent || !props.allAgents) return [];
  return props.allAgents.filter((a) => props.agent!.dependencies.includes(a.id));
});

function copyPath() {
  if (!props.agent) return;
  navigator.clipboard.writeText(props.agent.source_file);
  copiedPath.value = true;
  setTimeout(() => {
    copiedPath.value = false;
  }, 2000);
}

function copyPrompt() {
  if (!props.agent?.system_prompt) return;
  navigator.clipboard.writeText(props.agent.system_prompt);
  copiedPrompt.value = true;
  setTimeout(() => {
    copiedPrompt.value = false;
  }, 2000);
}

function copyJson(data: any, targetRef: "input" | "output") {
  if (!data) return;
  navigator.clipboard.writeText(JSON.stringify(data, null, 2));
  if (targetRef === "input") {
    copiedInput.value = true;
    setTimeout(() => {
      copiedInput.value = false;
    }, 2000);
  } else {
    copiedOutput.value = true;
    setTimeout(() => {
      copiedOutput.value = false;
    }, 2000);
  }
}
</script>

<template>
  <div>
    <!-- Backdrop Overlay -->
    <div
      v-if="open"
      class="fixed inset-0 z-[60] bg-black/60 backdrop-blur-sm transition-opacity"
      @click="emit('close')"
    />

    <!-- Slide-over Drawer (Wider & Richer: max-w-2xl) -->
    <div
      class="fixed inset-y-0 right-0 z-[70] flex w-full max-w-2xl flex-col border-l border-bd bg-[#10141d] shadow-2xl transition-transform duration-300 ease-in-out"
      :class="open ? 'translate-x-0' : 'translate-x-full'"
    >
      <div v-if="agent" class="flex h-full flex-col">
        <!-- Drawer Header with Stage and Status Badges -->
        <div class="border-b border-bd/80 bg-[#141924] p-5 pb-4">
          <div class="flex items-start justify-between gap-4">
            <div class="min-w-0 flex-1">
              <!-- Badges Row -->
              <div class="flex flex-wrap items-center gap-2">
                <span
                  class="rounded-lg border px-2.5 py-0.5 text-[10px] font-black uppercase tracking-wider shadow-sm"
                  :class="{
                    'bg-blue-500/15 text-blue-400 border-blue-500/30': agent.stage === 'planning',
                    'bg-emerald-500/15 text-emerald-400 border-emerald-500/30': agent.stage === 'search',
                    'bg-purple-500/15 text-purple-400 border-purple-500/30': agent.stage === 'synthesis',
                    'bg-amber-500/15 text-amber-400 border-amber-500/30': agent.stage === 'delivery',
                  }"
                >
                  {{ agent.stage }}
                </span>

                <span
                  v-if="agent.llm_model"
                  class="flex items-center gap-1 rounded-lg border border-bd/80 bg-surface/80 px-2 py-0.5 font-mono text-[10px] font-semibold text-accent"
                >
                  <span>◆</span>
                  <span>{{ agent.llm_model }}</span>
                </span>
                <span
                  v-else
                  class="flex items-center gap-1 rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-2 py-0.5 font-mono text-[10px] font-semibold text-emerald-400"
                >
                  <span>⚡</span>
                  <span>Native Rust / Async</span>
                </span>

                <span
                  v-if="agent.context_window"
                  class="rounded-lg border border-bd/60 bg-bg/80 px-2 py-0.5 font-mono text-[10px] text-muted"
                >
                  🪟 {{ agent.context_window }}
                </span>

                <span
                  v-if="agent.timeout_seconds"
                  class="rounded-lg border border-bd/60 bg-bg/80 px-2 py-0.5 font-mono text-[10px] text-muted"
                >
                  ⏱️ {{ agent.timeout_seconds }}s SLA
                </span>
              </div>

              <!-- Title & Subtitle -->
              <h2 class="mt-2.5 truncate text-xl font-black text-ink tracking-tight">
                {{ agent.name }}
              </h2>
              <p class="mt-0.5 text-xs font-medium text-accent">
                {{ agent.role }}
              </p>
            </div>

            <!-- Close Button -->
            <button
              class="grid h-8 w-8 place-items-center rounded-xl border border-bd/80 bg-surface/60 text-muted transition hover:border-bd hover:bg-surface hover:text-ink"
              :title="t('admin.agents.close')"
              @click="emit('close')"
            >
              ✕
            </button>
          </div>

          <!-- Quick Metrics Bar -->
          <div class="mt-3.5 grid grid-cols-2 gap-2 sm:grid-cols-4 font-mono text-[10.5px]">
            <div class="rounded-xl border border-bd/50 bg-[#161c28] p-2">
              <span class="block text-[9px] uppercase tracking-wider text-muted">{{ t('admin.agents.temperature') }}</span>
              <span class="font-bold text-ink">
                {{ agent.temperature !== null && agent.temperature !== undefined ? agent.temperature : '— (Heuristic)' }}
              </span>
            </div>
            <div class="rounded-xl border border-bd/50 bg-[#161c28] p-2">
              <span class="block text-[9px] uppercase tracking-wider text-muted">{{ t('admin.agents.maxTokens') }}</span>
              <span class="font-bold text-ink">
                {{ agent.max_tokens ? agent.max_tokens.toLocaleString() : 'Streaming' }}
              </span>
            </div>
            <div class="rounded-xl border border-bd/50 bg-[#161c28] p-2">
              <span class="block text-[9px] uppercase tracking-wider text-muted">{{ t('admin.agents.timeoutSla') }}</span>
              <span class="font-bold text-ink">
                {{ agent.timeout_seconds ? `${agent.timeout_seconds}s` : '30s' }}
              </span>
            </div>
            <div class="rounded-xl border border-bd/50 bg-[#161c28] p-2">
              <span class="block text-[9px] uppercase tracking-wider text-muted">{{ t('admin.agents.cacheTtl') }}</span>
              <span class="truncate font-bold text-ink" :title="agent.cache_ttl || 'No cache'">
                {{ agent.cache_ttl ? agent.cache_ttl.split(' ')[0] + ' ' + (agent.cache_ttl.split(' ')[1] || '') : 'None' }}
              </span>
            </div>
          </div>

          <!-- Tabs Navigation -->
          <div class="mt-4 flex items-center gap-1 border-t border-bd/60 pt-3 text-xs overflow-x-auto">
            <button
              class="flex items-center gap-1.5 rounded-lg px-3 py-1.5 font-semibold transition"
              :class="activeTab === 'overview' ? 'bg-accent text-white shadow' : 'text-muted hover:bg-surface hover:text-ink'"
              @click="activeTab = 'overview'"
            >
              <span>📋</span>
              <span>{{ t('admin.agents.tabOverview') }}</span>
            </button>

            <button
              class="flex items-center gap-1.5 rounded-lg px-3 py-1.5 font-semibold transition"
              :class="activeTab === 'prompt' ? 'bg-accent text-white shadow' : 'text-muted hover:bg-surface hover:text-ink'"
              @click="activeTab = 'prompt'"
            >
              <span>🧠</span>
              <span>{{ t('admin.agents.tabPrompt') }}</span>
            </button>

            <button
              class="flex items-center gap-1.5 rounded-lg px-3 py-1.5 font-semibold transition"
              :class="activeTab === 'model' ? 'bg-accent text-white shadow' : 'text-muted hover:bg-surface hover:text-ink'"
              @click="activeTab = 'model'"
            >
              <span>⚙️</span>
              <span>{{ t('admin.agents.tabModel') }}</span>
            </button>

            <button
              class="flex items-center gap-1.5 rounded-lg px-3 py-1.5 font-semibold transition"
              :class="activeTab === 'schemas' ? 'bg-accent text-white shadow' : 'text-muted hover:bg-surface hover:text-ink'"
              @click="activeTab = 'schemas'"
            >
              <span>📦</span>
              <span>{{ t('admin.agents.tabSchemas') }}</span>
            </button>

            <button
              class="flex items-center gap-1.5 rounded-lg px-3 py-1.5 font-semibold transition"
              :class="activeTab === 'code' ? 'bg-accent text-white shadow' : 'text-muted hover:bg-surface hover:text-ink'"
              @click="activeTab = 'code'"
            >
              <span>💻</span>
              <span>{{ t('admin.agents.tabSource') }}</span>
            </button>
          </div>
        </div>

        <!-- Drawer Body Tabs Content -->
        <div class="flex-1 space-y-5 overflow-y-auto p-5 text-xs">
          <!-- TAB 1: OVERVIEW -->
          <div v-if="activeTab === 'overview'" class="space-y-5">
            <!-- Detailed Description -->
            <div>
              <h4 class="text-[10px] font-bold uppercase tracking-wider text-muted">
                {{ t('admin.agents.role') }} & Назначение
              </h4>
              <p class="mt-1.5 leading-relaxed text-ink/90 text-[13px]">
                {{ agent.description }}
              </p>
            </div>

            <!-- Trigger Card -->
            <div>
              <h4 class="text-[10px] font-bold uppercase tracking-wider text-muted">
                {{ t("admin.agents.trigger") }}
              </h4>
              <div class="mt-1.5 flex items-start gap-2.5 rounded-xl border border-orange-500/30 bg-orange-500/10 p-3 text-ink">
                <span class="text-base text-orange-400">⚡</span>
                <div class="text-xs leading-relaxed">
                  <span class="font-bold text-orange-400">Event Trigger:</span>
                  <p class="mt-0.5 text-ink/90 font-medium">{{ agent.trigger }}</p>
                </div>
              </div>
            </div>

            <!-- Execution Guarantees & SLA Grid -->
            <div>
              <h4 class="text-[10px] font-bold uppercase tracking-wider text-muted">
                Параметры надёжности и SLA
              </h4>
              <div class="mt-2 grid grid-cols-1 gap-2.5 sm:grid-cols-2">
                <div class="rounded-xl border border-bd bg-surface/50 p-3">
                  <div class="text-[10px] uppercase text-muted font-bold">{{ t('admin.agents.timeoutSla') }}</div>
                  <div class="mt-1 text-xs font-semibold text-ink">
                    ⏱️ {{ agent.timeout_seconds ? `${agent.timeout_seconds} секунд` : 'Стандартный (30с)' }}
                  </div>
                </div>

                <div class="rounded-xl border border-bd bg-surface/50 p-3">
                  <div class="text-[10px] uppercase text-muted font-bold">{{ t('admin.agents.retryPolicy') }}</div>
                  <div class="mt-1 text-xs font-semibold text-ink">
                    🔁 {{ agent.retry_policy || '3 попытки с экспоненциальным backoff' }}
                  </div>
                </div>

                <div class="rounded-xl border border-bd bg-surface/50 p-3">
                  <div class="text-[10px] uppercase text-muted font-bold">{{ t('admin.agents.cacheTtl') }}</div>
                  <div class="mt-1 text-xs font-semibold text-ink">
                    💾 {{ agent.cache_ttl || 'Сессионный кэш (без сохранения)' }}
                  </div>
                </div>

                <div class="rounded-xl border border-bd bg-surface/50 p-3">
                  <div class="text-[10px] uppercase text-muted font-bold">{{ t('admin.agents.responseFormat') }}</div>
                  <div class="mt-1 font-mono text-[11px] font-semibold text-accent truncate" :title="agent.response_format || 'Pydantic Model'">
                    📄 {{ agent.response_format || 'Pydantic Model' }}
                  </div>
                </div>
              </div>
            </div>

            <!-- Pipeline Graph Connections -->
            <div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
              <!-- Inputs / Dependencies -->
              <div>
                <h4 class="text-[10px] font-bold uppercase tracking-wider text-muted">
                  {{ t("admin.agents.dependencies") }} (Входы)
                </h4>
                <div v-if="upstreamAgents.length === 0" class="mt-2 rounded-xl border border-bd/60 bg-surface/30 p-2.5 text-muted italic text-[11px]">
                  ⚡ Входная нода графа (Root Dispatcher)
                </div>
                <div v-else class="mt-2 space-y-1.5">
                  <button
                    v-for="dep in upstreamAgents"
                    :key="dep.id"
                    class="flex w-full items-center justify-between rounded-xl border border-bd bg-surface/60 px-3 py-2 text-left text-xs font-medium text-ink transition hover:border-accent hover:bg-surface hover:text-accent"
                    @click="emit('select-agent', dep.id)"
                  >
                    <span class="truncate">🔗 {{ dep.name }}</span>
                    <span class="font-mono text-[10px] text-muted uppercase">← In</span>
                  </button>
                </div>
              </div>

              <!-- Outputs / Downstream Consumers -->
              <div>
                <h4 class="text-[10px] font-bold uppercase tracking-wider text-muted">
                  {{ t("admin.agents.downstream") }} (Выходы)
                </h4>
                <div v-if="downstreamAgents.length === 0" class="mt-2 rounded-xl border border-bd/60 bg-surface/30 p-2.5 text-muted italic text-[11px]">
                  🏁 Финальная нода доставки (Terminal Output)
                </div>
                <div v-else class="mt-2 space-y-1.5">
                  <button
                    v-for="down in downstreamAgents"
                    :key="down.id"
                    class="flex w-full items-center justify-between rounded-xl border border-bd bg-surface/60 px-3 py-2 text-left text-xs font-medium text-ink transition hover:border-accent hover:bg-surface hover:text-accent"
                    @click="emit('select-agent', down.id)"
                  >
                    <span class="truncate">➔ {{ down.name }}</span>
                    <span class="font-mono text-[10px] text-accent uppercase">Out →</span>
                  </button>
                </div>
              </div>
            </div>
          </div>

          <!-- TAB 2: PROMPT & LOGIC -->
          <div v-if="activeTab === 'prompt'" class="space-y-4">
            <div class="flex items-center justify-between">
              <div>
                <h4 class="text-xs font-bold text-ink">
                  {{ agent.llm_model ? 'Системный промпт агента' : 'Алгоритм и правила работы' }}
                </h4>
                <p class="text-[11px] text-muted">
                  {{ agent.llm_model ? 'Инструкции для нейросети, определяющие поведение и ограничения' : 'Детерминированная логика обработки и правила фильтрации' }}
                </p>
              </div>

              <button
                v-if="agent.system_prompt"
                class="flex items-center gap-1 rounded-lg border border-accent/40 bg-accent/15 px-2.5 py-1 font-mono text-[11px] font-semibold text-accent transition hover:bg-accent/25"
                @click="copyPrompt"
              >
                <span>{{ copiedPrompt ? '✓' : '📋' }}</span>
                <span>{{ copiedPrompt ? t('admin.agents.copied') : t('admin.agents.copyPrompt') }}</span>
              </button>
            </div>

            <!-- Prompt Code Box -->
            <div class="relative overflow-hidden rounded-2xl border border-bd/90 bg-[#0c1017] shadow-inner">
              <div class="flex items-center justify-between border-b border-bd/50 bg-[#141923] px-3.5 py-2">
                <div class="flex items-center gap-2">
                  <span class="h-2.5 w-2.5 rounded-full bg-red-500/80" />
                  <span class="h-2.5 w-2.5 rounded-full bg-amber-500/80" />
                  <span class="h-2.5 w-2.5 rounded-full bg-emerald-500/80" />
                  <span class="ml-2 font-mono text-[10px] text-muted uppercase">SYSTEM_INSTRUCTION</span>
                </div>
                <span class="font-mono text-[10px] text-accent font-medium">
                  {{ agent.response_format || 'Markdown' }}
                </span>
              </div>

              <div class="max-h-[460px] overflow-y-auto p-4 font-mono text-[11px] leading-relaxed text-slate-200 whitespace-pre-wrap select-text">
                {{ agent.system_prompt || 'Детерминированный исполнитель без внешнего LLM-промпта. Логика реализована нативным кодом.' }}
              </div>
            </div>
          </div>

          <!-- TAB 3: MODEL & TOOLS -->
          <div v-if="activeTab === 'model'" class="space-y-5">
            <!-- LLM Engine Card -->
            <div class="rounded-2xl border border-bd bg-surface/40 p-4">
              <h4 class="text-xs font-bold text-ink flex items-center gap-2">
                <span>🧠</span>
                <span>Архитектура вычислений</span>
              </h4>

              <div class="mt-3 grid grid-cols-2 gap-3 font-mono text-xs">
                <div>
                  <span class="block text-[10px] text-muted uppercase">Модель / Движок</span>
                  <span class="font-bold text-accent">
                    {{ agent.llm_model || 'Native Rust Component' }}
                  </span>
                </div>
                <div>
                  <span class="block text-[10px] text-muted uppercase">Провайдер</span>
                  <span class="font-bold text-ink">
                    {{ agent.llm_model?.includes('deepseek') ? 'DeepSeek AI' : 'In-House Async Worker' }}
                  </span>
                </div>
                <div>
                  <span class="block text-[10px] text-muted uppercase">Температура генерации</span>
                  <div class="flex items-center gap-2 mt-0.5">
                    <div class="h-2 w-24 rounded-full bg-surface border border-bd overflow-hidden">
                      <div
                        class="h-full bg-accent"
                        :style="{ width: `${Math.min(100, ((agent.temperature || 0) / 1.0) * 100)}%` }"
                      />
                    </div>
                    <span class="font-bold text-ink">{{ agent.temperature ?? 0.0 }}</span>
                  </div>
                </div>
                <div>
                  <span class="block text-[10px] text-muted uppercase">Контекстное окно</span>
                  <span class="font-bold text-ink">{{ agent.context_window || '128k' }}</span>
                </div>
              </div>
            </div>

            <!-- Tools & Integrations List -->
            <div>
              <div class="flex items-center justify-between mb-2">
                <h4 class="text-xs font-bold text-ink flex items-center gap-2">
                  <span>🛠️</span>
                  <span>{{ t('admin.agents.toolsTitle') }} ({{ agent.tools?.length || 0 }})</span>
                </h4>
              </div>

              <div v-if="!agent.tools || agent.tools.length === 0" class="rounded-xl border border-bd/60 bg-surface/30 p-3 text-muted italic">
                Внешние тулы не подключены (автономная обработка).
              </div>
              <div v-else class="grid grid-cols-1 gap-2 sm:grid-cols-2">
                <div
                  v-for="tool in agent.tools"
                  :key="tool"
                  class="flex items-center gap-2.5 rounded-xl border border-bd/80 bg-[#151a24] p-2.5 text-xs font-medium text-ink"
                >
                  <span class="text-base text-accent">⚙️</span>
                  <span class="font-mono text-[11px] truncate">{{ tool }}</span>
                </div>
              </div>
            </div>
          </div>

          <!-- TAB 4: I/O SCHEMAS & EXAMPLES -->
          <div v-if="activeTab === 'schemas'" class="space-y-5">
            <!-- Contracts Badges -->
            <div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
              <div>
                <h4 class="text-[10px] font-bold uppercase tracking-wider text-muted">
                  {{ t("admin.agents.inputs") }}
                </h4>
                <div class="mt-1.5 flex flex-wrap gap-1.5">
                  <span
                    v-for="inp in agent.inputs"
                    :key="inp"
                    class="rounded-lg border border-bd bg-surface px-2.5 py-1 font-mono text-[11px] text-ink shadow-sm"
                  >
                    📥 {{ inp }}
                  </span>
                </div>
              </div>

              <div>
                <h4 class="text-[10px] font-bold uppercase tracking-wider text-muted">
                  {{ t("admin.agents.outputs") }}
                </h4>
                <div class="mt-1.5 flex flex-wrap gap-1.5">
                  <span
                    v-for="out in agent.outputs"
                    :key="out"
                    class="rounded-lg border border-accent/40 bg-accent/15 px-2.5 py-1 font-mono text-[11px] font-semibold text-accent shadow-sm"
                  >
                    📤 {{ out }}
                  </span>
                </div>
              </div>
            </div>

            <!-- Example Input Payload -->
            <div v-if="agent.example_input" class="space-y-1.5">
              <div class="flex items-center justify-between">
                <h4 class="text-[11px] font-bold text-ink">
                  {{ t('admin.agents.exampleInput') }}
                </h4>
                <button
                  class="text-[10px] font-mono text-accent hover:underline"
                  @click="copyJson(agent.example_input, 'input')"
                >
                  {{ copiedInput ? t('admin.agents.copied') : 'Скопировать JSON' }}
                </button>
              </div>
              <pre class="rounded-xl border border-bd/80 bg-[#0b0e14] p-3 font-mono text-[10.5px] leading-normal text-emerald-400 overflow-x-auto select-text">{{ JSON.stringify(agent.example_input, null, 2) }}</pre>
            </div>

            <!-- Example Output Payload -->
            <div v-if="agent.example_output" class="space-y-1.5">
              <div class="flex items-center justify-between">
                <h4 class="text-[11px] font-bold text-ink">
                  {{ t('admin.agents.exampleOutput') }}
                </h4>
                <button
                  class="text-[10px] font-mono text-accent hover:underline"
                  @click="copyJson(agent.example_output, 'output')"
                >
                  {{ copiedOutput ? t('admin.agents.copied') : 'Скопировать JSON' }}
                </button>
              </div>
              <pre class="rounded-xl border border-bd/80 bg-[#0b0e14] p-3 font-mono text-[10.5px] leading-normal text-sky-400 overflow-x-auto select-text">{{ JSON.stringify(agent.example_output, null, 2) }}</pre>
            </div>
          </div>

          <!-- TAB 5: SOURCE CODE -->
          <div v-if="activeTab === 'code'" class="space-y-4">
            <div class="rounded-2xl border border-bd bg-surface/40 p-4">
              <h4 class="text-xs font-bold text-ink flex items-center gap-2">
                <span>📁</span>
                <span>Расположение файла в репозитории</span>
              </h4>

              <div class="mt-3 flex items-center justify-between rounded-xl border border-bd/80 bg-[#0d111a] p-2.5 font-mono text-[11px]">
                <span class="truncate text-ink font-semibold">{{ agent.source_file }}:{{ agent.line_number }}</span>
                <button
                  class="ml-2 rounded-lg bg-accent/20 border border-accent/40 px-2.5 py-1 text-[10px] font-sans font-bold text-accent hover:bg-accent hover:text-white transition"
                  @click="copyPath"
                >
                  {{ copiedPath ? t('admin.agents.copied') : t('admin.agents.copyCode') }}
                </button>
              </div>

              <div class="mt-4 space-y-2 text-xs text-muted">
                <p>
                  • Стек: <span class="text-ink font-medium">Python 3.12 / Pydantic v2 / AsyncIO</span>
                </p>
                <p>
                  • Вызов: <span class="text-ink font-medium">FastAPI REST / LangGraph State Node / Redis Stream Worker</span>
                </p>
                <p>
                  • Трейсинг: <span class="text-ink font-medium">LangSmith @maybe_traceable & Prometheus Metrics</span>
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
