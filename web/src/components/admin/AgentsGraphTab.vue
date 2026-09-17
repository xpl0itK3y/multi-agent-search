<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
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
const hoveredAgentId = ref<string | null>(null);
const hoveredEdgeId = ref<string | null>(null);
const drawerOpen = ref(false);
const zoom = ref(1.0);

const canvasViewportRef = ref<HTMLElement | null>(null);
const zoomContainerRef = ref<HTMLElement | null>(null);

// ── Wire Data Payloads ────────────────────────────────────────────────────────
const EDGE_PAYLOADS: Record<string, string> = {
  "clarifier->optimizer": "clarified_intent",
  "optimizer->orchestrator": "optimized_prompt",
  "orchestrator->cross_language": "target_queries",
  "orchestrator->search": "primary_queries",
  "cross_language->search": "translated_queries",
  "search->source_critic": "scraped_pages",
  "source_critic->source_reputation": "filtered_sources",
  "source_reputation->source_independence": "trusted_domains",
  "source_independence->evidence_mapper": "canonical_clusters",
  "evidence_mapper->replan": "evidence_blocks",
  "replan->analyzer": "verified_evidence",
  "analyzer->numeric_check": "draft_report",
  "numeric_check->claim_verifier": "numeric_metrics",
  "claim_verifier->citation_audit": "verified_claims",
  "claim_verifier->red_team": "core_theses",
  "citation_audit->retraction": "cited_dois",
  "red_team->stance": "counter_arguments",
  "retraction->report_critic": "clean_sources",
  "stance->report_critic": "consensus_matrix",
  "report_critic->confidence": "polished_draft",
  "confidence->chat": "final_report",
};

// ── Simulation Walkthrough Steps ("Как они работают") ────────────────────────
interface SimulationStep {
  stepNumber: number;
  stageName: string;
  stageColor: string;
  agentIds: string[];
  activeEdges: string[];
  title: string;
  description: string;
  payloadInfo: string;
}

const SIMULATION_STEPS: SimulationStep[] = [
  {
    stepNumber: 1,
    stageName: "Planning",
    stageColor: "text-blue-400 border-blue-500/30 bg-blue-500/10",
    agentIds: ["clarifier"],
    activeEdges: [],
    title: "1. ClarifierAgent: Оценка однозначности запроса",
    description:
      "Анализирует входящий запрос пользователя. Если запрос слишком короткий, двусмысленный или не содержит ключевых критериев, агент генерирует точечные уточняющие вопросы до запуска тяжёлых поисковых циклов.",
    payloadInfo: "Вход: prompt, user_context ➔ Выход: clarification_needed, suggested_followups",
  },
  {
    stepNumber: 2,
    stageName: "Planning",
    stageColor: "text-blue-400 border-blue-500/30 bg-blue-500/10",
    agentIds: ["optimizer"],
    activeEdges: ["clarifier->optimizer"],
    title: "2. PromptOptimizerAgent: Обогащение контекстом и гипотезами",
    description:
      "Преобразует пользовательскую мысль в глубокий исследовательский бриф: добавляет академические термины, отраслевые контексты, противоположные точки зрения и формулирует проверяемые гипотезы.",
    payloadInfo: "Передано: clarified_intent ➔ Выход: optimized_prompt, angles, hypotheses",
  },
  {
    stepNumber: 3,
    stageName: "Planning",
    stageColor: "text-blue-400 border-blue-500/30 bg-blue-500/10",
    agentIds: ["orchestrator", "cross_language"],
    activeEdges: ["optimizer->orchestrator", "orchestrator->cross_language"],
    title: "3. Orchestrator & CrossLanguage: Декомпозиция и языковая экспансия",
    description:
      "Orchestrator декомпозирует тему на граф параллельных подзадач. CrossLanguageAgent переводит поисковые запросы на нативные языки (русский, немецкий, китайский) для выхода за пределы англоязычного информационного пузыря.",
    payloadInfo: "Передано: optimized_prompt ➔ Выход: subtasks, translated_queries",
  },
  {
    stepNumber: 4,
    stageName: "Search & Ingest",
    stageColor: "text-emerald-400 border-emerald-500/30 bg-emerald-500/10",
    agentIds: ["search"],
    activeEdges: ["orchestrator->search", "cross_language->search"],
    title: "4. SearchAgent: Параллельный поиск и нативная экстракция",
    description:
      "Воркеры распределяют запросы по поисковым провайдерам (Tavily, SearXNG, DuckDuckGo), а потоковый Rust/Trafilatura экстрактор мгновенно скачивает HTML и PDF-документы с контролем безопасности сети.",
    payloadInfo: "Передано: primary_queries + translated_queries ➔ Выход: scraped_pages, raw_snippets",
  },
  {
    stepNumber: 5,
    stageName: "Search & Ingest",
    stageColor: "text-emerald-400 border-emerald-500/30 bg-emerald-500/10",
    agentIds: ["source_critic", "source_reputation", "source_independence"],
    activeEdges: [
      "search->source_critic",
      "source_critic->source_reputation",
      "source_reputation->source_independence",
    ],
    title: "5. Фильтрация источников, оценка репутации и дедупликация",
    description:
      "SourceCritic отсекает SEO-фермы и дорвеи. SourceReputation взвешивает авторитетность доменов (.edu, .gov, рецензируемые журналы). SourceIndependence удаляет синдицированные новости (AP/Reuters), исключая ложный консенсус.",
    payloadInfo: "Передано: scraped_pages ➔ Выход: trusted_domains, canonical_clusters",
  },
  {
    stepNumber: 6,
    stageName: "Search & Ingest",
    stageColor: "text-emerald-400 border-emerald-500/30 bg-emerald-500/10",
    agentIds: ["evidence_mapper"],
    activeEdges: ["source_independence->evidence_mapper"],
    title: "6. EvidenceMapperAgent: Привязка доказательств к гипотезам",
    description:
      "Нарезает тексты на смысловые фрагменты и сопоставляет каждый абзац с конкретной подзадачей и целевой гипотезой, создавая матрицу фактологического покрытия темы.",
    payloadInfo: "Передано: canonical_clusters ➔ Выход: attributed_evidence_blocks, coverage_matrix",
  },
  {
    stepNumber: 7,
    stageName: "Synthesis & Logic",
    stageColor: "text-purple-400 border-purple-500/30 bg-purple-500/10",
    agentIds: ["replan"],
    activeEdges: ["evidence_mapper->replan"],
    title: "7. ReplanAgent: Поиск пробелов (LangGraph Gap Analysis)",
    description:
      "Анализирует матрицу фактов. Если обнаружены пробелы в аргументации или неполнота данных, динамически перенаправляет воркеры на дополнительный целевой цикл сбора.",
    payloadInfo: "Передано: evidence_blocks ➔ Выход: gap_detected, verified_evidence",
  },
  {
    stepNumber: 8,
    stageName: "Synthesis & Logic",
    stageColor: "text-purple-400 border-purple-500/30 bg-purple-500/10",
    agentIds: ["analyzer"],
    activeEdges: ["replan->analyzer"],
    title: "8. AnalyzerAgent (DeepSeek-Reasoner): Глубокий синтез аргументов",
    description:
      "Флагманская reasoning-модель DeepSeek выполняет многоуровневый логический синтез, генерируя черновик детального аналитического отчёта со сквозной цепочкой рассуждений (Chain of Thought).",
    payloadInfo: "Передано: verified_evidence ➔ Выход: draft_report, reasoning_steps, key_findings",
  },
  {
    stepNumber: 9,
    stageName: "Synthesis & Logic",
    stageColor: "text-purple-400 border-purple-500/30 bg-purple-500/10",
    agentIds: ["numeric_check", "claim_verifier"],
    activeEdges: ["analyzer->numeric_check", "numeric_check->claim_verifier"],
    title: "9. NumericCheck & ClaimVerifier: Сверка чисел и фактов",
    description:
      "NumericCheck сопоставляет все проценты, даты и финансовые суммы с исходными таблицами. ClaimVerifier проверяет каждый ключевой факт отчёта, исключая галлюцинации LLM.",
    payloadInfo: "Передано: draft_report ➔ Выход: verified_numbers, verified_claims",
  },
  {
    stepNumber: 10,
    stageName: "Synthesis & Verification",
    stageColor: "text-purple-400 border-purple-500/30 bg-purple-500/10",
    agentIds: ["citation_audit", "retraction", "red_team", "stance"],
    activeEdges: [
      "claim_verifier->citation_audit",
      "claim_verifier->red_team",
      "citation_audit->retraction",
      "red_team->stance",
    ],
    title: "10. RedTeam, Stance & Integrity: Стресс-тестирование и аудит ссылок",
    description:
      "RedTeam выступает в роли «адвоката дьявола» и атакует выводы контр-примерами. Stance формирует матрицу консенсуса и мнений меньшинств. CitationAudit и Retraction сверяют DOI со списком отозванных научных статей.",
    payloadInfo: "Передано: verified_claims ➔ Выход: counter_arguments, consensus_matrix, clean_sources",
  },
  {
    stepNumber: 11,
    stageName: "Synthesis & Verification",
    stageColor: "text-purple-400 border-purple-500/30 bg-purple-500/10",
    agentIds: ["report_critic", "confidence"],
    activeEdges: [
      "stance->report_critic",
      "retraction->report_critic",
      "report_critic->confidence",
    ],
    title: "11. ReportCritic & Confidence: Финальная полировка и скоринг",
    description:
      "ReportCritic шлифует текст по принципу перевёрнутой пирамиды, удаляя повторы. ConfidenceAgent рассчитывает калиброванный индекс достоверности (0-100%) и формирует бейдж доверия.",
    payloadInfo: "Передано: consensus_matrix + clean_sources ➔ Выход: polished_draft, trust_indicators",
  },
  {
    stepNumber: 12,
    stageName: "Delivery & Follow-Up",
    stageColor: "text-amber-400 border-amber-500/30 bg-amber-500/10",
    agentIds: ["chat"],
    activeEdges: ["confidence->chat"],
    title: "12. ChatAgent: Интерактивный диалог по доказательной базе",
    description:
      "Отчёт доставлен пользователю. ChatAgent готов отвечать на любые последующие вопросы и углубляться в детали, строго опираясь на собранную базу цитат и проверенных фактов.",
    payloadInfo: "Передано: final_report + trust_badge ➔ Выход: grounded_interactive_answers",
  },
];

// Simulation state
const isSimulating = ref(false);
const currentStepIndex = ref(0);
const simSpeed = ref<1 | 2>(1);
let simTimer: any = null;

const currentStep = computed(() => SIMULATION_STEPS[currentStepIndex.value]);

function startSimulation() {
  isSimulating.value = true;
  runSimLoop();
}

function pauseSimulation() {
  isSimulating.value = false;
  if (simTimer) {
    clearTimeout(simTimer);
    simTimer = null;
  }
}

function resetSimulation() {
  pauseSimulation();
  currentStepIndex.value = 0;
}

function goToStep(idx: number) {
  currentStepIndex.value = Math.max(0, Math.min(idx, SIMULATION_STEPS.length - 1));
}

function nextStep() {
  if (currentStepIndex.value < SIMULATION_STEPS.length - 1) {
    currentStepIndex.value++;
  } else {
    currentStepIndex.value = 0;
  }
}

function prevStep() {
  if (currentStepIndex.value > 0) {
    currentStepIndex.value--;
  }
}

function runSimLoop() {
  if (!isSimulating.value) return;
  const interval = simSpeed.value === 2 ? 1800 : 3500;
  simTimer = setTimeout(() => {
    if (!isSimulating.value) return;
    if (currentStepIndex.value < SIMULATION_STEPS.length - 1) {
      currentStepIndex.value++;
      runSimLoop();
    } else {
      isSimulating.value = false;
    }
  }, interval);
}

// ── Node status for simulation ────────────────────────────────────────────────
function getAgentSimStatus(agentId: string): "idle" | "active" | "completed" {
  if (!isSimulating.value && currentStepIndex.value === 0) return "idle";
  const step = currentStep.value;
  if (step.agentIds.includes(agentId)) {
    return "active";
  }
  // Check if agent participated in an earlier step
  for (let i = 0; i < currentStepIndex.value; i++) {
    if (SIMULATION_STEPS[i].agentIds.includes(agentId)) {
      return "completed";
    }
  }
  return "idle";
}

// ── Graph Data & Filtering ────────────────────────────────────────────────────
async function fetchAgents() {
  try {
    loading.value = true;
    error.value = null;
    agents.value = await adminApi.getAgents();
    await nextTick();
    updateEdgeCoordinates();
  } catch (err: any) {
    error.value = err.message || "Failed to load agent catalog";
  } finally {
    loading.value = false;
  }
}

onMounted(() => {
  fetchAgents();
  window.addEventListener("resize", handleResize);
  setupResizeObserver();
});

onBeforeUnmount(() => {
  window.removeEventListener("resize", handleResize);
  if (resizeObserver) resizeObserver.disconnect();
  if (simTimer) clearTimeout(simTimer);
});

let resizeObserver: ResizeObserver | null = null;
function setupResizeObserver() {
  if (typeof ResizeObserver !== "undefined" && zoomContainerRef.value) {
    resizeObserver = new ResizeObserver(() => {
      updateEdgeCoordinates();
    });
    resizeObserver.observe(zoomContainerRef.value);
  }
}

function handleResize() {
  updateEdgeCoordinates();
}

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

// 5 Balanced Columns layout
const columns = computed(() => {
  const all = filteredAgents.value;
  return [
    {
      id: "planning",
      title: t("admin.agents.stagePlanning"),
      badgeClass: "bg-blue-500/15 text-blue-400",
      stageFilter: "planning",
      agents: all.filter((a) => a.stage === "planning"),
    },
    {
      id: "search",
      title: t("admin.agents.stageSearch"),
      badgeClass: "bg-emerald-500/15 text-emerald-400",
      stageFilter: "search",
      agents: all.filter((a) => a.stage === "search"),
    },
    {
      id: "synthesis_core",
      title: t("admin.agents.stageSynthesis"),
      badgeClass: "bg-purple-500/15 text-purple-400",
      stageFilter: "synthesis",
      agents: all.filter(
        (a) =>
          a.stage === "synthesis" &&
          ["replan", "analyzer", "numeric_check", "claim_verifier"].includes(a.id)
      ),
    },
    {
      id: "synthesis_verify",
      title: t("admin.agents.stageVerification"),
      badgeClass: "bg-purple-500/15 text-purple-400",
      stageFilter: "synthesis",
      agents: all.filter(
        (a) =>
          a.stage === "synthesis" &&
          ["citation_audit", "retraction", "red_team", "stance", "report_critic", "confidence"].includes(a.id)
      ),
    },
    {
      id: "delivery",
      title: t("admin.agents.stageDelivery"),
      badgeClass: "bg-amber-500/15 text-amber-400",
      stageFilter: "delivery",
      agents: all.filter((a) => a.stage === "delivery"),
    },
  ];
});

// ── SVG Edge Coordinate Calculation ──────────────────────────────────────────
interface ComputedEdge {
  id: string;
  sourceId: string;
  targetId: string;
  d: string;
  midX: number;
  midY: number;
  payload: string;
  labelWidth: number;
  isHighlighted: boolean;
  isActive: boolean;
  color: string;
  width: number;
  markerId: string;
  activeColor: string;
}

const computedEdges = ref<ComputedEdge[]>([]);

function updateEdgeCoordinates() {
  if (!zoomContainerRef.value) return;

  const containerRect = zoomContainerRef.value.getBoundingClientRect();
  const currentZoom = zoom.value || 1.0;

  // Query all ports
  const inPorts = zoomContainerRef.value.querySelectorAll<HTMLElement>('[data-port="in"]');
  const outPorts = zoomContainerRef.value.querySelectorAll<HTMLElement>('[data-port="out"]');

  const inPortMap = new Map<string, { x: number; y: number }>();
  const outPortMap = new Map<string, { x: number; y: number }>();

  inPorts.forEach((el) => {
    const aid = el.getAttribute("data-agent-id");
    if (aid) {
      const r = el.getBoundingClientRect();
      inPortMap.set(aid, {
        x: (r.left + r.width / 2 - containerRect.left) / currentZoom,
        y: (r.top + r.height / 2 - containerRect.top) / currentZoom,
      });
    }
  });

  outPorts.forEach((el) => {
    const aid = el.getAttribute("data-agent-id");
    if (aid) {
      const r = el.getBoundingClientRect();
      outPortMap.set(aid, {
        x: (r.left + r.width / 2 - containerRect.left) / currentZoom,
        y: (r.top + r.height / 2 - containerRect.top) / currentZoom,
      });
    }
  });

  const edges: ComputedEdge[] = [];

  for (const agent of filteredAgents.value) {
    for (const depId of agent.dependencies) {
      const p1 = outPortMap.get(depId);
      const p2 = inPortMap.get(agent.id);

      if (p1 && p2) {
        const edgeKey = `${depId}->${agent.id}`;
        const dx = Math.max(60, Math.abs(p2.x - p1.x) * 0.5);
        const d = `M ${p1.x} ${p1.y} C ${p1.x + dx} ${p1.y}, ${p2.x - dx} ${p2.y}, ${p2.x} ${p2.y}`;
        const midX = (p1.x + p2.x) / 2;
        const midY = (p1.y + p2.y) / 2;

        const payload = EDGE_PAYLOADS[edgeKey] || "data_payload";
        const labelWidth = Math.max(68, payload.length * 6.5 + 14);

        const isWireHovered = hoveredEdgeId.value === edgeKey;
        const isAgentHovered =
          hoveredAgentId.value === depId || hoveredAgentId.value === agent.id;
        const isAgentSelected =
          selectedAgent.value?.id === depId || selectedAgent.value?.id === agent.id;

        const isHighlighted = isWireHovered || isAgentHovered || isAgentSelected;
        const isActive =
          (isSimulating.value || currentStepIndex.value > 0) &&
          currentStep.value.activeEdges.includes(edgeKey);

        let color = "rgba(148, 163, 184, 0.25)";
        let markerId = "arrow-default";
        let width = 1.8;
        const activeColor = "#38bdf8";

        if (isActive) {
          color = "#38bdf8";
          markerId = "arrow-active";
          width = 3.2;
        } else if (isHighlighted) {
          color = "#818cf8";
          markerId = "arrow-highlight";
          width = 2.6;
        }

        edges.push({
          id: edgeKey,
          sourceId: depId,
          targetId: agent.id,
          d,
          midX,
          midY,
          payload,
          labelWidth,
          isHighlighted,
          isActive,
          color,
          width,
          markerId,
          activeColor,
        });
      }
    }
  }

  computedEdges.value = edges;
}

watch(
  [agents, zoom, filteredAgents, selectedStage, currentStepIndex, isSimulating, hoveredAgentId, hoveredEdgeId, selectedAgent],
  () => {
    nextTick(() => {
      updateEdgeCoordinates();
    });
  }
);

// ── Interactivity ─────────────────────────────────────────────────────────────
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

function onCardHover(agentId: string | null) {
  hoveredAgentId.value = agentId;
}

function onEdgeHover(edgeKey: string | null) {
  hoveredEdgeId.value = edgeKey;
}

function isHighlighted(agent: AgentMetadataItem): boolean {
  if (hoveredEdgeId.value) {
    const [from, to] = hoveredEdgeId.value.split("->");
    if (agent.id === from || agent.id === to) return true;
  }
  if (hoveredAgentId.value) {
    if (agent.id === hoveredAgentId.value) return true;
    const target = agents.value.find((a) => a.id === hoveredAgentId.value);
    if (target?.dependencies.includes(agent.id) || agent.dependencies.includes(hoveredAgentId.value)) {
      return true;
    }
  }
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
  if (hoveredAgentId.value || hoveredEdgeId.value || selectedAgent.value) {
    return !isHighlighted(agent);
  }
  return false;
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
    <!-- Top Control Bar: Search, Filters, Zoom, Simulation Trigger -->
    <div class="flex flex-wrap items-center justify-between gap-3 border-b border-bd pb-4">
      <!-- Search Filter -->
      <div class="relative w-72">
        <span class="absolute left-3 top-1/2 -translate-y-1/2 text-muted text-xs">🔍</span>
        <input
          v-model="searchQuery"
          type="text"
          :placeholder="t('admin.agents.searchPlaceholder')"
          class="w-full rounded-xl border border-bd bg-surface/60 py-1.5 pl-8 pr-3 text-xs text-ink placeholder:text-muted focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
        />
        <button
          v-if="searchQuery"
          class="absolute right-2.5 top-1/2 -translate-y-1/2 text-xs text-muted hover:text-ink"
          @click="searchQuery = ''"
        >
          ✕
        </button>
      </div>

      <!-- Stage Column Filters -->
      <div class="flex flex-wrap items-center gap-1.5">
        <button
          class="rounded-lg px-2.5 py-1 text-xs font-medium transition"
          :class="selectedStage === 'all' ? 'bg-accent text-white font-semibold' : 'bg-surface text-muted hover:text-ink'"
          @click="selectedStage = 'all'"
        >
          All ({{ agents.length }})
        </button>
        <button
          class="rounded-lg px-2.5 py-1 text-xs font-medium capitalize transition"
          :class="selectedStage === 'planning' ? 'bg-accent text-white font-semibold' : 'bg-surface text-muted hover:text-ink'"
          @click="selectedStage = 'planning'"
        >
          Planning (4)
        </button>
        <button
          class="rounded-lg px-2.5 py-1 text-xs font-medium capitalize transition"
          :class="selectedStage === 'search' ? 'bg-accent text-white font-semibold' : 'bg-surface text-muted hover:text-ink'"
          @click="selectedStage = 'search'"
        >
          Search (5)
        </button>
        <button
          class="rounded-lg px-2.5 py-1 text-xs font-medium capitalize transition"
          :class="selectedStage === 'synthesis' ? 'bg-accent text-white font-semibold' : 'bg-surface text-muted hover:text-ink'"
          @click="selectedStage = 'synthesis'"
        >
          Synthesis (10)
        </button>
        <button
          class="rounded-lg px-2.5 py-1 text-xs font-medium capitalize transition"
          :class="selectedStage === 'delivery' ? 'bg-accent text-white font-semibold' : 'bg-surface text-muted hover:text-ink'"
          @click="selectedStage = 'delivery'"
        >
          Delivery (1)
        </button>
      </div>

      <!-- Right Action Group: Simulation Controls & Zoom -->
      <div class="flex items-center gap-2">
        <!-- Simulation Run / Pause Toggle -->
        <div class="flex items-center gap-1 rounded-xl border border-bd bg-surface/70 p-1">
          <button
            v-if="!isSimulating"
            class="flex items-center gap-1.5 rounded-lg bg-accent px-3 py-1 text-xs font-bold text-white shadow transition hover:bg-accent/90"
            @click="startSimulation"
          >
            <span>▶</span>
            <span>{{ currentStepIndex === 0 ? t("admin.agents.startSim") : "Продолжить" }}</span>
          </button>
          <button
            v-else
            class="flex items-center gap-1.5 rounded-lg bg-amber-500 px-3 py-1 text-xs font-bold text-white shadow transition hover:bg-amber-600 animate-pulse"
            @click="pauseSimulation"
          >
            <span>⏸</span>
            <span>{{ t("admin.agents.pauseSim") }}</span>
          </button>

          <button
            v-if="currentStepIndex > 0 || isSimulating"
            class="rounded-lg px-2 py-1 text-xs text-muted hover:bg-surface hover:text-ink transition"
            :title="t('admin.agents.resetSim')"
            @click="resetSimulation"
          >
            ↺
          </button>

          <!-- Sim speed toggle -->
          <button
            class="rounded px-2 py-1 font-mono text-[10px] font-semibold text-muted hover:text-ink"
            @click="simSpeed = simSpeed === 1 ? 2 : 1"
          >
            {{ simSpeed }}x
          </button>
        </div>

        <!-- Zoom Controls -->
        <div class="flex items-center gap-1 rounded-lg border border-bd bg-surface/60 p-1 text-xs text-muted">
          <button class="rounded px-2 py-1 hover:bg-surface hover:text-ink" title="Zoom In" @click="zoomIn">
            +
          </button>
          <button class="px-1.5 py-1 font-mono text-[11px] hover:text-ink" @click="resetZoom">
            {{ Math.round(zoom * 100) }}%
          </button>
          <button class="rounded px-2 py-1 hover:bg-surface hover:text-ink" title="Zoom Out" @click="zoomOut">
            −
          </button>
        </div>
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
      ref="canvasViewportRef"
      class="relative flex-1 overflow-auto rounded-2xl border border-bd/80 bg-canvas p-6 shadow-inner min-h-[640px]"
      style="background-image: radial-gradient(circle, rgba(255, 255, 255, 0.08) 1px, transparent 1px); background-size: 22px 22px;"
    >
      <!-- Scaled Content Wrapper -->
      <div
        ref="zoomContainerRef"
        class="relative inline-flex min-w-full p-6 transition-transform duration-150 origin-top-left"
        :style="{ transform: `scale(${zoom})` }"
      >
        <!-- SVG Connections Layer (Exact Coordinates between ports) -->
        <svg
          class="pointer-events-none absolute inset-0 z-0 h-full w-full overflow-visible"
        >
          <defs>
            <!-- Default subtle arrowhead -->
            <marker
              id="arrow-default"
              viewBox="0 0 10 10"
              refX="7"
              refY="5"
              markerWidth="6"
              markerHeight="6"
              orient="auto-start-reverse"
            >
              <path d="M 0 1.5 L 8 5 L 0 8.5 z" fill="rgba(148, 163, 184, 0.4)" />
            </marker>

            <!-- Highlighted wire arrowhead -->
            <marker
              id="arrow-highlight"
              viewBox="0 0 10 10"
              refX="7"
              refY="5"
              markerWidth="6"
              markerHeight="6"
              orient="auto-start-reverse"
            >
              <path d="M 0 1.5 L 8 5 L 0 8.5 z" fill="#818cf8" />
            </marker>

            <!-- Active simulation wire arrowhead -->
            <marker
              id="arrow-active"
              viewBox="0 0 10 10"
              refX="8"
              refY="5"
              markerWidth="7"
              markerHeight="7"
              orient="auto-start-reverse"
            >
              <path d="M 0 1 L 9 5 L 0 9 z" fill="#38bdf8" />
            </marker>

            <!-- Glow filter for traveling particles -->
            <filter id="wire-glow" x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="3.5" result="coloredBlur" />
              <feMerge>
                <feMergeNode in="coloredBlur" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
          </defs>

          <!-- SVG Connecting Curves -->
          <g v-for="edge in computedEdges" :key="edge.id">
            <!-- Curve wire line -->
            <path
              :d="edge.d"
              :stroke="edge.color"
              :stroke-width="edge.width"
              fill="none"
              :stroke-dasharray="edge.isActive ? '7,7' : 'none'"
              :class="{ 'animate-wire-flow': edge.isActive }"
              :marker-end="`url(#${edge.markerId})`"
              class="transition-all duration-300 pointer-events-auto cursor-pointer"
              @mouseenter="onEdgeHover(edge.id)"
              @mouseleave="onEdgeHover(null)"
            />

            <!-- Traveling Data Particle on active simulation wires -->
            <circle
              v-if="edge.isActive"
              r="4.5"
              fill="#38bdf8"
              filter="url(#wire-glow)"
            >
              <animateMotion
                :path="edge.d"
                :dur="simSpeed === 2 ? '0.9s' : '1.8s'"
                repeatCount="indefinite"
              />
            </circle>

            <!-- Data payload badge on curve midpoint -->
            <g
              v-if="edge.isHighlighted || edge.isActive || zoom >= 0.9"
              :transform="`translate(${edge.midX}, ${edge.midY})`"
              class="pointer-events-auto cursor-pointer transition-transform hover:scale-110"
              @mouseenter="onEdgeHover(edge.id)"
              @mouseleave="onEdgeHover(null)"
            >
              <rect
                :x="-edge.labelWidth / 2"
                y="-10"
                :width="edge.labelWidth"
                height="20"
                rx="6"
                class="stroke-[1.5]"
                :class="[
                  edge.isActive
                    ? 'fill-slate-900 stroke-sky-400'
                    : edge.isHighlighted
                    ? 'fill-slate-900 stroke-indigo-400'
                    : 'fill-surface/95 stroke-bd/80',
                ]"
              />
              <text
                x="0"
                y="3.5"
                text-anchor="middle"
                class="font-mono text-[9px] font-semibold select-none"
                :class="[
                  edge.isActive
                    ? 'fill-sky-400'
                    : edge.isHighlighted
                    ? 'fill-indigo-300'
                    : 'fill-muted',
                ]"
              >
                {{ edge.payload }}
              </text>
            </g>
          </g>
        </svg>

        <!-- Columns of Agent Cards -->
        <div class="relative z-10 flex gap-20">
          <div
            v-for="col in columns"
            :key="col.id"
            class="flex flex-col space-y-4"
            :class="{ hidden: selectedStage !== 'all' && selectedStage !== col.stageFilter }"
          >
            <!-- Column Header -->
            <div class="flex items-center justify-between border-b border-bd/60 pb-2 px-1">
              <h3 class="text-xs font-bold uppercase tracking-wider text-muted">
                {{ col.title }}
              </h3>
              <span class="rounded-full px-2 py-0.5 text-[10px] font-bold" :class="col.badgeClass">
                {{ col.agents.length }}
              </span>
            </div>

            <!-- Stack of Cards -->
            <div class="flex flex-col space-y-4">
              <AgentNodeCard
                v-for="agent in col.agents"
                :key="agent.id"
                :agent="agent"
                :selected="selectedAgent?.id === agent.id"
                :highlighted="isHighlighted(agent)"
                :dimmed="isDimmed(agent)"
                :sim-status="getAgentSimStatus(agent.id)"
                @select="openInspector"
                @hover="onCardHover"
              />
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Floating Simulation Walkthrough Banner ("Как они работают") -->
    <div
      v-if="isSimulating || currentStepIndex > 0"
      class="rounded-2xl border border-accent/40 bg-surface/95 p-4 shadow-2xl backdrop-blur transition-all duration-300"
    >
      <div class="flex flex-wrap items-center justify-between gap-3 border-b border-bd/60 pb-3">
        <div class="flex items-center gap-2">
          <span
            class="rounded-lg border px-2 py-0.5 text-[10px] font-black uppercase tracking-wider"
            :class="currentStep.stageColor"
          >
            {{ currentStep.stageName }}
          </span>
          <h4 class="text-sm font-bold text-ink">{{ currentStep.title }}</h4>
        </div>

        <!-- Step dots timeline and navigator controls -->
        <div class="flex items-center gap-2">
          <div class="hidden sm:flex items-center gap-1 mr-2">
            <button
              v-for="(step, idx) in SIMULATION_STEPS"
              :key="step.stepNumber"
              class="h-2 rounded-full transition-all"
              :class="[
                currentStepIndex === idx
                  ? 'bg-accent w-5'
                  : idx < currentStepIndex
                  ? 'bg-emerald-500/70 w-2.5'
                  : 'bg-muted/40 hover:bg-muted w-2',
              ]"
              :title="`Шаг ${step.stepNumber}: ${step.title}`"
              @click="goToStep(idx)"
            />
          </div>

          <button
            class="rounded-lg border border-bd bg-surface px-2.5 py-1 text-xs font-medium text-ink transition hover:bg-surface/80 disabled:opacity-40"
            :disabled="currentStepIndex === 0"
            @click="prevStep"
          >
            ◀ {{ t("admin.agents.prevStep") }}
          </button>
          <span class="font-mono text-xs text-muted">
            {{ currentStep.stepNumber }} / {{ SIMULATION_STEPS.length }}
          </span>
          <button
            class="rounded-lg border border-bd bg-surface px-2.5 py-1 text-xs font-medium text-ink transition hover:bg-surface/80 disabled:opacity-40"
            :disabled="currentStepIndex === SIMULATION_STEPS.length - 1"
            @click="nextStep"
          >
            {{ t("admin.agents.nextStep") }} ▶
          </button>

          <button
            class="ml-2 rounded-lg p-1 text-xs text-muted hover:text-ink"
            title="Закрыть симуляцию"
            @click="resetSimulation"
          >
            ✕
          </button>
        </div>
      </div>

      <div class="mt-3 flex flex-wrap items-center justify-between gap-4 text-xs">
        <p class="max-w-3xl leading-relaxed text-ink/90">
          {{ currentStep.description }}
        </p>

        <div class="flex items-center gap-2 rounded-lg border border-bd/80 bg-bg/80 px-3 py-1.5 font-mono text-[11px]">
          <span class="text-muted font-sans font-medium text-[10px] uppercase">Payload:</span>
          <span class="text-accent font-semibold">{{ currentStep.payloadInfo }}</span>
        </div>
      </div>
    </div>

    <!-- Slide-over Inspector Drawer -->
    <AgentInspectorDrawer
      :agent="selectedAgent"
      :all-agents="agents"
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

@keyframes wireFlow {
  from {
    stroke-dashoffset: 28;
  }
  to {
    stroke-dashoffset: 0;
  }
}

.animate-wire-flow {
  animation: wireFlow 1.2s linear infinite;
}
</style>
