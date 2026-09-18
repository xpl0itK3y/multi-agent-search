<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import { useI18n } from "vue-i18n";
import { useUiStore } from "@/stores/ui";
import { adminApi } from "@/lib/api";
import type { AgentMetadataItem } from "@/lib/types";
import AgentInspectorDrawer from "./AgentInspectorDrawer.vue";

const { t, te } = useI18n();
const ui = useUiStore();

function getNodeName(nodeId: string, fallback: string): string {
  const key = `admin.agents.names.${nodeId}`;
  return te(key) ? t(key) : fallback;
}

function getNodeSubtitle(nodeId: string, fallback: string): string {
  const key = `admin.agents.subtitles.${nodeId}`;
  return te(key) ? t(key) : fallback;
}

const agents = ref<AgentMetadataItem[]>([]);
const loading = ref(true);
const error = ref<string | null>(null);

const searchQuery = ref("");
const selectedAgent = ref<AgentMetadataItem | null>(null);
const hoveredAgentId = ref<string | null>(null);
const hoveredEdgeId = ref<string | null>(null);
const drawerOpen = ref(false);

// ── Pan & Zoom Canvas State (n8n Style) ───────────────────────────────────────
const zoom = ref(0.55);
const panX = ref(40);
const panY = ref(60);
const isPanning = ref(false);
const isZooming = ref(false);
let zoomTimeout: any = null;
const startPan = ref({ x: 0, y: 0 });
const canvasViewportRef = ref<HTMLElement | null>(null);

function onMouseDown(e: MouseEvent) {
  if (e.button !== 0) return;
  const target = e.target as HTMLElement;
  if (
    target.closest(".interactive-node") ||
    target.closest("button") ||
    target.closest("input")
  ) {
    return;
  }
  isPanning.value = true;
  startPan.value = { x: e.clientX - panX.value, y: e.clientY - panY.value };
}

function onWheel(e: WheelEvent) {
  e.preventDefault();
  if (!canvasViewportRef.value) return;

  const rect = canvasViewportRef.value.getBoundingClientRect();
  const mouseX = e.clientX - rect.left;
  const mouseY = e.clientY - rect.top;

  // World coordinates under cursor before zoom
  const worldX = (mouseX - panX.value) / zoom.value;
  const worldY = (mouseY - panY.value) / zoom.value;

  // Soft, smooth sensitivity: damp large trackpad & wheel impulses
  const sensitivity = 0.0009;
  const factor = Math.exp(-e.deltaY * sensitivity);
  // Cap single-event scaling to max ±4% change so it never jumps abruptly
  const clampedFactor = Math.max(0.96, Math.min(1.04, factor));

  const newZoom = Math.max(0.3, Math.min(1.5, zoom.value * clampedFactor));

  // Anchor zoom around cursor position
  panX.value = Math.round(mouseX - worldX * newZoom);
  panY.value = Math.round(mouseY - worldY * newZoom);
  zoom.value = Math.round(newZoom * 1000) / 1000;

  isZooming.value = true;
  if (zoomTimeout) clearTimeout(zoomTimeout);
  zoomTimeout = setTimeout(() => {
    isZooming.value = false;
  }, 120);
}

function resetView() {
  zoom.value = 0.55;
  panX.value = 40;
  panY.value = 60;
}

function zoomIn() {
  const newZoom = Math.min(1.5, Math.round((zoom.value + 0.05) * 100) / 100);
  zoom.value = newZoom;
}

function zoomOut() {
  const newZoom = Math.max(0.3, Math.round((zoom.value - 0.05) * 100) / 100);
  zoom.value = newZoom;
}

// ── Fixed n8n Grid Coordinates for all Agents (Screenshot 2 Style) ────────────
interface VisualNode {
  id: string;
  name: string;
  subtitle: string;
  stage: "planning" | "search" | "synthesis" | "delivery" | "trigger";
  icon: string;
  iconBg: string;
  iconColor: string;
  x: number;
  y: number;
  width: number;
  height: number;
  hasInput: boolean;
  hasOutput: boolean;
  llmModel?: string;
  isTrigger?: boolean;
}

const NODE_WIDTH = 250;
const NODE_HEIGHT = 76;

const VISUAL_NODES_CONFIG: Record<string, Omit<VisualNode, "id">> = {
  trigger_start: {
    name: "User Research Query",
    subtitle: "Entry trigger / HTTP POST",
    stage: "trigger",
    icon: "⚡",
    iconBg: "bg-orange-500/15 border-orange-500/30",
    iconColor: "text-orange-400",
    x: 60,
    y: 300,
    width: 230,
    height: NODE_HEIGHT,
    hasInput: false,
    hasOutput: true,
    isTrigger: true,
  },
  clarifier: {
    name: "ClarifierAgent",
    subtitle: "Query Ambiguity Resolver",
    stage: "planning",
    icon: "💬",
    iconBg: "bg-blue-500/15 border-blue-500/30",
    iconColor: "text-blue-400",
    x: 420,
    y: 300,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
    llmModel: "deepseek-chat",
  },
  optimizer: {
    name: "PromptOptimizerAgent",
    subtitle: "Perspective Expansion",
    stage: "planning",
    icon: "✨",
    iconBg: "bg-blue-500/15 border-blue-500/30",
    iconColor: "text-blue-400",
    x: 790,
    y: 300,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
    llmModel: "deepseek-chat",
  },
  orchestrator: {
    name: "OrchestratorAgent",
    subtitle: "Task Decomposition & DAG",
    stage: "planning",
    icon: "🧭",
    iconBg: "bg-blue-500/15 border-blue-500/30",
    iconColor: "text-blue-400",
    x: 1160,
    y: 300,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
    llmModel: "deepseek-chat",
  },
  cross_language: {
    name: "CrossLanguageAgent",
    subtitle: "Multilingual Expansion",
    stage: "planning",
    icon: "🌐",
    iconBg: "bg-blue-500/15 border-blue-500/30",
    iconColor: "text-blue-400",
    x: 1490,
    y: 450,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
    llmModel: "deepseek-chat",
  },
  search: {
    name: "SearchAgent",
    subtitle: "Web Crawler & Extractor",
    stage: "search",
    icon: "🔎",
    iconBg: "bg-emerald-500/15 border-emerald-500/30",
    iconColor: "text-emerald-400",
    x: 1850,
    y: 300,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
  },
  source_critic: {
    name: "SourceCriticAgent",
    subtitle: "SEO Spam Gatekeeper",
    stage: "search",
    icon: "🛡️",
    iconBg: "bg-emerald-500/15 border-emerald-500/30",
    iconColor: "text-emerald-400",
    x: 2220,
    y: 300,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
  },
  source_reputation: {
    name: "SourceReputationAgent",
    subtitle: "Domain Authority Scorer",
    stage: "search",
    icon: "⭐",
    iconBg: "bg-emerald-500/15 border-emerald-500/30",
    iconColor: "text-emerald-400",
    x: 2590,
    y: 300,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
  },
  source_independence: {
    name: "SourceIndependenceAgent",
    subtitle: "Syndication & Clustering",
    stage: "search",
    icon: "🔗",
    iconBg: "bg-emerald-500/15 border-emerald-500/30",
    iconColor: "text-emerald-400",
    x: 2960,
    y: 300,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
  },
  evidence_mapper: {
    name: "EvidenceMapperAgent",
    subtitle: "Evidence Attribution",
    stage: "search",
    icon: "📑",
    iconBg: "bg-emerald-500/15 border-emerald-500/30",
    iconColor: "text-emerald-400",
    x: 3330,
    y: 300,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
  },
  replan: {
    name: "ReplanAgent",
    subtitle: "Gap Analysis & Loop",
    stage: "synthesis",
    icon: "🔁",
    iconBg: "bg-purple-500/15 border-purple-500/30",
    iconColor: "text-purple-400",
    x: 3700,
    y: 300,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
    llmModel: "deepseek-chat",
  },
  analyzer: {
    name: "AnalyzerAgent",
    subtitle: "Deep Multi-Perspective",
    stage: "synthesis",
    icon: "🧠",
    iconBg: "bg-purple-500/15 border-purple-500/30",
    iconColor: "text-purple-400",
    x: 4070,
    y: 300,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
    llmModel: "deepseek-reasoner",
  },
  numeric_check: {
    name: "NumericCheckAgent",
    subtitle: "Quantitative Cross-Check",
    stage: "synthesis",
    icon: "🔢",
    iconBg: "bg-purple-500/15 border-purple-500/30",
    iconColor: "text-purple-400",
    x: 4440,
    y: 300,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
  },
  claim_verifier: {
    name: "ClaimVerifierAgent",
    subtitle: "Hallucination Elimination",
    stage: "synthesis",
    icon: "✓",
    iconBg: "bg-purple-500/15 border-purple-500/30",
    iconColor: "text-purple-400",
    x: 4810,
    y: 300,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
    llmModel: "deepseek-chat",
  },
  citation_audit: {
    name: "CitationAuditAgent",
    subtitle: "Inline Citation Integrity",
    stage: "synthesis",
    icon: "📌",
    iconBg: "bg-purple-500/15 border-purple-500/30",
    iconColor: "text-purple-400",
    x: 5180,
    y: 160,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
  },
  retraction: {
    name: "RetractionAgent",
    subtitle: "Retraction Watchdog",
    stage: "synthesis",
    icon: "⚠️",
    iconBg: "bg-purple-500/15 border-purple-500/30",
    iconColor: "text-purple-400",
    x: 5550,
    y: 160,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
  },
  red_team: {
    name: "RedTeamAgent",
    subtitle: "Adversarial Stress Test",
    stage: "synthesis",
    icon: "🎯",
    iconBg: "bg-purple-500/15 border-purple-500/30",
    iconColor: "text-purple-400",
    x: 5180,
    y: 440,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
    llmModel: "deepseek-chat",
  },
  stance: {
    name: "StanceAgent",
    subtitle: "Consensus Taxonomy",
    stage: "synthesis",
    icon: "⚖️",
    iconBg: "bg-purple-500/15 border-purple-500/30",
    iconColor: "text-purple-400",
    x: 5550,
    y: 440,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
    llmModel: "deepseek-chat",
  },
  report_critic: {
    name: "ReportCriticAgent",
    subtitle: "Editorial Polish & Tone",
    stage: "synthesis",
    icon: "📝",
    iconBg: "bg-purple-500/15 border-purple-500/30",
    iconColor: "text-purple-400",
    x: 5920,
    y: 300,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
    llmModel: "deepseek-chat",
  },
  confidence: {
    name: "ConfidenceAgent",
    subtitle: "Calibrated Trust Score",
    stage: "synthesis",
    icon: "📊",
    iconBg: "bg-purple-500/15 border-purple-500/30",
    iconColor: "text-purple-400",
    x: 6290,
    y: 300,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: true,
  },
  chat: {
    name: "ChatAgent",
    subtitle: "Interactive Evidence Q&A",
    stage: "delivery",
    icon: "💬",
    iconBg: "bg-amber-500/15 border-amber-500/30",
    iconColor: "text-amber-400",
    x: 6660,
    y: 300,
    width: NODE_WIDTH,
    height: NODE_HEIGHT,
    hasInput: true,
    hasOutput: false,
    llmModel: "deepseek-chat",
  },
};

// ── Node Positions & Drag-and-Drop (Movable Nodes) ───────────────────────────
const LOCAL_STORAGE_POSITIONS_KEY = "multi-agent-search:admin-nodes-pos-v3";

const defaultNodePositions: Record<string, { x: number; y: number }> = Object.fromEntries(
  Object.entries(VISUAL_NODES_CONFIG).map(([id, conf]) => [id, { x: conf.x, y: conf.y }])
);

function loadSavedPositions(): Record<string, { x: number; y: number }> {
  try {
    // Clear legacy keys with cramped coordinates
    localStorage.removeItem("multi-agent-search:admin-nodes-pos");
    localStorage.removeItem("multi-agent-search:admin-nodes-pos-v2");

    const raw = localStorage.getItem(LOCAL_STORAGE_POSITIONS_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object") {
        return { ...defaultNodePositions, ...parsed };
      }
    }
  } catch {
    // Ignore localStorage errors
  }
  return { ...defaultNodePositions };
}

const nodePositions = ref<Record<string, { x: number; y: number }>>(loadSavedPositions());

function savePositions() {
  try {
    localStorage.setItem(LOCAL_STORAGE_POSITIONS_KEY, JSON.stringify(nodePositions.value));
  } catch {
    // Ignore
  }
}

function resetNodePositions() {
  nodePositions.value = { ...defaultNodePositions };
  try {
    localStorage.removeItem(LOCAL_STORAGE_POSITIONS_KEY);
    localStorage.removeItem("multi-agent-search:admin-nodes-pos");
    localStorage.removeItem("multi-agent-search:admin-nodes-pos-v2");
  } catch {
    // Ignore
  }
}

const visualNodes = computed<VisualNode[]>(() => {
  return Object.entries(VISUAL_NODES_CONFIG).map(([id, conf]) => {
    const pos = nodePositions.value[id] || { x: conf.x, y: conf.y };
    return {
      id,
      ...conf,
      x: pos.x,
      y: pos.y,
    };
  });
});

// ── Node Dragging State ───────────────────────────────────────────────────────
const draggingNodeId = ref<string | null>(null);
const dragStartMouse = ref({ x: 0, y: 0 });
const dragStartNodePos = ref({ x: 0, y: 0 });
const hasDraggedNode = ref(false);

function onNodeMouseDown(e: MouseEvent, nodeId: string) {
  if (e.button !== 0) return;
  e.stopPropagation();

  draggingNodeId.value = nodeId;
  dragStartMouse.value = { x: e.clientX, y: e.clientY };
  const currentPos = nodePositions.value[nodeId] || {
    x: VISUAL_NODES_CONFIG[nodeId]?.x ?? 0,
    y: VISUAL_NODES_CONFIG[nodeId]?.y ?? 0,
  };
  dragStartNodePos.value = { x: currentPos.x, y: currentPos.y };
  hasDraggedNode.value = false;
}

function onGlobalMouseMove(e: MouseEvent) {
  // 1. If dragging a single node
  if (draggingNodeId.value) {
    const dx = (e.clientX - dragStartMouse.value.x) / zoom.value;
    const dy = (e.clientY - dragStartMouse.value.y) / zoom.value;

    if (Math.abs(dx) > 3 || Math.abs(dy) > 3) {
      hasDraggedNode.value = true;
    }

    if (hasDraggedNode.value) {
      const newX = Math.round(dragStartNodePos.value.x + dx);
      const newY = Math.round(dragStartNodePos.value.y + dy);

      nodePositions.value = {
        ...nodePositions.value,
        [draggingNodeId.value]: {
          x: Math.max(10, Math.min(7200, newX)),
          y: Math.max(10, Math.min(850, newY)),
        },
      };
    }
    return;
  }

  // 2. If panning canvas
  if (isPanning.value) {
    panX.value = e.clientX - startPan.value.x;
    panY.value = e.clientY - startPan.value.y;
  }
}

function onGlobalMouseUp() {
  if (draggingNodeId.value) {
    if (hasDraggedNode.value) {
      savePositions();
    }
    setTimeout(() => {
      draggingNodeId.value = null;
      hasDraggedNode.value = false;
    }, 50);
  }
  isPanning.value = false;
}

function handleNodeClick(nodeId: string) {
  if (hasDraggedNode.value) {
    return;
  }
  openInspector(nodeId);
}

// ── Graph Edges (Forward & Feedback Loops) ───────────────────────────────────
const EDGE_CONNECTIONS: Array<{ from: string; to: string; payload: string }> = [
  { from: "trigger_start", to: "clarifier", payload: "user_query" },
  { from: "clarifier", to: "optimizer", payload: "clarified_intent" },
  { from: "optimizer", to: "orchestrator", payload: "optimized_prompt" },
  { from: "orchestrator", to: "cross_language", payload: "subtasks" },
  { from: "orchestrator", to: "search", payload: "primary_queries" },
  { from: "cross_language", to: "search", payload: "translated_queries" },
  { from: "search", to: "source_critic", payload: "scraped_pages" },
  { from: "source_critic", to: "source_reputation", payload: "filtered_domains" },
  { from: "source_reputation", to: "source_independence", payload: "trusted_domains" },
  { from: "source_independence", to: "evidence_mapper", payload: "canonical_sources" },
  { from: "evidence_mapper", to: "replan", payload: "evidence_blocks" },
  { from: "replan", to: "analyzer", payload: "verified_evidence" },
  { from: "analyzer", to: "numeric_check", payload: "draft_report" },
  { from: "numeric_check", to: "claim_verifier", payload: "numeric_metrics" },
  { from: "claim_verifier", to: "citation_audit", payload: "verified_claims" },
  { from: "claim_verifier", to: "red_team", payload: "core_theses" },
  { from: "citation_audit", to: "retraction", payload: "cited_dois" },
  { from: "red_team", to: "stance", payload: "counter_arguments" },
  { from: "retraction", to: "report_critic", payload: "clean_sources" },
  { from: "stance", to: "report_critic", payload: "consensus_matrix" },
  { from: "report_critic", to: "confidence", payload: "polished_draft" },
  { from: "confidence", to: "chat", payload: "final_report" },
];

interface ReturnConnectionConfig {
  id: string;
  from: string;
  to: string;
  payloadKey: string;
  defaultPayload: string;
  arcY: number;
  sourceOffsetX?: number;
  targetOffsetX?: number;
}

const RETURN_PORT_OFFSET = 36;

const RETURN_CONNECTIONS: ReturnConnectionConfig[] = [
  {
    id: "source_critic->search",
    from: "source_critic",
    to: "search",
    payloadKey: "admin.agents.returnPayloads.source_critic",
    defaultPayload: "↩ spam_filter_retry",
    arcY: 460,
  },
  {
    id: "replan->search",
    from: "replan",
    to: "search",
    payloadKey: "admin.agents.returnPayloads.replan",
    defaultPayload: "↩ gap_analysis_loop",
    arcY: 530,
  },
  {
    id: "claim_verifier->analyzer",
    from: "claim_verifier",
    to: "analyzer",
    payloadKey: "admin.agents.returnPayloads.claim_verifier",
    defaultPayload: "↩ hallucination_retry",
    arcY: 480,
  },
  {
    id: "report_critic->analyzer",
    from: "report_critic",
    to: "analyzer",
    payloadKey: "admin.agents.returnPayloads.report_critic",
    defaultPayload: "↩ draft_revision",
    arcY: 600,
  },
  {
    id: "report_critic->replan",
    from: "report_critic",
    to: "replan",
    payloadKey: "admin.agents.returnPayloads.report_critic_replan",
    defaultPayload: "↩ tie_break_search",
    arcY: 670,
  },
];

const showReturnLoops = ref(true);

const RETURN_CAPABLE_NODES = new Set(["report_critic", "source_critic", "replan", "claim_verifier"]);
const RETURN_RECEIVER_NODES = new Set(["search", "analyzer", "replan"]);

function hasReturnCapability(nodeId: string): boolean {
  return RETURN_CAPABLE_NODES.has(nodeId);
}

function hasReturnReceiver(nodeId: string): boolean {
  return RETURN_RECEIVER_NODES.has(nodeId);
}

function getReturnCapabilityTooltip(nodeId: string): string {
  const map: Record<string, string> = {
    report_critic: "admin.agents.feedbackLoop.reportCriticReason",
    source_critic: "admin.agents.feedbackLoop.sourceCriticReason",
    replan: "admin.agents.feedbackLoop.replanReason",
    claim_verifier: "admin.agents.feedbackLoop.claimVerifierReason",
  };
  const key = map[nodeId];
  return key && te(key) ? t(key) : t("admin.agents.canReturnBadgeFull");
}

interface RenderedEdge {
  id: string;
  from: string;
  to: string;
  d: string;
  midX: number;
  midY: number;
  payload: string;
  labelWidth: number;
  isHighlighted: boolean;
  isActive: boolean;
  isReturn: boolean;
}

const renderedEdges = computed<RenderedEdge[]>(() => {
  const nodeMap = new Map<string, VisualNode>();
  for (const n of visualNodes.value) {
    nodeMap.set(n.id, n);
  }

  const forwardEdges: RenderedEdge[] = EDGE_CONNECTIONS.map((conn) => {
    const src = nodeMap.get(conn.from);
    const tgt = nodeMap.get(conn.to);
    if (!src || !tgt) {
      return {
        id: `${conn.from}->${conn.to}`,
        from: conn.from,
        to: conn.to,
        d: "",
        midX: 0,
        midY: 0,
        payload: conn.payload,
        labelWidth: 60,
        isHighlighted: false,
        isActive: false,
        isReturn: false,
      };
    }

    // Output port: right edge center
    const x1 = src.x + src.width;
    const y1 = src.y + src.height / 2;

    // Input port: left edge center
    const x2 = tgt.x;
    const y2 = tgt.y + tgt.height / 2;

    const dx = Math.max(50, Math.abs(x2 - x1) * 0.5);
    const d = `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`;
    const midX = (x1 + x2) / 2;
    const midY = (y1 + y2) / 2;

    const edgeKey = `${conn.from}->${conn.to}`;
    const isWireHovered = hoveredEdgeId.value === edgeKey;
    const isHighlighted = isWireHovered;
    const isActive =
      (isSimulating.value || currentStepIndex.value > 0) &&
      currentStep.value.activeEdges.includes(edgeKey);

    const labelWidth = Math.max(70, conn.payload.length * 6.5 + 16);

    return {
      id: edgeKey,
      from: conn.from,
      to: conn.to,
      d,
      midX,
      midY,
      payload: conn.payload,
      labelWidth,
      isHighlighted,
      isActive,
      isReturn: false,
    };
  });

  const returnEdges: RenderedEdge[] = !showReturnLoops.value
    ? []
    : RETURN_CONNECTIONS.map((conn) => {
        const src = nodeMap.get(conn.from);
        const tgt = nodeMap.get(conn.to);
        const payload = te(conn.payloadKey) ? t(conn.payloadKey) : conn.defaultPayload;
        if (!src || !tgt) {
          return {
            id: conn.id,
            from: conn.from,
            to: conn.to,
            d: "",
            midX: 0,
            midY: 0,
            payload,
            labelWidth: 80,
            isHighlighted: false,
            isActive: false,
            isReturn: true,
          };
        }

        // Outgoing port at bottom right of source
        const x1 = src.x + src.width - (conn.sourceOffsetX ?? RETURN_PORT_OFFSET);
        const y1 = src.y + src.height;

        // Incoming port at bottom left of target
        const x2 = tgt.x + (conn.targetOffsetX ?? RETURN_PORT_OFFSET);
        const y2 = tgt.y + tgt.height;

        const arcY = conn.arcY;
        const cornerRadius = Math.min(35, Math.abs(x1 - x2) / 4);

        // Orthogonal rounded bus curve going down, left, and up:
        const d =
          `M ${x1} ${y1} ` +
          `V ${arcY - cornerRadius} ` +
          `Q ${x1} ${arcY} ${x1 - cornerRadius} ${arcY} ` +
          `L ${x2 + cornerRadius} ${arcY} ` +
          `Q ${x2} ${arcY} ${x2} ${arcY - cornerRadius} ` +
          `V ${y2}`;

        const midX = (x1 + x2) / 2;
        const midY = arcY;

        const isWireHovered = hoveredEdgeId.value === conn.id;
        const isHighlighted = isWireHovered;
        const isActive =
          (isSimulating.value || currentStepIndex.value > 0) &&
          currentStep.value.activeEdges.includes(conn.id);

        const labelWidth = Math.max(90, payload.length * 6.8 + 20);

        return {
          id: conn.id,
          from: conn.from,
          to: conn.to,
          d,
          midX,
          midY,
          payload,
          labelWidth,
          isHighlighted,
          isActive,
          isReturn: true,
        };
      });

  return [...forwardEdges, ...returnEdges];
});

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
    stageColor: "text-orange-400 border-orange-500/30 bg-orange-500/10",
    agentIds: ["trigger_start", "clarifier"],
    activeEdges: ["trigger_start->clarifier"],
    title: "1. Trigger ➔ ClarifierAgent: Оценка однозначности запроса",
    description:
      "Пользователь отправляет запрос на исследование. ClarifierAgent оценивает входящий текст на неполноту или двусмысленность и при необходимости запрашивает уточнения.",
    payloadInfo: "user_query ➔ clarification_needed, suggested_followups",
  },
  {
    stepNumber: 2,
    stageName: "Planning",
    stageColor: "text-blue-400 border-blue-500/30 bg-blue-500/10",
    agentIds: ["optimizer"],
    activeEdges: ["clarifier->optimizer"],
    title: "2. PromptOptimizerAgent: Обогащение контекстом и гипотезами",
    description:
      "Преобразует пользовательский запрос в развёрнутый аналитический бриф: добавляет академические термины, отраслевые контексты и формулирует проверяемые гипотезы.",
    payloadInfo: "clarified_intent ➔ optimized_prompt, angles, hypotheses",
  },
  {
    stepNumber: 3,
    stageName: "Planning",
    stageColor: "text-blue-400 border-blue-500/30 bg-blue-500/10",
    agentIds: ["orchestrator", "cross_language"],
    activeEdges: ["optimizer->orchestrator", "orchestrator->cross_language"],
    title: "3. Orchestrator & CrossLanguage: Декомпозиция и языковая экспансия",
    description:
      "Orchestrator декомпозирует тему на граф подзадач, а CrossLanguageAgent генерирует поисковые запросы на нативных языках (русский, китайский, немецкий) для доступа к региональным источникам.",
    payloadInfo: "optimized_prompt ➔ subtasks, translated_queries",
  },
  {
    stepNumber: 4,
    stageName: "Search & Ingest",
    stageColor: "text-emerald-400 border-emerald-500/30 bg-emerald-500/10",
    agentIds: ["search"],
    activeEdges: ["orchestrator->search", "cross_language->search"],
    title: "4. SearchAgent: Параллельный веб-поиск и Rust-экстракция",
    description:
      "Воркеры распределяют запросы по Tavily, SearXNG и DuckDuckGo, а высокоскоростной Rust/Trafilatura экстрактор потоково скачивает веб-страницы и PDF-документы.",
    payloadInfo: "primary_queries + translated_queries ➔ scraped_pages, raw_snippets",
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
      "source_critic->search",
    ],
    title: "5. Фильтрация источников, оценка репутации и дедупликация",
    description:
      "SourceCritic отсекает спам-фермы и дорвеи (при необходимости возвращает на повторный поиск). SourceReputation взвешивает домены. SourceIndependence удаляет синдицированные копии.",
    payloadInfo: "scraped_pages ➔ trusted_domains, canonical_sources / ↩ spam_retry",
  },
  {
    stepNumber: 6,
    stageName: "Search & Ingest",
    stageColor: "text-emerald-400 border-emerald-500/30 bg-emerald-500/10",
    agentIds: ["evidence_mapper"],
    activeEdges: ["source_independence->evidence_mapper"],
    title: "6. EvidenceMapperAgent: Привязка доказательств к гипотезам",
    description:
      "Нарезает тексты на смысловые фрагменты и сопоставляет каждый абзац с конкретной подзадачей и целевой гипотезой, формируя фактологическую матрицу исследования.",
    payloadInfo: "canonical_sources ➔ evidence_blocks, coverage_matrix",
  },
  {
    stepNumber: 7,
    stageName: "Synthesis & Logic",
    stageColor: "text-purple-400 border-purple-500/30 bg-purple-500/10",
    agentIds: ["replan", "search"],
    activeEdges: ["evidence_mapper->replan", "replan->search"],
    title: "7. ReplanAgent: Поиск пробелов (LangGraph Gap Analysis Loop)",
    description:
      "Анализирует матрицу фактов. Если обнаружены белые пятна или нехватка доказательств, динамически возвращает воркеры на дополнительный целевой цикл сбора источников.",
    payloadInfo: "evidence_blocks ➔ gap_detected, ↩ gap_queries (на SearchAgent)",
  },
  {
    stepNumber: 8,
    stageName: "Synthesis & Logic",
    stageColor: "text-purple-400 border-purple-500/30 bg-purple-500/10",
    agentIds: ["analyzer"],
    activeEdges: ["replan->analyzer"],
    title: "8. AnalyzerAgent (DeepSeek-Reasoner): Глубокий синтез аргументов",
    description:
      "Флагманская reasoning-модель DeepSeek выполняет глубокий логический синтез, генерируя черновик детального отчёта со сквозной цепочкой рассуждений (Chain-of-Thought).",
    payloadInfo: "verified_evidence ➔ draft_report, reasoning_steps, key_findings",
  },
  {
    stepNumber: 9,
    stageName: "Synthesis & Logic",
    stageColor: "text-purple-400 border-purple-500/30 bg-purple-500/10",
    agentIds: ["numeric_check", "claim_verifier"],
    activeEdges: ["analyzer->numeric_check", "numeric_check->claim_verifier", "claim_verifier->analyzer"],
    title: "9. NumericCheck & ClaimVerifier: Сверка чисел и фактов",
    description:
      "NumericCheck сверяет проценты, даты и финансовые суммы. ClaimVerifier проверяет каждый ключевой факт отчёта, исключая галлюцинации и возвращая сомнительные фрагменты на правку.",
    payloadInfo: "draft_report ➔ verified_numbers, verified_claims / ↩ fact_fix",
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
      "RedTeam атакует гипотезы выводами «адвоката дьявола». Stance формирует баланс мнений меньшинств. CitationAudit и Retraction сверяют DOI со списком отозванных научных статей.",
    payloadInfo: "verified_claims ➔ counter_arguments, consensus_matrix, clean_sources",
  },
  {
    stepNumber: 11,
    stageName: "Synthesis & Polish",
    stageColor: "text-rose-400 border-rose-500/30 bg-rose-500/10",
    agentIds: ["report_critic", "analyzer", "replan"],
    activeEdges: [
      "stance->report_critic",
      "retraction->report_critic",
      "report_critic->analyzer",
      "report_critic->replan",
    ],
    title: "11. ReportCritic: Контроль качества и возврат на доработку (Feedback Loop)",
    description:
      "ReportCritic оценивает черновик. При обнаружении логических пробелов или слабых аргументов он возвращает задачу назад: в AnalyzerAgent (на пересинтез) или в ReplanAgent (на добор фактов).",
    payloadInfo: "draft_review ➔ ↩ draft_revision (в Analyzer) / ↩ tie_break (в Replan)",
  },
  {
    stepNumber: 12,
    stageName: "Synthesis & Polish",
    stageColor: "text-purple-400 border-purple-500/30 bg-purple-500/10",
    agentIds: ["report_critic", "confidence"],
    activeEdges: [
      "report_critic->confidence",
    ],
    title: "12. ConfidenceAgent: Калибровка достоверности и скоринг",
    description:
      "После одобрения критиками ConfidenceAgent рассчитывает калиброванный индекс достоверности (0-100%) и формирует бейдж прозрачности отчёта.",
    payloadInfo: "approved_draft ➔ calibrated_trust_score, trust_indicators",
  },
  {
    stepNumber: 13,
    stageName: "Delivery & Follow-Up",
    stageColor: "text-amber-400 border-amber-500/30 bg-amber-500/10",
    agentIds: ["chat"],
    activeEdges: ["confidence->chat"],
    title: "13. ChatAgent: Интерактивный эксперт по доказательной базе",
    description:
      "Отчёт доставлен. ChatAgent готов отвечать на любые последующие вопросы пользователя, строго опираясь на собранную базу цитат и проверенных фактов.",
    payloadInfo: "final_report + trust_badge ➔ grounded_interactive_answers",
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

function getAgentSimStatus(agentId: string): "idle" | "active" | "completed" {
  if (!isSimulating.value && currentStepIndex.value === 0) return "idle";
  const step = currentStep.value;
  if (step.agentIds.includes(agentId)) {
    return "active";
  }
  for (let i = 0; i < currentStepIndex.value; i++) {
    if (SIMULATION_STEPS[i].agentIds.includes(agentId)) {
      return "completed";
    }
  }
  return "idle";
}

// ── Interactivity ─────────────────────────────────────────────────────────────
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

// ── Fullscreen Viewport Mode ──────────────────────────────────────────────────
const isFullscreen = ref(false);

function toggleFullscreen() {
  isFullscreen.value = !isFullscreen.value;
}

function onKeyDown(e: KeyboardEvent) {
  if (e.key === "Escape") {
    if (drawerOpen.value) {
      drawerOpen.value = false;
      return;
    }
    if (isFullscreen.value) {
      isFullscreen.value = false;
    }
  }
}

onMounted(() => {
  fetchAgents();
  window.addEventListener("mousemove", onGlobalMouseMove);
  window.addEventListener("mouseup", onGlobalMouseUp);
  window.addEventListener("keydown", onKeyDown);
});

onBeforeUnmount(() => {
  if (simTimer) clearTimeout(simTimer);
  if (zoomTimeout) clearTimeout(zoomTimeout);
  window.removeEventListener("mousemove", onGlobalMouseMove);
  window.removeEventListener("mouseup", onGlobalMouseUp);
  window.removeEventListener("keydown", onKeyDown);
});

function openInspector(nodeId: string) {
  if (nodeId === "trigger_start") return;
  const target = agents.value.find((a) => a.id === nodeId);
  if (target) {
    selectedAgent.value = target;
    drawerOpen.value = true;
  }
}

function selectAgentById(id: string) {
  const target = agents.value.find((a) => a.id === id);
  if (target) {
    selectedAgent.value = target;
    drawerOpen.value = true;
  }
}

function onNodeHover(nodeId: string | null) {
  hoveredAgentId.value = nodeId;
}

function onEdgeHover(edgeKey: string | null) {
  hoveredEdgeId.value = edgeKey;
}

function isNodeHighlighted(nodeId: string): boolean {
  if (hoveredEdgeId.value) {
    const [from, to] = hoveredEdgeId.value.split("->");
    if (nodeId === from || nodeId === to) return true;
  }
  if (hoveredAgentId.value && nodeId === hoveredAgentId.value) {
    return true;
  }
  if (selectedAgent.value && nodeId === selectedAgent.value.id) {
    return true;
  }
  return false;
}

function isNodeDimmed(nodeId: string): boolean {
  if (searchQuery.value.trim()) {
    const q = searchQuery.value.trim().toLowerCase();
    const node = VISUAL_NODES_CONFIG[nodeId];
    if (!node || (!node.name.toLowerCase().includes(q) && !node.subtitle.toLowerCase().includes(q))) {
      return true;
    }
  }
  return false;
}
</script>

<template>
  <div
    class="flex flex-col space-y-3"
    :class="[
      isFullscreen
        ? 'fixed inset-0 z-50 bg-[#0b0e14] p-4 h-screen w-screen overflow-hidden'
        : 'relative h-full'
    ]"
  >
    <!-- Top Control Bar: Search, Pan/Zoom Controls, Simulation Trigger, Tools Toggle -->
    <div class="flex flex-wrap items-center justify-between gap-3 border-b border-bd pb-3">
      <!-- Search Filter -->
      <div class="relative w-64">
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

      <!-- Quick Stage Legend Indicators -->
      <div class="hidden lg:flex items-center gap-2 text-[11px] font-mono">
        <div class="flex items-center gap-1.5 rounded-lg border border-orange-500/30 bg-orange-500/10 px-2.5 py-1 text-orange-400">
          <span>⚡</span>
          <span>{{ t("admin.agents.legendTrigger") }}</span>
        </div>
        <div class="flex items-center gap-1.5 rounded-lg border border-blue-500/30 bg-blue-500/10 px-2.5 py-1 text-blue-400">
          <span>🟣</span>
          <span>{{ t("admin.agents.legendPlanning") }}</span>
        </div>
        <div class="flex items-center gap-1.5 rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-2.5 py-1 text-emerald-400">
          <span>🟢</span>
          <span>{{ t("admin.agents.legendSearch") }}</span>
        </div>
        <div class="flex items-center gap-1.5 rounded-lg border border-purple-500/30 bg-purple-500/10 px-2.5 py-1 text-purple-400">
          <span>🟠</span>
          <span>{{ t("admin.agents.legendSynthesis") }}</span>
        </div>
        <div class="flex items-center gap-1.5 rounded-lg border border-amber-500/30 bg-amber-500/10 px-2.5 py-1 text-amber-400">
          <span>🔵</span>
          <span>{{ t("admin.agents.legendDelivery") }}</span>
        </div>
        <div
          class="flex items-center gap-1.5 rounded-lg border border-rose-500/30 bg-rose-500/10 px-2.5 py-1 text-rose-400 cursor-pointer hover:bg-rose-500/20 transition"
          :title="t('admin.agents.toggleReturnLoopsTooltip')"
          @click="showReturnLoops = !showReturnLoops"
        >
          <span>↩</span>
          <span>{{ t("admin.agents.legendReturn") }}</span>
        </div>
      </div>

      <!-- Right Action Group: Simulation Controls, Zoom, Language & Layout -->
      <div class="flex items-center gap-2">
        <!-- Toggle Return / Feedback Loops Button -->
        <button
          class="flex items-center gap-1.5 rounded-xl border px-2.5 py-1.5 text-xs font-medium transition shadow-sm"
          :class="[
            showReturnLoops
              ? 'border-rose-500/60 bg-rose-500/20 text-rose-300 ring-1 ring-rose-500/40'
              : 'border-bd bg-surface/70 text-muted hover:text-ink hover:border-rose-500/40'
          ]"
          :title="t('admin.agents.toggleReturnLoopsTooltip')"
          @click="showReturnLoops = !showReturnLoops"
        >
          <span class="text-sm">↩</span>
          <span class="hidden sm:inline">{{ t("admin.agents.showReturnLoops") }}</span>
          <span
            class="rounded-full px-1.5 py-0.2 text-[9.5px] font-mono font-bold"
            :class="showReturnLoops ? 'bg-rose-500/30 text-rose-200' : 'bg-surface text-muted'"
          >
            {{ RETURN_CONNECTIONS.length }}
          </span>
        </button>

        <!-- Language Switcher in Graph Toolbar -->
        <div class="flex items-center gap-0.5 rounded-xl border border-bd bg-surface/70 p-1 text-xs font-mono mr-1">
          <button
            v-for="loc in (['ru', 'en', 'es'] as const)"
            :key="loc"
            class="rounded-lg px-2 py-1 text-[10.5px] font-bold uppercase transition"
            :class="ui.locale === loc ? 'bg-accent text-white shadow' : 'text-muted hover:text-ink'"
            @click="ui.setLocale(loc)"
          >
            {{ loc }}
          </button>
        </div>

        <!-- Simulation Run / Pause Toggle -->
        <div class="flex items-center gap-1 rounded-xl border border-bd bg-surface/70 p-1">
          <button
            v-if="!isSimulating"
            class="flex items-center gap-1.5 rounded-lg bg-accent px-3 py-1 text-xs font-bold text-white shadow transition hover:bg-accent/90"
            @click="startSimulation"
          >
            <span>▶</span>
            <span>{{ currentStepIndex === 0 ? t("admin.agents.startSim") : t("admin.agents.continueSim") }}</span>
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

          <button
            class="rounded px-2 py-1 font-mono text-[10px] font-semibold text-muted hover:text-ink"
            @click="simSpeed = simSpeed === 1 ? 2 : 1"
          >
            {{ simSpeed }}x
          </button>
        </div>

        <!-- Zoom & Pan Controls -->
        <div class="flex items-center gap-1 rounded-lg border border-bd bg-surface/60 p-1 text-xs text-muted">
          <button class="rounded px-2 py-1 hover:bg-surface hover:text-ink" :title="t('admin.agents.zoomIn')" @click="zoomIn">
            +
          </button>
          <button class="px-1.5 py-1 font-mono text-[11px] hover:text-ink" :title="t('admin.agents.resetZoom')" @click="resetView">
            {{ Math.round(zoom * 100) }}%
          </button>
          <button class="rounded px-2 py-1 hover:bg-surface hover:text-ink" :title="t('admin.agents.zoomOut')" @click="zoomOut">
            −
          </button>
        </div>

        <!-- Reset Node Layout Button -->
        <button
          class="flex items-center gap-1.5 rounded-xl border border-bd bg-surface/70 px-2.5 py-1.5 text-xs text-muted hover:text-ink hover:border-accent/40 transition"
          :title="t('admin.agents.resetLayoutTooltip')"
          @click="resetNodePositions"
        >
          <span>↺</span>
          <span class="hidden sm:inline">{{ t("admin.agents.resetLayout") }}</span>
        </button>

        <!-- Fullscreen / Expand Toggle Button -->
        <button
          class="flex items-center gap-1.5 rounded-xl border px-2.5 py-1.5 text-xs font-medium transition shadow-sm"
          :class="[
            isFullscreen
              ? 'border-accent bg-accent/20 text-accent hover:bg-accent/30 ring-1 ring-accent/40'
              : 'border-bd bg-surface/70 text-muted hover:text-ink hover:border-accent/40'
          ]"
          :title="isFullscreen ? `${t('admin.agents.exitFullscreen')} (Esc)` : t('admin.agents.fullscreen')"
          @click="toggleFullscreen"
        >
          <span class="text-sm leading-none">{{ isFullscreen ? '🗗' : '⛶' }}</span>
          <span class="hidden sm:inline">
            {{ isFullscreen ? t("admin.agents.exitFullscreen") : t("admin.agents.fullscreen") }}
          </span>
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

    <!-- Main Workflow Canvas Viewport (n8n Style) -->
    <div
      v-else
      ref="canvasViewportRef"
      class="relative flex-1 overflow-hidden select-none rounded-2xl border border-bd/80 bg-[#0d111a] shadow-inner cursor-grab active:cursor-grabbing"
      :class="isFullscreen ? 'min-h-[calc(100vh-140px)]' : 'min-h-[640px]'"
      style="background-image: radial-gradient(circle, rgba(255, 255, 255, 0.12) 1.2px, transparent 1.2px); background-size: 20px 20px;"
      @mousedown="onMouseDown"
      @wheel="onWheel"
    >
      <!-- Quick Floating Fullscreen Button on Canvas -->
      <div class="absolute right-4 top-4 z-20 flex items-center gap-2">
        <button
          class="flex items-center gap-1.5 rounded-xl border border-bd/80 bg-[#151922]/90 px-3 py-1.5 text-xs font-medium text-muted shadow-lg backdrop-blur hover:text-ink hover:border-accent/40 transition"
          :class="{ 'border-accent text-accent bg-accent/20 ring-1 ring-accent/30': isFullscreen }"
          :title="isFullscreen ? `${t('admin.agents.exitFullscreen')} (Esc)` : t('admin.agents.fullscreen')"
          @click.stop="toggleFullscreen"
        >
          <span class="text-sm leading-none">{{ isFullscreen ? '🗗' : '⛶' }}</span>
          <span>{{ isFullscreen ? t("admin.agents.exitFullscreen") : t("admin.agents.fullscreen") }}</span>
        </button>
      </div>
      <!-- Scalable & Pannable Canvas World -->
      <div
        class="absolute origin-top-left"
        :class="isZooming || isPanning || draggingNodeId ? 'transition-none' : 'transition-transform duration-100 ease-out'"
        :style="{ transform: `translate(${panX}px, ${panY}px) scale(${zoom})`, width: '7400px', height: '850px' }"
      >
        <!-- SVG Connections Layer (n8n Smooth Bezier Curves) -->
        <svg class="pointer-events-none absolute inset-0 z-0 h-full w-full overflow-visible">
          <defs>
            <!-- Default subtle arrowhead -->
            <marker
              id="n8n-arrow-default"
              viewBox="0 0 10 10"
              refX="8"
              refY="5"
              markerWidth="6"
              markerHeight="6"
              orient="auto-start-reverse"
            >
              <path d="M 0 1.5 L 8 5 L 0 8.5 z" fill="rgba(148, 163, 184, 0.45)" />
            </marker>

            <!-- Highlighted wire arrowhead -->
            <marker
              id="n8n-arrow-highlight"
              viewBox="0 0 10 10"
              refX="8"
              refY="5"
              markerWidth="7"
              markerHeight="7"
              orient="auto-start-reverse"
            >
              <path d="M 0 1.5 L 8 5 L 0 8.5 z" fill="#818cf8" />
            </marker>

            <!-- Active simulation wire arrowhead -->
            <marker
              id="n8n-arrow-active"
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="8"
              markerHeight="8"
              orient="auto-start-reverse"
            >
              <path d="M 0 1 L 9 5 L 0 9 z" fill="#38bdf8" />
            </marker>

            <!-- Return / Feedback wire arrowheads -->
            <marker
              id="n8n-arrow-return-default"
              viewBox="0 0 10 10"
              refX="8"
              refY="5"
              markerWidth="7"
              markerHeight="7"
              orient="auto-start-reverse"
            >
              <path d="M 0 1.5 L 8 5 L 0 8.5 z" fill="#fb7185" opacity="0.85" />
            </marker>

            <marker
              id="n8n-arrow-return-highlight"
              viewBox="0 0 10 10"
              refX="8"
              refY="5"
              markerWidth="8"
              markerHeight="8"
              orient="auto-start-reverse"
            >
              <path d="M 0 1.5 L 8 5 L 0 8.5 z" fill="#f43f5e" />
            </marker>

            <marker
              id="n8n-arrow-return-active"
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="8"
              markerHeight="8"
              orient="auto-start-reverse"
            >
              <path d="M 0 1 L 9 5 L 0 9 z" fill="#fda4af" />
            </marker>

            <!-- Glow filter for traveling particles -->
            <filter id="n8n-glow" x="-50%" y="-50%" width="200%" height="200%">
              <feGaussianBlur stdDeviation="3.5" result="coloredBlur" />
              <feMerge>
                <feMergeNode in="coloredBlur" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
          </defs>

          <!-- Render All Connecting Wires -->
          <g v-for="edge in renderedEdges" :key="edge.id">
            <!-- Smooth Bezier Line -->
            <path
              :d="edge.d"
              :stroke="
                edge.isReturn
                  ? edge.isActive
                    ? '#fda4af'
                    : edge.isHighlighted
                    ? '#f43f5e'
                    : 'rgba(244, 63, 94, 0.55)'
                  : edge.isActive
                  ? '#38bdf8'
                  : edge.isHighlighted
                  ? '#818cf8'
                  : 'rgba(148, 163, 184, 0.3)'
              "
              :stroke-width="edge.isActive ? 3.2 : edge.isHighlighted ? 2.6 : edge.isReturn ? 2.2 : 2"
              fill="none"
              :stroke-dasharray="edge.isReturn ? (edge.isActive ? '5,4' : '6,4') : (edge.isActive ? '7,7' : 'none')"
              :class="[
                draggingNodeId !== null ? 'transition-none' : 'transition-[stroke,stroke-width] duration-150',
                { 'animate-n8n-wire': edge.isActive }
              ]"
              :marker-end="!edge.isReturn ? `url(#${
                edge.isActive
                  ? 'n8n-arrow-active'
                  : edge.isHighlighted
                  ? 'n8n-arrow-highlight'
                  : 'n8n-arrow-default'
              })` : undefined"
              class="pointer-events-auto cursor-pointer"
              @mouseenter="onEdgeHover(edge.id)"
              @mouseleave="onEdgeHover(null)"
            />

            <!-- Animated Traveling Particle along active simulation wires -->
            <circle
              v-if="edge.isActive"
              r="4.5"
              :fill="edge.isReturn ? '#fda4af' : '#38bdf8'"
              filter="url(#n8n-glow)"
            >
              <animateMotion
                :path="edge.d"
                :dur="simSpeed === 2 ? '1.0s' : '2.0s'"
                repeatCount="indefinite"
              />
            </circle>

            <!-- Data Payload Badge in Middle of Wire -->
            <g
              v-if="edge.isHighlighted || edge.isActive || zoom >= 0.85"
              :transform="`translate(${edge.midX}, ${edge.midY})`"
              class="pointer-events-auto cursor-pointer"
              @mouseenter="onEdgeHover(edge.id)"
              @mouseleave="onEdgeHover(null)"
            >
              <g :class="draggingNodeId !== null ? 'transition-none' : 'transition-transform duration-100 hover:scale-110'">
                <rect
                  :x="-edge.labelWidth / 2"
                  y="-10"
                  :width="edge.labelWidth"
                  height="20"
                  rx="6"
                  class="stroke-[1.5]"
                  :class="[
                    edge.isReturn
                      ? edge.isActive
                        ? 'fill-slate-950 stroke-rose-400 shadow-lg'
                        : edge.isHighlighted
                        ? 'fill-slate-950 stroke-rose-400'
                        : 'fill-[#171015] stroke-rose-500/50'
                      : edge.isActive
                      ? 'fill-slate-900 stroke-sky-400'
                      : edge.isHighlighted
                      ? 'fill-slate-900 stroke-indigo-400'
                      : 'fill-[#151922] stroke-bd/80',
                  ]"
                />
                <text
                  x="0"
                  y="3.5"
                  text-anchor="middle"
                  class="font-mono text-[9px] font-semibold select-none"
                  :class="[
                    edge.isReturn
                      ? edge.isActive || edge.isHighlighted
                        ? 'fill-rose-300'
                        : 'fill-rose-400/90'
                      : edge.isActive
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
          </g>
        </svg>

        <!-- Render All Visual Nodes (n8n Node Cards) -->
        <div
          v-for="node in visualNodes"
          :key="node.id"
          class="interactive-node absolute group select-none transition-none"
          :class="[
            draggingNodeId === node.id
              ? 'z-30 cursor-grabbing'
              : 'cursor-grab',
          ]"
          :style="{
            transform: `translate(${node.x}px, ${node.y}px)`,
            width: `${node.width}px`,
            height: `${node.height}px`,
          }"
          @mousedown="onNodeMouseDown($event, node.id)"
          @click="handleNodeClick(node.id)"
          @mouseenter="onNodeHover(node.id)"
          @mouseleave="onNodeHover(null)"
        >
          <!-- Node Card Container -->
          <div
            class="relative flex h-full items-center gap-3 rounded-2xl border p-3 shadow-lg backdrop-blur"
            :class="[
              draggingNodeId === node.id
                ? 'transition-none border-accent bg-[#1c2233] ring-4 ring-accent/60 shadow-2xl scale-[1.03]'
                : 'transition-colors duration-150',
              isNodeDimmed(node.id)
                ? 'opacity-30'
                : 'opacity-100',
              selectedAgent?.id === node.id
                ? 'border-accent bg-[#1c2233] ring-2 ring-accent/60 shadow-accent/20 scale-[1.02]'
                : isNodeHighlighted(node.id)
                ? 'border-indigo-400/80 bg-[#191f2e] ring-2 ring-indigo-400/40 scale-[1.01]'
                : getAgentSimStatus(node.id) === 'active'
                ? 'border-sky-400 bg-[#192338] ring-4 ring-sky-400/50 shadow-xl shadow-sky-400/25 scale-[1.03]'
                : getAgentSimStatus(node.id) === 'completed'
                ? 'border-emerald-500/60 bg-[#161d26]'
                : 'border-[#2a3449] bg-[#161a24] hover:border-slate-500 hover:bg-[#1a202d]',
            ]"
          >
            <!-- Left Input Port (Handle) -->
            <div
              v-if="node.hasInput"
              class="absolute -left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 rounded-full border-2 border-[#151922] bg-slate-400 shadow transition group-hover:scale-125 group-hover:bg-accent"
              :class="[
                getAgentSimStatus(node.id) === 'active' ? 'bg-sky-400 ring-2 ring-sky-400/60 scale-125' : '',
                isNodeHighlighted(node.id) ? 'bg-indigo-400' : '',
              ]"
              title="Input Connection"
            />

            <!-- Right Output Port (Handle) -->
            <div
              v-if="node.hasOutput"
              class="absolute -right-2.5 top-1/2 -translate-y-1/2 h-4 w-4 rounded-full border-2 border-[#151922] bg-slate-400 shadow transition group-hover:scale-125 group-hover:bg-accent"
              :class="[
                getAgentSimStatus(node.id) === 'active' ? 'bg-sky-400 ring-2 ring-sky-400/60 scale-125' : '',
                isNodeHighlighted(node.id) ? 'bg-indigo-400' : '',
              ]"
              title="Output Connection"
            />

            <!-- Node Icon Box (Matching Screenshot 2) -->
            <div
              class="grid h-11 w-11 shrink-0 place-items-center rounded-xl border text-xl font-bold shadow-inner"
              :class="[node.iconBg, node.iconColor]"
            >
              {{ node.icon }}
            </div>

            <!-- Node Label Details -->
            <div class="min-w-0 flex-1">
              <div class="flex items-center justify-between gap-1">
                <span class="truncate font-bold text-xs text-ink group-hover:text-accent transition-colors">
                  {{ getNodeName(node.id, node.name) }}
                </span>
              </div>
              <p class="truncate text-[10px] text-muted mt-0.5 font-sans">
                {{ getNodeSubtitle(node.id, node.subtitle) }}
              </p>
            </div>

            <!-- Return Capability Badge (Critics / Loop Nodes) -->
            <div
              v-if="hasReturnCapability(node.id)"
              class="absolute -top-2.5 left-2 flex items-center gap-1 rounded-full bg-rose-500/20 border border-rose-500/40 px-2 py-0.5 text-[8.5px] font-bold text-rose-300 shadow backdrop-blur transition-transform hover:scale-105 cursor-help"
              :title="getReturnCapabilityTooltip(node.id)"
            >
              <span class="text-[9px]">↩</span>
              <span>{{ t("admin.agents.canReturnBadge") }}</span>
            </div>

            <!-- Active / Done Simulation Status Badges -->
            <div v-if="getAgentSimStatus(node.id) === 'active'" class="absolute -top-2 right-2">
              <span class="flex items-center gap-1 rounded-full bg-sky-500/20 border border-sky-500/40 px-2 py-0.5 text-[9px] font-bold text-sky-400 animate-pulse shadow">
                ● {{ t("admin.agents.activeBadge") }}
              </span>
            </div>
            <div v-else-if="getAgentSimStatus(node.id) === 'completed'" class="absolute -top-2 right-2">
              <span class="flex items-center gap-1 rounded-full bg-emerald-500/20 border border-emerald-500/30 px-2 py-0.5 text-[9px] font-bold text-emerald-400 shadow">
                ✓ {{ t("admin.agents.doneBadge") }}
              </span>
            </div>

            <!-- Bottom Outgoing Return Port (Handle for Feedback Emitters) -->
            <div
              v-if="hasReturnCapability(node.id)"
              class="absolute -bottom-1.5 h-3 w-3 -translate-x-1/2 rounded-full border border-[#151922] bg-rose-500/80 shadow transition group-hover:scale-125 group-hover:bg-rose-400"
              :style="{ left: `${node.width - RETURN_PORT_OFFSET}px` }"
              :class="[
                isNodeHighlighted(node.id) ? 'bg-rose-400 ring-2 ring-rose-400/60 scale-125' : '',
              ]"
              :title="t('admin.agents.returnOutgoingPort')"
            />

            <!-- Bottom Incoming Return Port (Handle for Feedback Receivers) -->
            <div
              v-if="hasReturnReceiver(node.id)"
              class="absolute -bottom-1.5 h-3 w-3 -translate-x-1/2 rounded-full border border-[#151922] bg-rose-500/60 shadow transition group-hover:scale-125 group-hover:bg-rose-400"
              :style="{ left: `${RETURN_PORT_OFFSET}px` }"
              :class="[
                isNodeHighlighted(node.id) ? 'bg-rose-400 ring-2 ring-rose-400/60 scale-125' : '',
              ]"
              :title="t('admin.agents.returnIncomingPort')"
            />

            <!-- Bottom Diamond Port + Model Badge (Screenshot 2 Style) -->
            <div
              v-if="node.llmModel"
              class="absolute -bottom-2.5 left-1/2 -translate-x-1/2 flex items-center gap-1 rounded-full border border-bd/90 bg-[#10141d] px-2 py-0.2 font-mono text-[8.5px] text-muted whitespace-nowrap shadow-md"
            >
              <span class="text-accent text-[7px]">◆</span>
              <span>{{ node.llmModel }}</span>
            </div>
          </div>
        </div>
      </div>

      <!-- Canvas Hint Badge -->
      <div
        class="absolute bottom-3 left-4 pointer-events-none z-10 flex items-center gap-2 rounded-lg border border-bd/60 bg-[#151922]/85 px-3 py-1.5 text-[11px] text-muted backdrop-blur font-sans shadow"
      >
        <span class="text-xs">✋</span>
        <span>{{ t("admin.agents.dragHint") }}</span>
      </div>
    </div>

    <!-- Floating Simulation Walkthrough Banner ("Как они работают") -->
    <div
      v-if="isSimulating || currentStepIndex > 0"
      class="rounded-2xl border border-accent/40 bg-[#151922]/95 p-4 shadow-2xl backdrop-blur transition-all duration-300"
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
            :title="t('admin.agents.close')"
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
@keyframes n8nWireFlow {
  from {
    stroke-dashoffset: 28;
  }
  to {
    stroke-dashoffset: 0;
  }
}

.animate-n8n-wire {
  animation: n8nWireFlow 1.2s linear infinite;
}
</style>
