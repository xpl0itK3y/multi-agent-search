<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import { adminApi, apiErrorMessage } from "@/lib/api";
import { createVelocityTracker, decay, springTo, type Animation, type VelocityTracker } from "@/lib/gesture";
import { useReducedMotion } from "@/lib/motion";
import type { AgentMetadataItem } from "@/lib/types";
import AgentInspectorDrawer from "./AgentInspectorDrawer.vue";
import {
  boundsOf,
  clampPan,
  clampPanSoft,
  fitBounds,
  limitPan,
  panRange,
  pinch,
  wheelUnit,
  wheelZoomFactor,
  zoomAt,
  STAGE_TEXT,
  STAGE_TONE,
  ZOOM_LIMITS,
  type BBox,
  type PanRange,
  type Point,
  type Size,
  type View,
  unclampPanSoft,
} from "./graphView";

const { t, te } = useI18n();

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
// True while wheel events keep arriving (a trackpad swipe or pinch in progress).
const isWheeling = ref(false);
const canvasViewportRef = ref<HTMLElement | null>(null);

// ── Gestures on Pointer Events (apple-design §2, §3, §10) ─────────────────────
// One set of listeners on the viewport serves mouse, pen and touch. The canvas and a
// dragged node follow the pointer 1:1 from where they were grabbed (deltas from the
// start point, never snapped to a centre); moves are applied once per animation frame;
// two fingers pinch around their midpoint. pointercancel, a lost capture, a mouse
// released outside the window or leaving the window all end the gesture, so the canvas
// can never be left "stuck" panning.
type Gesture =
  | {
      kind: "pan";
      pointerId: number;
      pointerType: string;
      start: Point;
      // The raw (finger) pan at the start; what's shown is its soft-clamped value.
      pan0: Point;
      range: PanRange | null;
      size: Size;
      tracker: VelocityTracker;
    }
  | {
      kind: "node";
      pointerId: number;
      pointerType: string;
      nodeId: string;
      start: Point;
      node0: Point;
      dragging: boolean;
    }
  | { kind: "pinch"; ids: [number, number]; view0: View; a0: Point; b0: Point };

// Presses on real controls inside the canvas are theirs, not a pan.
const INTERACTIVE_SELECTOR = "button, input, a, select, textarea";
// A press becomes a node drag only past this many screen px (hysteresis, §10): enough
// for a hand's jitter on a click, and a fingertip's on a tap.
const DRAG_THRESHOLD_MOUSE = 4;
const DRAG_THRESHOLD_TOUCH = 10;

const pointers = new Map<number, Point>();
let gesture: Gesture | null = null;
let viewportRect: DOMRect | null = null;
let frameId = 0;
// Set when a press turned into a real drag, so the click that may follow it doesn't
// open the inspector; read by the click, and cleared by the next press.
let suppressClick = false;
const pressedNodeId = ref<string | null>(null);
const pinching = ref(false);

function localPoint(e: PointerEvent): Point {
  const r = viewportRect ?? canvasViewportRef.value?.getBoundingClientRect();
  return { x: e.clientX - (r?.left ?? 0), y: e.clientY - (r?.top ?? 0) };
}

function capturePointer(id: number) {
  try {
    canvasViewportRef.value?.setPointerCapture?.(id);
  } catch {
    // The pointer is already gone (released between the event and this call).
  }
}

const requestFrame = (cb: () => void): number =>
  typeof requestAnimationFrame === "function" ? requestAnimationFrame(cb) : (setTimeout(cb, 16) as unknown as number);
const cancelFrame = (id: number) =>
  typeof cancelAnimationFrame === "function" ? cancelAnimationFrame(id) : clearTimeout(id);

function scheduleFrame() {
  if (!frameId) frameId = requestFrame(applyFrame);
}

function flushFrame() {
  if (!frameId) return;
  cancelFrame(frameId);
  applyFrame();
}

function applyFrame() {
  frameId = 0;
  // A glide's latest values: both axes land in one write, one render per frame.
  if (glideTarget.x !== null) panX.value = glideTarget.x;
  if (glideTarget.y !== null) panY.value = glideTarget.y;
  glideTarget.x = glideTarget.y = null;
  const g = gesture;
  if (!g) return;
  if (g.kind === "pinch") {
    const a = pointers.get(g.ids[0]);
    const b = pointers.get(g.ids[1]);
    if (!a || !b) return;
    const v = pinch(g.view0, g.a0, g.b0, a, b);
    zoom.value = v.zoom;
    panX.value = v.panX;
    panY.value = v.panY;
    return;
  }
  const p = pointers.get(g.pointerId);
  if (!p) return;
  if (g.kind === "node") {
    if (!g.dragging) return;
    // In place, so a drag frame is one small reactive write, not a new positions map.
    const pos = nodePositions.value[g.nodeId];
    pos.x = Math.max(10, Math.min(7200, Math.round(g.node0.x + (p.x - g.start.x) / zoom.value)));
    pos.y = Math.max(10, Math.min(850, Math.round(g.node0.y + (p.y - g.start.y) / zoom.value)));
    return;
  }
  // Past the soft limits the canvas follows less and less (§9), never a hard stop.
  const rawX = g.pan0.x + (p.x - g.start.x);
  const rawY = g.pan0.y + (p.y - g.start.y);
  panX.value = g.range ? clampPanSoft(rawX, g.range.minX, g.range.maxX, g.size.w) : rawX;
  panY.value = g.range ? clampPanSoft(rawY, g.range.minY, g.range.maxY, g.size.h) : rawY;
}

// ── Release: settle, or glide (§5, §6, §9) ─────────────────────────────────────
// Letting go of a canvas pulled past its limits brings it back; a touch flick glides on
// with the finger's velocity and slows like a scroll, and if the glide runs into a
// limit it springs back to it (critically damped, taking the glide's velocity). A new
// press stops any of this where it is on screen, and the drag carries on from there.
const FLICK_PX_S = 300;
const DECAY_RATE = 0.998;
const SETTLE_RESPONSE = 0.4;
const reducedMotion = useReducedMotion();
const glideTarget: { x: number | null; y: number | null } = { x: null, y: null };
const glides: { x: Animation | null; y: Animation | null } = { x: null, y: null };
const isGliding = ref(false);

function stopGlides() {
  glides.x?.stop();
  glides.y?.stop();
  glides.x = glides.y = null;
  // What is on screen is the presentation value; a value not yet drawn is dropped.
  glideTarget.x = glideTarget.y = null;
  isGliding.value = false;
}

function glideDone(axis: "x" | "y") {
  glides[axis] = null;
  if (!glides.x && !glides.y) isGliding.value = false;
}

function writeAxis(axis: "x" | "y", value: number) {
  glideTarget[axis] = value;
  scheduleFrame();
}

function springAxis(axis: "x" | "y", from: number, to: number, velocity: number) {
  glides[axis] = springTo({
    from,
    to,
    velocity,
    response: SETTLE_RESPONSE,
    onUpdate: (v) => writeAxis(axis, v),
    onComplete: () => glideDone(axis),
  });
}

// One axis of a released pan: shown value, finger velocity (px/s), limits, viewport size.
function releaseAxis(axis: "x" | "y", shown: number, velocity: number, min: number, max: number, dim: number, flick: boolean) {
  const raw = unclampPanSoft(shown, min, max, dim);
  if (raw < min || raw > max) {
    // Out in the band: back to the limit. Inside the band the canvas moves at the
    // band's slope (0.55), so that is the velocity it hands on.
    springAxis(axis, shown, clampPan(raw, min, max), flick ? velocity * 0.55 : 0);
    return;
  }
  if (!flick) return;
  let last = raw;
  let lastT = performance.now();
  let rawVelocity = velocity;
  const anim = decay({
    from: raw,
    velocity,
    rate: DECAY_RATE,
    onUpdate: (x) => {
      const now = performance.now();
      if (now > lastT) rawVelocity = ((x - last) / (now - lastT)) * 1000;
      last = x;
      lastT = now;
      if (x < min || x > max) {
        // The glide ran into a limit: stop it and settle there with its velocity.
        anim.stop();
        const edge = clampPan(x, min, max);
        springAxis(axis, clampPanSoft(x, min, max, dim), edge, rawVelocity * 0.55);
        return;
      }
      writeAxis(axis, x);
    },
    onComplete: () => {
      if (glides[axis] === anim) glideDone(axis);
    },
  });
  glides[axis] = anim;
}

function releasePan(g: Extract<Gesture, { kind: "pan" }>, released: boolean) {
  const range = g.range;
  if (!range) return;
  const { vx, vy } = released ? g.tracker.velocity(performance.now()) : { vx: 0, vy: 0 };
  const flick = released && g.pointerType !== "mouse" && Math.hypot(vx, vy) > FLICK_PX_S && !reducedMotion.value;
  if (!flick) {
    settleIntoRange();
    return;
  }
  isGliding.value = true;
  releaseAxis("x", panX.value, vx, range.minX, range.maxX, g.size.w, true);
  releaseAxis("y", panY.value, vy, range.minY, range.maxY, g.size.h, true);
  if (!glides.x && !glides.y) isGliding.value = false;
}

// A mouse release, a slow touch or a pinch that ended out of bounds: the canvas eases
// back inside (the 200 ms view transition; instant under reduced motion).
function settleIntoRange() {
  const range = currentPanRange();
  if (!range) return;
  const x = clampPan(panX.value, range.minX, range.maxX);
  const y = clampPan(panY.value, range.minY, range.maxY);
  if (x === panX.value && y === panY.value) return;
  animateView({ zoom: zoom.value, panX: x, panY: y });
}

function startPinch() {
  const ids = [...pointers.keys()].slice(-2) as [number, number];
  const a0 = pointers.get(ids[0])!;
  const b0 = pointers.get(ids[1])!;
  // A second finger turns whatever the first one was doing into a pinch.
  if (gesture) endGesture();
  ids.forEach(capturePointer);
  gesture = { kind: "pinch", ids, view0: { zoom: zoom.value, panX: panX.value, panY: panY.value }, a0, b0 };
  pinching.value = true;
}

function onPointerDown(e: PointerEvent) {
  const target = e.target instanceof Element ? e.target : null;
  if (!target || target.closest(INTERACTIVE_SELECTOR)) return;
  if (e.pointerType === "mouse" && e.button !== 0) return;
  if (!canvasViewportRef.value) return;
  suppressClick = false;
  stopGlides();
  settleView();
  if (!pointers.size) viewportRect = canvasViewportRef.value.getBoundingClientRect();
  pointers.set(e.pointerId, localPoint(e));

  if (pointers.size >= 2) {
    startPinch();
    return;
  }

  const nodeEl = target.closest<HTMLElement>(".interactive-node");
  const nodeId = nodeEl?.dataset.nodeId;
  if (nodeId) {
    const pos = nodePositions.value[nodeId] ?? defaultNodePositions[nodeId] ?? { x: 0, y: 0 };
    // Not a drag yet: the pointer isn't captured, so a plain click still reaches the node.
    gesture = {
      kind: "node",
      pointerId: e.pointerId,
      pointerType: e.pointerType,
      nodeId,
      start: pointers.get(e.pointerId)!,
      node0: { x: pos.x, y: pos.y },
      dragging: false,
    };
    pressedNodeId.value = nodeId;
    return;
  }

  const range = currentPanRange();
  const size = viewportSize();
  // Grabbed mid-band: continue from the raw pan that shows what is on screen now.
  const pan0 = range
    ? {
        x: unclampPanSoft(panX.value, range.minX, range.maxX, size.w),
        y: unclampPanSoft(panY.value, range.minY, range.maxY, size.h),
      }
    : { x: panX.value, y: panY.value };
  const tracker = createVelocityTracker();
  const start = pointers.get(e.pointerId)!;
  tracker.add(start.x, start.y, performance.now());
  gesture = { kind: "pan", pointerId: e.pointerId, pointerType: e.pointerType, start, pan0, range, size, tracker };
  capturePointer(e.pointerId);
  isPanning.value = true;
}

function onPointerMove(e: PointerEvent) {
  if (!pointers.has(e.pointerId)) return;
  // A mouse released outside the window never sends pointerup.
  if (e.pointerType === "mouse" && e.buttons === 0) {
    onPointerUp(e);
    return;
  }
  const p = localPoint(e);
  pointers.set(e.pointerId, p);
  const g = gesture;
  if (g?.kind === "pan" && g.pointerId === e.pointerId) g.tracker.add(p.x, p.y, performance.now());
  if (g?.kind === "node" && !g.dragging) {
    if (g.pointerId !== e.pointerId) return;
    const threshold = g.pointerType === "mouse" ? DRAG_THRESHOLD_MOUSE : DRAG_THRESHOLD_TOUCH;
    if (Math.hypot(p.x - g.start.x, p.y - g.start.y) <= threshold) return;
    // Past the threshold: a drag. Tracking continues from the original start point, so
    // the grab offset is kept (the node catches up by the threshold, under the pointer).
    g.dragging = true;
    capturePointer(e.pointerId);
    pressedNodeId.value = null;
    draggingNodeId.value = g.nodeId;
    // A fresh entry: the drag never writes into the shared default positions.
    nodePositions.value[g.nodeId] = { x: g.node0.x, y: g.node0.y };
  }
  scheduleFrame();
}

// released: a real lift of the pointer (pointerup), which may throw the canvas; any
// other end (cancel, lost capture, blur, a second finger) only settles it.
function endGesture(released = false) {
  flushFrame();
  const g = gesture;
  gesture = null;
  if (!g) return;
  if (g.kind === "pinch") {
    pinching.value = false;
    settleIntoRange();
  } else if (g.kind === "node") {
    if (g.dragging) {
      savePositions();
      suppressClick = true;
    }
    pressedNodeId.value = null;
    draggingNodeId.value = null;
  } else {
    isPanning.value = false;
    releasePan(g, released);
  }
}

function onPointerUp(e: PointerEvent) {
  if (!pointers.has(e.pointerId)) return;
  // A cancel (or lost capture) carries no meaningful position; keep the last move's.
  if (e.type === "pointerup") pointers.set(e.pointerId, localPoint(e));
  const g = gesture;
  if (g?.kind === "pinch") {
    pointers.delete(e.pointerId);
    // Down to one finger: the pinch ends, and the remaining finger doesn't start a pan.
    if (g.ids.includes(e.pointerId)) endGesture();
    return;
  }
  if (g && g.pointerId === e.pointerId) endGesture(e.type === "pointerup");
  pointers.delete(e.pointerId);
  if (!pointers.size) viewportRect = null;
}

// Capture taken away (the element went away, another capture, a system gesture).
// Only the viewport's own capture counts: a finger's implicit capture on a node is
// handed over to the viewport when its drag starts, and that must not end the drag.
function onLostPointerCapture(e: PointerEvent) {
  if (e.target !== e.currentTarget || !pointers.has(e.pointerId)) return;
  onPointerUp(e);
}

function cancelAllGestures() {
  if (gesture) endGesture();
  pointers.clear();
  viewportRect = null;
}

// ── Wheel, trackpad and the zoom buttons (§1, §3, §7) ──────────────────────────
// Canvas conventions: the wheel and two-finger swipes pan both axes; Ctrl/⌘ + wheel
// (which is also how a trackpad pinch arrives) zooms around the cursor. Units are
// normalised, so a Firefox line-mode wheel moves like Chrome's pixel one. A wheel pan
// that starts with the canvas already at its vertical limit is left to the page, so
// scrolling the admin page past the inline canvas never gets trapped; the choice holds
// for the whole wheel sequence, as native nested scrollers do.
const WHEEL_SEQUENCE_MS = 200;
let wheelOwner: "canvas" | "page" | null = null;
let wheelTimer: ReturnType<typeof setTimeout> | null = null;

function viewportSize(): Size {
  const el = canvasViewportRef.value;
  return { w: el?.clientWidth ?? 0, h: el?.clientHeight ?? 0 };
}

function currentView(): View {
  return { zoom: zoom.value, panX: panX.value, panY: panY.value };
}

function setView(v: View) {
  zoom.value = v.zoom;
  panX.value = v.panX;
  panY.value = v.panY;
}

function onWheel(e: WheelEvent) {
  const el = canvasViewportRef.value;
  if (!el) return;
  stopGlides();
  settleView();
  const unit = wheelUnit(e.deltaMode, el.clientHeight);

  if (e.ctrlKey || e.metaKey) {
    e.preventDefault(); // otherwise the browser zooms the whole page
    const rect = el.getBoundingClientRect();
    setView(zoomAt(currentView(), wheelZoomFactor(e.deltaY, unit), { x: e.clientX - rect.left, y: e.clientY - rect.top }));
  } else {
    let dx: number;
    let dy: number;
    if (e.shiftKey) {
      // Shift + a vertical wheel pans sideways (some systems already send it as deltaX).
      dx = (e.deltaY || e.deltaX) * unit;
      dy = 0;
    } else {
      dx = e.deltaX * unit;
      dy = e.deltaY * unit;
    }
    const range = currentPanRange();
    const nextX = range ? limitPan(panX.value, panX.value - dx, range.minX, range.maxX) : panX.value - dx;
    const nextY = range ? limitPan(panY.value, panY.value - dy, range.minY, range.maxY) : panY.value - dy;
    if (wheelOwner === null) {
      const vertical = Math.abs(dy) > Math.abs(dx);
      const stuck = nextX === panX.value && nextY === panY.value;
      wheelOwner = vertical && stuck && !isFullscreen.value ? "page" : "canvas";
    }
    if (wheelOwner === "page") {
      restartWheelSequence();
      return; // the page scrolls on
    }
    e.preventDefault();
    panX.value = nextX;
    panY.value = nextY;
  }

  isWheeling.value = true;
  restartWheelSequence();
}

function restartWheelSequence() {
  if (wheelTimer) clearTimeout(wheelTimer);
  wheelTimer = setTimeout(() => {
    wheelTimer = null;
    wheelOwner = null;
    isWheeling.value = false;
  }, WHEEL_SEQUENCE_MS);
}

// A compositor layer for the world only while it moves: a permanent one would be
// rasterised once and scaled, blurring the node text after a zoom (§11).
const isMoving = computed(
  () => isPanning.value || pinching.value || draggingNodeId.value !== null || isWheeling.value || isGliding.value,
);

// Button-driven changes glide for 200 ms on the emphasized curve (no bounce: a button
// carries no momentum). Wheel, drag and pinch stay 1:1, with no transition at all.
const VIEW_ANIMATION_MS = 200;
const isAnimatingView = ref(false);
const worldRef = ref<HTMLElement | null>(null);
let viewAnimationTimer: ReturnType<typeof setTimeout> | null = null;

function animateView(v: View) {
  if (reducedMotion.value) {
    setView(v);
    return;
  }
  isAnimatingView.value = true;
  setView(v);
  if (viewAnimationTimer) clearTimeout(viewAnimationTimer);
  viewAnimationTimer = setTimeout(() => {
    viewAnimationTimer = null;
    isAnimatingView.value = false;
  }, VIEW_ANIMATION_MS + 20);
}

// A new gesture during a button glide starts from where the canvas is on screen (the
// presentation value), not from the glide's target, so it never jumps (§3).
function settleView() {
  if (!isAnimatingView.value) return;
  if (viewAnimationTimer) clearTimeout(viewAnimationTimer);
  viewAnimationTimer = null;
  const el = worldRef.value;
  const shown = el && typeof getComputedStyle === "function" ? getComputedStyle(el).transform : "";
  const m = /^matrix\(([^)]+)\)$/.exec(shown ?? "");
  if (m) {
    const [a, , , , e, f] = m[1].split(",").map(Number);
    if ([a, e, f].every(Number.isFinite) && a > 0) setView({ zoom: a, panX: e, panY: f });
  }
  isAnimatingView.value = false;
}

// Quick repeated presses compound from the target (1.2 × 1.2 …); the CSS transition
// itself retargets from wherever the canvas is on screen.
function zoomBy(factor: number) {
  stopGlides();
  const { w, h } = viewportSize();
  animateView(zoomAt(currentView(), factor, { x: w / 2, y: h / 2 }));
}

function zoomIn() {
  zoomBy(1.2);
}

function zoomOut() {
  zoomBy(1 / 1.2);
}

// The whole graph in view, centred, never past 100% (the % button).
function fitView(animate = true, pad = 48) {
  const size = viewportSize();
  const bbox = contentBounds.value;
  if (!bbox || size.w <= 0 || size.h <= 0) return;
  stopGlides();
  const v = fitBounds(bbox, size, pad, ZOOM_LIMITS, 1);
  if (animate) animateView(v);
  else setView(v);
}

// The first view: the fit when it leaves the cards readable; otherwise (the long
// pipeline on a laptop fits only at ~15%, where no name can be read) the readable
// default zoom, starting at the trigger and centred vertically. The % button still
// gives the whole-graph overview.
const READABLE_ZOOM = 0.45;
const START_ZOOM = 0.55;
function showInitialView(pad = 48) {
  const size = viewportSize();
  const bbox = contentBounds.value;
  if (!bbox || size.w <= 0 || size.h <= 0) return;
  const fit = fitBounds(bbox, size, pad, ZOOM_LIMITS, 1);
  if (fit.zoom >= READABLE_ZOOM) {
    setView(fit);
    return;
  }
  const z = START_ZOOM;
  setView({ zoom: z, panX: pad - bbox.minX * z, panY: (size.h - (bbox.maxY - bbox.minY) * z) / 2 - bbox.minY * z });
}

function currentPanRange(): PanRange | null {
  const bbox = contentBounds.value;
  const size = viewportSize();
  if (!bbox || size.w <= 0 || size.h <= 0) return null;
  return panRange(bbox, zoom.value, size);
}

// The dot grid is drawn in world space: it pans and scales with the content, so the
// surface under the pointer moves with it. Below 50% the step doubles to stay legible.
const gridStyle = computed(() => {
  const g = 20 * zoom.value * (zoom.value < 0.5 ? 2 : 1);
  return {
    backgroundImage: "radial-gradient(circle, rgb(var(--c-muted) / 0.22) 1.2px, transparent 1.2px)",
    backgroundSize: `${g}px ${g}px`,
    backgroundPosition: `${panX.value}px ${panY.value}px`,
    transition: isAnimatingView.value
      ? `background-size ${VIEW_ANIMATION_MS}ms var(--ease-emph), background-position ${VIEW_ANIMATION_MS}ms var(--ease-emph)`
      : "none",
  };
});

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
    iconColor: STAGE_TEXT.trigger,
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
    iconColor: STAGE_TEXT.planning,
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
    iconColor: STAGE_TEXT.planning,
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
    iconColor: STAGE_TEXT.planning,
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
    iconColor: STAGE_TEXT.planning,
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
    iconColor: STAGE_TEXT.search,
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
    iconColor: STAGE_TEXT.search,
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
    iconColor: STAGE_TEXT.search,
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
    iconColor: STAGE_TEXT.search,
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
    iconColor: STAGE_TEXT.search,
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
    iconColor: STAGE_TEXT.synthesis,
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
    iconColor: STAGE_TEXT.synthesis,
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
    iconColor: STAGE_TEXT.synthesis,
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
    iconColor: STAGE_TEXT.synthesis,
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
    iconColor: STAGE_TEXT.synthesis,
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
    iconColor: STAGE_TEXT.synthesis,
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
    iconColor: STAGE_TEXT.synthesis,
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
    iconColor: STAGE_TEXT.synthesis,
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
    iconColor: STAGE_TEXT.synthesis,
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
    iconColor: STAGE_TEXT.synthesis,
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
    iconColor: STAGE_TEXT.delivery,
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

// Copies, never the default objects themselves: a drag writes positions in place.
function freshDefaultPositions(): Record<string, { x: number; y: number }> {
  return Object.fromEntries(Object.entries(defaultNodePositions).map(([id, p]) => [id, { x: p.x, y: p.y }]));
}

function loadSavedPositions(): Record<string, { x: number; y: number }> {
  try {
    // Clear legacy keys with cramped coordinates
    localStorage.removeItem("multi-agent-search:admin-nodes-pos");
    localStorage.removeItem("multi-agent-search:admin-nodes-pos-v2");

    const raw = localStorage.getItem(LOCAL_STORAGE_POSITIONS_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object") {
        return { ...freshDefaultPositions(), ...parsed };
      }
    }
  } catch {
    // Ignore localStorage errors
  }
  return freshDefaultPositions();
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
  nodePositions.value = freshDefaultPositions();
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

// ── Node press, drag and click ────────────────────────────────────────────────
// The drag itself runs in the pointer handlers above; these are its visible states:
// pressed (down, not moved past the threshold) and dragging (lifted).
const draggingNodeId = ref<string | null>(null);

// Exactly one visual state per card, so only one scale and one ring apply at a time.
// Pressed dips at once (100 ms); lifted rises over 150 ms; letting go eases back over
// 200 ms. The lift appears only once a press has become a drag (§1, §10).
function nodeCardState(nodeId: string): string {
  if (draggingNodeId.value === nodeId) {
    return "scale-[1.03] border-accent bg-surface shadow-e3 ring-2 ring-accent/50 duration-150 ease-out";
  }
  if (pressedNodeId.value === nodeId) {
    return "scale-[0.985] border-accent/60 bg-surface duration-100 ease-out";
  }
  if (selectedAgent.value?.id === nodeId) {
    return "scale-[1.02] border-accent bg-surface ring-2 ring-accent/60 shadow-accent/20 duration-200 ease-out";
  }
  if (isNodeHighlighted(nodeId)) {
    return "scale-[1.01] border-indigo-400/80 bg-surface ring-2 ring-indigo-400/40 duration-200 ease-out";
  }
  const sim = getAgentSimStatus(nodeId);
  if (sim === "active") {
    return "scale-[1.03] border-sky-400 bg-surface ring-4 ring-sky-400/50 shadow-xl shadow-sky-400/25 duration-200 ease-out";
  }
  if (sim === "completed") return "border-success/60 bg-surface duration-200 ease-out";
  return "border-bd bg-surface hover:border-accent/50 hover:bg-surfaceHover hover:shadow-lg duration-200 ease-out";
}

function handleNodeClick(nodeId: string) {
  if (suppressClick) {
    suppressClick = false;
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
    sourceOffsetX: 36,
    targetOffsetX: 54,
  },
  {
    id: "replan->search",
    from: "replan",
    to: "search",
    payloadKey: "admin.agents.returnPayloads.replan",
    defaultPayload: "↩ gap_analysis_loop",
    arcY: 530,
    sourceOffsetX: 36,
    targetOffsetX: 28,
  },
  {
    id: "claim_verifier->analyzer",
    from: "claim_verifier",
    to: "analyzer",
    payloadKey: "admin.agents.returnPayloads.claim_verifier",
    defaultPayload: "↩ hallucination_retry",
    arcY: 480,
    sourceOffsetX: 36,
    targetOffsetX: 54,
  },
  {
    id: "report_critic->analyzer",
    from: "report_critic",
    to: "analyzer",
    payloadKey: "admin.agents.returnPayloads.report_critic",
    defaultPayload: "↩ draft_revision",
    arcY: 600,
    sourceOffsetX: 54,
    targetOffsetX: 28,
  },
  {
    id: "report_critic->replan",
    from: "report_critic",
    to: "replan",
    payloadKey: "admin.agents.returnPayloads.report_critic_replan",
    defaultPayload: "↩ tie_break_search",
    arcY: 670,
    sourceOffsetX: 28,
    targetOffsetX: 36,
  },
];

const showReturnLoops = ref(true);

const RETURN_CAPABLE_NODES = new Set(["report_critic", "source_critic", "replan", "claim_verifier"]);

function hasReturnCapability(nodeId: string): boolean {
  return RETURN_CAPABLE_NODES.has(nodeId);
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

interface NodeReturnPort {
  id: string;
  connId: string;
  type: "incoming" | "outgoing";
  left: number;
  title: string;
  isHighlighted: boolean;
  isActive: boolean;
}

function getNodeReturnPorts(nodeId: string, nodeWidth: number): NodeReturnPort[] {
  if (!showReturnLoops.value) return [];
  const ports: NodeReturnPort[] = [];
  for (const conn of RETURN_CONNECTIONS) {
    if (conn.from === nodeId) {
      const offset = conn.sourceOffsetX ?? RETURN_PORT_OFFSET;
      const left = nodeWidth - offset;
      const isWireHovered = hoveredEdgeId.value === conn.id;
      const isActive =
        (isSimulating.value || currentStepIndex.value > 0) &&
        currentStep.value.activeEdges.includes(conn.id);
      const payload = te(conn.payloadKey) ? t(conn.payloadKey) : conn.defaultPayload;
      ports.push({
        id: `out-${conn.id}`,
        connId: conn.id,
        type: "outgoing",
        left,
        title: `${t("admin.agents.returnOutgoingPort")}: ${payload}`,
        isHighlighted: isWireHovered || isNodeHighlighted(nodeId),
        isActive,
      });
    }
    if (conn.to === nodeId) {
      const offset = conn.targetOffsetX ?? RETURN_PORT_OFFSET;
      const left = offset;
      const isWireHovered = hoveredEdgeId.value === conn.id;
      const isActive =
        (isSimulating.value || currentStepIndex.value > 0) &&
        currentStep.value.activeEdges.includes(conn.id);
      const payload = te(conn.payloadKey) ? t(conn.payloadKey) : conn.defaultPayload;
      ports.push({
        id: `in-${conn.id}`,
        connId: conn.id,
        type: "incoming",
        left,
        title: `${t("admin.agents.returnIncomingPort")}: ${payload}`,
        isHighlighted: isWireHovered || isNodeHighlighted(nodeId),
        isActive,
      });
    }
  }
  return ports;
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

        const effectiveArcY = Math.max(conn.arcY, Math.max(y1, y2) + 45);
        const drop = Math.min(effectiveArcY - y1, effectiveArcY - y2);
        const dx = Math.abs(x1 - x2);
        const cornerRadius = Math.min(24, Math.max(4, Math.min(dx / 2, drop / 2)));

        let d = "";
        if (x1 >= x2) {
          d =
            `M ${x1} ${y1} ` +
            `V ${effectiveArcY - cornerRadius} ` +
            `A ${cornerRadius} ${cornerRadius} 0 0 1 ${x1 - cornerRadius} ${effectiveArcY} ` +
            `L ${x2 + cornerRadius} ${effectiveArcY} ` +
            `A ${cornerRadius} ${cornerRadius} 0 0 1 ${x2} ${effectiveArcY - cornerRadius} ` +
            `V ${y2}`;
        } else {
          d =
            `M ${x1} ${y1} ` +
            `V ${effectiveArcY - cornerRadius} ` +
            `A ${cornerRadius} ${cornerRadius} 0 0 0 ${x1 + cornerRadius} ${effectiveArcY} ` +
            `L ${x2 - cornerRadius} ${effectiveArcY} ` +
            `A ${cornerRadius} ${cornerRadius} 0 0 0 ${x2} ${effectiveArcY - cornerRadius} ` +
            `V ${y2}`;
        }

        const midX = (x1 + x2) / 2;
        const midY = effectiveArcY;

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

// What Fit shows and the pan limits keep on screen: the nodes, plus the return loops'
// arcs and their labels below them when those are shown.
const contentBounds = computed<BBox | null>(() => {
  const box = boundsOf(visualNodes.value);
  if (!box || !showReturnLoops.value) return box;
  const byId = new Map(visualNodes.value.map((n) => [n.id, n]));
  for (const conn of RETURN_CONNECTIONS) {
    const src = byId.get(conn.from);
    const tgt = byId.get(conn.to);
    if (!src || !tgt) continue;
    const arcY = Math.max(conn.arcY, Math.max(src.y + src.height, tgt.y + tgt.height) + 45);
    box.maxY = Math.max(box.maxY, arcY + 12);
  }
  return box;
});

// ── Simulation Walkthrough Steps ("How they work") ───────────────────────────
interface SimulationStep {
  stepNumber: number;
  stageName: string;
  stageColor: string;
  agentIds: string[];
  activeEdges: string[];
  payloadInfo: string;
}

// Step texts live in i18n (admin.agents.walkthroughSteps.s<N>) so they follow the UI locale.
function stepTitle(step: SimulationStep): string {
  return t(`admin.agents.walkthroughSteps.s${step.stepNumber}.title`);
}
function stepDescription(step: SimulationStep): string {
  return t(`admin.agents.walkthroughSteps.s${step.stepNumber}.description`);
}

const SIMULATION_STEPS: SimulationStep[] = [
  {
    stepNumber: 1,
    stageName: "Planning",
    stageColor: STAGE_TONE.trigger,
    agentIds: ["trigger_start", "clarifier"],
    activeEdges: ["trigger_start->clarifier"],
    payloadInfo: "user_query ➔ clarification_needed, suggested_followups",
  },
  {
    stepNumber: 2,
    stageName: "Planning",
    stageColor: STAGE_TONE.planning,
    agentIds: ["optimizer"],
    activeEdges: ["clarifier->optimizer"],
    payloadInfo: "clarified_intent ➔ optimized_prompt, angles, hypotheses",
  },
  {
    stepNumber: 3,
    stageName: "Planning",
    stageColor: STAGE_TONE.planning,
    agentIds: ["orchestrator", "cross_language"],
    activeEdges: ["optimizer->orchestrator", "orchestrator->cross_language"],
    payloadInfo: "optimized_prompt ➔ subtasks, translated_queries",
  },
  {
    stepNumber: 4,
    stageName: "Search & Ingest",
    stageColor: STAGE_TONE.search,
    agentIds: ["search"],
    activeEdges: ["orchestrator->search", "cross_language->search"],
    payloadInfo: "primary_queries + translated_queries ➔ scraped_pages, raw_snippets",
  },
  {
    stepNumber: 5,
    stageName: "Search & Ingest",
    stageColor: STAGE_TONE.search,
    agentIds: ["source_critic", "source_reputation", "source_independence"],
    activeEdges: [
      "search->source_critic",
      "source_critic->source_reputation",
      "source_reputation->source_independence",
      "source_critic->search",
    ],
    payloadInfo: "scraped_pages ➔ trusted_domains, canonical_sources / ↩ spam_retry",
  },
  {
    stepNumber: 6,
    stageName: "Search & Ingest",
    stageColor: STAGE_TONE.search,
    agentIds: ["evidence_mapper"],
    activeEdges: ["source_independence->evidence_mapper"],
    payloadInfo: "canonical_sources ➔ evidence_blocks, coverage_matrix",
  },
  {
    stepNumber: 7,
    stageName: "Synthesis & Logic",
    stageColor: STAGE_TONE.synthesis,
    agentIds: ["replan", "search"],
    activeEdges: ["evidence_mapper->replan", "replan->search"],
    payloadInfo: "evidence_blocks ➔ gap_detected, ↩ gap_queries ➔ SearchAgent",
  },
  {
    stepNumber: 8,
    stageName: "Synthesis & Logic",
    stageColor: STAGE_TONE.synthesis,
    agentIds: ["analyzer"],
    activeEdges: ["replan->analyzer"],
    payloadInfo: "verified_evidence ➔ draft_report, reasoning_steps, key_findings",
  },
  {
    stepNumber: 9,
    stageName: "Synthesis & Logic",
    stageColor: STAGE_TONE.synthesis,
    agentIds: ["numeric_check", "claim_verifier"],
    activeEdges: ["analyzer->numeric_check", "numeric_check->claim_verifier", "claim_verifier->analyzer"],
    payloadInfo: "draft_report ➔ verified_numbers, verified_claims / ↩ fact_fix",
  },
  {
    stepNumber: 10,
    stageName: "Synthesis & Verification",
    stageColor: STAGE_TONE.synthesis,
    agentIds: ["citation_audit", "retraction", "red_team", "stance"],
    activeEdges: [
      "claim_verifier->citation_audit",
      "claim_verifier->red_team",
      "citation_audit->retraction",
      "red_team->stance",
    ],
    payloadInfo: "verified_claims ➔ counter_arguments, consensus_matrix, clean_sources",
  },
  {
    stepNumber: 11,
    stageName: "Synthesis & Polish",
    stageColor: STAGE_TONE.return,
    agentIds: ["report_critic", "analyzer", "replan"],
    activeEdges: [
      "stance->report_critic",
      "retraction->report_critic",
      "report_critic->analyzer",
      "report_critic->replan",
    ],
    payloadInfo: "draft_review ➔ ↩ draft_revision ➔ Analyzer / ↩ tie_break ➔ Replan",
  },
  {
    stepNumber: 12,
    stageName: "Synthesis & Polish",
    stageColor: STAGE_TONE.synthesis,
    agentIds: ["report_critic", "confidence"],
    activeEdges: [
      "report_critic->confidence",
    ],
    payloadInfo: "approved_draft ➔ calibrated_trust_score, trust_indicators",
  },
  {
    stepNumber: 13,
    stageName: "Delivery & Follow-Up",
    stageColor: STAGE_TONE.delivery,
    agentIds: ["chat"],
    activeEdges: ["confidence->chat"],
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
  } catch (err) {
    error.value = apiErrorMessage(err, t);
  } finally {
    loading.value = false;
  }
}

// The first time the canvas appears, frame the graph (no glide: nothing to follow yet).
let fittedOnce = false;
watch(loading, async (isLoading) => {
  if (isLoading || fittedOnce) return;
  await nextTick();
  if (!canvasViewportRef.value) return;
  fittedOnce = true;
  showInitialView();
});

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
  window.addEventListener("keydown", onKeyDown);
  // Alt-Tab mid-drag: the release happens elsewhere, so end the gesture here.
  window.addEventListener("blur", cancelAllGestures);
});

onBeforeUnmount(() => {
  if (simTimer) clearTimeout(simTimer);
  if (wheelTimer) clearTimeout(wheelTimer);
  if (viewAnimationTimer) clearTimeout(viewAnimationTimer);
  if (frameId) cancelFrame(frameId);
  stopGlides();
  window.removeEventListener("keydown", onKeyDown);
  window.removeEventListener("blur", cancelAllGestures);
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
        ? 'fixed inset-0 z-50 bg-bg p-4 h-screen w-screen overflow-hidden'
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
          class="w-full rounded-xl border border-bd bg-surface/60 py-1.5 pl-8 pr-3 text-xs text-ink placeholder:text-muted"
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
        <div class="flex items-center gap-1.5 rounded-lg border px-2.5 py-1" :class="STAGE_TONE.trigger">
          <span>⚡</span>
          <span>{{ t("admin.agents.legendTrigger") }}</span>
        </div>
        <div class="flex items-center gap-1.5 rounded-lg border px-2.5 py-1" :class="STAGE_TONE.planning">
          <span>🟣</span>
          <span>{{ t("admin.agents.legendPlanning") }}</span>
        </div>
        <div class="flex items-center gap-1.5 rounded-lg border px-2.5 py-1" :class="STAGE_TONE.search">
          <span>🟢</span>
          <span>{{ t("admin.agents.legendSearch") }}</span>
        </div>
        <div class="flex items-center gap-1.5 rounded-lg border px-2.5 py-1" :class="STAGE_TONE.synthesis">
          <span>🟠</span>
          <span>{{ t("admin.agents.legendSynthesis") }}</span>
        </div>
        <div class="flex items-center gap-1.5 rounded-lg border px-2.5 py-1" :class="STAGE_TONE.delivery">
          <span>🔵</span>
          <span>{{ t("admin.agents.legendDelivery") }}</span>
        </div>
        <!-- A real toggle (keyboard, state announced), not a clickable div. -->
        <button
          type="button"
          class="flex items-center gap-1.5 rounded-lg border px-2.5 py-1 hover:bg-rose-500/20"
          :class="STAGE_TONE.return"
          :title="t('admin.agents.toggleReturnLoopsTooltip')"
          :aria-pressed="showReturnLoops ? 'true' : 'false'"
          @click="showReturnLoops = !showReturnLoops"
        >
          <span aria-hidden="true">↩</span>
          <span>{{ t("admin.agents.legendReturn") }}</span>
        </button>
      </div>

      <!-- Right Action Group: Simulation Controls, Zoom, Language & Layout -->
      <div class="flex items-center gap-2">
        <!-- Toggle Return / Feedback Loops Button -->
        <button
          class="flex items-center gap-1.5 rounded-xl border px-2.5 py-1.5 text-xs font-medium transition shadow-sm"
          :class="[
            showReturnLoops
              ? 'border-rose-500/60 bg-rose-500/15 text-rose-700 dark:text-rose-300'
              : 'border-bd bg-surface/70 text-muted hover:text-ink hover:border-rose-500/40'
          ]"
          :title="t('admin.agents.toggleReturnLoopsTooltip')"
          @click="showReturnLoops = !showReturnLoops"
        >
          <span class="text-sm">↩</span>
          <span class="hidden sm:inline">{{ t("admin.agents.showReturnLoops") }}</span>
          <span
            class="rounded-full px-1.5 py-px text-3xs font-semibold tabular-nums"
            :class="showReturnLoops ? 'bg-rose-500/20 text-rose-700 dark:text-rose-200' : 'bg-surface text-muted'"
          >
            {{ RETURN_CONNECTIONS.length }}
          </span>
        </button>

        <!-- Play / Pause Simulation Walkthrough -->
        <div class="flex items-center gap-1.5 rounded-xl border border-bd bg-surface/80 p-1 shadow-sm">
          <button
            v-if="!isSimulating"
            class="flex items-center gap-1.5 rounded-lg bg-accent/15 border border-accent/30 px-3 py-1 text-xs font-semibold text-accent hover:bg-accent/25 transition"
            :title="t('admin.agents.startSim')"
            @click="startSimulation"
          >
            <span>▶</span>
            <span>{{ currentStepIndex > 0 ? t("admin.agents.resumeSim") : t("admin.agents.walkthrough") }}</span>
          </button>
          <button
            v-else
            class="flex items-center gap-1.5 rounded-lg bg-warning/15 border border-warning/40 px-3 py-1 text-xs font-semibold text-warning hover:bg-warning/25 transition"
            :title="t('admin.agents.pauseSim')"
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
            class="rounded px-2 py-1 text-[10px] font-semibold tabular-nums text-muted hover:text-ink"
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
          <!-- Fit: the whole graph in view (the label keeps showing the live zoom). -->
          <button class="min-w-[3.25rem] px-1.5 py-1 text-center text-[11px] tabular-nums hover:text-ink" :title="t('admin.agents.resetZoom')" data-test="graph-fit" @click="fitView()">
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
    <div v-if="error" class="rounded-lg border border-danger/30 bg-danger/10 p-4 text-xs text-danger" role="alert">
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
      class="relative flex-1 overflow-hidden select-none rounded-2xl border border-bd bg-bg/95 shadow-inner"
      :class="[
        isFullscreen ? 'min-h-[calc(100vh-140px)] touch-none' : 'h-[min(640px,calc(100dvh-260px))] min-h-[360px] touch-pan-y',
        isPanning ? 'cursor-grabbing' : 'cursor-grab',
      ]"
      :style="gridStyle"
      data-test="graph-viewport"
      @pointerdown="onPointerDown"
      @pointermove="onPointerMove"
      @pointerup="onPointerUp"
      @pointercancel="onPointerUp"
      @lostpointercapture="onLostPointerCapture"
      @wheel="onWheel"
    >
      <!-- Quick Floating Fullscreen Button on Canvas -->
      <div class="absolute right-4 top-4 z-20 flex items-center gap-2">
        <button
          class="flex items-center gap-1.5 rounded-xl border border-bd bg-surface/90 px-3 py-1.5 text-xs font-medium text-muted shadow-lg backdrop-blur hover:text-ink hover:border-accent/40 transition"
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
        ref="worldRef"
        class="absolute origin-top-left"
        :class="[
          isAnimatingView ? 'transition-transform duration-200 ease-emphasized' : 'transition-none',
          { 'will-change-transform': isMoving },
        ]"
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
              <path d="M 0 1.5 L 8 5 L 0 8.5 z" fill="rgba(148, 163, 184, 0.65)" />
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
                    : 'rgba(244, 63, 94, 0.65)'
                  : edge.isActive
                  ? '#38bdf8'
                  : edge.isHighlighted
                  ? '#818cf8'
                  : 'rgba(148, 163, 184, 0.55)'
              "
              :stroke-width="edge.isActive ? 3.2 : edge.isHighlighted ? 2.6 : edge.isReturn ? 2.2 : 2"
              fill="none"
              :stroke-dasharray="edge.isReturn ? (edge.isActive ? '5,4' : '6,4') : (edge.isActive ? '7,7' : 'none')"
              :class="[
                draggingNodeId !== null ? 'transition-none' : 'transition-[stroke,stroke-width] duration-150',
                { 'animate-n8n-wire': edge.isActive && !reducedMotion }
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
            <!-- SMIL: CSS can't stop it, so under reduced motion it isn't rendered at all. -->
            <circle
              v-if="edge.isActive && !reducedMotion"
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
                        ? 'fill-surface stroke-rose-400 shadow-lg'
                        : edge.isHighlighted
                        ? 'fill-surface stroke-rose-400'
                        : 'fill-surface stroke-rose-500/50'
                      : edge.isActive
                      ? 'fill-surface stroke-sky-400'
                      : edge.isHighlighted
                      ? 'fill-surface stroke-indigo-400'
                      : 'fill-surface stroke-bd',
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
                        ? 'fill-rose-700 dark:fill-rose-300'
                        : 'fill-rose-700 dark:fill-rose-400'
                      : edge.isActive
                      ? 'fill-sky-700 dark:fill-sky-400'
                      : edge.isHighlighted
                      ? 'fill-indigo-700 dark:fill-indigo-400'
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
          class="interactive-node press-none absolute group select-none rounded-2xl transition-none"
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
          :data-node-id="node.id"
          :role="node.isTrigger ? undefined : 'button'"
          :tabindex="node.isTrigger ? undefined : 0"
          :aria-label="getNodeName(node.id, node.name)"
          @click="handleNodeClick(node.id)"
          @keydown.enter.prevent="openInspector(node.id)"
          @keydown.space.prevent="openInspector(node.id)"
          @mouseenter="onNodeHover(node.id)"
          @mouseleave="onNodeHover(null)"
        >
          <!-- Node Card Container: the wrapper above carries the position and never
               animates; this card shows press (a dip), lift (while dragged) and state. -->
          <div
            class="relative flex h-full items-center gap-3 rounded-2xl border p-3 shadow-e2 transition-[border-color,box-shadow,opacity,transform]"
            :class="[isNodeDimmed(node.id) ? 'opacity-30' : 'opacity-100', nodeCardState(node.id)]"
          >
            <!-- Left Input Port (Handle) -->
            <div
              v-if="node.hasInput"
              class="absolute -left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 rounded-full border-2 border-surface bg-muted/60 shadow transition group-hover:scale-125 group-hover:bg-accent"
              :class="[
                getAgentSimStatus(node.id) === 'active' ? 'bg-sky-400 ring-2 ring-sky-400/60 scale-125' : '',
                isNodeHighlighted(node.id) ? 'bg-indigo-400' : '',
              ]"
              title="Input Connection"
            />

            <!-- Right Output Port (Handle) -->
            <div
              v-if="node.hasOutput"
              class="absolute -right-2.5 top-1/2 -translate-y-1/2 h-4 w-4 rounded-full border-2 border-surface bg-muted/60 shadow transition group-hover:scale-125 group-hover:bg-accent"
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
              <!-- Too small to read below 45%: the name alone carries the node. -->
              <p v-if="zoom >= 0.45" class="truncate text-[10px] text-muted mt-0.5 font-sans">
                {{ getNodeSubtitle(node.id, node.subtitle) }}
              </p>
            </div>

            <!-- Return Capability Badge (Critics / Loop Nodes) -->
            <div
              v-if="hasReturnCapability(node.id)"
              class="absolute -top-2.5 left-2 flex items-center gap-1 rounded-full bg-rose-500/20 border border-rose-500/40 px-2 py-0.5 text-[8.5px] font-bold text-rose-700 dark:text-rose-300 shadow transition-transform hover:scale-105 cursor-help"
              :title="getReturnCapabilityTooltip(node.id)"
            >
              <span class="text-[9px]">↩</span>
              <span>{{ t("admin.agents.canReturnBadge") }}</span>
            </div>

            <!-- Active / Done Simulation Status Badges -->
            <div v-if="getAgentSimStatus(node.id) === 'active'" class="absolute -top-2 right-2">
              <span class="flex items-center gap-1 rounded-full bg-sky-500/20 border border-sky-500/40 px-2 py-0.5 text-[9px] font-bold text-info animate-pulse shadow">
                ● {{ t("admin.agents.activeBadge") }}
              </span>
            </div>
            <div v-else-if="getAgentSimStatus(node.id) === 'completed'" class="absolute -top-2 right-2">
              <span class="flex items-center gap-1 rounded-full bg-success/15 border border-success/30 px-2 py-0.5 text-[9px] font-bold text-success shadow">
                ✓ {{ t("admin.agents.doneBadge") }}
              </span>
            </div>

            <!-- Bottom Return Ports (Dedicated handles for each feedback loop) -->
            <div
              v-for="port in getNodeReturnPorts(node.id, node.width)"
              :key="port.id"
              class="absolute -bottom-1.5 h-3 w-3 -translate-x-1/2 rounded-full border border-surface shadow transition cursor-pointer group-hover:scale-125"
              :class="[
                port.type === 'outgoing' ? 'bg-rose-500/85 hover:bg-rose-400' : 'bg-rose-500/65 hover:bg-rose-400',
                port.isActive
                  ? 'bg-rose-300 ring-2 ring-rose-400/80 scale-125 animate-pulse'
                  : port.isHighlighted
                  ? 'bg-rose-400 ring-2 ring-rose-400/60 scale-125'
                  : '',
              ]"
              :style="{ left: `${port.left}px` }"
              :title="port.title"
              @mouseenter="onEdgeHover(port.connId)"
              @mouseleave="onEdgeHover(null)"
            />

            <!-- Bottom Diamond Port + Model Badge (Screenshot 2 Style) -->
            <div
              v-if="node.llmModel && zoom >= 0.45"
              class="absolute -bottom-2.5 left-1/2 -translate-x-1/2 flex items-center gap-1 rounded-full border border-bd bg-surface px-2 py-px font-mono text-[8.5px] text-muted whitespace-nowrap shadow-sm"
            >
              <span class="text-accent text-[7px]">◆</span>
              <span>{{ node.llmModel }}</span>
            </div>
          </div>
        </div>
      </div>

      <!-- Canvas Hint Badge -->
      <div
        class="absolute bottom-3 left-4 pointer-events-none z-10 flex items-center gap-2 rounded-lg border border-bd bg-surface/85 px-3 py-1.5 text-[11px] text-muted backdrop-blur font-sans shadow"
      >
        <span class="text-xs">✋</span>
        <span>{{ t("admin.agents.dragHint") }}</span>
      </div>
    </div>

    <!-- Floating Simulation Walkthrough Banner ("How they work") -->
    <div
      v-if="isSimulating || currentStepIndex > 0"
      class="rounded-2xl border border-accent/40 bg-surface/95 p-4 shadow-e2"
    >
      <div class="flex flex-wrap items-center justify-between gap-3 border-b border-bd/60 pb-3">
        <div class="flex items-center gap-2">
          <span
            class="rounded-lg border px-2 py-0.5 text-3xs font-semibold uppercase tracking-wider"
            :class="currentStep.stageColor"
          >
            {{ currentStep.stageName }}
          </span>
          <h4 class="text-sm font-bold text-ink">{{ stepTitle(currentStep) }}</h4>
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
              :title="t('admin.agents.stepTitle', { n: step.stepNumber, title: stepTitle(step) })"
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
          <span class="text-xs tabular-nums text-muted">
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
          {{ stepDescription(currentStep) }}
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
