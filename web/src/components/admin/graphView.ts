// Pure view maths for the admin agent graph (apple-design §2, §3, §7): the canvas maps a
// world point w to the screen as  screen = w · zoom + pan.  Every helper returns a new
// view and keeps some world point fixed under a screen point, so zooming, pinching and
// fitting never make the content drift away from where the reader is looking.

import { rubberband } from "@/lib/gesture";

export interface Point {
  x: number;
  y: number;
}

export interface View {
  zoom: number;
  panX: number;
  panY: number;
}

export interface ZoomLimits {
  min: number;
  max: number;
}

export interface BBox {
  minX: number;
  minY: number;
  maxX: number;
  maxY: number;
}

export interface Size {
  w: number;
  h: number;
}

export const ZMIN = 0.15;
export const ZMAX = 1.5;
export const ZOOM_LIMITS: ZoomLimits = { min: ZMIN, max: ZMAX };

export function clampZoom(zoom: number, limits: ZoomLimits = ZOOM_LIMITS): number {
  return Math.min(limits.max, Math.max(limits.min, zoom));
}

/** The world point shown at a screen point. */
export function toWorld(view: View, p: Point): Point {
  return { x: (p.x - view.panX) / view.zoom, y: (p.y - view.panY) / view.zoom };
}

/** Scale by `factor` around `anchor` (screen px): the world point under it stays put. */
export function zoomAt(view: View, factor: number, anchor: Point, limits: ZoomLimits = ZOOM_LIMITS): View {
  const zoom = clampZoom(view.zoom * factor, limits);
  const w = toWorld(view, anchor);
  return { zoom, panX: anchor.x - w.x * zoom, panY: anchor.y - w.y * zoom };
}

const midpoint = (a: Point, b: Point): Point => ({ x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 });
const distance = (a: Point, b: Point): number => Math.hypot(b.x - a.x, b.y - a.y);

/**
 * Two-finger pinch from the view at its start (fingers at a0, b0) to fingers at a, b:
 * the zoom follows the finger spread, and the world point that was under the starting
 * midpoint follows the current midpoint (so a pinch also pans, 1:1).
 */
export function pinch(view0: View, a0: Point, b0: Point, a: Point, b: Point, limits: ZoomLimits = ZOOM_LIMITS): View {
  const d0 = distance(a0, b0);
  const zoom = clampZoom(d0 > 0 ? (view0.zoom * distance(a, b)) / d0 : view0.zoom, limits);
  const w = toWorld(view0, midpoint(a0, b0));
  const m = midpoint(a, b);
  return { zoom, panX: m.x - w.x * zoom, panY: m.y - w.y * zoom };
}

/** The smallest box around a set of rectangles (node cards). */
export function boundsOf(rects: { x: number; y: number; width: number; height: number }[]): BBox | null {
  if (!rects.length) return null;
  const box = { minX: Infinity, minY: Infinity, maxX: -Infinity, maxY: -Infinity };
  for (const r of rects) {
    box.minX = Math.min(box.minX, r.x);
    box.minY = Math.min(box.minY, r.y);
    box.maxX = Math.max(box.maxX, r.x + r.width);
    box.maxY = Math.max(box.maxY, r.y + r.height);
  }
  return box;
}

/**
 * The view that shows the whole box inside the viewport with `pad` px to spare, centred,
 * never closer than `maxZoom` (a fit doesn't blow a small graph up past 100%).
 */
export function fitBounds(bbox: BBox, viewport: Size, pad = 48, limits: ZoomLimits = ZOOM_LIMITS, maxZoom = 1): View {
  const bw = Math.max(bbox.maxX - bbox.minX, 1);
  const bh = Math.max(bbox.maxY - bbox.minY, 1);
  const zx = (viewport.w - 2 * pad) / bw;
  const zy = (viewport.h - 2 * pad) / bh;
  const zoom = clampZoom(Math.min(zx, zy, maxZoom), limits);
  return {
    zoom,
    panX: (viewport.w - bw * zoom) / 2 - bbox.minX * zoom,
    panY: (viewport.h - bh * zoom) / 2 - bbox.minY * zoom,
  };
}

export interface PanRange {
  minX: number;
  maxX: number;
  minY: number;
  maxY: number;
}

/**
 * The pans that keep at least `keep` px of the content box on screen, per axis, so the
 * graph can never be pushed out of sight (less when the content itself is smaller).
 */
export function panRange(bbox: BBox, zoom: number, viewport: Size, keep = 120): PanRange {
  const kx = Math.min(keep, (bbox.maxX - bbox.minX) * zoom, viewport.w);
  const ky = Math.min(keep, (bbox.maxY - bbox.minY) * zoom, viewport.h);
  return {
    minX: kx - bbox.maxX * zoom,
    maxX: viewport.w - kx - bbox.minX * zoom,
    minY: ky - bbox.maxY * zoom,
    maxY: viewport.h - ky - bbox.minY * zoom,
  };
}

/**
 * A hard limit that never jumps: a pan already outside [min, max] (after a zoom moved the
 * range) may come back towards it, but not go further out.
 */
export function limitPan(current: number, next: number, min: number, max: number): number {
  return Math.min(Math.max(max, current), Math.max(Math.min(min, current), next));
}

/** px per wheel unit for WheelEvent.deltaMode: pixels, lines (Firefox), pages. */
export function wheelUnit(deltaMode: number, pageHeight: number): number {
  return deltaMode === 1 ? 16 : deltaMode === 2 ? pageHeight : 1;
}

/**
 * The zoom factor for a Ctrl/⌘ wheel event (a trackpad pinch arrives this way, as
 * small deltas). Pinch deltas pass through untouched, so the zoom follows the fingers;
 * a mouse notch (100 px in Chrome, 3 lines in Firefox) is held to about a 22% step
 * instead of a 63% jump.
 */
export function wheelZoomFactor(deltaY: number, unit = 1): number {
  const d = Math.max(-25, Math.min(25, deltaY * unit));
  return Math.exp(-d * 0.01);
}

// Soft pan limits (§9): past the limit the canvas follows less and less, as a rubber band
// (lib/gesture's rubberband(o, d, c) = o·d·c / (d + c·|o|), with its default c).
const RUBBER_C = 0.55;

/** The pan to show for a raw (finger-driven) pan: 1:1 inside [min, max], banded outside. */
export function clampPanSoft(raw: number, min: number, max: number, dim: number): number {
  if (raw < min) return min + rubberband(raw - min, dim, RUBBER_C);
  if (raw > max) return max + rubberband(raw - max, dim, RUBBER_C);
  return raw;
}

/**
 * The raw pan that clampPanSoft would show as `shown`: grabbing a canvas that is still
 * out in the band continues from where it is on screen instead of jumping.
 */
export function unclampPanSoft(shown: number, min: number, max: number, dim: number): number {
  const limit = shown < min ? min : shown > max ? max : null;
  if (limit === null || dim <= 0) return shown;
  // Invert r = o·d·c / (d + c·|o|):  o = r·d / (c·(d − |r|)).  |r| < d always.
  const r = Math.max(-dim * 0.999, Math.min(dim * 0.999, shown - limit));
  return limit + (r * dim) / (RUBBER_C * (dim - Math.abs(r)));
}

/** The pan inside [min, max] closest to `value` (where a released band settles). */
export function clampPan(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}
