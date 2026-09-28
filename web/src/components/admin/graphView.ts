// Pure view maths for the admin agent graph (apple-design §2, §3, §7): the canvas maps a
// world point w to the screen as  screen = w · zoom + pan.  Every helper returns a new
// view and keeps some world point fixed under a screen point, so zooming, pinching and
// fitting never make the content drift away from where the reader is looking.

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
