<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, useId, watch } from "vue";
import { useI18n } from "vue-i18n";
import MarkdownIt from "markdown-it";
import renderMathInElement from "katex/contrib/auto-render";
import "katex/dist/katex.min.css";
import type { CitationGround, SourceIndependence, SourcePreview } from "@/lib/types";
import { safeHttpUrl } from "@/lib/url";
import { useDismiss } from "@/lib/useDismiss";

const props = defineProps<{
  source: string;
  sources?: SourcePreview[];
  grounding?: CitationGround[];
  // Inline verification inputs (all optional — the report renders fine without them):
  independence?: SourceIndependence | null; // origin clusters → independent-source count
  weakClaims?: string[];                     // claims the audit could not back with their sources
  contradictions?: string[];                 // sentences in an internal numeric contradiction
  verify?: boolean;                          // decorate each cited sentence with a support band
}>();

const { t } = useI18n();

// Escape text destined for an HTML attribute (quote shown on [Sn] hover).
function escAttr(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// A source URL safe to place inside a hand-built href: an http(s) URL that parses,
// percent-encoded (lib/url) and attribute-escaped. Anything else gets no link ("").
function safeHref(u: string | undefined): string {
  const href = safeHttpUrl(u);
  return href ? escAttr(href) : "";
}

// Claim sentinels (verify mode): OPEN idx MID … OPEN idx END wraps one cited sentence.
// Control characters, so they survive markdown rendering and never occur in real text.
const OPEN = "\u0001";
const MID = "\u0002";
const END = "\u0003";
const SENTINEL = /\u0001(\d+)([\u0002\u0003])/g;
// In rendered text: a claim sentinel (idx, kind) or an inline citation [Sn] (n).
const CLAIM_OR_CITATION = /\u0001(\d+)([\u0002\u0003])|\[S(\d+)\]/g;

// html:false — report text comes from LLM/web content, never render raw HTML (XSS-safe).
const md = new MarkdownIt({ html: false, linkify: true, breaks: false });

// Open links in a new tab safely.
const defaultLinkOpen =
  md.renderer.rules.link_open ||
  ((tokens, idx, options, _env, self) => self.renderToken(tokens, idx, options));
md.renderer.rules.link_open = (tokens, idx, options, env, self) => {
  tokens[idx].attrSet("target", "_blank");
  tokens[idx].attrSet("rel", "noopener noreferrer");
  return defaultLinkOpen(tokens, idx, options, env, self);
};

// A table scrolls sideways inside its own box, so a table wider than the column never
// widens the report: in a narrow split view the whole panel scrolled sideways instead.
// The wrapper is real markup, so the claim rewrite below sees it as tags and skips it.
const defaultTableOpen =
  md.renderer.rules.table_open ||
  ((tokens, idx, options, _env, self) => self.renderToken(tokens, idx, options));
const defaultTableClose =
  md.renderer.rules.table_close ||
  ((tokens, idx, options, _env, self) => self.renderToken(tokens, idx, options));
md.renderer.rules.table_open = (tokens, idx, options, env, self) =>
  `<div class="md-table-scroll">${defaultTableOpen(tokens, idx, options, env, self)}`;
md.renderer.rules.table_close = (tokens, idx, options, env, self) =>
  `${defaultTableClose(tokens, idx, options, env, self)}</div>`;

// Map Sn -> url from the explicit API map, with the report's Sources section as
// a backward-compatible fallback for older stored reports. The explicit map is
// authoritative: any report line mentioning [Sn] next to a URL (body text, not
// just the Sources section) may only fill ids the map does not have.
function sourceUrlMap(source: string, explicitSources: SourcePreview[] = []): Map<string, string> {
  const map = new Map<string, string>();
  for (const source of explicitSources) {
    const idMatch = (source.source_id || "").match(/^S(\d+)$/);
    if (idMatch && source.url) map.set(idMatch[1], source.url);
  }
  for (const line of source.split("\n")) {
    const idMatch = line.match(/\[S(\d+)\\?\]/);
    if (!idMatch || map.has(idMatch[1])) continue;
    const urlMatch = line.match(/\((https?:\/\/[^)\s]+)\)/) || line.match(/(https?:\/\/[^)\s]+)/);
    if (urlMatch) map.set(idMatch[1], urlMatch[1]);
  }
  return map;
}

// ── Inline verification ───────────────────────────────────────────────────────
// Each report sentence carrying [Sn] citations is graded on the trust signals the
// app already computed: how many INDEPENDENT origins back it, whether the cited
// sources actually support it, and whether it sits in a numeric contradiction.
type Band = "strong" | "medium" | "weak" | "contested";
// The bands that ask for a second look are underlined; strong and medium claims are not.
const UNDERLINED_BANDS: ReadonlySet<Band> = new Set<Band>(["weak", "contested"]);
interface Claim {
  band: Band;
  title: string;
  badge: string;
}

function norm(s: string): string {
  return s.replace(/\[S\d+\]/g, " ").replace(/\s+/g, " ").trim().toLowerCase();
}

// Build the per-sentence grader from the current trust props.
function buildGrader(): (sentence: string) => Claim {
  const ground = new Map((props.grounding || []).map((g) => [g.source_id, g]));
  const clusterOf = new Map<string, number>();
  (props.independence?.clusters || []).forEach((c, i) => {
    for (const sid of c.source_ids || []) clusterOf.set(sid, i);
  });
  const weak = (props.weakClaims || []).map(norm).filter((x) => x.length >= 12);
  const contra = (props.contradictions || []).map(norm).filter((x) => x.length >= 12);

  return (sentence: string): Claim => {
    const cites = Array.from(new Set(Array.from(sentence.matchAll(/\[S(\d+)\]/g)).map((m) => m[1])));
    // Independent origins: sources in the same echo-cluster count once.
    const clusters = new Set<number>();
    let solo = 0;
    for (const n of cites) {
      const ci = clusterOf.get(`S${n}`);
      if (ci === undefined) solo += 1;
      else clusters.add(ci);
    }
    const origins = clusters.size + solo || cites.length;
    const supported = cites.filter((n) => ground.get(`S${n}`)?.supported !== false).length;
    const ns = norm(sentence);
    const isContra = ns.length >= 12 && contra.some((c) => c.includes(ns) || ns.includes(c));
    const isWeak = supported === 0 || (ns.length >= 12 && weak.some((w) => w.includes(ns) || ns.includes(w)));

    let band: Band;
    if (isContra) band = "contested";
    else if (isWeak) band = "weak";
    else if (origins >= 2) band = "strong";
    else band = "medium";

    const badge =
      band === "strong" ? `✓${origins}` : band === "medium" ? `${origins}` : band === "weak" ? "⚠" : "✕";
    const title =
      band === "strong"
        ? t("verify.tipStrong", { n: origins })
        : band === "medium"
          ? t("verify.tipMedium", { n: origins })
          : band === "weak"
            ? t("verify.tipWeak")
            : t("verify.tipContested");
    return { band, title, badge };
  };
}

// Split into sentences, breaking only on . ! ? that are followed by whitespace and NOT
// inside a decimal / version number (so "GPT-5.6" and "3.5" stay one token).
function splitSentences(text: string): string[] {
  const parts: string[] = [];
  const re = /[.!?]+["»”')\]]*(?=\s)/g;
  let last = 0;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    const end = m.index + m[0].length;
    const before = text[m.index - 1] || "";
    const nextChar = (text.slice(end).match(/^\s+(\S)/) || [])[1] || "";
    if (m[0] === "." && /\d/.test(before) && /\d/.test(nextChar)) continue; // decimal like 3.5
    parts.push(text.slice(last, end));
    last = end;
  }
  if (last < text.length) parts.push(text.slice(last));
  return parts.length ? parts : [text];
}

// Source lines that markdown-it renders as table rows (header and body), taken from a
// parse of the same source so the verify pass agrees with the renderer: rows without
// outer pipes and tables inside a blockquote or list count too.
function tableRowLines(source: string): Set<number> {
  const rows = new Set<number>();
  for (const token of md.parse(source, {})) {
    if (token.type === "tr_open" && token.map) rows.add(token.map[0]);
  }
  return rows;
}

// A cell separator as markdown-it's table rule sees it: any "|" not right after a "\".
const CELL_PIPE = /(?<!\\)\|/;

// Normalize escaped citation brackets (\[Sn\] -> [Sn]) so they render as citations and don't
// collide with KaTeX's \[…\] delimiter / show as literal backslashes.
// Control characters that double as claim sentinels are dropped so report text can't forge one.
// Line breaks become "\n" the way markdown-it normalizes them, so the verify pass below
// numbers lines as its parse does (a bare "\r" is a line break there, not for split("\n")).
const normalizedSource = computed(() =>
  (props.source || "")
    .replace(/\r\n?/g, "\n")
    .replace(/[\u0001-\u0003]/g, "")
    .replace(/\\\[(S\d+(?:[,\s]+S\d+)*)\\\]/g, "[$1]"),
);
const urlMap = computed(() => sourceUrlMap(normalizedSource.value, props.sources));
const groundMap = computed(() => new Map((props.grounding || []).map((g) => [g.source_id, g])));

// One popover per view, outside the v-html; citations point at it with aria-describedby.
const popId = `cite-pop-${useId()}`;

const html = computed(() => {
  const source = normalizedSource.value;
  const urls = urlMap.value;
  const ground = groundMap.value;

  // 1. Wrap each cited sentence with control-char sentinels BEFORE markdown runs, so
  //    the wrapping survives rendering and never breaks tag nesting (sentences stay
  //    within a block). The sentinels carry an index into `claims`.
  let prepared = source;
  const claims: Claim[] = [];
  if (props.verify) {
    const grade = buildGrader();
    const decorate = (text: string) =>
      splitSentences(text)
        .map((part) => {
          if (!/\[S\d+\]/.test(part)) return part;
          const wm = part.match(/^(\s*)([\s\S]*?)(\s*)$/);
          const lead = wm?.[1] ?? "";
          const core = wm?.[2] ?? part;
          const trail = wm?.[3] ?? "";
          if (!core) return part;
          const idx = claims.push(grade(core)) - 1;
          return `${lead}${OPEN}${idx}${MID}${core}${OPEN}${idx}${END}${trail}`;
        })
        .join("");
    const tableRows = tableRowLines(source);
    let inSources = false;
    prepared = source
      .split("\n")
      .map((line, lineNo) => {
        if (/^\s*#{1,6}\s+(sources|источники|fuentes)/i.test(line)) inSources = true;
        // Skip: the Sources section, headings, source-definition lines ("- **[S1]** …"),
        // and any line without inline citations.
        if (
          inSources ||
          /^\s*#{1,6}\s/.test(line) ||
          /^\s*(?:[-*+]\s+)?\*{0,2}\\?\[S\d+\\?\]/.test(line) ||
          !/\[S\d+\]/.test(line)
        )
          return line;
        const pm = line.match(/^(\s*(?:[-*+]\s+|\d+[.)]\s+|>\s+)?)([\s\S]*)$/);
        const prefix = pm?.[1] ?? "";
        const body = pm?.[2] ?? line;
        // A table row is decorated cell by cell, with every pipe left outside the
        // sentinels: a sentinel before a row's leading "|" reads as an extra first cell
        // (and the last cell is dropped), and a span across cells cannot nest.
        const decorated = tableRows.has(lineNo)
          ? body.split(CELL_PIPE).map(decorate).join("|")
          : decorate(body);
        return prefix + decorated;
      })
      .join("\n");
  }

  // markdown-it treats "\(" / "\)" as escaped punctuation and strips the backslash, which would
  // destroy KaTeX's inline-math delimiters before renderMathInElement runs. Shield them across
  // render with ASCII sentinels, then restore so renderMathInElement can find the math.
  prepared = prepared.replace(/\\\(/g, "@@KMO@@").replace(/\\\)/g, "@@KMC@@");
  const rendered = md.render(prepared).replace(/@@KMO@@/g, "\\(").replace(/@@KMC@@/g, "\\)");

  // 2 + 3. Rewrite TEXT only, never inside a tag. With html:false markdown-it escapes every
  //    "<" and ">" that is not its own markup — in text and in attribute values alike — so
  //    /<[^>]*>/ splits the output into exactly its real tags. An [Sn] or a sentinel inside
  //    an attribute (image alt, link title) must stay plain text there: an injected
  //    <a href="…"> would close the attribute and let the URL add event handlers (XSS).
  const parts = rendered.split(/(<[^>]*>)/);
  // A claim is decorated only when both of its sentinels sit in text; otherwise its
  // span could open inside an attribute or never close.
  const opened = new Set<string>();
  const closed = new Set<string>();
  parts.forEach((part, i) => {
    if (i % 2) return;
    for (const m of part.matchAll(SENTINEL)) (m[2] === MID ? opened : closed).add(m[1]);
  });

  //    Sentinels → a styled span + a trailing support badge; inline [Sn] → a link to the
  //    source. A citation with grounding carries data-cite instead of a title: the
  //    citation popover shows its quote, and a weak-citation flag when the source text
  //    doesn't actually back the claim. Without grounding it just links.
  //    A citation never starts a line: the whitespace before each [Sn] (and between
  //    consecutive ones) becomes a no-break space that glues it to the preceding word.
  //
  //    A flagged claim is underlined under its words only. A decoration set on the claim
  //    span would spread to every inline child, and so run under the [Sn] chips, the
  //    spaces that glue them and the badge. So each run of the claim's own text gets its
  //    own .md-claim-text span, and chips, glue and badge stay outside it. The parts are
  //    rewritten in order, and a claim can span inline tags (**bold**, a link), so the
  //    open claim's state carries from one text part to the next.
  let underlining = false;
  const underline = (text: string) => {
    // Glue and punctuation left between or after chips carry no words to mark.
    if (!underlining || !/[\p{L}\p{N}]/u.test(text)) return text;
    const m = text.match(/^([\s.,;:!?\u2026]*)([\s\S]*?)(\s*)$/)!;
    return `${m[1]}<span class="md-claim-text">${m[2]}</span>${m[3]}`;
  };
  const claimMark = (idx: string, kind: string) => {
    const c = claims[Number(idx)];
    if (!c || !opened.has(idx) || !closed.has(idx)) return "";
    if (kind === MID) {
      underlining = UNDERLINED_BANDS.has(c.band);
      return `<span class="md-claim md-claim-${c.band}" title="${escAttr(c.title)}">`;
    }
    underlining = false;
    return `<sup class="md-claim-badge md-claim-badge-${c.band}">${c.badge}</sup></span>`;
  };
  const citation = (n: string) => {
    const g = ground.get(`S${n}`);
    const url = safeHref(g?.url || urls.get(n));
    const cls = g && !g.supported ? "md-citation md-citation-weak" : "md-citation";
    const cite = g ? ` data-cite="S${n}" aria-describedby="${escAttr(popId)}"` : "";
    return url
      ? `<a href="${url}" target="_blank" rel="noopener noreferrer" class="${cls}"${cite}>[S${n}]</a>`
      : `<sup class="${cls}"${cite}${g ? ' tabindex="0"' : ""}>[S${n}]</sup>`;
  };
  const rewriteText = (raw: string) => {
    const text = raw.replace(/[ \t]+(?=\[S\d+\])/g, "\u00a0");
    let out = "";
    let last = 0;
    for (const m of text.matchAll(CLAIM_OR_CITATION)) {
      out += underline(text.slice(last, m.index));
      last = m.index + m[0].length;
      out += m[3] === undefined ? claimMark(m[1], m[2]) : citation(m[3]);
    }
    return out + underline(text.slice(last));
  };

  return parts.map((part, i) => (i % 2 ? part.replace(SENTINEL, "") : rewriteText(part))).join("");
});

// Render LaTeX math (\(…\), \[…\], $$…$$) in the article after each html update (KaTeX).
const articleEl = ref<HTMLElement | null>(null);
watch(
  html,
  () => {
    nextTick(() => {
      if (!articleEl.value) return;
      try {
        renderMathInElement(articleEl.value, {
          delimiters: [
            { left: "$$", right: "$$", display: true },
            { left: "\\(", right: "\\)", display: false },
          ],
          throwOnError: false,
        });
      } catch {
        /* ignore malformed math */
      }
      // After the math: rendered formulas change a table's width.
      updateTableFades();
    });
  },
  { immediate: true },
);

// A table wider than its scroller fades at the right edge while more of it waits there
// (apple-design §12 scroll edge effect; the fade goes once the end is reached).
function updateTableFades() {
  for (const el of articleEl.value?.querySelectorAll<HTMLElement>(".md-table-scroll") ?? []) {
    el.classList.toggle("edge-fade-x", el.scrollLeft + el.clientWidth < el.scrollWidth - 1);
  }
}
// Scroll does not bubble: the root listens in the capture phase for its tables' scrolls.
function onScrollCapture(e: Event) {
  if (e.target instanceof HTMLElement && e.target.classList.contains("md-table-scroll")) updateTableFades();
}
let tableObserver: ResizeObserver | undefined;
onMounted(() => {
  if (typeof ResizeObserver === "undefined" || !articleEl.value) return;
  tableObserver = new ResizeObserver(updateTableFades);
  tableObserver.observe(articleEl.value);
});

// ── citation popover ──────────────────────────────────────────────────────────
// The grounding quote for [Sn] used to live in a title tooltip: about a second of hover on
// a desktop, never on touch (the tap left for the source), unreliable for keyboards. Now
// (apple-design §1 response, §7 anchored origin, §16 feedback) it opens:
// - mouse or pen: after 150 ms of hover intent; it stays while the pointer moves onto it
//   and closes 200 ms after the pointer has left both;
// - keyboard: on focus;
// - touch: the first tap opens it instead of navigating, a second tap follows the link.
// It closes on an outside press or Escape (useDismiss), and on any scroll or resize.
const HOVER_INTENT_MS = 150;
const LEAVE_GRACE_MS = 200;
const GAP = 6;
const MARGIN = 8;

const popEl = ref<HTMLElement | null>(null);
const popOpen = ref(false);
const popAnchor = ref<HTMLElement | null>(null);
const popSid = ref("");
const popStyle = ref<Record<string, string>>({});
useDismiss(popEl, popOpen, { trigger: popAnchor });

function hostOf(href: string | null): string {
  if (!href) return "";
  try {
    return new URL(href).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}
const popInfo = computed(() => {
  const g = groundMap.value.get(popSid.value);
  if (!g) return null;
  const href = safeHttpUrl(g.url || urlMap.value.get(popSid.value.slice(1)));
  return { supported: !!g.supported, quote: g.quote || "", domain: hostOf(href), href };
});

let openTimer: ReturnType<typeof setTimeout> | undefined;
let closeTimer: ReturnType<typeof setTimeout> | undefined;
function clearTimers() {
  clearTimeout(openTimer);
  clearTimeout(closeTimer);
}
function closePop() {
  clearTimers();
  popOpen.value = false;
}
function scheduleClose() {
  clearTimeout(closeTimer);
  closeTimer = setTimeout(closePop, LEAVE_GRACE_MS);
}

// Fixed position from the citation's box: below it when it fits, else above; kept inside
// the viewport, and coming out of the citation (§7, §8). It grows from the edge next to
// the citation (transform-origin top when below, bottom when above), and the pop
// transition's small offset (--pop-dy, .cite-pop below) starts it on the citation's side
// too, so it moves away from the citation, not towards it.
async function place() {
  const anchor = popAnchor.value;
  if (!anchor) return;
  const r = anchor.getBoundingClientRect();
  popStyle.value = {
    top: `${r.bottom + GAP}px`,
    left: `${Math.max(MARGIN, r.left)}px`,
    transformOrigin: "left top",
    "--pop-dy": "-4px",
  };
  await nextTick();
  const pop = popEl.value;
  if (!pop || !popOpen.value || popAnchor.value !== anchor) return;
  const w = pop.offsetWidth;
  const h = pop.offsetHeight;
  const center = r.left + r.width / 2;
  const left = Math.min(Math.max(MARGIN, center - 16), Math.max(MARGIN, window.innerWidth - w - MARGIN));
  const below = r.bottom + GAP + h <= window.innerHeight - MARGIN || r.top - GAP - h < MARGIN;
  popStyle.value = {
    top: `${below ? r.bottom + GAP : r.top - GAP - h}px`,
    left: `${left}px`,
    transformOrigin: `${Math.min(Math.max(0, center - left), w)}px ${below ? "0" : "100%"}`,
    "--pop-dy": below ? "-4px" : "4px",
  };
}
function openFor(el: HTMLElement) {
  clearTimers();
  const sid = el.dataset.cite || "";
  if (!groundMap.value.has(sid)) return;
  popAnchor.value = el;
  popSid.value = sid;
  popOpen.value = true;
  place();
}

const citeOf = (target: EventTarget | null): HTMLElement | null =>
  target instanceof Element ? target.closest<HTMLElement>("[data-cite]") : null;

function onPointerOver(e: PointerEvent) {
  if (e.pointerType === "touch") return;
  const el = citeOf(e.target);
  if (!el) return;
  clearTimeout(closeTimer);
  if (popOpen.value && popAnchor.value === el) return;
  clearTimeout(openTimer);
  // Moving from one open citation to the next shows the next at once.
  if (popOpen.value) openFor(el);
  else openTimer = setTimeout(() => openFor(el), HOVER_INTENT_MS);
}
function onPointerOut(e: PointerEvent) {
  if (e.pointerType === "touch") return;
  const el = citeOf(e.target);
  if (!el || (e.relatedTarget instanceof Node && el.contains(e.relatedTarget))) return;
  clearTimeout(openTimer);
  if (popOpen.value) scheduleClose();
}
function onPopEnter(e: PointerEvent) {
  if (e.pointerType !== "touch") clearTimeout(closeTimer);
}
function onPopLeave(e: PointerEvent) {
  if (e.pointerType !== "touch" && popOpen.value) scheduleClose();
}

// A press on a citation: remembered so the focus it gives is not taken for keyboard focus,
// and, on touch, so the click that follows opens the popover instead of navigating.
let pressedCite: HTMLElement | null = null;
let pressedAt = 0;
let touchCite: HTMLElement | null = null;
let touchWasOpen = false;
function onPointerDown(e: PointerEvent) {
  const el = citeOf(e.target);
  pressedCite = el;
  pressedAt = Date.now();
  touchCite = el && e.pointerType === "touch" ? el : null;
  touchWasOpen = !!touchCite && popOpen.value && popAnchor.value === touchCite;
  if (el) clearTimeout(openTimer);
}
function onClick(e: MouseEvent) {
  const el = citeOf(e.target);
  const tapped = !!el && el === touchCite;
  touchCite = null;
  if (!tapped || touchWasOpen) return; // not a tap, or the second tap: follow the link
  e.preventDefault();
  openFor(el!);
}
// Escape hands focus back to the citation (useDismiss): that focus must not reopen it.
let closedAnchor: HTMLElement | null = null;
let closedAt = 0;
watch(
  popOpen,
  (open) => {
    if (open) return;
    closedAnchor = popAnchor.value;
    closedAt = Date.now();
  },
  { flush: "sync" },
);
function onFocusIn(e: FocusEvent) {
  const el = citeOf(e.target);
  if (!el || (el === pressedCite && Date.now() - pressedAt < 1000)) return;
  if (el === closedAnchor && Date.now() - closedAt < 300) return;
  openFor(el);
}
function onFocusOut(e: FocusEvent) {
  const el = citeOf(e.target);
  if (!el || popAnchor.value !== el || !popOpen.value) return;
  const next = e.relatedTarget;
  if (next instanceof Node && (popEl.value?.contains(next) || citeOf(next))) return;
  // Keyboard focus moved on; a hovering pointer keeps the popover until it leaves.
  if (Date.now() - pressedAt > 1000) closePop();
}

function onViewportChange() {
  if (popOpen.value) closePop();
}
watch(popOpen, (open) => {
  if (typeof window === "undefined") return;
  if (open) {
    window.addEventListener("scroll", onViewportChange, true);
    window.addEventListener("resize", onViewportChange);
  } else {
    window.removeEventListener("scroll", onViewportChange, true);
    window.removeEventListener("resize", onViewportChange);
  }
});
// A re-render replaces every citation element: a popover would point at a stale one.
watch(html, () => closePop());
onBeforeUnmount(() => {
  tableObserver?.disconnect();
  clearTimers();
  window.removeEventListener("scroll", onViewportChange, true);
  window.removeEventListener("resize", onViewportChange);
});
</script>

<template>
  <div
    @pointerover="onPointerOver"
    @pointerout="onPointerOut"
    @pointerdown="onPointerDown"
    @click="onClick"
    @focusin="onFocusIn"
    @focusout="onFocusOut"
    @scroll.capture.passive="onScrollCapture"
  >
    <article
      ref="articleEl"
      class="prose dark:prose-invert max-w-none prose-p:text-ink prose-li:text-ink prose-h1:font-serif prose-h2:font-serif prose-h3:font-serif prose-h4:font-serif prose-headings:text-ink prose-h1:text-[1.75rem] sm:prose-h1:text-[2.125rem] prose-h2:text-[1.3125rem] sm:prose-h2:text-2xl prose-p:text-pretty max-sm:prose-p:leading-[1.65] max-sm:prose-li:leading-[1.65] max-sm:hyphens-auto prose-a:text-accent prose-a:no-underline hover:prose-a:underline prose-strong:text-ink prose-li:marker:text-muted"
      v-html="html"
    />
    <Teleport to="body">
      <Transition name="pop">
        <div
          v-if="popOpen && popInfo"
          :id="popId"
          ref="popEl"
          class="cite-pop material-popover fixed z-50 w-max max-w-[min(20rem,calc(100vw-1rem))] rounded-xl border border-bd p-3 text-xs"
          :style="popStyle"
          @pointerenter="onPopEnter"
          @pointerleave="onPopLeave"
        >
          <div class="flex items-start gap-1.5 font-medium" :class="popInfo.supported ? 'text-success' : 'text-danger'">
            <span aria-hidden="true">{{ popInfo.supported ? "✓" : "⚠" }}</span>
            <span>{{ popInfo.supported ? $t("citation.supported") : $t("citation.weak") }}</span>
          </div>
          <p v-if="popInfo.quote" class="mt-1.5 line-clamp-6 border-l-2 border-bd pl-2 leading-relaxed text-ink">
            {{ popInfo.quote }}
          </p>
          <div v-if="popInfo.domain || popInfo.href" class="mt-2 flex items-center gap-3">
            <span v-if="popInfo.domain" class="min-w-0 truncate text-muted">{{ popInfo.domain }}</span>
            <a
              v-if="popInfo.href"
              :href="popInfo.href"
              target="_blank"
              rel="noopener noreferrer"
              class="press ml-auto shrink-0 font-medium text-accent hover:underline"
              @click="closePop"
            >
              {{ $t("citation.openSource") }} ↗
            </a>
          </div>
        </div>
      </Transition>
    </Teleport>
  </div>
</template>

<style scoped>
/* A report table scrolls inside its own box (see table_open). A sideways swipe that
   reaches the end stays in the table instead of turning into a back gesture. */
:deep(.md-table-scroll) {
  overflow-x: auto;
  overscroll-behavior-x: contain;
}

/* The shared pop transition always starts 4px higher. A citation popover placed above
   its citation must start 4px lower instead, so it rises out of the citation while it
   grows from its bottom edge (place() sets --pop-dy; §7, §8). Reduced motion keeps its
   global `transform: none !important`. */
.cite-pop.pop-enter-from,
.cite-pop.pop-leave-to {
  transform: translateY(var(--pop-dy, -4px)) scale(0.96);
}

/* Inline verification: confirm strong claims subtly, flag the problem ones loudly.
   The marks live in v-html, which never carries this component's data-v attribute, so
   every rule goes through :deep(). Colours are the theme's status tokens, readable on
   their own tint in light and dark. The underline sits on the claim's words
   (.md-claim-text), never on the claim span: it would spread to the [Sn] chips. */
:deep(.md-claim-weak .md-claim-text) {
  border-bottom: 1.5px dotted rgb(var(--c-warning) / 0.85);
}
:deep(.md-claim-contested .md-claim-text) {
  text-decoration: underline wavy rgb(var(--c-danger) / 0.9);
  text-underline-offset: 3px;
}
:deep(.md-claim-badge) {
  font-size: 0.62em;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
  letter-spacing: 0.01em;
  line-height: 1;
  vertical-align: super;
  margin-left: 2px;
  padding: 1px 4px;
  border-radius: 999px;
  white-space: nowrap;
  user-select: none;
  cursor: help;
}
:deep(.md-claim-badge-strong) {
  color: rgb(var(--c-success));
  background: rgb(var(--c-success) / 0.12);
}
:deep(.md-claim-badge-medium) {
  color: rgb(var(--c-muted));
  background: rgb(var(--c-muted) / 0.12);
}
:deep(.md-claim-badge-weak) {
  color: rgb(var(--c-warning));
  background: rgb(var(--c-warning) / 0.16);
}
:deep(.md-claim-badge-contested) {
  color: rgb(var(--c-danger));
  background: rgb(var(--c-danger) / 0.16);
}
</style>
