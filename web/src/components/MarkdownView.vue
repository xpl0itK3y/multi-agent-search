<script setup lang="ts">
import { computed, nextTick, ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import MarkdownIt from "markdown-it";
import renderMathInElement from "katex/contrib/auto-render";
import "katex/dist/katex.min.css";
import type { CitationGround, SourceIndependence } from "@/lib/types";

const props = defineProps<{
  source: string;
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

// A source URL safe to place inside an href: http(s) only, attribute-escaped so a URL
// containing a quote can't break out of href="…" and inject an event handler (XSS).
function safeHref(u: string | undefined): string {
  if (!u || !/^https?:\/\//i.test(u)) return "";
  return escAttr(u);
}

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

// Map Sn -> url, parsed from the report's own "Sources" section so inline
// citations become clickable links to the actual source (correct numbering).
function sourceUrlMap(source: string): Map<string, string> {
  const map = new Map<string, string>();
  for (const line of source.split("\n")) {
    const idMatch = line.match(/\[S(\d+)\\?\]/);
    if (!idMatch) continue;
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

const html = computed(() => {
  // Normalize escaped citation brackets (\[Sn\] -> [Sn]) so they render as citations and don't
  // collide with KaTeX's \[…\] delimiter / show as literal backslashes.
  const source = (props.source || "").replace(/\\\[(S\d+(?:[,\s]+S\d+)*)\\\]/g, "[$1]");
  const urls = sourceUrlMap(source);
  const ground = new Map((props.grounding || []).map((g) => [g.source_id, g]));

  // 1. Wrap each cited sentence with control-char sentinels BEFORE markdown runs, so
  //    the wrapping survives rendering and never breaks tag nesting (sentences stay
  //    within a block). The sentinels carry an index into `claims`.
  let prepared = source;
  const claims: Claim[] = [];
  if (props.verify) {
    const grade = buildGrader();
    let inSources = false;
    prepared = source
      .split("\n")
      .map((line) => {
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
        const decorated = splitSentences(body)
          .map((part) => {
            if (!/\[S\d+\]/.test(part)) return part;
            const wm = part.match(/^(\s*)([\s\S]*?)(\s*)$/);
            const lead = wm?.[1] ?? "";
            const core = wm?.[2] ?? part;
            const trail = wm?.[3] ?? "";
            if (!core) return part;
            const idx = claims.push(grade(core)) - 1;
            return `${lead}${idx}${core}${idx}${trail}`;
          })
          .join("");
        return prefix + decorated;
      })
      .join("\n");
  }

  // markdown-it treats "\(" / "\)" as escaped punctuation and strips the backslash, which would
  // destroy KaTeX's inline-math delimiters before renderMathInElement runs. Shield them across
  // render with ASCII sentinels, then restore so renderMathInElement can find the math.
  prepared = prepared.replace(/\\\(/g, "@@KMO@@").replace(/\\\)/g, "@@KMC@@");
  let rendered = md.render(prepared).replace(/@@KMO@@/g, "\\(").replace(/@@KMC@@/g, "\\)");

  // 2. Turn the sentinels into a styled span + a trailing support badge.
  if (props.verify) {
    rendered = rendered
      .replace(/(\d+)/g, (_f, i: string) => {
        const c = claims[+i];
        return c ? `<span class="md-claim md-claim-${c.band}" title="${escAttr(c.title)}">` : "";
      })
      .replace(/(\d+)/g, (_f, i: string) => {
        const c = claims[+i];
        return c
          ? `<sup class="md-claim-badge md-claim-badge-${c.band}">${c.badge}</sup></span>`
          : "</span>";
      });
  }

  // 3. Inline [Sn] -> clickable link to the source; hover shows the grounding quote and
  //    a weak-citation flag when the source text doesn't actually back the claim.
  return rendered.replace(/\[S(\d+)\]/g, (_full, n: string) => {
    const g = ground.get(`S${n}`);
    const url = safeHref(g?.url || urls.get(n));
    const cls = g && !g.supported ? "md-citation md-citation-weak" : "md-citation";
    const tip = g?.quote ? ` title="${escAttr((g.supported ? "✓ " : "⚠ ") + g.quote)}"` : "";
    return url
      ? `<a href="${url}" target="_blank" rel="noopener noreferrer" class="${cls}"${tip}>[S${n}]</a>`
      : `<sup class="${cls}"${tip}>[S${n}]</sup>`;
  });
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
    });
  },
  { immediate: true },
);
</script>

<template>
  <article
    ref="articleEl"
    class="prose dark:prose-invert max-w-none prose-p:text-ink prose-li:text-ink prose-headings:font-serif prose-headings:text-ink prose-a:text-accent prose-a:no-underline hover:prose-a:underline prose-strong:text-ink prose-li:marker:text-muted"
    v-html="html"
  />
</template>

<style scoped>
/* Inline verification: confirm strong claims subtly, flag the problem ones loudly. */
.md-claim-weak {
  border-bottom: 1.5px dotted rgb(245 158 11 / 0.85);
}
.md-claim-contested {
  text-decoration: underline wavy rgb(248 113 113 / 0.9);
  text-underline-offset: 3px;
}
.md-claim-badge {
  font-size: 0.62em;
  font-weight: 600;
  line-height: 1;
  vertical-align: super;
  margin-left: 2px;
  padding: 1px 4px;
  border-radius: 999px;
  white-space: nowrap;
  user-select: none;
  cursor: help;
}
.md-claim-badge-strong {
  color: rgb(16 185 129);
  background: rgb(16 185 129 / 0.12);
}
.md-claim-badge-medium {
  color: rgb(var(--c-muted));
  background: rgb(var(--c-muted) / 0.12);
}
.md-claim-badge-weak {
  color: rgb(245 158 11);
  background: rgb(245 158 11 / 0.16);
}
.md-claim-badge-contested {
  color: rgb(248 113 113);
  background: rgb(248 113 113 / 0.16);
}
</style>
