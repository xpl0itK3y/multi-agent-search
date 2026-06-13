<script setup lang="ts">
import { computed } from "vue";
import MarkdownIt from "markdown-it";
import type { CitationGround } from "@/lib/types";

const props = defineProps<{ source: string; grounding?: CitationGround[] }>();

// Escape text destined for an HTML attribute (quote shown on [Sn] hover).
function escAttr(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
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

const html = computed(() => {
  const source = props.source || "";
  const urls = sourceUrlMap(source);
  const ground = new Map((props.grounding || []).map((g) => [g.source_id, g]));
  const rendered = md.render(source);
  // Inline [Sn] -> clickable link to the source; hover shows the grounding quote and
  // a weak-citation flag when the source text doesn't actually back the claim.
  return rendered.replace(/\[S(\d+)\]/g, (_full, n: string) => {
    const g = ground.get(`S${n}`);
    const url = g?.url || urls.get(n);
    const cls = g && !g.supported ? "md-citation md-citation-weak" : "md-citation";
    const tip = g?.quote ? ` title="${escAttr((g.supported ? "✓ " : "⚠ ") + g.quote)}"` : "";
    return url
      ? `<a href="${url}" target="_blank" rel="noopener noreferrer" class="${cls}"${tip}>[S${n}]</a>`
      : `<sup class="${cls}"${tip}>[S${n}]</sup>`;
  });
});
</script>

<template>
  <article
    class="prose dark:prose-invert max-w-none prose-p:text-ink prose-li:text-ink prose-headings:font-serif prose-headings:text-ink prose-a:text-accent prose-a:no-underline hover:prose-a:underline prose-strong:text-ink prose-li:marker:text-muted"
    v-html="html"
  />
</template>
