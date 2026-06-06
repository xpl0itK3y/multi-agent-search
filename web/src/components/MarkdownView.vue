<script setup lang="ts">
import { computed } from "vue";
import MarkdownIt from "markdown-it";

const props = defineProps<{ source: string }>();

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
  const rendered = md.render(source);
  // Inline [Sn] -> clickable link to the source when known, else a styled badge.
  return rendered.replace(/\[S(\d+)\]/g, (_full, n: string) => {
    const url = urls.get(n);
    return url
      ? `<a href="${url}" target="_blank" rel="noopener noreferrer" class="md-citation">[S${n}]</a>`
      : `<sup class="md-citation">[S${n}]</sup>`;
  });
});
</script>

<template>
  <article
    class="prose prose-invert max-w-none prose-headings:font-serif prose-headings:text-ink prose-a:text-accent prose-a:no-underline hover:prose-a:underline prose-strong:text-ink prose-li:marker:text-muted"
    v-html="html"
  />
</template>
