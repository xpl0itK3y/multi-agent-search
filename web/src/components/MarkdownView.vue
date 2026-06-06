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

const html = computed(() => {
  const rendered = md.render(props.source || "");
  // Style inline source citations like [S1] as small accent badges.
  // Safe: hrefs are URLs and never contain the [Sn] pattern.
  return rendered.replace(/\[S(\d+)\]/g, '<sup class="md-citation">[S$1]</sup>');
});
</script>

<template>
  <article
    class="prose prose-invert max-w-none prose-headings:font-serif prose-headings:text-ink prose-a:text-accent prose-a:no-underline hover:prose-a:underline prose-strong:text-ink prose-li:marker:text-muted"
    v-html="html"
  />
</template>
