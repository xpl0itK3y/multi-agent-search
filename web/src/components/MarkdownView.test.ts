// @vitest-environment jsdom
import { describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";

import { i18n } from "@/i18n";
import MarkdownView from "./MarkdownView.vue";
import type { SourcePreview } from "@/lib/types";

function render(source: string, sources?: SourcePreview[]) {
  return mount(MarkdownView, { props: { source, sources }, global: { plugins: [i18n] } });
}

function citationHrefs(wrapper: ReturnType<typeof render>): Record<string, string | null> {
  const out: Record<string, string | null> = {};
  for (const el of wrapper.findAll(".md-citation")) out[el.text()] = el.attributes("href") ?? null;
  return out;
}

describe("MarkdownView citations", () => {
  it("links [Sn] to the explicit source map and escapes attribute-breaking URLs", () => {
    const wrapper = render("Claim one [S1]. Claim two [S2].", [
      { source_id: "S1", url: "https://one.example/a" },
      { source_id: "S2", url: 'https://two.example/"onmouseover="x' },
    ]);

    const links = wrapper.findAll("a.md-citation");
    expect(links).toHaveLength(2);
    expect(links[0].attributes()).toMatchObject({
      href: "https://one.example/a",
      target: "_blank",
      rel: "noopener noreferrer",
    });
    // The quote stays inside href — no injected event-handler attribute.
    expect(links[1].attributes("href")).toBe('https://two.example/"onmouseover="x');
    expect(links[1].attributes("onmouseover")).toBeUndefined();
  });

  it("renders a citation without a known URL as plain text, never as a link", () => {
    const wrapper = render("Unbacked claim [S7].", [{ source_id: "S7", url: "javascript:alert(1)" }]);

    expect(wrapper.find("a.md-citation").exists()).toBe(false);
    expect(wrapper.find("sup.md-citation").text()).toBe("[S7]");
  });

  it("falls back to the report's Sources section for older reports without a map", () => {
    const wrapper = render("Claim [S3].\n\n## Sources\n\n- [S3] Example — https://legacy.example/page");

    expect(citationHrefs(wrapper)).toEqual({ "[S3]": "https://legacy.example/page" });
  });
});
