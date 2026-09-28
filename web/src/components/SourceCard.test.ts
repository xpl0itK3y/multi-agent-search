// @vitest-environment jsdom
import { describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";

import { i18n } from "@/i18n";
import type { SourcePreview } from "@/lib/types";
import SourceCard from "./SourceCard.vue";

function card(source: SourcePreview) {
  return mount(SourceCard, { props: { source, index: 1 }, global: { plugins: [i18n] } });
}

function href(url: string): string | undefined {
  return card({ url, source_id: "S1" }).find("a").attributes("href");
}

describe("SourceCard link", () => {
  it("links an http(s) source", () => {
    expect(href("https://example.com/article")).toBe("https://example.com/article");
  });

  it("never turns a script or data URL into a link", () => {
    expect(href("javascript:alert(document.domain)")).toBeUndefined();
    expect(href("data:text/html,<script>alert(1)</script>")).toBeUndefined();
  });
});

describe("SourceCard quality", () => {
  it.each([
    ["high", "dashboard.qHigh", "text-success"],
    ["medium", "dashboard.qMedium", "text-warning"],
    [null, "dashboard.qLow", "text-muted"],
  ] as const)("labels %s quality in the UI language, in a status token", (q, key, cls) => {
    const chip = card({ url: "https://example.com", source_id: "S1", source_quality: q }).find(".rounded-full");

    expect(chip.text()).toBe(i18n.global.t(key));
    expect(chip.classes()).toContain(cls);
  });
});
