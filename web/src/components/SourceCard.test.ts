// @vitest-environment jsdom
import { describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";

import SourceCard from "./SourceCard.vue";

function href(url: string): string | undefined {
  return mount(SourceCard, { props: { source: { url, source_id: "S1" }, index: 1 } }).find("a").attributes("href");
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
