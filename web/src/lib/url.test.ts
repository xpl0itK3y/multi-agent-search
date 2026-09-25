import { describe, expect, it } from "vitest";

import { safeHttpUrl } from "./url";

describe("safeHttpUrl", () => {
  it("keeps ordinary http(s) URLs", () => {
    expect(safeHttpUrl("https://example.com/a?b=1#c")).toBe("https://example.com/a?b=1#c");
    expect(safeHttpUrl("  http://example.com  ")).toBe("http://example.com/");
    expect(safeHttpUrl("https://en.wikipedia.org/wiki/Foo_(bar)")).toBe("https://en.wikipedia.org/wiki/Foo_(bar)");
  });

  it.each([
    "javascript:alert(1)",
    " JavaScript:alert(1)",
    "data:text/html,<script>alert(1)</script>",
    "vbscript:msgbox",
    "//evil.example/x",
    "/relative/path",
    "https://",
    "",
    null,
    undefined,
  ])("rejects %j", (value) => {
    expect(safeHttpUrl(value)).toBeNull();
  });

  it("percent-encodes characters that could leave an attribute or start script", () => {
    expect(safeHttpUrl('https://x.example/"onmouseover="x')).toBe("https://x.example/%22onmouseover=%22x");
    expect(safeHttpUrl("https://x.example/p/onerror=alert`1`//")).toBe("https://x.example/p/onerror=alert%601%60//");
    expect(safeHttpUrl("https://x.example/p/it's <b>")).toBe("https://x.example/p/it%27s%20%3Cb%3E");
  });
});
