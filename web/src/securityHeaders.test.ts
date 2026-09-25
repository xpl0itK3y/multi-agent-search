import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";

import indexHtml from "../index.html?raw";
import nginxConf from "../nginx.conf?raw";

// Guards for the security headers that web/nginx.conf sends with the SPA. The policy
// has to follow index.html (its inline script is allowed by hash) and nginx's
// add_header inheritance rule, and neither drift shows up anywhere but in a browser.

const SECURITY_HEADERS = ["Content-Security-Policy", "X-Frame-Options", "X-Content-Type-Options", "Referrer-Policy"];

// nginx.conf without comments, split into lines.
const confLines = nginxConf
  .replace(/\r\n/g, "\n")
  .split("\n")
  .map((line) => line.replace(/#.*$/, "").trim());

// The SPA's CSP, assembled from the `set $spa_csp` lines in order.
function spaCsp(): Map<string, string[]> {
  let value = "";
  for (const line of confLines) {
    const m = line.match(/^set \$spa_csp "([^"]*)";$/);
    if (m) value = m[1].replace("${spa_csp}", value);
  }
  const directives = new Map<string, string[]>();
  for (const part of value.split(";")) {
    const [name, ...sources] = part.trim().split(/\s+/);
    if (name) directives.set(name, sources);
  }
  return directives;
}

// The CSP source expressions that allow each of index.html's inline scripts, for the
// file as checked out with LF and with CRLF line endings.
function inlineScriptHashes(): string[] {
  const scripts = [...indexHtml.matchAll(/<script\b(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/g)].map((m) =>
    m[1].replace(/\r\n/g, "\n"),
  );
  expect(scripts.length).toBeGreaterThan(0);
  const hash = (text: string) => `'sha256-${createHash("sha256").update(text, "utf8").digest("base64")}'`;
  return scripts.flatMap((text) => [hash(text), hash(text.replace(/\n/g, "\r\n"))]);
}

interface Block {
  header: string;
  headers: Map<string, string>;
}

// The server block and every location block, each with the add_header lines of its own
// level (nginx inherits add_header into a block only when the block has none).
function headerBlocks(): Block[] {
  const blocks: Block[] = [];
  const stack: Block[] = [];
  for (const line of confLines) {
    if (line.endsWith("{")) {
      const block = { header: line.slice(0, -1).trim(), headers: new Map<string, string>() };
      stack.push(block);
      if (/^(server|location)\b/.test(block.header)) blocks.push(block);
    } else if (line === "}") {
      stack.pop();
    } else {
      const m = line.match(/^add_header\s+(\S+)\s+(.+);$/);
      if (m && stack.length) stack[stack.length - 1].headers.set(m[1], m[2]);
    }
  }
  return blocks;
}

// The regex keys of the `map $request_uri $spa_referrer_policy` block, as JS regexes.
function referrerPolicyFor(uri: string): string {
  const start = confLines.indexOf("map $request_uri $spa_referrer_policy {");
  expect(start).toBeGreaterThanOrEqual(0);
  let policy = "";
  for (const line of confLines.slice(start + 1)) {
    if (line === "}") break;
    const m = line.match(/^(\S+|"[^"]*")\s+"([^"]*)";$/);
    if (!m) continue;
    const key = m[1].replace(/^"|"$/g, "");
    if (key === "default") policy ||= m[2];
    else if (key.startsWith("~") && new RegExp(key.replace(/^~\*?/, ""), key.startsWith("~*") ? "i" : "").test(uri))
      return m[2];
  }
  return policy;
}

describe("SPA security headers (web/nginx.conf)", () => {
  it("allows exactly index.html's inline scripts, by hash, for LF and CRLF checkouts", () => {
    const scriptSrc = spaCsp().get("script-src") ?? [];
    const listed = scriptSrc.filter((source) => source.startsWith("'sha256-"));
    expect(new Set(listed)).toEqual(new Set(inlineScriptHashes()));
  });

  it("never allows inline, eval'd or foreign script", () => {
    const csp = spaCsp();
    const scriptSrc = csp.get("script-src") ?? [];
    expect(scriptSrc[0]).toBe("'self'");
    expect(scriptSrc.slice(1).every((source) => source.startsWith("'sha256-"))).toBe(true);
    expect(csp.get("default-src")).toEqual(["'self'"]);
    expect(csp.get("object-src")).toEqual(["'none'"]);
    expect(csp.get("base-uri")).toEqual(["'none'"]);
    expect(csp.get("frame-ancestors")).toEqual(["'none'"]);
  });

  it("allows inline styles as attributes only (KaTeX, Vue static markup), not as <style> elements", () => {
    const csp = spaCsp();
    expect(csp.get("style-src-attr")).toEqual(["'unsafe-inline'"]);
    expect(csp.get("style-src-elem")).not.toContain("'unsafe-inline'");
  });

  it("repeats every security header, with always, in each block that sets its own add_header", () => {
    const blocks = headerBlocks();
    expect(blocks.map((b) => b.header)).toEqual(
      expect.arrayContaining(["server", "location /v1/", "location /health", "location = /index.html"]),
    );
    const missing = blocks
      .filter((b) => b.header === "server" || b.headers.size > 0)
      .flatMap((b) =>
        SECURITY_HEADERS.filter((name) => !/\balways$/.test(b.headers.get(name) ?? "")).map((name) => `${b.header}: ${name}`),
      );
    expect(missing).toEqual([]);
  });

  it("serves index.html with the same headers as the rest of the SPA", () => {
    const blocks = headerBlocks();
    const server = blocks.find((b) => b.header === "server")!;
    const index = blocks.find((b) => b.header === "location = /index.html")!;
    for (const name of SECURITY_HEADERS) expect(index.headers.get(name)).toBe(server.headers.get(name));
    expect(server.headers.get("Content-Security-Policy")).toBe("$spa_csp always");
  });

  it("sends no Referer at all from the public share page", () => {
    expect(referrerPolicyFor("/r/abc123")).toBe("no-referrer");
    expect(referrerPolicyFor("/R/abc123?x=1")).toBe("no-referrer");
    expect(referrerPolicyFor("//r/abc123")).toBe("no-referrer");
    expect(referrerPolicyFor("/")).toBe("strict-origin-when-cross-origin");
    expect(referrerPolicyFor("/research/r/1")).toBe("strict-origin-when-cross-origin");
    expect(referrerPolicyFor("/rx/1")).toBe("strict-origin-when-cross-origin");
  });
});
