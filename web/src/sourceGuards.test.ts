import { describe, expect, it } from "vitest";

import { i18n } from "@/i18n";

// Static checks over the app sources for regressions that type-checking
// cannot catch.
const sources = import.meta.glob<string>(["./**/*.vue", "./stores/*.ts", "./lib/*.ts", "!./**/*.test.ts"], {
  query: "?raw",
  import: "default",
  eager: true,
});
const components = Object.fromEntries(Object.entries(sources).filter(([path]) => path.endsWith(".vue")));

function offendingLines(files: Record<string, string>, pattern: RegExp): string[] {
  return Object.entries(files).flatMap(([path, source]) =>
    source
      .split("\n")
      .map((line, i) => ({ line, n: i + 1 }))
      .filter(({ line }) => pattern.test(line))
      .map(({ line, n }) => `${path}:${n}: ${line.trim()}`),
  );
}

// The component's <template> block without HTML comments (script strings such as
// data matching on server text are allowed to contain Cyrillic).
function templateOf(source: string): string {
  const start = source.indexOf("<template>");
  const end = source.lastIndexOf("</template>");
  if (start < 0 || end < 0) return "";
  return source.slice(start, end).replace(/<!--[\s\S]*?-->/g, (c) => c.replace(/[^\n]/g, ""));
}

describe("source guards", () => {
  it("scans the component sources", () => {
    expect(Object.keys(components).length).toBeGreaterThan(20);
  });

  it("shows errors through apiErrorMessage, never raw messages or alert()", () => {
    // ApiError.message is "<status> <server detail>" — views must localize it.
    const files = Object.fromEntries(Object.entries(sources).filter(([path]) => !path.endsWith("lib/api.ts")));
    expect(offendingLines(files, /\b(?:e|err|error)\.message\b|as Error\)\.message|\balert\(/)).toEqual([]);
  });

  it("has no hard-coded Cyrillic text in component templates (use i18n keys)", () => {
    const templates = Object.fromEntries(Object.entries(components).map(([path, src]) => [path, templateOf(src)]));
    expect(offendingLines(templates, /[А-Яа-яЁё]/)).toEqual([]);
  });

  it("never pairs the 100vh and 100dvh heights on one element without a variant", () => {
    // Both are single-class utilities and .h-screen comes later in the built CSS, so the pair
    // side by side always computed to 100vh: the tallest viewport on phones, with the end of
    // the page under the browser toolbar. 100dvh goes through supports-[height:100dvh]: instead.
    const classAttr = /(?<![\w-]):?class="([^"]*)"/g; // one attribute, even over several lines
    const bare = (value: string, utility: string) => new RegExp(`(?<![:\\w-])${utility}(?![\\w-])`).test(value);
    const pairs = Object.entries(components).flatMap(([path, source]) =>
      [...source.replace(/<!--[\s\S]*?-->/g, "").matchAll(classAttr)]
        .map(([, value]) => value)
        .filter((value) => bare(value, "h-screen") && bare(value, "h-dvh"))
        .map((value) => `${path}: ${value.replace(/\s+/g, " ").trim()}`),
    );
    expect(pairs).toEqual([]);
  });

  it("uses smooth scrolling only through lib/motion.ts (reduced motion)", () => {
    // smoothOrAuto() turns a smooth scroll into a jump for readers who asked for reduced
    // motion; a literal behavior: "smooth" elsewhere would ignore that setting.
    const files = Object.fromEntries(Object.entries(sources).filter(([path]) => path !== "./lib/motion.ts"));
    expect(offendingLines(files, /behavior:\s*['"]smooth['"]|scroll-behavior:\s*smooth/)).toEqual([]);
  });

  it("references only i18n keys that exist", () => {
    // Literal keys only; dynamic ones (t(`status.${s}`)) are guarded by te() at the call site.
    const keyCall = /(?:\$t|\bt|\bte)\(\s*(["'])([A-Za-z0-9_.]+)\1\s*[,)]/g;
    const refs = Object.entries(sources).flatMap(([path, source]) =>
      [...source.matchAll(keyCall)].map((m) => ({ path, key: m[2] })).filter(({ key }) => key.includes(".")),
    );
    expect(refs.length).toBeGreaterThan(300);
    expect(refs.filter(({ key }) => !i18n.global.te(key, "ru")).map(({ path, key }) => `${path}: ${key}`)).toEqual([]);
  });
});
