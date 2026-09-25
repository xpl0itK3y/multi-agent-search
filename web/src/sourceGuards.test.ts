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
