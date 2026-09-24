import { describe, expect, it } from "vitest";

// Static checks over the component sources for regressions that type-checking
// cannot catch.
const components = import.meta.glob<string>(["./**/*.vue", "./stores/*.ts"], {
  query: "?raw",
  import: "default",
  eager: true,
});

function offendingLines(pattern: RegExp): string[] {
  return Object.entries(components).flatMap(([path, source]) =>
    source
      .split("\n")
      .map((line, i) => ({ line, n: i + 1 }))
      .filter(({ line }) => pattern.test(line))
      .map(({ line, n }) => `${path}:${n}: ${line.trim()}`),
  );
}

describe("source guards", () => {
  it("scans the component sources", () => {
    expect(Object.keys(components).length).toBeGreaterThan(20);
  });

  it("shows errors through apiErrorMessage, never raw messages or alert()", () => {
    // ApiError.message is "<status> <server detail>" — views must localize it.
    expect(offendingLines(/\b(?:e|err|error)\.message\b|as Error\)\.message|\balert\(/)).toEqual([]);
  });
});
