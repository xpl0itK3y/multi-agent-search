import { describe, expect, it } from "vitest";

import { i18n, LOCALES } from "./index";

const g = i18n.global;

function flatKeys(obj: Record<string, unknown>, prefix = ""): string[] {
  return Object.entries(obj).flatMap(([k, v]) =>
    v && typeof v === "object"
      ? flatKeys(v as Record<string, unknown>, `${prefix}${k}.`)
      : [`${prefix}${k}`],
  );
}

describe("i18n", () => {
  it("exposes the three configured locales", () => {
    expect(LOCALES.map((l) => l.value)).toEqual(["ru", "en", "es"]);
  });

  it("resolves a known key in every locale", () => {
    for (const { value } of LOCALES) {
      g.locale.value = value;
      const text = g.t("common.cancel");
      expect(typeof text).toBe("string");
      expect(text.length).toBeGreaterThan(0);
    }
  });

  it("has identical translation keys across ru/en/es (no drift)", () => {
    const base = new Set(flatKeys(g.getLocaleMessage("ru") as Record<string, unknown>));
    for (const loc of ["en", "es"] as const) {
      const keys = new Set(flatKeys(g.getLocaleMessage(loc) as Record<string, unknown>));
      const missing = [...base].filter((k) => !keys.has(k));
      const extra = [...keys].filter((k) => !base.has(k));
      expect({ loc, missing, extra }).toEqual({ loc, missing: [], extra: [] });
    }
  });
});
