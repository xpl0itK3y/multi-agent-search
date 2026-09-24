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

  it("uses the three Russian plural forms and singular/plural elsewhere", () => {
    g.locale.value = "ru";
    expect([1, 3, 5, 11, 21, 22].map((n) => g.t("plan.items", n))).toEqual([
      "1 пункт",
      "3 пункта",
      "5 пунктов",
      "11 пунктов",
      "21 пункт",
      "22 пункта",
    ]);
    g.locale.value = "en";
    expect([1, 2].map((n) => g.t("plan.items", n))).toEqual(["1 item", "2 items"]);
  });

  it("resolves every ru key to a non-empty string in every locale", () => {
    const keys = flatKeys(g.getLocaleMessage("ru") as Record<string, unknown>);
    expect(keys.length).toBeGreaterThan(0);
    for (const { value } of LOCALES) {
      g.locale.value = value;
      for (const key of keys) {
        const text = g.t(key);
        expect(`${value}:${key}`, `${value} ${text}`).not.toBe("");
        expect(typeof text).toBe("string");
      }
    }
  });
});
