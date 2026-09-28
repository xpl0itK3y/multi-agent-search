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

  // The raw messages, not t(): t() falls back to en for a missing string, which would hide it.
  function leaves(obj: Record<string, unknown>, prefix = ""): [string, unknown][] {
    return Object.entries(obj).flatMap(([k, v]) =>
      v && typeof v === "object"
        ? leaves(v as Record<string, unknown>, `${prefix}${k}.`)
        : [[`${prefix}${k}`, v] as [string, unknown]],
    );
  }

  const raw = (locale: string) => new Map(leaves(g.getLocaleMessage(locale) as Record<string, unknown>));

  it("has a non-empty string for every key in every locale", () => {
    for (const { value } of LOCALES) {
      const entries = [...raw(value)];
      expect(entries.length).toBeGreaterThan(300);
      const empty = entries.filter(([, text]) => typeof text !== "string" || !text.trim()).map(([key]) => key);
      expect({ locale: value, empty }).toEqual({ locale: value, empty: [] });
    }
  });

  it("gives every ru plural its three forms, and makes it a plural in en and es too", () => {
    // ruPluralRule picks among exactly three forms (one | few | many); en and es take two or three.
    const forms = (text: unknown) => String(text).split("|").length;
    const ru = raw("ru");
    const plurals = [...ru].filter(([, text]) => forms(text) > 1).map(([key]) => key);
    expect(plurals.length).toBeGreaterThan(0);
    const wrong = plurals.flatMap((key) => [
      ...(forms(ru.get(key)) === 3 ? [] : [`ru:${key}`]),
      ...(["en", "es"] as const).filter((loc) => forms(raw(loc).get(key)) < 2).map((loc) => `${loc}:${key}`),
    ]);
    expect(wrong).toEqual([]);
  });
});
