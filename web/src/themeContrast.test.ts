import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

// WCAG checks over the theme tokens in style.css. The palettes are hand-tuned
// numbers, and a contrast regression never shows up in a unit test of a component.

const css = readFileSync(new URL("./style.css", import.meta.url), "utf8").replace(/\r\n/g, "\n");
const tailwindConfig = readFileSync(new URL("../tailwind.config.js", import.meta.url), "utf8");

type RGB = [number, number, number];
type Palette = Record<string, RGB>;

// `--c-name: r g b;` declarations of the first rule whose selector is exactly `selector`.
function tokens(selector: string): Palette {
  const start = css.indexOf(`\n${selector} {`);
  if (start < 0) throw new Error(`no rule for ${selector}`);
  const body = css.slice(start, css.indexOf("\n}", start));
  const out: Palette = {};
  for (const m of body.matchAll(/--(c-[\w-]+):\s*(\d+)\s+(\d+)\s+(\d+);/g)) out[m[1]] = [+m[2], +m[3], +m[4]];
  return out;
}

const light = tokens(":root");
const darkBase = tokens("html.dark");
const THEMES: Record<string, Palette> = {
  light,
  sand: { ...light, ...tokens('html[data-theme="sand"]') },
  ...Object.fromEntries(
    ["dark", "midnight", "emerald", "rose"].map((t) => [t, { ...light, ...darkBase, ...tokens(`html[data-theme="${t}"]`) }]),
  ),
};

const channel = (c: number) => {
  const s = c / 255;
  return s <= 0.04045 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
};
const luminance = ([r, g, b]: RGB) => 0.2126 * channel(r) + 0.7152 * channel(g) + 0.0722 * channel(b);
function ratio(a: RGB, b: RGB): number {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
}
// `fg` at `alpha` composited over `bg`.
const over = (fg: RGB, bg: RGB, alpha: number): RGB => fg.map((v, i) => v * alpha + bg[i] * (1 - alpha)) as RGB;

const BASES = ["c-bg", "c-rail", "c-surface", "c-surface-hover"];

describe("theme contrast", () => {
  it("parses all six palettes", () => {
    for (const [name, p] of Object.entries(THEMES)) {
      for (const key of ["c-accent", "c-accent-soft", ...BASES]) expect(p[key], `${name} ${key}`).toHaveLength(3);
    }
  });

  it("text-accent resolves to the text-safe accent shade", () => {
    expect(tailwindConfig).toMatch(/textColor:\s*{\s*accent:\s*"rgb\(var\(--c-accent-soft\) \/ <alpha-value>\)"/);
  });

  it("accent text is at least 4.5:1 on its own tints in every theme", () => {
    const failures: string[] = [];
    for (const [name, p] of Object.entries(THEMES)) {
      const accent = p["c-accent"];
      for (const base of BASES) {
        // bg-accent/5 … /30 (hover states included), and a /20 badge inside a /15 row.
        const tints: [string, RGB][] = [5, 10, 15, 20, 25, 30].map((n) => [`/${n}`, over(accent, p[base], n / 100)]);
        tints.push(["/20 on /15", over(accent, over(accent, p[base], 0.15), 0.2)]);
        for (const [label, tint] of tints) {
          const r = ratio(p["c-accent-soft"], tint);
          if (r < 4.5) failures.push(`${name}: accent text on accent${label} over ${base} = ${r.toFixed(2)}`);
        }
      }
    }
    expect(failures).toEqual([]);
  });

  it("the accent focus ring is at least 3:1 on every surface", () => {
    const failures: string[] = [];
    for (const [name, p] of Object.entries(THEMES)) {
      for (const base of BASES) {
        const r = ratio(p["c-accent"], p[base]);
        if (r < 3) failures.push(`${name}: ring on ${base} = ${r.toFixed(2)}`);
      }
    }
    expect(failures).toEqual([]);
  });

  it("a focused field host border is at least 3:1 and differs from its hover border", () => {
    const rule = css.match(/\n\.field-host:has\(\.field-bare:focus\) {([^}]*)}/);
    const alpha = Number(rule?.[1].match(/border-color: rgb\(var\(--c-accent\) \/ ([\d.]+)\)/)?.[1]);
    expect(alpha).toBeGreaterThan(0);
    // A halo marks focus as well as the border, so a hovered row never looks focused.
    expect(rule?.[1]).toMatch(/box-shadow: 0 0 0 3px rgb\(var\(--c-accent\)/);
    const failures: string[] = [];
    for (const [name, p] of Object.entries(THEMES)) {
      for (const base of ["c-bg", "c-surface"]) {
        const r = ratio(over(p["c-accent"], p[base], alpha), p[base]);
        if (r < 3) failures.push(`${name}: focused host on ${base} = ${r.toFixed(2)}`);
      }
      // PlanCard rows hover to border-accent/40.
      const apart = ratio(over(p["c-accent"], p["c-surface"], alpha), over(p["c-accent"], p["c-surface"], 0.4));
      if (apart < 2) failures.push(`${name}: focus vs hover border = ${apart.toFixed(2)}`);
    }
    expect(failures).toEqual([]);
  });

  it("checkboxes, radios and sliders get the solid accent ring", () => {
    expect(css).toMatch(
      /input:is\(\[type="checkbox"\], \[type="radio"\], \[type="range"\]\):focus-visible {\s*outline: 2px solid rgb\(var\(--c-accent\)\);/,
    );
    // It has to come after the field rule, which makes the outline transparent.
    expect(css.indexOf('input:is([type="checkbox"]')).toBeGreaterThan(css.indexOf(":is(input, textarea, select):focus-visible"));
  });

  it("forced colours hide reserved borders, mark the selected tab and keep status dots", () => {
    const forced = [...css.matchAll(/@media \(forced-colors: active\) {([\s\S]*?)\n}/g)].map((m) => m[1]).join("\n");
    expect(forced).toMatch(/\.border-transparent,\s*\[role="tab"\]:not\(\[aria-selected="true"\]\) {\s*border-color: Canvas;/);
    expect(forced).toMatch(/\[role="tab"\]\[aria-selected="true"\] {\s*border-color: Highlight;/);
    expect(forced).toMatch(/\.rounded-full:empty:is\([^)]*\.live-dot[^)]*\) {\s*forced-color-adjust: none;/);
  });
});
