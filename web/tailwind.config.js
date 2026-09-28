import typography from "@tailwindcss/typography";

/** @type {import('tailwindcss').Config} */
export default {
  darkMode: "class",
  content: ["./index.html", "./src/**/*.{vue,ts}"],
  // hover: variants apply only where a real hover exists, so a tap never leaves a
  // control stuck in its hover state on touch screens.
  future: { hoverOnlyWhenSupported: true },
  theme: {
    extend: {
      colors: {
        bg: "rgb(var(--c-bg) / <alpha-value>)",
        rail: "rgb(var(--c-rail) / <alpha-value>)",
        surface: "rgb(var(--c-surface) / <alpha-value>)",
        surfaceHover: "rgb(var(--c-surface-hover) / <alpha-value>)",
        bd: "rgb(var(--c-bd) / <alpha-value>)",
        ink: "rgb(var(--c-ink) / <alpha-value>)",
        muted: "rgb(var(--c-muted) / <alpha-value>)",
        accent: "rgb(var(--c-accent) / <alpha-value>)",
        accentSoft: "rgb(var(--c-accent-soft) / <alpha-value>)",
        onAccent: "rgb(var(--c-on-accent) / <alpha-value>)",
        // Theme-adaptive status colours (style.css): readable on their own tint in
        // light and dark. Tailwind's own palette is left as it is.
        success: "rgb(var(--c-success) / <alpha-value>)",
        warning: "rgb(var(--c-warning) / <alpha-value>)",
        danger: "rgb(var(--c-danger) / <alpha-value>)",
        info: "rgb(var(--c-info) / <alpha-value>)",
      },
      // Accent text uses the text-safe shade, so a label on an accent tint (badges,
      // selected rows) stays ≥4.5:1 in every theme. bg-/border-/ring-accent keep the
      // brand accent.
      textColor: {
        accent: "rgb(var(--c-accent-soft) / <alpha-value>)",
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "-apple-system", "sans-serif"],
        serif: ["Lora", "Georgia", "Times New Roman", "serif"],
      },
      // Tracking is size-specific (§15): small labels open up slightly, headings
      // tighten as they grow. An explicit tracking-* class still overrides these.
      fontSize: {
        "3xs": ["0.625rem", { lineHeight: "0.875rem", letterSpacing: "0.02em" }],
        "2xs": ["0.6875rem", { lineHeight: "1rem", letterSpacing: "0.01em" }],
        xl: ["1.25rem", { lineHeight: "1.75rem", letterSpacing: "-0.01em" }],
        "2xl": ["1.5rem", { lineHeight: "2rem", letterSpacing: "-0.015em" }],
        "3xl": ["1.875rem", { lineHeight: "2.25rem", letterSpacing: "-0.02em" }],
        "4xl": ["2.25rem", { lineHeight: "2.5rem", letterSpacing: "-0.022em" }],
      },
      borderRadius: { card: "16px" },
      maxWidth: { composer: "48rem" },
      // Report headings: real weights (Inter now loads up to 700), tight leading and
      // tracking as they grow, balanced lines; tables use tabular figures.
      typography: () => ({
        DEFAULT: {
          css: {
            h1: { fontWeight: "600", lineHeight: "1.15", letterSpacing: "-0.015em", textWrap: "balance" },
            h2: { fontWeight: "600", lineHeight: "1.25", letterSpacing: "-0.01em", textWrap: "balance" },
            h3: { fontWeight: "600", lineHeight: "1.35", letterSpacing: "-0.005em" },
            th: { fontWeight: "600" },
            table: { fontVariantNumeric: "tabular-nums" },
          },
        },
      }),
      // Elevation: soft shadows in light, a lit rim + outline in dark (style.css).
      boxShadow: { e1: "var(--elev-1)", e2: "var(--elev-2)", e3: "var(--elev-3)" },
      transitionTimingFunction: {
        emphasized: "cubic-bezier(0.22, 1, 0.36, 1)",
        sheet: "cubic-bezier(0.32, 0.72, 0, 1)",
      },
      transitionDuration: { 160: "160ms", 240: "240ms", 320: "320ms" },
    },
  },
  plugins: [typography],
};
