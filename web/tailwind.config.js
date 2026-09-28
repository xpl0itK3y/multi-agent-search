import typography from "@tailwindcss/typography";

/** @type {import('tailwindcss').Config} */
export default {
  darkMode: "class",
  content: ["./index.html", "./src/**/*.{vue,ts}"],
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
      fontFamily: {
        sans: ["Inter", "system-ui", "-apple-system", "sans-serif"],
        serif: ["Lora", "Georgia", "Times New Roman", "serif"],
      },
      borderRadius: { card: "16px" },
      maxWidth: { composer: "768px" },
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
