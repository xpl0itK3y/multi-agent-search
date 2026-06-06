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
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "-apple-system", "sans-serif"],
        serif: ["Lora", "Georgia", "Times New Roman", "serif"],
      },
      borderRadius: { card: "16px" },
      maxWidth: { composer: "768px" },
    },
  },
  plugins: [typography],
};
