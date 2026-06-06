/** @type {import('tailwindcss').Config} */
export default {
  darkMode: "class",
  content: ["./index.html", "./src/**/*.{vue,ts}"],
  theme: {
    extend: {
      colors: {
        bg: "#262624",
        rail: "#1f1e1d",
        surface: "#30302e",
        surfaceHover: "#3a3a37",
        bd: "rgba(255,255,255,0.08)",
        ink: "#ECEAE3",
        muted: "#9A9890",
        accent: "#D97757",
        accentSoft: "#e0a288",
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "-apple-system", "sans-serif"],
        serif: ["Lora", "Georgia", "Times New Roman", "serif"],
      },
      borderRadius: { card: "16px" },
      maxWidth: { composer: "768px" },
    },
  },
  plugins: [],
};
