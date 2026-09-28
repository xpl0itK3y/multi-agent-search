// First: an emailed link's token leaves the address before anything else runs.
import "./linkCapture";
import { createApp } from "vue";
import { createPinia } from "pinia";
import App from "./App.vue";
import router from "./router";
import { i18n } from "./i18n";
import { useAuthStore } from "./stores/auth";
import "./style.css";

// iOS Safari applies :active (the instant press feedback in style.css) only when the
// document has a touchstart listener. Passive, so it never delays scrolling.
document.addEventListener("touchstart", () => {}, { passive: true });

const app = createApp(App);
const pinia = createPinia();
app.use(pinia).use(i18n);

// Resolve the session before mounting so the router guard sees auth state (and so
// telemetry starts only for a signed-in user — see stores/auth.ts).
useAuthStore(pinia)
  .fetchMe()
  .finally(() => {
    app.use(router).mount("#app");
  });
