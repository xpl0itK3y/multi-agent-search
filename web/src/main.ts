import { createApp } from "vue";
import { createPinia } from "pinia";
import App from "./App.vue";
import router from "./router";
import { i18n } from "./i18n";
import { useAuthStore } from "./stores/auth";
import "./style.css";

const app = createApp(App);
const pinia = createPinia();
app.use(pinia).use(i18n);

// Resolve the session before mounting so the router guard sees auth state.
useAuthStore(pinia)
  .fetchMe()
  .finally(() => {
    app.use(router).mount("#app");
  });
