import { createRouter, createWebHistory } from "vue-router";
import { useAuthStore } from "@/stores/auth";

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: "/login", name: "login", component: () => import("@/views/LoginView.vue") },
    { path: "/", name: "home", component: () => import("@/views/HomeView.vue") },
    {
      path: "/research/:id",
      name: "research",
      component: () => import("@/views/ResearchView.vue"),
      props: true,
    },
    {
      path: "/thread/:threadId",
      name: "thread",
      component: () => import("@/views/ThreadView.vue"),
      props: true,
    },
  ],
});

// Redirect to /login when unauthenticated (auth.user is set even in single-tenant mode).
router.beforeEach((to) => {
  const auth = useAuthStore();
  if (to.name !== "login" && !auth.user) return { name: "login" };
  if (to.name === "login" && auth.user) return { name: "home" };
  return true;
});

export default router;
