import { createRouter, createWebHistory } from "vue-router";
import { useAuthStore } from "@/stores/auth";

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: "/login", name: "login", component: () => import("@/views/LoginView.vue") },
    { path: "/set-password", name: "set-password", component: () => import("@/views/SetPasswordView.vue") },
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
    {
      path: "/r/:token",
      name: "public-report",
      component: () => import("@/views/PublicReportView.vue"),
      props: true,
      meta: { public: true },
    },
  ],
});

// Redirect to /login when unauthenticated (auth.user is set even in single-tenant mode).
// Public routes (a shared read-only report) are reachable without a session.
router.beforeEach((to) => {
  if (to.meta.public) return true;
  const auth = useAuthStore();
  if (to.name !== "login" && !auth.user) return { name: "login" };
  if (to.name === "login" && auth.user) return { name: "home" };
  return true;
});

export default router;
