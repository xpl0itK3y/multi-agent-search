import { createRouter, createWebHistory } from "vue-router";
import { useAuthStore } from "@/stores/auth";
import { googleReturnRedirect } from "@/lib/googleSignIn";

const router = createRouter({
  history: createWebHistory(),
  routes: [
    // meta.bare: a sign-in screen, drawn without the app shell (App.vue).
    { path: "/login", name: "login", component: () => import("@/views/LoginView.vue"), meta: { bare: true } },
    {
      path: "/forgot-password",
      name: "forgot-password",
      component: () => import("@/views/ForgotPasswordView.vue"),
      meta: { public: true, bare: true },
    },
    // meta.linkToken: opened from an emailed link, whose one-time token lib/linkToken.ts
    // takes out of the address before the router ever sees it (src/linkCapture.ts).
    {
      path: "/reset-password",
      name: "reset-password",
      component: () => import("@/views/ResetPasswordView.vue"),
      meta: { public: true, bare: true, linkToken: true },
    },
    // Not public: the server confirms an address only for the signed-in account the link
    // was sent to, so a signed-out visitor signs in first (/login?redirect=/verify-email)
    // while the link waits in this tab (lib/linkToken.ts).
    {
      path: "/verify-email",
      name: "verify-email",
      component: () => import("@/views/VerifyEmailView.vue"),
      meta: { bare: true, linkToken: true },
    },
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
    {
      path: "/admin",
      name: "admin",
      component: () => import("@/views/AdminView.vue"),
      meta: { requiresAdmin: true },
    },
    {
      path: "/settings",
      name: "settings",
      component: () => import("@/views/SettingsView.vue"),
    },
  ],
});

// The first navigation of a page load may be the landing of a Google sign-in that a
// page started to come back to (see lib/googleSignIn.ts).
let pageLoadNavigation = true;

// Redirect to /login when unauthenticated (auth.user is set even in single-tenant mode).
// Public routes (a shared read-only report) are reachable without a session.
router.beforeEach((to) => {
  const auth = useAuthStore();
  if (pageLoadNavigation) {
    pageLoadNavigation = false;
    const back = googleReturnRedirect(to, auth.user !== null);
    if (back) return back;
  }
  if (to.meta.public) return true;
  if (to.name !== "login" && !auth.user) {
    return { name: "login", query: { redirect: to.fullPath } };
  }
  if (to.name === "login" && auth.user) {
    return (to.query.redirect as string) || { name: "home" };
  }
  return true;
});

export default router;
