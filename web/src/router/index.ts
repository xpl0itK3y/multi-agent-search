import { watch } from "vue";
import { createRouter, createWebHistory, type RouteLocationNormalized } from "vue-router";
import { i18n } from "@/i18n";
import { useAuthStore } from "@/stores/auth";
import { googleReturnRedirect } from "@/lib/googleSignIn";

declare module "vue-router" {
  interface RouteMeta {
    /** A sign-in screen, drawn without the app shell (App.vue). */
    bare?: boolean;
    /** Reachable without a session. */
    public?: boolean;
    /** Opened from an emailed link whose token lib/linkToken.ts holds. */
    linkToken?: boolean;
    requiresAdmin?: boolean;
    /** i18n key of the page's name, for the browser tab. */
    titleKey?: string;
  }
}

const router = createRouter({
  history: createWebHistory(),
  routes: [
    // meta.bare: a sign-in screen, drawn without the app shell (App.vue).
    {
      path: "/login",
      name: "login",
      component: () => import("@/views/LoginView.vue"),
      meta: { bare: true, titleKey: "auth.loginTitle" },
    },
    {
      path: "/forgot-password",
      name: "forgot-password",
      component: () => import("@/views/ForgotPasswordView.vue"),
      meta: { public: true, bare: true, titleKey: "forgotPassword.title" },
    },
    // meta.linkToken: opened from an emailed link, whose one-time token lib/linkToken.ts
    // takes out of the address before the router ever sees it (src/linkCapture.ts).
    {
      path: "/reset-password",
      name: "reset-password",
      component: () => import("@/views/ResetPasswordView.vue"),
      meta: { public: true, bare: true, linkToken: true, titleKey: "resetPassword.title" },
    },
    // Not public: the server confirms an address only for the signed-in account the link
    // was sent to, so a signed-out visitor signs in first (/login?redirect=/verify-email)
    // while the link waits in this tab (lib/linkToken.ts).
    {
      path: "/verify-email",
      name: "verify-email",
      component: () => import("@/views/VerifyEmailView.vue"),
      meta: { bare: true, linkToken: true, titleKey: "verifyEmail.title" },
    },
    {
      path: "/set-password",
      name: "set-password",
      component: () => import("@/views/SetPasswordView.vue"),
      meta: { titleKey: "setPassword.title" },
    },
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
      meta: { requiresAdmin: true, titleKey: "admin.title" },
    },
    {
      path: "/settings",
      name: "settings",
      component: () => import("@/views/SettingsView.vue"),
      meta: { titleKey: "settings.title" },
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

// The browser tab says where you are (apple-design §16 wayfinding): the page's name, or the
// product's for pages that name themselves once loaded (a thread sets its question).
export const DEFAULT_TITLE = "Veris — verifiable research";
function titleFor(to: RouteLocationNormalized): string {
  const key = to.meta.titleKey;
  return key ? `${i18n.global.t(key)} — Veris` : DEFAULT_TITLE;
}
router.afterEach((to, _from, failure) => {
  if (failure || typeof document === "undefined") return;
  document.title = titleFor(to);
});
// A language switch renames the tab too (pages without a titleKey keep their own title).
watch(
  () => i18n.global.locale.value,
  () => {
    const to = router.currentRoute.value;
    if (to.meta.titleKey && typeof document !== "undefined") document.title = titleFor(to);
  },
);

export default router;
