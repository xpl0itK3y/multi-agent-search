import type { RouteLocationNormalized } from "vue-router";
import { api, setAuthToken } from "./api";

// Some actions need a Google sign-in from the last few minutes (the server's
// `reauth_required`, e.g. setting a first password). The page sends the user through
// Google and wants them back afterwards, but the OAuth callback always lands on its
// configured page ("/" for a returning user). So the page to come back to waits in this
// tab's sessionStorage until the router's first navigation of the next page load.
const RETURN_KEY = "auth.google_return_to";
// Enough for Google's own sign-in (password, 2-step), and an abandoned attempt does not
// steer a much later visit.
const RETURN_TTL_MS = 10 * 60 * 1000;
const RETURN_BASE = "http://return.invalid";

/** The query parameter that brings a failed re-auth's reason back to the page that asked. */
export const REAUTH_ERROR_PARAM = "reauth_error";
// The callback's failure codes (/login?error=<code>), as seen by a signed-in user who went
// through Google again only to confirm an action: the i18n key of the explanation.
const REAUTH_ERRORS = new Map([
  ["oauth_conflict", "auth.reauthConflict"],
  ["oauth_failed", "auth.reauthFailed"],
]);

/** The i18n key for `?reauth_error=<code>`, or null for no or an unknown code. */
export function googleReauthErrorKey(code: unknown): string | null {
  return (typeof code === "string" && REAUTH_ERRORS.get(code)) || null;
}

/**
 * `value` as a path on this site ("/settings?tab=security"), else null. Rejects
 * absolute URLs, the protocol-relative forms browsers read as another host
 * ("//evil.example", "/\evil.example", and "/..//evil.example", whose dot segment
 * resolves away), and backslashes or control characters anywhere (the URL parser drops
 * tabs and newlines, so "/\t/evil.example" is "//evil.example"). Returns the resolved
 * form ("/a/../settings" is "/settings"), which stays the same path when resolved again.
 */
export function safeReturnPath(value: unknown): string | null {
  if (typeof value !== "string" || value.length > 512) return null;
  if (!value.startsWith("/") || value.startsWith("//")) return null;
  if (/[\\\u0000-\u001f\u007f]/.test(value)) return null;
  let url: URL;
  try {
    url = new URL(value, RETURN_BASE);
  } catch {
    return null;
  }
  if (url.origin !== RETURN_BASE || url.pathname.startsWith("//")) return null;
  const path = url.pathname + url.search + url.hash;
  return path.length > 512 ? null : path;
}

function forgetReturnPath(): void {
  try {
    sessionStorage.removeItem(RETURN_KEY);
  } catch {
    /* storage blocked */
  }
}

/** Start Google's sign-in; with a safe `returnTo`, come back to that page afterwards. */
export function startGoogleSignIn(returnTo?: string): void {
  const path = safeReturnPath(returnTo);
  try {
    if (path) sessionStorage.setItem(RETURN_KEY, JSON.stringify({ path, at: Date.now() }));
    else sessionStorage.removeItem(RETURN_KEY);
  } catch {
    /* storage blocked: the sign-in still works, it only lands on the default page */
  }
  // Leaving Google with Back restores this page from the bfcache, with no new page
  // load: the attempt was abandoned, and its return path must not steer a later one.
  window.addEventListener(
    "pageshow",
    (e) => {
      if ((e as PageTransitionEvent).persisted) forgetReturnPath();
    },
    { once: true },
  );
  // The callback's new session is the cookie. A bearer token kept from a password
  // sign-in would win over it (the server reads the Authorization header first), and
  // the server would keep seeing the old session, which is not a fresh Google sign-in.
  // Without the bearer, the old cookie still carries the session if the user comes back.
  setAuthToken(null);
  window.location.assign(api.googleLoginUrl());
}

type Landing = Pick<RouteLocationNormalized, "name" | "fullPath" | "meta" | "query">;

/**
 * Where the first navigation after a page load should go instead of `to`, if a Google
 * sign-in started by startGoogleSignIn() asked to come back somewhere. Reads the
 * stored path once and drops it. A failed sign-in (/login?error=<code>) of a user who is
 * still signed in goes back there too, with ?reauth_error=<code> so that page can say
 * why. A brand-new account's /set-password landing and the public pages keep their own
 * page.
 */
export function googleReturnRedirect(to: Landing, signedIn: boolean, now = Date.now()): string | null {
  let raw: string | null;
  try {
    raw = sessionStorage.getItem(RETURN_KEY);
  } catch {
    return null;
  }
  if (raw === null) return null;
  forgetReturnPath();

  let stored: { path?: unknown; at?: unknown };
  try {
    stored = JSON.parse(raw);
  } catch {
    return null;
  }
  const age = typeof stored?.at === "number" ? now - stored.at : NaN;
  if (!(age >= 0 && age <= RETURN_TTL_MS)) return null;
  const path = safeReturnPath(stored.path);
  if (!path || !signedIn || to.meta.public) return null;
  // A cancelled or refused Google step lands on /login?error=<code>, but the old session
  // cookie still signs the user in, so the router would send them on to Home with no word.
  const error = to.query.error;
  if (to.name === "login" && error) {
    const code = typeof error === "string" && REAUTH_ERRORS.has(error) ? error : "oauth_failed";
    const back = new URL(path, RETURN_BASE);
    back.searchParams.set(REAUTH_ERROR_PARAM, code);
    // Checked again: the rebuilt URL is written out anew and must still be a path here.
    return safeReturnPath(back.pathname + back.search + back.hash);
  }
  if (to.name === "login" || to.name === "set-password" || to.fullPath === path) return null;
  return path;
}
