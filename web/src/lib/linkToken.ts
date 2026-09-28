// The one-time token of an emailed link: /reset-password#token=<token> and
// /verify-email#token=<token>. The server puts it in the URL fragment, which browsers
// never send to a server or put in a Referer. captureLinkToken() takes it out of the
// address first thing on a page load (src/linkCapture.ts, the first import of main.ts),
// before the router records the address and before the app makes any request, so it
// does not stay in the history, a bookmark or a copied address. It is held here for its
// page, for LINK_TOKEN_TTL_MS at most.
//
// A reset token is held in memory only, and its page takes it once: the reset itself
// needs no session. A verification token is also kept in this tab's sessionStorage,
// because the server redeems it only for the signed-in account the link was sent to: a
// signed-out visitor signs in first, maybe through Google, which leaves the page and
// comes back to /verify-email. It stays until the page redeems it or the server refuses
// it (dropLinkToken), and a wrong account's refusal leaves it for the right one.
//
// A bare history.replaceState is enough there: the router does not exist yet, so it
// builds its record of the entry (history.state.current) from the clean address. A link
// opened again into a tab already on its page is a same-document navigation to the new
// fragment, with no page load: watchLinkTokens() catches it in a popstate listener that
// runs before the router's own, which then only sees the clean address too. The router
// never holds the token, so no route (redirectedFrom, query, params) carries it.

/** How long a captured token waits for its page: enough to sign in first. */
export const LINK_TOKEN_TTL_MS = 30 * 60 * 1000;

interface Held {
  token: string;
  at: number; // when it was captured (ms)
}

const held = new Map<string, Held>();
// The pages whose token is kept in sessionStorage as well, by storage key.
const STORAGE_KEYS = new Map([["verify-email", "auth.verify_email_link"]]);

// The pages an emailed link opens, by path, as the route names they have.
const LINK_PAGES = new Map([
  ["/reset-password", "reset-password"],
  ["/verify-email", "verify-email"],
]);

/** The route name of the link page at `pathname`, matched the way vue-router matches
 * routes (any case, trailing slashes aside), or null. */
export function linkPageOf(pathname: string): string | null {
  return LINK_PAGES.get(pathname.toLowerCase().replace(/\/+$/, "")) ?? null;
}

/** The `token` parameter of a URL fragment ("#token=abc"), or null. */
export function linkTokenFromHash(hash: string): string | null {
  // vue-router hands the fragment over already percent-decoded, and tokens are URL-safe.
  const match = /(?:^#?|&)token=([^&\s]+)/.exec(hash);
  return match ? match[1] : null;
}

function stored(page: string): Held | null {
  const key = STORAGE_KEYS.get(page);
  if (!key) return null;
  try {
    const entry: unknown = JSON.parse(sessionStorage.getItem(key) ?? "null");
    const { token, at } = (entry ?? {}) as Partial<Held>;
    return typeof token === "string" && token && typeof at === "number" ? { token, at } : null;
  } catch {
    return null; // storage blocked, or not our JSON
  }
}

// The entry held for `page`, however old: this page load's, else this tab's stored one.
function current(page: string): Held | null {
  return held.get(page) ?? stored(page);
}

// The open pages that want a link's new token (onLinkToken).
const listeners = new Set<{ page: string; fn: () => void }>();

/** Holds the token of `hash`, for `page` (a route name), in place of any held before, and
 * tells that page if it is open. A fragment without a token is no link: it changes nothing. */
export function holdLinkToken(page: string, hash: string, now = Date.now()): void {
  const token = linkTokenFromHash(hash);
  if (!token) return;
  const entry = { token, at: now };
  held.set(page, entry);
  const key = STORAGE_KEYS.get(page);
  if (key) {
    try {
      sessionStorage.setItem(key, JSON.stringify(entry));
    } catch {
      /* storage blocked: held for this page load only */
    }
  }
  for (const listener of [...listeners]) if (listener.page === page) listener.fn();
}

/** Calls `fn` whenever a new token is held for `page`: a link opened again into this tab
 * while the page is open, which mounts nothing new. Returns the function that stops it. */
export function onLinkToken(page: string, fn: () => void): () => void {
  const listener = { page, fn };
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** The token held for `page`, if captured less than LINK_TOKEN_TTL_MS ago; it stays held. */
export function peekLinkToken(page: string, now = Date.now()): string | null {
  const entry = current(page);
  if (!entry) return null;
  const age = now - entry.at;
  if (age >= 0 && age <= LINK_TOKEN_TTL_MS) return entry.token;
  dropLinkToken(page);
  return null;
}

/** Stops holding the token of `page`; with `token`, only if that is still the one held
 * (not a newer link's). */
export function dropLinkToken(page: string, token?: string): void {
  if (token !== undefined && current(page)?.token !== token) return;
  held.delete(page);
  const key = STORAGE_KEYS.get(page);
  if (!key) return;
  try {
    sessionStorage.removeItem(key);
  } catch {
    /* storage blocked: nothing was stored */
  }
}

/** The token held for `page`, once: the next call returns null. */
export function takeLinkToken(page: string, now = Date.now()): string | null {
  const token = peekLinkToken(page, now);
  dropLinkToken(page);
  return token;
}

/**
 * On a link page, holds the token of the address's fragment and takes the fragment out
 * of the address (any fragment: those pages have no other use for one). Elsewhere it
 * does nothing.
 */
export function captureLinkToken(): void {
  const { pathname, search, hash } = window.location;
  const page = linkPageOf(pathname);
  if (!page || !hash) return;
  // The address first: an open page told of the token must not find it still there.
  window.history.replaceState(window.history.state, "", pathname + search);
  holdLinkToken(page, hash);
}

/**
 * Captures the token of a link opened into the running page as well: a same-document
 * navigation to a new fragment fires popstate (and hashchange, the fallback where it does
 * not). Must be called before the router is created, so that its popstate listener runs
 * after this one. Returns the function that stops it.
 */
export function watchLinkTokens(): () => void {
  window.addEventListener("popstate", captureLinkToken);
  window.addEventListener("hashchange", captureLinkToken);
  return () => {
    window.removeEventListener("popstate", captureLinkToken);
    window.removeEventListener("hashchange", captureLinkToken);
  };
}
