// The one-time token of an emailed link: /reset-password#token=<token> and
// /verify-email#token=<token>. The server puts it in the URL fragment, which browsers
// never send to a server or put in a Referer. captureLinkToken() takes it out of the
// address first thing on a page load (src/linkCapture.ts, the first import of main.ts),
// before the router records the address and before the app makes any request, so it
// does not stay in the history, a bookmark or a copied address. It is held here, in
// memory only, for that page to take once.
//
// A bare history.replaceState is enough there: the router does not exist yet, so it
// builds its record of the entry (history.state.current) from the clean address. A link
// opened again into a tab already on its page is a same-document navigation to the new
// fragment, with no page load: watchLinkTokens() catches it in a popstate listener that
// runs before the router's own, which then only sees the clean address too. The router
// never holds the token, so no route (redirectedFrom, query, params) carries it.

let held: { page: string; token: string } | null = null;

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

// The open pages that want a link's new token (onLinkToken).
const listeners = new Set<{ page: string; fn: () => void }>();

/** Holds the token of `hash`, for `page` (a route name), in place of any held before, and
 * tells that page if it is open. */
export function holdLinkToken(page: string, hash: string): void {
  const token = linkTokenFromHash(hash);
  held = token ? { page, token } : null;
  if (!token) return;
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

/** The token held for `page`, once: the next call returns null. */
export function takeLinkToken(page: string): string | null {
  const token = held?.page === page ? held.token : null;
  held = null;
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
  holdLinkToken(page, hash);
  window.history.replaceState(window.history.state, "", pathname + search);
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
