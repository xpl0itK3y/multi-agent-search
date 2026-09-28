// The one-time token of an emailed link: /reset-password#token=<token> and
// /verify-email#token=<token>. The server puts it in the URL fragment, which browsers
// never send to a server or put in a Referer. captureLinkToken() takes it out of the
// address first thing on a page load (src/linkCapture.ts, the first import of main.ts),
// before the router records the address and before the app makes any request, so it
// does not stay in the history, a bookmark or a copied address. It is held here, in
// memory only, for that page to take once.
//
// A bare history.replaceState is enough there: the router does not exist yet, so it
// builds its record of the entry (history.state.current) from the clean address.

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

/** Holds the token of `hash`, for `page` (a route name), in place of any held before. */
export function holdLinkToken(page: string, hash: string): void {
  const token = linkTokenFromHash(hash);
  held = token ? { page, token } : null;
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
