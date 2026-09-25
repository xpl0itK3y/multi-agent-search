// The one-time token of an emailed link: /reset-password#token=<token> and
// /verify-email#token=<token>. The server puts it in the URL fragment, which browsers
// never send to a server or put in a Referer. The router takes it out of the address as
// soon as such a page opens, before the page renders (router/index.ts), so it does not
// stay in the history, a bookmark or a copied address, and holds it here, in memory
// only, for that page to take once.
//
// The router does it with a replace redirect, not a bare history.replaceState: vue-router
// keeps the entry's URL in history.state.current and writes it back to the address on
// the next navigation, and a direct replaceState would leave the token there.

let held: { page: string; token: string } | null = null;

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
