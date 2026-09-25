// Source URLs come from web search results and LLM output — untrusted. Vue escapes a
// bound :href, but it does not stop a `javascript:` URL, so every link to a source goes
// through here first.

// Characters URL() leaves as they are that still mean something in HTML or script.
const UNSAFE_IN_ATTRIBUTE = /['"`<>\\\s]/g;

function percentEncode(c: string): string {
  return "%" + c.charCodeAt(0).toString(16).toUpperCase().padStart(2, "0");
}

/**
 * The http(s) URL to link to, or null when `value` is not one. The result is normalized
 * by URL() (spaces, quotes, angle brackets, backticks and non-ASCII come out
 * percent-encoded) with the remaining quote-like characters encoded too. Placing it in
 * a hand-built HTML attribute still needs attribute escaping (`&`).
 */
export function safeHttpUrl(value: string | null | undefined): string | null {
  const raw = (value ?? "").trim();
  if (!/^https?:\/\//i.test(raw)) return null;
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    return null;
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return null;
  return parsed.href.replace(UNSAFE_IN_ATTRIBUTE, percentEncode);
}
