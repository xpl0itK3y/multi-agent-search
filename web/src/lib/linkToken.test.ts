// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  captureLinkToken,
  dropLinkToken,
  holdLinkToken,
  LINK_TOKEN_TTL_MS,
  linkPageOf,
  linkTokenFromHash,
  onLinkToken,
  peekLinkToken,
  takeLinkToken,
  watchLinkTokens,
} from "./linkToken";

const VERIFY_KEY = "auth.verify_email_link";

describe("linkTokenFromHash", () => {
  it("reads the token parameter of a fragment", () => {
    expect(linkTokenFromHash("#token=Ab-9_x")).toBe("Ab-9_x");
    expect(linkTokenFromHash("token=Ab-9_x")).toBe("Ab-9_x");
    expect(linkTokenFromHash("#lang=en&token=abc&x=1")).toBe("abc");
  });

  it("finds none in an empty, token-less or look-alike fragment", () => {
    for (const hash of ["", "#", "#token=", "#token", "#xtoken=abc", "#token= abc", "#section-2"]) {
      expect(linkTokenFromHash(hash), hash).toBeNull();
    }
  });
});

describe("holdLinkToken / takeLinkToken", () => {
  beforeEach(() => {
    sessionStorage.clear();
    dropLinkToken("reset-password");
    dropLinkToken("verify-email");
  });

  it("hands the held token to its page once", () => {
    holdLinkToken("reset-password", "#token=abc");

    expect(takeLinkToken("reset-password")).toBe("abc");
    expect(takeLinkToken("reset-password")).toBeNull();
  });

  it("keeps a token to the page it came for", () => {
    holdLinkToken("reset-password", "#token=abc");

    expect(takeLinkToken("verify-email")).toBeNull();
    expect(takeLinkToken("reset-password")).toBe("abc");
  });

  it("a newer link replaces the one held before; a fragment without a token is no link", () => {
    holdLinkToken("verify-email", "#token=old");
    holdLinkToken("verify-email", "#token=new");
    holdLinkToken("verify-email", "#section");

    expect(takeLinkToken("verify-email")).toBe("new");
  });

  it("lets a token go once it waited LINK_TOKEN_TTL_MS for its page", () => {
    const at = 1_000_000;
    holdLinkToken("reset-password", "#token=abc", at);
    holdLinkToken("verify-email", "#token=def", at);

    expect(peekLinkToken("verify-email", at + LINK_TOKEN_TTL_MS)).toBe("def");
    expect(peekLinkToken("verify-email", at + LINK_TOKEN_TTL_MS + 1)).toBeNull();
    expect(sessionStorage.getItem(VERIFY_KEY)).toBeNull();
    expect(takeLinkToken("reset-password", at + LINK_TOKEN_TTL_MS + 1)).toBeNull();
    // Nor one from a clock that went back.
    holdLinkToken("verify-email", "#token=ghi", at);
    expect(peekLinkToken("verify-email", at - 1)).toBeNull();
  });
});

describe("a verification link's token", () => {
  beforeEach(() => {
    sessionStorage.clear();
    dropLinkToken("verify-email");
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("stays held for its page until it is dropped", () => {
    holdLinkToken("verify-email", "#token=vt");

    expect(peekLinkToken("verify-email")).toBe("vt");
    expect(peekLinkToken("verify-email")).toBe("vt");
    dropLinkToken("verify-email");
    expect(peekLinkToken("verify-email")).toBeNull();
  });

  it("waits in this tab's sessionStorage through a page load (a Google sign-in), unlike a reset's", async () => {
    holdLinkToken("verify-email", "#token=vt");
    holdLinkToken("reset-password", "#token=rt");

    vi.resetModules();
    const later = await import("./linkToken");

    expect(later.peekLinkToken("verify-email")).toBe("vt");
    expect(later.takeLinkToken("reset-password")).toBeNull();
    expect(Object.keys(sessionStorage)).toEqual([VERIFY_KEY]);
    expect(sessionStorage.getItem(VERIFY_KEY)).not.toContain("rt");
  });

  it("is dropped only while it is the one held: a newer link stays", () => {
    holdLinkToken("verify-email", "#token=old");
    holdLinkToken("verify-email", "#token=new");

    dropLinkToken("verify-email", "old");
    expect(peekLinkToken("verify-email")).toBe("new");
    dropLinkToken("verify-email", "new");
    expect(peekLinkToken("verify-email")).toBeNull();
  });

  it("is held for this page load when storage is blocked", () => {
    const blocked = () => {
      throw new DOMException("blocked", "SecurityError");
    };
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(blocked);
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(blocked);
    vi.spyOn(Storage.prototype, "removeItem").mockImplementation(blocked);

    holdLinkToken("verify-email", "#token=vt");
    expect(peekLinkToken("verify-email")).toBe("vt");
    dropLinkToken("verify-email");
    expect(peekLinkToken("verify-email")).toBeNull();
  });

  it("ignores what is not its own entry in storage", () => {
    for (const raw of ["not json", "null", '{"token":""}', '{"token":"x"}', '{"token":1,"at":1}']) {
      sessionStorage.setItem(VERIFY_KEY, raw);
      expect(peekLinkToken("verify-email"), raw).toBeNull();
    }
  });
});

describe("linkPageOf", () => {
  it("names the link pages in the spellings vue-router matches", () => {
    expect(linkPageOf("/reset-password")).toBe("reset-password");
    expect(linkPageOf("/Reset-Password/")).toBe("reset-password");
    expect(linkPageOf("/verify-email")).toBe("verify-email");
    for (const path of ["/", "/forgot-password", "/reset-password/x", "/login"]) expect(linkPageOf(path), path).toBeNull();
  });
});

describe("captureLinkToken", () => {
  beforeEach(() => {
    sessionStorage.clear();
    dropLinkToken("reset-password");
    dropLinkToken("verify-email");
  });

  it("holds a link page's token and takes the fragment out of the address", () => {
    window.history.replaceState({ kept: 1 }, "", "/reset-password?lang=en#token=cap-tok");

    captureLinkToken();

    expect(window.location.pathname + window.location.search + window.location.hash).toBe("/reset-password?lang=en");
    // The entry is replaced, not added, and keeps its state.
    expect(window.history.state).toEqual({ kept: 1 });
    expect(takeLinkToken("reset-password")).toBe("cap-tok");
  });

  it("keeps a verification link's token for the sign-in to come, never in the address", () => {
    window.history.replaceState(null, "", "/verify-email#token=cap-vt");
    const seen: string[] = [];
    const stop = onLinkToken("verify-email", () => seen.push(window.location.hash));

    captureLinkToken();
    stop();

    expect(window.location.hash).toBe("");
    // An open page is told only once the address is clean.
    expect(seen).toEqual([""]);
    expect(JSON.parse(sessionStorage.getItem(VERIFY_KEY)!).token).toBe("cap-vt");
    expect(peekLinkToken("verify-email")).toBe("cap-vt");
  });

  it("drops any other fragment of a link page", () => {
    window.history.replaceState(null, "", "/verify-email#section");

    captureLinkToken();

    expect(window.location.hash).toBe("");
    expect(takeLinkToken("verify-email")).toBeNull();
  });

  it("leaves other pages alone", () => {
    window.history.replaceState(null, "", "/forgot-password#token=not-a-link");

    captureLinkToken();

    expect(window.location.hash).toBe("#token=not-a-link");
    expect(takeLinkToken("forgot-password")).toBeNull();
  });
});

describe("watchLinkTokens", () => {
  it("captures a link opened into the running page, until stopped", async () => {
    window.history.replaceState(null, "", "/reset-password");
    const stop = watchLinkTokens();

    window.location.hash = "#token=again-tok";
    await vi.waitFor(() => expect(window.location.hash).toBe(""));
    expect(takeLinkToken("reset-password")).toBe("again-tok");

    stop();
    window.location.hash = "#token=later-tok";
    await new Promise((resolve) => window.addEventListener("hashchange", resolve, { once: true }));
    expect(window.location.hash).toBe("#token=later-tok");
    expect(takeLinkToken("reset-password")).toBeNull();
  });
});

describe("onLinkToken", () => {
  it("tells an open page of each new token for it, until stopped", () => {
    const seen: string[] = [];
    const stop = onLinkToken("reset-password", () => seen.push(takeLinkToken("reset-password") ?? "none"));

    holdLinkToken("reset-password", "#token=one");
    holdLinkToken("verify-email", "#token=other-page");
    holdLinkToken("reset-password", "#section");
    holdLinkToken("reset-password", "#token=two");
    stop();
    holdLinkToken("reset-password", "#token=three");

    expect(seen).toEqual(["one", "two"]);
    expect(takeLinkToken("reset-password")).toBe("three");
  });
});
