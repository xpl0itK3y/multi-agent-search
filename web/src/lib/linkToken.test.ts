// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  captureLinkToken,
  holdLinkToken,
  linkPageOf,
  linkTokenFromHash,
  takeLinkToken,
  watchLinkTokens,
} from "./linkToken";

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
  it("hands the held token to its page once", () => {
    holdLinkToken("reset-password", "#token=abc");

    expect(takeLinkToken("reset-password")).toBe("abc");
    expect(takeLinkToken("reset-password")).toBeNull();
  });

  it("keeps a token to the page it came for", () => {
    holdLinkToken("reset-password", "#token=abc");

    expect(takeLinkToken("verify-email")).toBeNull();
    // Dropped all the same: a later visit to the right page gets nothing either.
    expect(takeLinkToken("reset-password")).toBeNull();
  });

  it("a fragment without a token drops the one held before", () => {
    holdLinkToken("verify-email", "#token=old");
    holdLinkToken("verify-email", "#section");

    expect(takeLinkToken("verify-email")).toBeNull();
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
    takeLinkToken("reset-password");
  });

  it("holds a link page's token and takes the fragment out of the address", () => {
    window.history.replaceState({ kept: 1 }, "", "/reset-password?lang=en#token=cap-tok");

    captureLinkToken();

    expect(window.location.pathname + window.location.search + window.location.hash).toBe("/reset-password?lang=en");
    // The entry is replaced, not added, and keeps its state.
    expect(window.history.state).toEqual({ kept: 1 });
    expect(takeLinkToken("reset-password")).toBe("cap-tok");
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
