import { describe, expect, it } from "vitest";

import { holdLinkToken, linkTokenFromHash, takeLinkToken } from "./linkToken";

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
