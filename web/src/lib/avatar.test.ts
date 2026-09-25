import { describe, expect, it } from "vitest";

import { avatarGlyph, isAvatarImage } from "./avatar";

describe("avatar helpers", () => {
  it("treats only http(s) and root-relative URLs as images", () => {
    expect(isAvatarImage("https://lh3.googleusercontent.com/a/photo")).toBe(true);
    expect(isAvatarImage("HTTP://example.com/me.png")).toBe(true);
    expect(isAvatarImage("/static/avatar.png")).toBe(true);
    for (const value of ["🤖", "🛡️", "javascript:alert(1)", "data:image/png;base64,AA", "", null, undefined]) {
      expect(isAvatarImage(value), String(value)).toBe(false);
    }
  });

  it("renders an emoji preset as text, otherwise the name's initial", () => {
    expect(avatarGlyph("🤖", "denis")).toBe("🤖");
    expect(avatarGlyph("🛡️", "denis")).toBe("🛡️");
    expect(avatarGlyph(null, "denis")).toBe("D");
    expect(avatarGlyph("https://example.com/me.png", "denis")).toBe("D");
    expect(avatarGlyph("not an avatar at all", "denis")).toBe("D");
    expect(avatarGlyph("", "", "👤")).toBe("👤");
  });
});
