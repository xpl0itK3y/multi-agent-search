// A user's avatar_url is either an image URL (Google photo, a pasted link) or one
// of the emoji presets picked in Settings. Only a real URL may become <img src> —
// an emoji there requests "/%F0%9F%A4%96" and renders a broken image.
export function isAvatarImage(value: string | null | undefined): boolean {
  const v = (value ?? "").trim();
  return /^https?:\/\//i.test(v) || v.startsWith("/");
}

// Longest non-URL value still shown as a glyph (emoji presets with variation
// selectors or ZWJ sequences); anything longer is not an avatar, use the initial.
const MAX_GLYPH_LENGTH = 8;

// What to render when there is no image: the emoji preset, else the initial.
export function avatarGlyph(value: string | null | undefined, name: string, fallback = ""): string {
  const v = (value ?? "").trim();
  if (v && !isAvatarImage(v) && v.length <= MAX_GLYPH_LENGTH) return v;
  return name.trim() ? name.trim().charAt(0).toUpperCase() : fallback;
}
