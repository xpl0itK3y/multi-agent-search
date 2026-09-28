// Reduced-motion and media-query helpers (apple-design §14). The one place a smooth
// scroll behaviour is chosen: everything else asks smoothOrAuto(), so a reader who
// asked for reduced motion gets an instant jump instead.
import { getCurrentScope, onScopeDispose, ref, type Ref } from "vue";

const REDUCED_MOTION = "(prefers-reduced-motion: reduce)";

function mediaQueryList(query: string): MediaQueryList | null {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function") return null;
  try {
    return window.matchMedia(query);
  } catch {
    return null;
  }
}

/** True when the reader asked for reduced motion; false where matchMedia is missing (jsdom). */
export function prefersReducedMotion(): boolean {
  return mediaQueryList(REDUCED_MOTION)?.matches ?? false;
}

/** The scroll behaviour to pass to scrollTo/scrollIntoView. */
export function smoothOrAuto(): ScrollBehavior {
  return prefersReducedMotion() ? "auto" : "smooth";
}

/** A live boolean for a media query, updated on 'change' and released with the scope. */
export function useMediaQuery(query: string): Ref<boolean> {
  const list = mediaQueryList(query);
  const matches = ref(list?.matches ?? false);
  if (!list) return matches;

  const onChange = (e: MediaQueryListEvent) => {
    matches.value = e.matches;
  };
  if (typeof list.addEventListener === "function") list.addEventListener("change", onChange);
  else list.addListener?.(onChange); // Safari < 14

  if (getCurrentScope()) {
    onScopeDispose(() => {
      if (typeof list.removeEventListener === "function") list.removeEventListener("change", onChange);
      else list.removeListener?.(onChange);
    });
  }
  return matches;
}

export const useReducedMotion = (): Ref<boolean> => useMediaQuery(REDUCED_MOTION);
