/**
 * Client Telemetry Collector & Tracker
 * Captures non-sensitive browser & device metrics, tracks user events and keeps sessions alive.
 *
 * The ingest endpoint is authenticated-only and CSRF-checked, so nothing is sent while
 * signed out: the auth store calls startTelemetry() once a user is known (sign-in,
 * registration, restored session) and stopTelemetry() on sign-out.
 */

import { authHeaders } from "./api";

// Same API prefix as api.ts / stream.ts (empty => same origin via the dev proxy).
const BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? "";
const SESSION_KEY = "telemetry_session_id";
// Who the stored id belongs to, so a different user signing in on this tab never
// inherits it — even without an explicit sign-in (a Google sign-in after the previous
// user's session expired comes back as a restored session).
const OWNER_KEY = "telemetry_session_owner";
const HEARTBEAT_MS = 30_000;

// The server accepts exactly these names (UserTelemetryEventInput); others get a 422.
export type TelemetryEventName = "session_start" | "session_end" | "heartbeat" | "tab_focus" | "tab_blur";
export type TelemetryCategory = "system" | "ui";

let activeUserId: string | null = null;
let listenersInstalled = false;

function readSession(key: string): string | null {
  try {
    return sessionStorage.getItem(key);
  } catch {
    return null; // storage blocked
  }
}

function writeSession(key: string, value: string | null): void {
  try {
    if (value === null) sessionStorage.removeItem(key);
    else sessionStorage.setItem(key, value);
  } catch {
    // Storage blocked: the id just won't survive a reload.
  }
}

// A fresh id for `owner` (null: nobody yet, the next user to sign in adopts it).
function mintSessionId(owner: string | null): string {
  const sid = "sess_" + Math.random().toString(36).substring(2, 11) + "_" + Date.now();
  writeSession(SESSION_KEY, sid);
  writeSession(OWNER_KEY, owner);
  return sid;
}

function getSessionId(): string {
  return readSession(SESSION_KEY) || mintSessionId(activeUserId);
}

export function getClientDeviceInfo(): Record<string, any> {
  const ua = navigator.userAgent;
  let deviceType = "desktop";
  if (/mobile/i.test(ua)) deviceType = "mobile";
  else if (/tablet|ipad/i.test(ua)) deviceType = "tablet";

  let os = "Other";
  if (/windows/i.test(ua)) os = "Windows";
  else if (/macintosh|mac os x/i.test(ua)) os = "macOS";
  else if (/android/i.test(ua)) os = "Android";
  else if (/iphone|ipad|ipod/i.test(ua)) os = "iOS";
  else if (/linux/i.test(ua)) os = "Linux";

  let browser = "Other";
  if (/edg/i.test(ua)) browser = "Edge";
  else if (/chrome|crios/i.test(ua)) browser = "Chrome";
  else if (/firefox|fxios/i.test(ua)) browser = "Firefox";
  else if (/safari/i.test(ua)) browser = "Safari";

  let timezone = "UTC";
  try {
    timezone = Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  } catch {
    // fallback
  }

  return {
    device_type: deviceType,
    browser,
    os,
    screen_res: typeof window !== "undefined" && window.screen ? `${window.screen.width}x${window.screen.height}` : undefined,
    viewport: typeof window !== "undefined" ? `${window.innerWidth}x${window.innerHeight}` : undefined,
    language: typeof navigator !== "undefined" ? navigator.language : undefined,
    timezone,
  };
}

export function trackEvent(
  eventName: TelemetryEventName,
  eventCategory: TelemetryCategory = "ui",
  details: Record<string, any> = {},
  includeDeviceInfo: boolean = false
): void {
  if (!activeUserId) return;
  try {
    const payload: Record<string, any> = {
      session_id: getSessionId(),
      event_name: eventName,
      event_category: eventCategory,
      details,
    };

    if (includeDeviceInfo) {
      payload.device_info = getClientDeviceInfo();
    }

    // keepalive lets session_end on pagehide outlive the page; the bearer/CSRF headers
    // are why this is a fetch and not navigator.sendBeacon (which cannot carry them).
    fetch(`${BASE}/v1/telemetry/event`, {
      method: "POST",
      headers: { "Content-Type": "application/json", ...authHeaders("POST") },
      body: JSON.stringify(payload),
      keepalive: true,
      credentials: "include",
    }).catch(() => {
      // Telemetry errors should never disrupt user experience
    });
  } catch {
    // Silently ignore telemetry transmission failures
  }
}

function installListeners(): void {
  if (listenersInstalled) return;
  listenersInstalled = true;

  // Periodic heartbeat every 30 seconds while tab is active/visible
  setInterval(() => {
    if (document.visibilityState === "visible") {
      trackEvent("heartbeat", "system", { active: true });
    }
  }, HEARTBEAT_MS);

  // Track visibility change (focus / blur)
  document.addEventListener("visibilitychange", () => {
    trackEvent(document.visibilityState === "visible" ? "tab_focus" : "tab_blur", "ui");
  });

  // Track page hide / window closing
  window.addEventListener("pagehide", () => {
    trackEvent("session_end", "system");
  });
}

/**
 * Begin (or resume) telemetry for a signed-in user and announce the session with its
 * device info. A tab never reuses the id of whoever was signed in before: `newSession`
 * (an explicit sign-in) mints a fresh one, and so does a stored id owned by another
 * user. A reload by the same user keeps its id, and an id with no recorded owner
 * (minted at sign-out, or stored by an older build) is adopted.
 */
export function startTelemetry(userId: string, { newSession = false }: { newSession?: boolean } = {}): void {
  if (typeof window === "undefined") return;
  if (activeUserId === userId && !newSession) return;
  const owner = readSession(OWNER_KEY);
  const someoneElses = (owner !== null && owner !== userId) || (activeUserId !== null && activeUserId !== userId);
  if (newSession || someoneElses) mintSessionId(userId);
  else writeSession(OWNER_KEY, userId);
  activeUserId = userId;
  installListeners();
  trackEvent("session_start", "system", { path: window.location.pathname }, true);
}

/** Stop sending (signed out) and rotate the session id for whoever signs in next. */
export function stopTelemetry(): void {
  activeUserId = null;
  mintSessionId(null);
}
