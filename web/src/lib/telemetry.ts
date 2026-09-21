/**
 * Client Telemetry Collector & Tracker
 * Captures non-sensitive browser & device metrics, tracks user events and keeps sessions alive.
 */

import { authHeaders } from "./api";

function getSessionId(): string {
  let sid = sessionStorage.getItem("telemetry_session_id");
  if (!sid) {
    sid = "sess_" + Math.random().toString(36).substring(2, 11) + "_" + Date.now();
    sessionStorage.setItem("telemetry_session_id", sid);
  }
  return sid;
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

export async function trackEvent(
  eventName: string,
  eventCategory: string = "ui",
  details: Record<string, any> = {},
  includeDeviceInfo: boolean = false
): Promise<void> {
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

    const body = JSON.stringify(payload);
    const url = "/v1/telemetry/event";

    // Non-blocking fetch with keepalive & auth headers
    if (typeof fetch === "function") {
      const headers: Record<string, string> = {
        "Content-Type": "application/json",
        ...authHeaders("POST"),
      };
      if (!headers["Authorization"]) {
        const token = typeof localStorage !== "undefined" ? localStorage.getItem("access_token") : null;
        if (token) headers["Authorization"] = `Bearer ${token}`;
      }

      fetch(url, {
        method: "POST",
        headers,
        body,
        keepalive: true,
        credentials: "include",
      }).catch(() => {
        // Telemetry errors should never disrupt user experience
      });
    } else if (typeof navigator !== "undefined" && navigator.sendBeacon) {
      const blob = new Blob([body], { type: "application/json" });
      navigator.sendBeacon(url, blob);
    }
  } catch {
    // Silently ignore telemetry transmission failures
  }
}

let initialized = false;

export function initTelemetry(): void {
  if (initialized || typeof window === "undefined") return;
  initialized = true;

  // Send initial session / page view with full device info
  trackEvent("session_start", "system", { path: window.location.pathname }, true);

  // Periodic heartbeat every 30 seconds while tab is active/visible
  setInterval(() => {
    if (typeof document !== "undefined" && document.visibilityState === "visible") {
      trackEvent("heartbeat", "system", { active: true });
    }
  }, 30_000);

  // Track visibility change (focus / blur)
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      trackEvent("tab_focus", "ui");
    } else {
      trackEvent("tab_blur", "ui");
    }
  });

  // Track page hide / window closing
  window.addEventListener("pagehide", () => {
    trackEvent("session_end", "system");
  });
}
