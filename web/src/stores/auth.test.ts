import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";

const telemetry = vi.hoisted(() => ({ startTelemetry: vi.fn(), stopTelemetry: vi.fn() }));
vi.mock("@/lib/telemetry", () => telemetry);

const user = { id: "u1", email: "u1@example.com" };
vi.mock("@/lib/api", () => ({
  api: {
    me: vi.fn(async () => user),
    login: vi.fn(async () => ({ access_token: "t", user })),
    register: vi.fn(async () => ({ access_token: "t", user })),
    logout: vi.fn(async () => ({ status: "ok" })),
    resetPassword: vi.fn(async () => ({ status: "ok" })),
  },
}));

import { api } from "@/lib/api";
import { useAuthStore } from "./auth";

describe("auth store telemetry hooks", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    vi.clearAllMocks();
  });

  it.each(["login", "register"] as const)("%s starts a fresh telemetry session", async (action) => {
    await useAuthStore()[action]("u1@example.com", "secret123");

    expect(telemetry.startTelemetry).toHaveBeenCalledWith("u1", { newSession: true });
  });

  it("a restored session resumes telemetry under the existing id", async () => {
    await useAuthStore().fetchMe();

    expect(telemetry.startTelemetry).toHaveBeenCalledWith("u1");
  });

  it("no session: telemetry never starts", async () => {
    vi.mocked(api.me).mockRejectedValueOnce(new Error("401"));

    await useAuthStore().fetchMe();

    expect(telemetry.startTelemetry).not.toHaveBeenCalled();
  });

  it("logout stops telemetry before the session is dropped", async () => {
    const order: string[] = [];
    telemetry.stopTelemetry.mockImplementation(() => order.push("stop"));
    vi.mocked(api.logout).mockImplementation(async () => {
      order.push("logout");
      return { status: "ok" };
    });

    await useAuthStore().logout();

    expect(order).toEqual(["stop", "logout"]);
  });
});

describe("auth store refresh", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    vi.clearAllMocks();
  });

  it("re-reads the signed-in user, without a new telemetry session", async () => {
    const auth = useAuthStore();
    auth.user = { ...user, email_verified: false };
    vi.mocked(api.me).mockResolvedValueOnce({ ...user, email_verified: true });

    await auth.refreshUser();

    expect(auth.user).toEqual({ ...user, email_verified: true });
    expect(telemetry.startTelemetry).not.toHaveBeenCalled();
  });

  it("keeps the user when the refresh fails", async () => {
    const auth = useAuthStore();
    auth.user = user;
    vi.mocked(api.me).mockRejectedValueOnce(new TypeError("fetch failed"));

    await auth.refreshUser();

    expect(auth.user).toEqual(user);
  });

  it("asks nothing while signed out", async () => {
    await useAuthStore().refreshUser();

    expect(api.me).not.toHaveBeenCalled();
    expect(useAuthStore().user).toBeNull();
  });
});

describe("auth store password reset", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    vi.clearAllMocks();
  });

  it("signs out whoever was signed in here: the reset revoked every session", async () => {
    const auth = useAuthStore();
    auth.user = user;

    await auth.resetPassword("reset-tok", "new-password");

    expect(api.resetPassword).toHaveBeenCalledWith("reset-tok", "new-password");
    expect(auth.user).toBeNull();
    expect(telemetry.stopTelemetry).toHaveBeenCalledOnce();
    // No logout call: the server ended the session already, and a logout (which signs out
    // every device) of another account's session would reach well beyond this page.
    expect(api.logout).not.toHaveBeenCalled();
  });

  it("keeps the session when the reset is refused", async () => {
    vi.mocked(api.resetPassword).mockRejectedValueOnce(new Error("400 reset_token_invalid"));
    const auth = useAuthStore();
    auth.user = user;

    await expect(auth.resetPassword("dead-tok", "new-password")).rejects.toThrow();

    expect(auth.user).toEqual(user);
    expect(telemetry.stopTelemetry).not.toHaveBeenCalled();
  });

  it("a signed-out reset touches no telemetry", async () => {
    await useAuthStore().resetPassword("reset-tok", "new-password");

    expect(telemetry.stopTelemetry).not.toHaveBeenCalled();
  });
});
