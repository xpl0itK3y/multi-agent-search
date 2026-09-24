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
