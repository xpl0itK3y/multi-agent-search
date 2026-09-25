import { defineStore } from "pinia";
import { ref } from "vue";
import { api } from "@/lib/api";
import { startTelemetry, stopTelemetry } from "@/lib/telemetry";
import type { AuthUser } from "@/lib/types";

export const useAuthStore = defineStore("auth", () => {
  const user = ref<AuthUser | null>(null);
  const checked = ref(false);

  async function fetchMe() {
    try {
      user.value = await api.me();
      startTelemetry(user.value.id);
    } catch {
      user.value = null;
    } finally {
      checked.value = true;
    }
  }

  async function login(email: string, password: string) {
    user.value = (await api.login(email, password)).user;
    startTelemetry(user.value.id, { newSession: true });
  }

  async function register(email: string, password: string) {
    user.value = (await api.register(email, password)).user;
    startTelemetry(user.value.id, { newSession: true });
  }

  async function logout() {
    // Before the request: nothing may be sent once the session is gone.
    stopTelemetry();
    try {
      await api.logout();
    } catch {
      /* ignore */
    }
    user.value = null;
  }

  async function updateProfile(payload: { name?: string; avatar_url?: string }) {
    user.value = await api.updateProfile(payload);
    return user.value;
  }

  // A password reset revokes every session of its account and signs nobody in (api drops
  // the stored token): whoever was signed in here is signed out, as after a logout.
  async function resetPassword(token: string, password: string) {
    await api.resetPassword(token, password);
    if (user.value) {
      stopTelemetry();
      user.value = null;
    }
  }

  return { user, checked, fetchMe, login, register, logout, updateProfile, resetPassword };
});
