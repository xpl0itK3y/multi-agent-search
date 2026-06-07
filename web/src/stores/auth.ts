import { defineStore } from "pinia";
import { ref } from "vue";
import { api } from "@/lib/api";
import type { AuthUser } from "@/lib/types";

export const useAuthStore = defineStore("auth", () => {
  const user = ref<AuthUser | null>(null);
  const checked = ref(false);

  async function fetchMe() {
    try {
      user.value = await api.me();
    } catch {
      user.value = null;
    } finally {
      checked.value = true;
    }
  }

  async function login(email: string, password: string) {
    user.value = await api.login(email, password);
  }

  async function register(email: string, password: string) {
    user.value = await api.register(email, password);
  }

  async function logout() {
    try {
      await api.logout();
    } catch {
      /* ignore */
    }
    user.value = null;
  }

  return { user, checked, fetchMe, login, register, logout };
});
