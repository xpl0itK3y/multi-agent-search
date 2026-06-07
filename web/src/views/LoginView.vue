<script setup lang="ts">
import { ref } from "vue";
import { useRouter } from "vue-router";
import { useAuthStore } from "@/stores/auth";
import SparkLogo from "@/components/SparkLogo.vue";

const router = useRouter();
const auth = useAuthStore();

const mode = ref<"login" | "register">("login");
const email = ref("");
const password = ref("");
const busy = ref(false);
const error = ref<string | null>(null);

async function submit() {
  if (busy.value) return;
  busy.value = true;
  error.value = null;
  try {
    if (mode.value === "login") await auth.login(email.value.trim(), password.value);
    else await auth.register(email.value.trim(), password.value);
    router.push("/");
  } catch (e) {
    error.value = (e as Error).message;
  } finally {
    busy.value = false;
  }
}
</script>

<template>
  <div class="flex h-full items-center justify-center overflow-y-auto px-6">
    <div class="w-full max-w-sm">
      <div class="mb-6 flex items-center justify-center gap-3">
        <SparkLogo :size="28" />
        <h1 class="font-serif text-2xl text-ink">
          {{ mode === "login" ? $t("auth.loginTitle") : $t("auth.registerTitle") }}
        </h1>
      </div>

      <form class="space-y-3" @submit.prevent="submit">
        <input
          v-model="email"
          type="email"
          :placeholder="$t('auth.email')"
          autocomplete="email"
          class="w-full rounded-lg border border-bd bg-surface px-3 py-2 text-sm text-ink placeholder:text-muted focus:border-accent/40 focus:outline-none"
        />
        <input
          v-model="password"
          type="password"
          :placeholder="$t('auth.password')"
          :autocomplete="mode === 'login' ? 'current-password' : 'new-password'"
          class="w-full rounded-lg border border-bd bg-surface px-3 py-2 text-sm text-ink placeholder:text-muted focus:border-accent/40 focus:outline-none"
        />
        <p v-if="error" class="text-sm text-red-400">{{ error }}</p>
        <button
          type="submit"
          :disabled="busy"
          class="w-full rounded-lg bg-accent px-4 py-2 text-sm font-medium text-bg transition disabled:opacity-50"
        >
          {{ mode === "login" ? $t("auth.login") : $t("auth.register") }}
        </button>
      </form>

      <button
        class="mt-4 w-full text-center text-sm text-muted hover:text-ink"
        @click="mode = mode === 'login' ? 'register' : 'login'"
      >
        {{ mode === "login" ? $t("auth.toRegister") : $t("auth.toLogin") }}
      </button>
    </div>
  </div>
</template>
