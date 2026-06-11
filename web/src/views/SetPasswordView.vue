<script setup lang="ts">
import { ref } from "vue";
import { useRouter } from "vue-router";
import { api } from "@/lib/api";
import { useAuthStore } from "@/stores/auth";
import SparkLogo from "@/components/SparkLogo.vue";

// Shown right after a first-time Google sign-in: let the user set a password so
// they can also log in with email + password next time (optional — skippable).
const router = useRouter();
const auth = useAuthStore();

const password = ref("");
const confirm = ref("");
const busy = ref(false);
const error = ref<string | null>(null);

async function submit() {
  if (busy.value) return;
  if (password.value.length < 6) {
    error.value = "min6";
    return;
  }
  if (password.value !== confirm.value) {
    error.value = "mismatch";
    return;
  }
  busy.value = true;
  error.value = null;
  try {
    await api.setPassword(password.value);
    router.push("/");
  } catch (e) {
    error.value = (e as Error).message;
  } finally {
    busy.value = false;
  }
}

function skip() {
  router.push("/");
}
</script>

<template>
  <div class="flex h-full items-center justify-center overflow-y-auto px-6">
    <div class="w-full max-w-sm">
      <div class="mb-2 flex items-center justify-center gap-3">
        <SparkLogo :size="28" />
        <h1 class="font-serif text-2xl text-ink">{{ $t("setPassword.title") }}</h1>
      </div>
      <p class="mb-6 text-center text-sm text-muted">
        {{ $t("setPassword.subtitle", { email: auth.user?.email ?? "" }) }}
      </p>

      <form class="space-y-3" @submit.prevent="submit">
        <input
          v-model="password"
          type="password"
          autocomplete="new-password"
          :placeholder="$t('setPassword.password')"
          class="w-full rounded-lg border border-bd bg-surface px-3 py-2 text-sm text-ink placeholder:text-muted focus:border-accent/40 focus:outline-none"
        />
        <input
          v-model="confirm"
          type="password"
          autocomplete="new-password"
          :placeholder="$t('setPassword.confirm')"
          class="w-full rounded-lg border border-bd bg-surface px-3 py-2 text-sm text-ink placeholder:text-muted focus:border-accent/40 focus:outline-none"
        />
        <p v-if="error === 'min6'" class="text-sm text-red-400">{{ $t("setPassword.min6") }}</p>
        <p v-else-if="error === 'mismatch'" class="text-sm text-red-400">{{ $t("setPassword.mismatch") }}</p>
        <p v-else-if="error" class="text-sm text-red-400">{{ error }}</p>
        <button
          type="submit"
          :disabled="busy"
          class="w-full rounded-lg bg-accent px-4 py-2 text-sm font-medium text-bg transition disabled:opacity-50"
        >
          {{ $t("setPassword.save") }}
        </button>
      </form>

      <button class="mt-4 w-full text-center text-sm text-muted hover:text-ink" @click="skip">
        {{ $t("setPassword.skip") }}
      </button>
    </div>
  </div>
</template>
