<script setup lang="ts">
import { onMounted, ref } from "vue";
import { useI18n } from "vue-i18n";
import { api, apiErrorMessage, isVerificationTokenInvalid } from "@/lib/api";
import { takeLinkToken } from "@/lib/linkToken";
import { useAuthStore } from "@/stores/auth";
import AuthScreen from "@/components/AuthScreen.vue";

// Opened from a verification email: /verify-email#token=<token>. The router already took
// the token out of the address (lib/linkToken.ts). The page confirms the address as soon
// as it opens, once; it needs no session. A link opened again has no token and is as
// dead as an expired one: a new link is sent from Settings.
const auth = useAuthStore();
const { t } = useI18n();

const token = takeLinkToken("verify-email");
const state = ref<"verifying" | "verified" | "invalid" | "failed">(token ? "verifying" : "invalid");
const error = ref<string | null>(null);
let busy = false;

async function verify() {
  if (!token || busy) return;
  busy = true;
  state.value = "verifying";
  error.value = null;
  try {
    await api.verifyEmail(token);
    // Whoever is signed in here sees the new status (e.g. in Settings) at once.
    await auth.refreshUser();
    state.value = "verified";
  } catch (e) {
    if (isVerificationTokenInvalid(e)) {
      state.value = "invalid";
    } else {
      // Not a verdict on the link (a 429, the network): the same token can be tried again.
      error.value = apiErrorMessage(e, t);
      state.value = "failed";
    }
  } finally {
    busy = false;
  }
}

onMounted(verify);
</script>

<template>
  <AuthScreen :title="$t('verifyEmail.title')">
    <p v-if="state === 'verifying'" role="status" class="text-center text-sm text-muted">
      {{ $t("verifyEmail.verifying") }}
    </p>

    <template v-else-if="state === 'verified'">
      <p role="status" class="rounded-lg border border-emerald-500/30 bg-emerald-500/10 p-3 text-sm leading-relaxed text-emerald-300">
        {{ $t("verifyEmail.verified") }}
      </p>
      <router-link
        :to="auth.user ? '/' : '/login'"
        class="mt-4 block w-full rounded-lg bg-accent px-4 py-2 text-center text-sm font-medium text-bg transition"
      >
        {{ auth.user ? $t("verifyEmail.continue") : $t("verifyEmail.toLogin") }}
      </router-link>
    </template>

    <template v-else-if="state === 'invalid'">
      <p role="alert" class="rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-sm leading-relaxed text-red-400">
        {{ $t("errors.api.verificationTokenInvalid") }}
      </p>
      <router-link
        :to="auth.user ? '/settings' : '/login'"
        class="mt-4 block w-full rounded-lg bg-accent px-4 py-2 text-center text-sm font-medium text-bg transition"
      >
        {{ auth.user ? $t("verifyEmail.toSettings") : $t("verifyEmail.toLogin") }}
      </router-link>
    </template>

    <template v-else>
      <p role="alert" class="rounded-lg border border-red-500/30 bg-red-500/10 p-3 text-sm leading-relaxed text-red-400">
        {{ error }}
      </p>
      <button
        type="button"
        class="mt-4 w-full rounded-lg bg-accent px-4 py-2 text-sm font-medium text-bg transition"
        @click="verify"
      >
        {{ $t("verifyEmail.retry") }}
      </button>
    </template>
  </AuthScreen>
</template>
