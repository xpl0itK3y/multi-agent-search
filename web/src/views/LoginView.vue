<script setup lang="ts">
import { onMounted, ref } from "vue";
import { useI18n } from "vue-i18n";
import { useRoute, useRouter } from "vue-router";
import { api, apiErrorMessage } from "@/lib/api";
import { useAuthStore } from "@/stores/auth";
import { useUiStore } from "@/stores/ui";
import SparkLogo from "@/components/SparkLogo.vue";

const route = useRoute();
const router = useRouter();
const auth = useAuthStore();
const ui = useUiStore();
const { t } = useI18n();

const mode = ref<"login" | "register">("login");
const email = ref("");
const password = ref("");
const busy = ref(false);
const error = ref<string | null>(null);
const googleEnabled = ref(false);

// A failed Google callback redirects here as /login?error=<code>. Keep the key
// (not the text) so the message follows a language switch on this page.
const OAUTH_ERRORS: Record<string, string> = {
  oauth_conflict: "auth.oauthConflict",
  oauth_failed: "auth.oauthFailed",
};
const oauthErrorKey = ref<string | null>(OAUTH_ERRORS[String(route.query.error ?? "")] ?? null);

async function loadAuthConfig(retries = 2) {
  try {
    googleEnabled.value = Boolean((await api.authConfig())?.google_oauth);
  } catch {
    if (retries > 0) {
      setTimeout(() => loadAuthConfig(retries - 1), 1000);
    }
  }
}

onMounted(() => {
  loadAuthConfig();
});

function googleLogin() {
  window.location.href = api.googleLoginUrl();
}

async function submit() {
  if (busy.value) return;
  busy.value = true;
  error.value = null;
  oauthErrorKey.value = null;
  try {
    if (mode.value === "login") await auth.login(email.value.trim(), password.value);
    else await auth.register(email.value.trim(), password.value);
    const redirect = (route.query.redirect as string) || "/";
    router.push(redirect);
  } catch (e) {
    error.value = apiErrorMessage(e, t);
  } finally {
    busy.value = false;
  }
}
</script>

<template>
  <div class="flex h-full items-center justify-center overflow-y-auto px-6">
    <div class="w-full max-w-sm">
      <!-- Language Switcher -->
      <div class="mb-4 flex justify-end">
        <div class="flex items-center gap-0.5 rounded-xl border border-bd bg-surface/70 p-1 text-xs font-mono">
          <button
            v-for="loc in (['ru', 'en', 'es'] as const)"
            :key="loc"
            type="button"
            class="rounded-lg px-2.5 py-1 text-[10.5px] font-bold uppercase transition"
            :class="ui.locale === loc ? 'bg-accent text-white shadow' : 'text-muted hover:text-ink'"
            @click="ui.setLocale(loc)"
          >
            {{ loc }}
          </button>
        </div>
      </div>

      <!-- Admin redirect notification -->
      <div
        v-if="route.query.redirect === '/admin'"
        class="mb-5 space-y-1.5 rounded-xl border border-accent/30 bg-accent/10 px-3.5 py-2.5 text-xs text-accent"
      >
        <div class="flex items-center gap-2.5">
          <span class="text-base">🛡️</span>
          <span class="font-medium">{{ $t("admin.loginPrompt") }}</span>
        </div>
        <p class="leading-relaxed text-accent/80">{{ $t("admin.authPasswordHint") }}</p>
      </div>

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
        <p v-else-if="oauthErrorKey" class="text-sm text-red-400">{{ $t(oauthErrorKey) }}</p>
        <button
          type="submit"
          :disabled="busy"
          class="w-full rounded-lg bg-accent px-4 py-2 text-sm font-medium text-bg transition disabled:opacity-50"
        >
          {{ mode === "login" ? $t("auth.login") : $t("auth.register") }}
        </button>
      </form>

      <template v-if="googleEnabled">
        <div class="my-4 flex items-center gap-3 text-xs text-muted">
          <span class="h-px flex-1 bg-bd" />{{ $t("auth.or") }}<span class="h-px flex-1 bg-bd" />
        </div>
        <button
          type="button"
          class="flex w-full items-center justify-center gap-2 rounded-lg border border-bd bg-surface px-4 py-2 text-sm font-medium text-ink transition hover:border-accent/40"
          @click="googleLogin"
        >
          <svg width="16" height="16" viewBox="0 0 48 48" aria-hidden="true">
            <path fill="#EA4335" d="M24 9.5c3.54 0 6.71 1.22 9.21 3.6l6.85-6.85C35.9 2.38 30.47 0 24 0 14.62 0 6.51 5.38 2.56 13.22l7.98 6.19C12.43 13.72 17.74 9.5 24 9.5z"/>
            <path fill="#4285F4" d="M46.98 24.55c0-1.57-.15-3.09-.38-4.55H24v9.02h12.94c-.58 2.96-2.26 5.48-4.78 7.18l7.73 6c4.51-4.18 7.09-10.36 7.09-17.65z"/>
            <path fill="#FBBC05" d="M10.53 28.59c-.48-1.45-.76-2.99-.76-4.59s.27-3.14.76-4.59l-7.98-6.19C.92 16.46 0 20.12 0 24c0 3.88.92 7.54 2.56 10.78l7.97-6.19z"/>
            <path fill="#34A853" d="M24 48c6.48 0 11.93-2.13 15.89-5.81l-7.73-6c-2.15 1.45-4.92 2.3-8.16 2.3-6.26 0-11.57-4.22-13.47-9.91l-7.98 6.19C6.51 42.62 14.62 48 24 48z"/>
          </svg>
          {{ $t("auth.google") }}
        </button>
      </template>

      <button
        class="mt-4 w-full text-center text-sm text-muted hover:text-ink"
        @click="mode = mode === 'login' ? 'register' : 'login'"
      >
        {{ mode === "login" ? $t("auth.toRegister") : $t("auth.toLogin") }}
      </button>
    </div>
  </div>
</template>
