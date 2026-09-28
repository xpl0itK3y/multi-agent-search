<script setup lang="ts">
import { ref, useId } from "vue";
import { useRoute, useRouter } from "vue-router";
import { useI18n } from "vue-i18n";
import { api, apiErrorMessage, isReauthRequired, PASSWORD_MIN_LENGTH } from "@/lib/api";
import { googleReauthErrorKey, REAUTH_ERROR_PARAM } from "@/lib/googleSignIn";
import { useAuthStore } from "@/stores/auth";
import SparkLogo from "@/components/SparkLogo.vue";
import GoogleReauthNotice from "@/components/GoogleReauthNotice.vue";
import PasswordRuleHint from "@/components/PasswordRuleHint.vue";

// Shown right after a first-time Google sign-in: let the user set a password so
// they can also log in with email + password next time (optional — skippable).
const route = useRoute();
const router = useRouter();
const auth = useAuthStore();
const { t } = useI18n();

const password = ref("");
const confirm = ref("");
const busy = ref(false);
const error = ref<string | null>(null);
// That sign-in must be recent: coming back to this page later (or from an old tab)
// the server asks for a new Google sign-in, which returns here.
const needsReauth = ref(false);
const pwHelpId = useId();
// Back from that sign-in without it (cancelled, or another Google account): the router
// returns here with ?reauth_error=<code> (lib/googleSignIn.ts) and we say why.
const reauthErrorKey = ref(googleReauthErrorKey(route.query[REAUTH_ERROR_PARAM]));

async function submit() {
  if (busy.value) return;
  reauthErrorKey.value = null;
  if (password.value.length < PASSWORD_MIN_LENGTH) {
    error.value = "min6";
    return;
  }
  if (password.value !== confirm.value) {
    error.value = "mismatch";
    return;
  }
  busy.value = true;
  error.value = null;
  needsReauth.value = false;
  try {
    await api.setPassword(password.value);
    router.push("/");
  } catch (e) {
    if (isReauthRequired(e)) needsReauth.value = true;
    else error.value = apiErrorMessage(e, t);
  } finally {
    busy.value = false;
  }
}

function skip() {
  router.push("/");
}
</script>

<template>
  <div class="flex h-full flex-col overflow-y-auto px-6 py-8">
    <!-- Centred with an auto margin inside the scroller: taller content stays reachable. -->
    <div class="my-auto w-full max-w-sm self-center">
      <div class="mb-2 flex items-center justify-center gap-3">
        <SparkLogo :size="28" />
        <h1 class="font-serif text-2xl text-ink">{{ $t("setPassword.title") }}</h1>
      </div>
      <p class="mb-6 text-center text-sm text-muted">
        {{ $t("setPassword.subtitle", { email: auth.user?.email ?? "" }) }}
      </p>

      <!-- novalidate: minlength stays a hint for password managers, while the translated
           inline check (and the hint's red state) says what is wrong, not the browser's
           own bubble in the browser's language. -->
      <form class="space-y-3" novalidate @submit.prevent="submit">
        <div>
          <input
            v-model="password"
            type="password"
            autocomplete="new-password"
            :minlength="PASSWORD_MIN_LENGTH"
            :placeholder="$t('setPassword.password')"
            :aria-label="$t('setPassword.password')"
            :aria-describedby="pwHelpId"
            class="w-full rounded-lg border border-bd bg-surface px-3 py-3 text-sm text-ink placeholder:text-muted sm:py-2"
          />
          <PasswordRuleHint :id="pwHelpId" :password="password" :tried="error === 'min6'" />
        </div>
        <input
          v-model="confirm"
          type="password"
          autocomplete="new-password"
          :placeholder="$t('setPassword.confirm')"
          :aria-label="$t('setPassword.confirm')"
          class="w-full rounded-lg border border-bd bg-surface px-3 py-3 text-sm text-ink placeholder:text-muted sm:py-2"
        />
        <p v-if="error === 'min6'" role="alert" class="text-sm text-danger">{{ $t("setPassword.min6") }}</p>
        <p v-else-if="error === 'mismatch'" role="alert" class="text-sm text-danger">{{ $t("setPassword.mismatch") }}</p>
        <p v-else-if="error" role="alert" class="text-sm text-danger">{{ error }}</p>
        <GoogleReauthNotice v-if="needsReauth" return-to="/set-password" class="text-sm" />
        <GoogleReauthNotice
          v-else-if="reauthErrorKey"
          return-to="/set-password"
          :message="t(reauthErrorKey)"
          class="text-sm"
        />
        <button
          type="submit"
          :disabled="busy"
          class="press w-full rounded-lg bg-accent px-4 py-3 text-sm font-medium text-onAccent disabled:opacity-50 sm:py-2"
        >
          {{ $t("setPassword.save") }}
        </button>
      </form>

      <button type="button" class="mt-4 w-full text-center text-sm text-muted hover:text-ink" @click="skip">
        {{ $t("setPassword.skip") }}
      </button>
    </div>
  </div>
</template>
