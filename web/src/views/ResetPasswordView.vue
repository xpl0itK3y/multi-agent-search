<script setup lang="ts">
import { onBeforeUnmount, ref, useId } from "vue";
import { useI18n } from "vue-i18n";
import { apiErrorMessage, isResetTokenInvalid, PASSWORD_MIN_LENGTH } from "@/lib/api";
import { onLinkToken, takeLinkToken } from "@/lib/linkToken";
import { useAuthStore } from "@/stores/auth";
import AuthScreen from "@/components/AuthScreen.vue";
import PasswordRuleHint from "@/components/PasswordRuleHint.vue";

// Opened from a password reset email: /reset-password#token=<token>. The token was taken
// out of the address before the app started (lib/linkToken.ts). A page without one (a
// reload, Back, a link copied without its fragment) knows nothing about the link, which
// may well still work: it asks to open the link from the email again, and only a 400
// from the server says the link is dead.
const auth = useAuthStore();
const { t } = useI18n();

const token = ref(takeLinkToken("reset-password"));
const state = ref<"form" | "done" | "invalid" | "nolink">(token.value ? "form" : "nolink");
const password = ref("");
const confirm = ref("");
const busy = ref(false);
const error = ref<string | null>(null);
const pwHelpId = useId();
// A save refused here for a too-short password: the rule line under the field turns red.
const passwordTried = ref(false);

// A link opened again into this tab while the page is open mounts nothing new: start over
// with its token.
const stopListening = onLinkToken("reset-password", () => {
  const next = takeLinkToken("reset-password");
  if (!next) return;
  token.value = next;
  state.value = "form";
  password.value = "";
  confirm.value = "";
  error.value = null;
  passwordTried.value = false;
});
onBeforeUnmount(stopListening);

async function submit() {
  const current = token.value;
  if (busy.value || !current) return;
  if (password.value.length < PASSWORD_MIN_LENGTH) {
    passwordTried.value = true;
    error.value = t("resetPassword.tooShort", { min: PASSWORD_MIN_LENGTH });
    return;
  }
  if (password.value !== confirm.value) {
    error.value = t("resetPassword.mismatch");
    return;
  }
  busy.value = true;
  error.value = null;
  try {
    // Signs out whoever was signed in here: the server revoked every session.
    await auth.resetPassword(current, password.value);
    if (token.value !== current) return; // a newer link took over meanwhile
    state.value = "done";
    password.value = "";
    confirm.value = "";
  } catch (e) {
    if (token.value !== current) return;
    if (isResetTokenInvalid(e)) state.value = "invalid";
    else error.value = apiErrorMessage(e, t);
  } finally {
    busy.value = false;
  }
}
</script>

<template>
  <AuthScreen :title="$t('resetPassword.title')">
    <template v-if="state === 'done'">
      <p role="status" class="rounded-lg border border-success/30 bg-success/10 p-3 text-sm leading-relaxed text-success">
        {{ $t("resetPassword.done") }}
      </p>
      <router-link
        to="/login"
        class="press mt-4 block w-full rounded-lg bg-accent px-4 py-3 text-center text-sm font-medium text-onAccent sm:py-2"
      >
        {{ $t("resetPassword.toLogin") }}
      </router-link>
    </template>

    <template v-else-if="state === 'invalid'">
      <p role="alert" class="rounded-lg border border-danger/30 bg-danger/10 p-3 text-sm leading-relaxed text-danger">
        {{ $t("errors.api.resetTokenInvalid") }}
      </p>
      <router-link
        to="/forgot-password"
        class="press mt-4 block w-full rounded-lg bg-accent px-4 py-3 text-center text-sm font-medium text-onAccent sm:py-2"
      >
        {{ $t("resetPassword.requestNew") }}
      </router-link>
      <router-link to="/login" class="mt-4 block text-center text-sm text-muted hover:text-ink">
        {{ $t("forgotPassword.backToLogin") }}
      </router-link>
    </template>

    <template v-else-if="state === 'nolink'">
      <p role="status" class="rounded-lg border border-bd bg-surface p-3 text-sm leading-relaxed text-muted">
        {{ $t("resetPassword.noLink") }}
      </p>
      <!-- Secondary: a new link costs one of the hourly sends and retires the emailed one. -->
      <router-link
        to="/forgot-password"
        class="press mt-4 block w-full rounded-lg border border-bd px-4 py-3 text-center text-sm font-medium text-ink hover:border-accent/40 sm:py-2"
      >
        {{ $t("resetPassword.requestNew") }}
      </router-link>
      <router-link to="/login" class="mt-4 block text-center text-sm text-muted hover:text-ink">
        {{ $t("forgotPassword.backToLogin") }}
      </router-link>
    </template>

    <template v-else>
      <p class="mb-4 text-center text-sm text-muted">{{ $t("resetPassword.subtitle") }}</p>
      <!-- novalidate: see SetPasswordView — the translated inline rule, not the browser's bubble. -->
      <form class="space-y-3" novalidate @submit.prevent="submit">
        <div>
          <input
            v-model="password"
            type="password"
            autocomplete="new-password"
            :minlength="PASSWORD_MIN_LENGTH"
            :placeholder="$t('resetPassword.password')"
            :aria-label="$t('resetPassword.password')"
            :aria-describedby="pwHelpId"
            class="w-full rounded-lg border border-bd bg-surface px-3 py-3 text-sm text-ink placeholder:text-muted sm:py-2"
          />
          <PasswordRuleHint :id="pwHelpId" :password="password" :tried="passwordTried" />
        </div>
        <input
          v-model="confirm"
          type="password"
          autocomplete="new-password"
          :placeholder="$t('resetPassword.confirm')"
          :aria-label="$t('resetPassword.confirm')"
          class="w-full rounded-lg border border-bd bg-surface px-3 py-3 text-sm text-ink placeholder:text-muted sm:py-2"
        />
        <p v-if="error" role="alert" class="text-sm text-danger">{{ error }}</p>
        <button
          type="submit"
          :disabled="busy"
          class="press w-full rounded-lg bg-accent px-4 py-3 text-sm font-medium text-onAccent disabled:opacity-50 sm:py-2"
        >
          {{ busy ? $t("resetPassword.saving") : $t("resetPassword.save") }}
        </button>
      </form>
    </template>
  </AuthScreen>
</template>
