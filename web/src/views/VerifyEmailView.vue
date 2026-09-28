<script setup lang="ts">
import { onBeforeUnmount, ref } from "vue";
import { useI18n } from "vue-i18n";
import { useRouter } from "vue-router";
import { api, apiErrorMessage, isVerificationTokenInvalid, isVerificationWrongAccount } from "@/lib/api";
import { dropLinkToken, onLinkToken, peekLinkToken } from "@/lib/linkToken";
import { useAuthStore } from "@/stores/auth";
import AuthScreen from "@/components/AuthScreen.vue";

// Opened from a verification email: /verify-email#token=<token>. The token was taken out
// of the address before the app started, and waits in this tab (lib/linkToken.ts). The
// server confirms an address only for the signed-in account the link was sent to: that
// proves whoever holds the account's password also reads its inbox. So the router sends a
// signed-out visitor to sign in first, and back here. Nothing is confirmed on opening
// (a mail scanner that runs the page, or a curious click on a mail for a sign-up someone
// else made): the page names the account and waits for its "Confirm email" button.
const PAGE = "verify-email";
const LOGIN_FIRST = { name: "login", query: { redirect: "/verify-email" } };

const auth = useAuthStore();
const router = useRouter();
const { t } = useI18n();

const token = ref(peekLinkToken(PAGE));
const state = ref<"confirm" | "verifying" | "verified" | "invalid" | "wrongAccount" | "failed" | "nolink">(
  token.value ? "confirm" : "nolink",
);
const error = ref<string | null>(null);
let busy = false;

// A link opened again into this tab while the page is open mounts nothing new.
const stopListening = onLinkToken(PAGE, () => {
  token.value = peekLinkToken(PAGE);
  if (!token.value) return;
  state.value = "confirm";
  error.value = null;
});
onBeforeUnmount(stopListening);

async function confirm() {
  const current = token.value;
  if (!current || busy) return;
  busy = true;
  state.value = "verifying";
  error.value = null;
  try {
    await api.verifyEmail(current);
    dropLinkToken(PAGE, current);
    if (token.value !== current) return; // a newer link took over meanwhile
    token.value = null;
    // The new status shows at once (e.g. in Settings).
    await auth.refreshUser();
    state.value = "verified";
  } catch (e) {
    if (isVerificationTokenInvalid(e)) dropLinkToken(PAGE, current);
    if (token.value !== current) return;
    if (isVerificationTokenInvalid(e)) {
      token.value = null;
      state.value = "invalid";
    } else if (isVerificationWrongAccount(e)) {
      // The link is still good, for the account it was sent to: it stays for that one.
      state.value = "wrongAccount";
    } else {
      // Not a verdict on the link (a 429, the network): the same token can be tried again.
      error.value = apiErrorMessage(e, t);
      state.value = "failed";
    }
  } finally {
    busy = false;
  }
}

// Signs out (every device: that is what a logout does here) and comes back here after
// signing in to the right account, with the link still waiting.
async function switchAccount() {
  await auth.logout();
  await router.push(LOGIN_FIRST);
}
</script>

<template>
  <AuthScreen :title="$t('verifyEmail.title')">
    <!-- Only while a sign-out is on its way to /login: the router lets nobody else in. -->
    <template v-if="!auth.user">
      <p role="status" class="text-center text-sm text-muted">{{ $t("verifyEmail.signInFirst") }}</p>
      <router-link
        :to="LOGIN_FIRST"
        class="press mt-4 block w-full rounded-lg bg-accent px-4 py-3 text-center text-sm font-medium text-onAccent sm:py-2"
      >
        {{ $t("verifyEmail.toLogin") }}
      </router-link>
    </template>

    <template v-else-if="state === 'confirm'">
      <p class="text-center text-sm text-ink">{{ $t("verifyEmail.signedInAs", { email: auth.user.email }) }}</p>
      <p class="mt-2 text-center text-sm leading-relaxed text-muted">{{ $t("verifyEmail.confirmPrompt") }}</p>
      <button
        type="button"
        class="press mt-4 w-full rounded-lg bg-accent px-4 py-3 text-sm font-medium text-onAccent sm:py-2"
        @click="confirm"
      >
        {{ $t("verifyEmail.confirm") }}
      </button>
    </template>

    <p v-else-if="state === 'verifying'" role="status" class="text-center text-sm text-muted">
      {{ $t("verifyEmail.verifying") }}
    </p>

    <template v-else-if="state === 'verified'">
      <p role="status" class="rounded-lg border border-success/30 bg-success/10 p-3 text-sm leading-relaxed text-success">
        {{ $t("verifyEmail.verified") }}
      </p>
      <router-link
        to="/"
        class="press mt-4 block w-full rounded-lg bg-accent px-4 py-3 text-center text-sm font-medium text-onAccent sm:py-2"
      >
        {{ $t("verifyEmail.continue") }}
      </router-link>
    </template>

    <template v-else-if="state === 'invalid'">
      <p role="alert" class="rounded-lg border border-danger/30 bg-danger/10 p-3 text-sm leading-relaxed text-danger">
        {{ $t("errors.api.verificationTokenInvalid") }}
      </p>
      <router-link
        to="/settings"
        class="press mt-4 block w-full rounded-lg bg-accent px-4 py-3 text-center text-sm font-medium text-onAccent sm:py-2"
      >
        {{ $t("verifyEmail.toSettings") }}
      </router-link>
    </template>

    <template v-else-if="state === 'wrongAccount'">
      <p role="alert" class="rounded-lg border border-danger/30 bg-danger/10 p-3 text-sm leading-relaxed text-danger">
        {{ $t("errors.api.verificationWrongAccount") }}
      </p>
      <p class="mt-3 text-center text-sm text-muted">{{ $t("verifyEmail.signedInAs", { email: auth.user.email }) }}</p>
      <button
        type="button"
        class="press mt-4 w-full rounded-lg bg-accent px-4 py-3 text-sm font-medium text-onAccent sm:py-2"
        @click="switchAccount"
      >
        {{ $t("verifyEmail.switchAccount") }}
      </button>
      <p class="mt-2 text-center text-2xs text-muted">{{ $t("auth.logoutEverywhereHint") }}</p>
      <router-link to="/" class="mt-4 block text-center text-sm text-muted hover:text-ink">
        {{ $t("verifyEmail.continue") }}
      </router-link>
    </template>

    <template v-else-if="state === 'nolink'">
      <!-- Nothing is known about the link here (a reload after it was used, Back, a link
           copied without its fragment, or one that waited too long): no verdict on it. -->
      <template v-if="auth.user.email_verified">
        <p role="status" class="rounded-lg border border-success/30 bg-success/10 p-3 text-sm leading-relaxed text-success">
          {{ $t("verifyEmail.alreadyVerified") }}
        </p>
        <router-link
          to="/"
          class="press mt-4 block w-full rounded-lg bg-accent px-4 py-3 text-center text-sm font-medium text-onAccent sm:py-2"
        >
          {{ $t("verifyEmail.continue") }}
        </router-link>
      </template>
      <template v-else>
        <p role="status" class="rounded-lg border border-bd bg-surface p-3 text-sm leading-relaxed text-muted">
          {{ $t("verifyEmail.noLink") }}
        </p>
        <router-link
          to="/settings"
          class="press mt-4 block w-full rounded-lg border border-bd px-4 py-3 text-center text-sm font-medium text-ink hover:border-accent/40 sm:py-2"
        >
          {{ $t("verifyEmail.toSettings") }}
        </router-link>
      </template>
    </template>

    <template v-else>
      <p role="alert" class="rounded-lg border border-danger/30 bg-danger/10 p-3 text-sm leading-relaxed text-danger">
        {{ error }}
      </p>
      <button
        type="button"
        class="press mt-4 w-full rounded-lg bg-accent px-4 py-3 text-sm font-medium text-onAccent sm:py-2"
        @click="confirm"
      >
        {{ $t("verifyEmail.retry") }}
      </button>
    </template>
  </AuthScreen>
</template>
