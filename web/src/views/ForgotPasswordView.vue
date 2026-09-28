<script setup lang="ts">
import { onMounted, ref } from "vue";
import { useI18n } from "vue-i18n";
import { api, apiErrorMessage } from "@/lib/api";
import AuthScreen from "@/components/AuthScreen.vue";

// Asks for a password reset link. The server answers the same whether or not an account
// has this email, so the page never tells either: "if an account exists, we sent it".
const { t } = useI18n();

const email = ref("");
const busy = ref(false);
const sent = ref(false);
const error = ref<string | null>(null);
// Whether this server can send the link at all (it has an email backend); null while
// unknown. Without one the request is still accepted, but no email would ever arrive.
const available = ref<boolean | null>(null);

onMounted(async () => {
  try {
    available.value = Boolean((await api.authConfig())?.password_reset);
  } catch {
    // Unknown: keep the form, the server answers it all the same.
  }
});

async function submit() {
  const address = email.value.trim();
  if (busy.value || !address) return;
  busy.value = true;
  error.value = null;
  try {
    await api.forgotPassword(address);
    sent.value = true;
  } catch (e) {
    // A 429 (asked too often for this address or from here) and network errors; the
    // server refuses those whether or not the account exists.
    error.value = apiErrorMessage(e, t);
  } finally {
    busy.value = false;
  }
}
</script>

<template>
  <AuthScreen :title="$t('forgotPassword.title')">
    <p v-if="available === false" role="alert" class="rounded-lg border border-warning/30 bg-warning/10 p-3 text-sm text-warning">
      {{ $t("forgotPassword.unavailable") }}
    </p>

    <template v-else-if="sent">
      <p role="status" class="rounded-lg border border-success/30 bg-success/10 p-3 text-sm leading-relaxed text-success">
        {{ $t("forgotPassword.sent") }}
      </p>
      <button type="button" class="mt-3 w-full text-center text-sm text-muted hover:text-ink" @click="sent = false">
        {{ $t("forgotPassword.again") }}
      </button>
    </template>

    <template v-else>
      <p class="mb-4 text-center text-sm text-muted">{{ $t("forgotPassword.subtitle") }}</p>
      <form class="space-y-3" @submit.prevent="submit">
        <input
          v-model="email"
          type="email"
          required
          autocomplete="email"
          :placeholder="$t('auth.email')"
          :aria-label="$t('auth.email')"
          class="w-full rounded-lg border border-bd bg-surface px-3 py-3 text-sm text-ink placeholder:text-muted sm:py-2"
        />
        <p v-if="error" role="alert" class="text-sm text-danger">{{ error }}</p>
        <button
          type="submit"
          :disabled="busy"
          class="press w-full rounded-lg bg-accent px-4 py-3 text-sm font-medium text-onAccent disabled:opacity-50 sm:py-2"
        >
          {{ busy ? $t("forgotPassword.sending") : $t("forgotPassword.submit") }}
        </button>
      </form>
    </template>

    <router-link to="/login" class="mt-4 block text-center text-sm text-muted hover:text-ink">
      {{ $t("forgotPassword.backToLogin") }}
    </router-link>
  </AuthScreen>
</template>
