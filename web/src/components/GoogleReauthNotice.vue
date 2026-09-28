<script setup lang="ts">
import { startGoogleSignIn } from "@/lib/googleSignIn";

// Shown when the server answered `reauth_required` (see isReauthRequired): the user
// signs in with Google again and comes back to `returnTo`. `message` is the translated
// explanation of the refused action; it defaults to the set-password one.
const props = defineProps<{ returnTo: string; message?: string }>();
</script>

<template>
  <div role="alert" class="space-y-2.5 rounded-lg border border-warning/30 bg-warning/10 p-3 text-warning">
    <p class="leading-relaxed">{{ props.message ?? $t("errors.api.reauthRequired") }}</p>
    <button
      type="button"
      class="press rounded-lg border border-warning/40 bg-surface px-3 py-1.5 font-medium text-ink hover:border-accent/40"
      @click="startGoogleSignIn(props.returnTo)"
    >
      {{ $t("auth.reauthGoogle") }}
    </button>
  </div>
</template>
