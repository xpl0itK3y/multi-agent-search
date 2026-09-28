<script setup lang="ts">
import { computed } from "vue";
import { PASSWORD_MIN_LENGTH } from "@/lib/api";

// The password rule, said before it is broken (apple-design §16: validate inline, not on
// submit): muted until the password meets it, then a quiet check; red after a submit it
// refused. The field links to it with aria-describedby="<id>".
const props = defineProps<{ id: string; password: string; tried?: boolean }>();
const ok = computed(() => props.password.length >= PASSWORD_MIN_LENGTH);
</script>

<template>
  <p :id="id" class="mt-1 text-2xs" :class="ok ? 'text-success' : tried ? 'text-danger' : 'text-muted'">
    <template v-if="ok"><span aria-hidden="true">✓ </span>{{ $t("auth.passwordOk") }}</template>
    <template v-else>{{ $t("auth.passwordRule", { min: PASSWORD_MIN_LENGTH }) }}</template>
  </p>
</template>
