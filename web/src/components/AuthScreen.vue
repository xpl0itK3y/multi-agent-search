<script setup lang="ts">
import { useUiStore } from "@/stores/ui";
import SparkLogo from "@/components/SparkLogo.vue";

// The frame of the account pages a signed-out visitor reaches from the sign-in page or
// an emailed link (password reset, email verification): the language picker, the logo
// with the page's title, and the page's content.
defineProps<{ title: string }>();
const ui = useUiStore();
</script>

<template>
  <div class="flex h-full items-center justify-center overflow-y-auto px-6">
    <div class="w-full max-w-sm">
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

      <div class="mb-6 flex items-center justify-center gap-3">
        <SparkLogo :size="28" />
        <h1 class="font-serif text-2xl text-ink">{{ title }}</h1>
      </div>

      <slot />
    </div>
  </div>
</template>
