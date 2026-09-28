<script setup lang="ts">
import { useUiStore } from "@/stores/ui";
import SparkLogo from "@/components/SparkLogo.vue";

// The frame of the account pages a signed-out visitor reaches from the sign-in page or
// an emailed link (password reset, email verification): the language picker, the logo
// with the page's title, and the page's content. It centres with an auto margin inside
// the scroller, so content taller than the screen stays reachable from the top.
defineProps<{ title: string }>();
const ui = useUiStore();
</script>

<template>
  <div class="flex h-full flex-col overflow-y-auto px-6 py-8">
    <div class="my-auto w-full max-w-sm self-center">
      <div class="mb-4 flex justify-end">
        <div class="flex items-center gap-0.5 rounded-xl border border-bd bg-surface/70 p-1 font-mono text-xs">
          <!-- The touch hit area grows up and down only, so it never covers a neighbour. -->
          <button
            v-for="loc in (['ru', 'en', 'es'] as const)"
            :key="loc"
            type="button"
            class="press hit rounded-lg px-2.5 py-1 text-2xs font-semibold uppercase after:inset-x-0"
            :class="ui.locale === loc ? 'bg-accent text-onAccent shadow' : 'text-muted hover:text-ink'"
            :aria-pressed="ui.locale === loc ? 'true' : 'false'"
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
