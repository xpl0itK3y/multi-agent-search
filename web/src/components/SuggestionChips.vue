<script setup lang="ts">
import { useI18n } from "vue-i18n";

const emit = defineEmits<{ pick: [string] }>();
const { t } = useI18n();

const chips: { icon: string; key: string }[] = [
  { icon: "◎", key: "market" },
  { icon: "⇄", key: "compare" },
  { icon: "❝", key: "lit" },
  { icon: "↗", key: "url" },
];
</script>

<template>
  <div class="flex flex-wrap items-center justify-center gap-2">
    <!-- .press owns the transition: an instant dip on press, a lift on real hover only
         (and none under reduced motion). -->
    <button
      v-for="chip in chips"
      :key="chip.key"
      type="button"
      class="press flex items-center gap-2 rounded-full border border-bd bg-surface/60 px-3.5 py-2 text-sm text-muted hover:border-accentSoft/50 hover:bg-surface hover:text-ink hover:shadow-sm motion-safe:hover:-translate-y-0.5"
      @click="emit('pick', t(`chips.${chip.key}.template`))"
    >
      <span class="text-accentSoft" aria-hidden="true">{{ chip.icon }}</span>
      {{ t(`chips.${chip.key}.label`) }}
    </button>
  </div>
</template>
