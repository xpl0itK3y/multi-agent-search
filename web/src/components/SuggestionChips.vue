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
    <button
      v-for="chip in chips"
      :key="chip.key"
      class="flex items-center gap-2 rounded-full border border-bd bg-surface/60 px-3.5 py-2 text-sm text-muted hover:bg-surface hover:text-ink"
      @click="emit('pick', t(`chips.${chip.key}.template`))"
    >
      <span class="text-accentSoft">{{ chip.icon }}</span>
      {{ t(`chips.${chip.key}.label`) }}
    </button>
  </div>
</template>
