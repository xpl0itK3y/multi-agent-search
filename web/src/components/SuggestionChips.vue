<script setup lang="ts">
const emit = defineEmits<{ pick: [string] }>();

const chips: { icon: string; label: string; template: string }[] = [
  { icon: "◎", label: "Обзор рынка", template: "Проведи обзор рынка: " },
  { icon: "⇄", label: "Сравнение", template: "Сравни и выбери лучшее: " },
  { icon: "❝", label: "Лит-обзор", template: "Сделай обзор исследований по теме: " },
  { icon: "↗", label: "Из ссылки", template: "Исследуй на основе источника: " },
];
</script>

<template>
  <div class="flex flex-wrap items-center justify-center gap-2">
    <button
      v-for="chip in chips"
      :key="chip.label"
      class="flex items-center gap-2 rounded-full border border-bd bg-surface/60 px-3.5 py-2 text-sm text-muted hover:bg-surface hover:text-ink"
      @click="emit('pick', chip.template)"
    >
      <span class="text-accentSoft">{{ chip.icon }}</span>
      {{ chip.label }}
    </button>
  </div>
</template>
