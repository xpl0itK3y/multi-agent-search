<script setup lang="ts">
import { computed } from "vue";
import { useI18n } from "vue-i18n";

const emit = defineEmits<{ pick: [string] }>();
const { t } = useI18n();

interface ExampleItem {
  id: "ai" | "robotaxi" | "rag" | "biotech";
  icon: string;
}

const items: ExampleItem[] = [
  { id: "ai", icon: "🤖" },
  { id: "robotaxi", icon: "📈" },
  { id: "rag", icon: "⚡" },
  { id: "biotech", icon: "🧬" },
];

const examples = computed(() =>
  items.map((item) => ({
    id: item.id,
    icon: item.icon,
    tag: t(`home.examples.${item.id}.tag`),
    title: t(`home.examples.${item.id}.title`),
    prompt: t(`home.examples.${item.id}.prompt`),
  })),
);
</script>

<template>
  <div class="w-full max-w-composer">
    <div class="mb-3 flex items-center justify-between text-xs text-muted">
      <span class="flex items-center gap-1.5 font-medium tracking-wide uppercase">
        <span class="text-accentSoft">✦</span>
        {{ t("home.examplesTitle") }}
      </span>
    </div>
    <div class="grid grid-cols-1 gap-2.5 sm:grid-cols-2">
      <button
        v-for="item in examples"
        :key="item.id"
        type="button"
        class="group flex flex-col justify-between rounded-xl border border-bd/70 bg-surface/50 p-3.5 text-left transition-all duration-200 hover:-translate-y-0.5 hover:border-accent/40 hover:bg-surface hover:shadow-md"
        @click="emit('pick', item.prompt)"
      >
        <div>
          <div class="mb-1.5 flex items-center justify-between">
            <span class="inline-flex items-center gap-1.5 text-xs font-medium text-accentSoft">
              <span>{{ item.icon }}</span>
              <span>{{ item.tag }}</span>
            </span>
            <span class="opacity-0 transition-opacity duration-200 group-hover:opacity-100 text-muted group-hover:text-ink text-xs flex items-center gap-1">
              <span>{{ t("home.insertPrompt") }}</span>
              <span>↳</span>
            </span>
          </div>
          <h3 class="text-sm font-medium text-ink transition-colors group-hover:text-accent">
            {{ item.title }}
          </h3>
          <p class="mt-1 line-clamp-2 text-xs leading-relaxed text-muted group-hover:text-ink/80">
            {{ item.prompt }}
          </p>
        </div>
      </button>
    </div>
  </div>
</template>
