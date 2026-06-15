<script setup lang="ts">
import { ref, watch } from "vue";

const props = defineProps<{ prompt: string; questions: string[]; busy?: boolean }>();
const emit = defineEmits<{ submit: [string[]] }>();

const answers = ref<string[]>([]);

watch(
  () => props.questions,
  (questions) => {
    answers.value = questions.map((_, i) => answers.value[i] ?? "");
  },
  { immediate: true },
);

function submit() {
  emit("submit", [...answers.value]);
}

function skip() {
  emit("submit", []);
}
</script>

<template>
  <div class="mx-auto w-full max-w-2xl px-6 py-10">
    <div class="mb-1 flex items-center gap-2 text-xs uppercase tracking-wide text-muted">
      <span class="text-accent">✶</span> {{ $t("clarify.tag") }}
    </div>
    <h1 v-if="prompt" class="mb-2 line-clamp-3 font-serif text-2xl leading-snug text-ink" :title="prompt">{{ prompt }}</h1>
    <p class="mb-6 text-sm text-muted">
      {{ $t("clarify.subtitle") }}
    </p>

    <div class="space-y-4">
      <div v-for="(question, i) in questions" :key="i">
        <label class="mb-1.5 block text-sm text-ink">{{ question }}</label>
        <input
          v-model="answers[i]"
          :placeholder="$t('clarify.answer')"
          class="w-full rounded-lg border border-bd bg-surface/50 px-3 py-2 text-sm text-ink placeholder:text-muted focus:outline-none focus:border-accent/40"
          @keydown.enter="submit"
        />
      </div>
    </div>

    <div class="mt-6 flex items-center justify-end gap-3">
      <button class="text-sm text-muted hover:text-ink" :disabled="busy" @click="skip">
        {{ $t("clarify.skip") }}
      </button>
      <button
        class="rounded-lg bg-accent px-4 py-2 text-sm font-medium text-bg transition disabled:cursor-not-allowed disabled:opacity-40"
        :disabled="busy"
        @click="submit"
      >
        <span v-if="busy">{{ $t("clarify.busy") }}</span>
        <span v-else>{{ $t("clarify.continue") }}</span>
      </button>
    </div>
  </div>
</template>
