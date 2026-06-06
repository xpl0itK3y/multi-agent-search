<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useResearchStore } from "@/stores/research";
import type { Depth } from "@/lib/types";

const prompt = defineModel<string>("prompt", { default: "" });

const props = defineProps<{ busy?: boolean }>();
const emit = defineEmits<{
  submit: [{ prompt: string; depth: Depth; model: string; planFirst: boolean }];
}>();

const store = useResearchStore();

const depths: Depth[] = ["easy", "medium", "hard"];
const depth = ref<Depth>("medium");
const planFirst = ref(true);

const model = ref<string>("");
watch(
  () => store.defaultModelId,
  (id) => {
    if (!model.value && id) model.value = id;
  },
  { immediate: true },
);

const canSubmit = computed(() => prompt.value.trim().length >= 5 && !props.busy);

function submit() {
  if (!canSubmit.value) return;
  emit("submit", {
    prompt: prompt.value.trim(),
    depth: depth.value,
    model: model.value,
    planFirst: planFirst.value,
  });
}

function onKeydown(e: KeyboardEvent) {
  if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
    e.preventDefault();
    submit();
  }
}
</script>

<template>
  <div class="w-full max-w-composer rounded-card border border-bd bg-surface px-4 py-3 shadow-lg">
    <textarea
      v-model="prompt"
      rows="2"
      :placeholder="$t('composer.placeholder')"
      class="block w-full resize-none bg-transparent text-[15px] leading-relaxed text-ink placeholder:text-muted focus:outline-none"
      @keydown="onKeydown"
    />

    <div class="mt-2 flex items-center justify-between gap-2">
      <div class="flex items-center gap-2">
        <button class="grid h-8 w-8 place-items-center rounded-full border border-bd text-muted hover:text-ink" :title="$t('composer.attach')">
          +
        </button>
        <button
          class="flex items-center gap-1.5 rounded-full border px-3 py-1.5 text-sm transition"
          :class="planFirst ? 'border-accent/40 bg-accent/15 text-accentSoft' : 'border-bd text-muted hover:text-ink'"
          :title="planFirst ? $t('composer.planOn') : $t('composer.planOff')"
          @click="planFirst = !planFirst"
        >
          <span>◳</span> {{ $t("composer.plan") }}
        </button>
      </div>

      <div class="flex items-center gap-2">
        <!-- Model selector -->
        <select
          v-model="model"
          class="cursor-pointer rounded-lg border border-bd bg-transparent px-2 py-1.5 text-sm text-muted hover:text-ink focus:outline-none"
          :title="$t('composer.model')"
        >
          <option v-if="!store.models.length" :value="''">{{ $t("composer.model") }}</option>
          <option v-for="m in store.models" :key="m.id" :value="m.id">{{ m.label }}</option>
        </select>

        <!-- Depth selector -->
        <select
          v-model="depth"
          class="cursor-pointer rounded-lg border border-bd bg-transparent px-2 py-1.5 text-sm text-muted hover:text-ink focus:outline-none"
          :title="$t('composer.depth')"
        >
          <option v-for="d in depths" :key="d" :value="d">{{ $t("depth." + d) }}</option>
        </select>

        <!-- Send -->
        <button
          class="grid h-8 w-8 place-items-center rounded-full bg-accent text-bg transition disabled:cursor-not-allowed disabled:opacity-40"
          :disabled="!canSubmit"
          :title="$t('composer.runTitle')"
          @click="submit"
        >
          <span v-if="props.busy" class="animate-pulse">…</span>
          <span v-else>↑</span>
        </button>
      </div>
    </div>
  </div>
</template>
