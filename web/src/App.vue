<script setup lang="ts">
import { onMounted } from "vue";
import AppSidebar from "@/components/AppSidebar.vue";
import { useResearchStore } from "@/stores/research";

const store = useResearchStore();

onMounted(() => {
  store.fetchModels();
  store.fetchHistory();
});
</script>

<template>
  <div class="flex h-screen w-screen overflow-hidden bg-bg text-ink">
    <AppSidebar />
    <main class="min-h-0 flex-1 overflow-hidden">
      <router-view />
    </main>
  </div>
</template>
