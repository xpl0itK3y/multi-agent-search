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
    <main class="flex-1 overflow-y-auto">
      <router-view />
    </main>
  </div>
</template>
