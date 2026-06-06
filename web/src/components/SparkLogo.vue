<script setup lang="ts">
defineProps<{ size?: number }>();
const angles = [0, 45, 90, 135, 180, 225, 270, 315];
</script>

<template>
  <svg
    :width="size ?? 28"
    :height="size ?? 28"
    viewBox="0 0 100 100"
    class="text-accent shrink-0"
    aria-hidden="true"
  >
    <g fill="currentColor">
      <path
        v-for="a in angles"
        :key="a"
        d="M50 8 C 53 30 53 40 50 50 C 47 40 47 30 50 8 Z"
        :transform="`rotate(${a} 50 50)`"
      />
    </g>
  </svg>
</template>
