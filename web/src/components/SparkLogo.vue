<script setup lang="ts">
defineProps<{ size?: number }>();
</script>

<template>
  <svg
    :width="size ?? 28"
    :height="size ?? 28"
    viewBox="0 0 100 100"
    class="veris-logo shrink-0"
    aria-hidden="true"
  >
    <defs>
      <linearGradient id="veris-grad" x1="0" y1="0" x2="1" y2="1">
        <stop offset="0%" style="stop-color: rgb(var(--c-grad-a))" />
        <stop offset="100%" style="stop-color: rgb(var(--c-grad-b))" />
      </linearGradient>
    </defs>
    <!-- gradient badge -->
    <rect x="8" y="8" width="84" height="84" rx="24" fill="url(#veris-grad)" />
    <!-- soft top highlight -->
    <rect x="8" y="8" width="84" height="40" rx="24" fill="white" opacity="0.12" />
    <!-- verified check -->
    <path
      d="M29 52 L44 67 L72 32"
      fill="none"
      stroke="white"
      stroke-width="9"
      stroke-linecap="round"
      stroke-linejoin="round"
    />
  </svg>
</template>
