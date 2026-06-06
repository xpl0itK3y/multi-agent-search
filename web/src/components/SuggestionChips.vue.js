const emit = defineEmits();
const chips = [
    { icon: "◎", label: "Обзор рынка", template: "Проведи обзор рынка: " },
    { icon: "⇄", label: "Сравнение", template: "Сравни и выбери лучшее: " },
    { icon: "❝", label: "Лит-обзор", template: "Сделай обзор исследований по теме: " },
    { icon: "↗", label: "Из ссылки", template: "Исследуй на основе источника: " },
];
debugger; /* PartiallyEnd: #3632/scriptSetup.vue */
const __VLS_ctx = {};
let __VLS_components;
let __VLS_directives;
__VLS_asFunctionalElement(__VLS_intrinsicElements.div, __VLS_intrinsicElements.div)({
    ...{ class: "flex flex-wrap items-center justify-center gap-2" },
});
for (const [chip] of __VLS_getVForSourceType((__VLS_ctx.chips))) {
    __VLS_asFunctionalElement(__VLS_intrinsicElements.button, __VLS_intrinsicElements.button)({
        ...{ onClick: (...[$event]) => {
                __VLS_ctx.emit('pick', chip.template);
            } },
        key: (chip.label),
        ...{ class: "flex items-center gap-2 rounded-full border border-bd bg-surface/60 px-3.5 py-2 text-sm text-muted hover:bg-surface hover:text-ink" },
    });
    __VLS_asFunctionalElement(__VLS_intrinsicElements.span, __VLS_intrinsicElements.span)({
        ...{ class: "text-accentSoft" },
    });
    (chip.icon);
    (chip.label);
}
/** @type {__VLS_StyleScopedClasses['flex']} */ ;
/** @type {__VLS_StyleScopedClasses['flex-wrap']} */ ;
/** @type {__VLS_StyleScopedClasses['items-center']} */ ;
/** @type {__VLS_StyleScopedClasses['justify-center']} */ ;
/** @type {__VLS_StyleScopedClasses['gap-2']} */ ;
/** @type {__VLS_StyleScopedClasses['flex']} */ ;
/** @type {__VLS_StyleScopedClasses['items-center']} */ ;
/** @type {__VLS_StyleScopedClasses['gap-2']} */ ;
/** @type {__VLS_StyleScopedClasses['rounded-full']} */ ;
/** @type {__VLS_StyleScopedClasses['border']} */ ;
/** @type {__VLS_StyleScopedClasses['border-bd']} */ ;
/** @type {__VLS_StyleScopedClasses['bg-surface/60']} */ ;
/** @type {__VLS_StyleScopedClasses['px-3.5']} */ ;
/** @type {__VLS_StyleScopedClasses['py-2']} */ ;
/** @type {__VLS_StyleScopedClasses['text-sm']} */ ;
/** @type {__VLS_StyleScopedClasses['text-muted']} */ ;
/** @type {__VLS_StyleScopedClasses['hover:bg-surface']} */ ;
/** @type {__VLS_StyleScopedClasses['hover:text-ink']} */ ;
/** @type {__VLS_StyleScopedClasses['text-accentSoft']} */ ;
var __VLS_dollars;
const __VLS_self = (await import('vue')).defineComponent({
    setup() {
        return {
            emit: emit,
            chips: chips,
        };
    },
    __typeEmits: {},
});
export default (await import('vue')).defineComponent({
    setup() {
        return {};
    },
    __typeEmits: {},
});
; /* PartiallyEnd: #4569/main.vue */
