const __VLS_props = defineProps();
const angles = [0, 45, 90, 135, 180, 225, 270, 315];
debugger; /* PartiallyEnd: #3632/scriptSetup.vue */
const __VLS_ctx = {};
let __VLS_components;
let __VLS_directives;
__VLS_asFunctionalElement(__VLS_intrinsicElements.svg, __VLS_intrinsicElements.svg)({
    width: (__VLS_ctx.size ?? 28),
    height: (__VLS_ctx.size ?? 28),
    viewBox: "0 0 100 100",
    ...{ class: "text-accent shrink-0" },
    'aria-hidden': "true",
});
__VLS_asFunctionalElement(__VLS_intrinsicElements.g, __VLS_intrinsicElements.g)({
    fill: "currentColor",
});
for (const [a] of __VLS_getVForSourceType((__VLS_ctx.angles))) {
    __VLS_asFunctionalElement(__VLS_intrinsicElements.path)({
        key: (a),
        d: "M50 8 C 53 30 53 40 50 50 C 47 40 47 30 50 8 Z",
        transform: (`rotate(${a} 50 50)`),
    });
}
/** @type {__VLS_StyleScopedClasses['text-accent']} */ ;
/** @type {__VLS_StyleScopedClasses['shrink-0']} */ ;
var __VLS_dollars;
const __VLS_self = (await import('vue')).defineComponent({
    setup() {
        return {
            angles: angles,
        };
    },
    __typeProps: {},
});
export default (await import('vue')).defineComponent({
    setup() {
        return {};
    },
    __typeProps: {},
});
; /* PartiallyEnd: #4569/main.vue */
