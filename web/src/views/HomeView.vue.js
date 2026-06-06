import { computed, ref } from "vue";
import { useRouter } from "vue-router";
import SparkLogo from "@/components/SparkLogo.vue";
import Composer from "@/components/Composer.vue";
import SuggestionChips from "@/components/SuggestionChips.vue";
import { useResearchStore } from "@/stores/research";
import { useUiStore } from "@/stores/ui";
const router = useRouter();
const store = useResearchStore();
const ui = useUiStore();
const prompt = ref("");
const busy = ref(false);
const errorMsg = ref(null);
const greeting = computed(() => {
    const h = new Date().getHours();
    const part = h < 6 ? "Доброй ночи" : h < 12 ? "Доброе утро" : h < 18 ? "Добрый день" : "Добрый вечер";
    return `${part}, ${ui.userName}`;
});
async function onSubmit(payload) {
    busy.value = true;
    errorMsg.value = null;
    try {
        const id = await store.createResearch(payload.prompt, payload.depth, payload.model);
        router.push({ name: "research", params: { id } });
    }
    catch (e) {
        errorMsg.value = e.message;
    }
    finally {
        busy.value = false;
    }
}
function onPick(template) {
    prompt.value = template;
}
debugger; /* PartiallyEnd: #3632/scriptSetup.vue */
const __VLS_ctx = {};
let __VLS_components;
let __VLS_directives;
__VLS_asFunctionalElement(__VLS_intrinsicElements.div, __VLS_intrinsicElements.div)({
    ...{ class: "flex min-h-full flex-col items-center justify-center px-6" },
});
__VLS_asFunctionalElement(__VLS_intrinsicElements.div, __VLS_intrinsicElements.div)({
    ...{ class: "mb-8 flex items-center gap-3" },
});
/** @type {[typeof SparkLogo, ]} */ ;
// @ts-ignore
const __VLS_0 = __VLS_asFunctionalComponent(SparkLogo, new SparkLogo({
    size: (34),
}));
const __VLS_1 = __VLS_0({
    size: (34),
}, ...__VLS_functionalComponentArgsRest(__VLS_0));
__VLS_asFunctionalElement(__VLS_intrinsicElements.h1, __VLS_intrinsicElements.h1)({
    ...{ class: "font-serif text-4xl font-medium tracking-tight text-ink" },
});
(__VLS_ctx.greeting);
/** @type {[typeof Composer, ]} */ ;
// @ts-ignore
const __VLS_3 = __VLS_asFunctionalComponent(Composer, new Composer({
    ...{ 'onSubmit': {} },
    prompt: (__VLS_ctx.prompt),
    busy: (__VLS_ctx.busy),
    ...{ class: "mb-4" },
}));
const __VLS_4 = __VLS_3({
    ...{ 'onSubmit': {} },
    prompt: (__VLS_ctx.prompt),
    busy: (__VLS_ctx.busy),
    ...{ class: "mb-4" },
}, ...__VLS_functionalComponentArgsRest(__VLS_3));
let __VLS_6;
let __VLS_7;
let __VLS_8;
const __VLS_9 = {
    onSubmit: (__VLS_ctx.onSubmit)
};
var __VLS_5;
if (__VLS_ctx.errorMsg) {
    __VLS_asFunctionalElement(__VLS_intrinsicElements.p, __VLS_intrinsicElements.p)({
        ...{ class: "mb-3 text-sm text-red-400" },
    });
    (__VLS_ctx.errorMsg);
}
/** @type {[typeof SuggestionChips, ]} */ ;
// @ts-ignore
const __VLS_10 = __VLS_asFunctionalComponent(SuggestionChips, new SuggestionChips({
    ...{ 'onPick': {} },
}));
const __VLS_11 = __VLS_10({
    ...{ 'onPick': {} },
}, ...__VLS_functionalComponentArgsRest(__VLS_10));
let __VLS_13;
let __VLS_14;
let __VLS_15;
const __VLS_16 = {
    onPick: (__VLS_ctx.onPick)
};
var __VLS_12;
/** @type {__VLS_StyleScopedClasses['flex']} */ ;
/** @type {__VLS_StyleScopedClasses['min-h-full']} */ ;
/** @type {__VLS_StyleScopedClasses['flex-col']} */ ;
/** @type {__VLS_StyleScopedClasses['items-center']} */ ;
/** @type {__VLS_StyleScopedClasses['justify-center']} */ ;
/** @type {__VLS_StyleScopedClasses['px-6']} */ ;
/** @type {__VLS_StyleScopedClasses['mb-8']} */ ;
/** @type {__VLS_StyleScopedClasses['flex']} */ ;
/** @type {__VLS_StyleScopedClasses['items-center']} */ ;
/** @type {__VLS_StyleScopedClasses['gap-3']} */ ;
/** @type {__VLS_StyleScopedClasses['font-serif']} */ ;
/** @type {__VLS_StyleScopedClasses['text-4xl']} */ ;
/** @type {__VLS_StyleScopedClasses['font-medium']} */ ;
/** @type {__VLS_StyleScopedClasses['tracking-tight']} */ ;
/** @type {__VLS_StyleScopedClasses['text-ink']} */ ;
/** @type {__VLS_StyleScopedClasses['mb-4']} */ ;
/** @type {__VLS_StyleScopedClasses['mb-3']} */ ;
/** @type {__VLS_StyleScopedClasses['text-sm']} */ ;
/** @type {__VLS_StyleScopedClasses['text-red-400']} */ ;
var __VLS_dollars;
const __VLS_self = (await import('vue')).defineComponent({
    setup() {
        return {
            SparkLogo: SparkLogo,
            Composer: Composer,
            SuggestionChips: SuggestionChips,
            prompt: prompt,
            busy: busy,
            errorMsg: errorMsg,
            greeting: greeting,
            onSubmit: onSubmit,
            onPick: onPick,
        };
    },
});
export default (await import('vue')).defineComponent({
    setup() {
        return {};
    },
});
; /* PartiallyEnd: #4569/main.vue */
