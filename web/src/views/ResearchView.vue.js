import { onBeforeUnmount, onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { api } from "@/lib/api";
const props = defineProps();
const router = useRouter();
const status = ref("processing");
const report = ref(null);
const loading = ref(true);
const errorMsg = ref(null);
let timer;
const DONE = new Set(["completed", "failed"]);
async function poll() {
    try {
        const res = await api.getReport(props.id);
        status.value = res.status;
        report.value = res.final_report;
        loading.value = false;
        if (DONE.has(res.status) && timer) {
            window.clearInterval(timer);
            timer = undefined;
        }
    }
    catch (e) {
        errorMsg.value = e.message;
        loading.value = false;
    }
}
function statusLabel(s) {
    const map = {
        processing: "Декомпозиция и поиск…",
        analyzing: "Синтез отчёта…",
        completed: "Готово",
        failed: "Ошибка",
    };
    return map[s] ?? s;
}
onMounted(() => {
    poll();
    timer = window.setInterval(poll, 3000);
});
onBeforeUnmount(() => {
    if (timer)
        window.clearInterval(timer);
});
debugger; /* PartiallyEnd: #3632/scriptSetup.vue */
const __VLS_ctx = {};
let __VLS_components;
let __VLS_directives;
__VLS_asFunctionalElement(__VLS_intrinsicElements.div, __VLS_intrinsicElements.div)({
    ...{ class: "mx-auto max-w-3xl px-6 py-10" },
});
__VLS_asFunctionalElement(__VLS_intrinsicElements.button, __VLS_intrinsicElements.button)({
    ...{ onClick: (...[$event]) => {
            __VLS_ctx.router.push('/');
        } },
    ...{ class: "mb-6 text-sm text-muted hover:text-ink" },
});
__VLS_asFunctionalElement(__VLS_intrinsicElements.div, __VLS_intrinsicElements.div)({
    ...{ class: "mb-6 flex items-center gap-3" },
});
__VLS_asFunctionalElement(__VLS_intrinsicElements.span)({
    ...{ class: "h-2 w-2 rounded-full" },
    ...{ class: ({
            'bg-emerald-400': __VLS_ctx.status === 'completed',
            'bg-red-400': __VLS_ctx.status === 'failed',
            'bg-accent animate-pulse': __VLS_ctx.status !== 'completed' && __VLS_ctx.status !== 'failed',
        }) },
});
__VLS_asFunctionalElement(__VLS_intrinsicElements.span, __VLS_intrinsicElements.span)({
    ...{ class: "text-sm text-muted" },
});
(__VLS_ctx.statusLabel(__VLS_ctx.status));
if (__VLS_ctx.errorMsg) {
    __VLS_asFunctionalElement(__VLS_intrinsicElements.p, __VLS_intrinsicElements.p)({
        ...{ class: "text-sm text-red-400" },
    });
    (__VLS_ctx.errorMsg);
}
else if (__VLS_ctx.loading) {
    __VLS_asFunctionalElement(__VLS_intrinsicElements.p, __VLS_intrinsicElements.p)({
        ...{ class: "text-muted" },
    });
}
else if (__VLS_ctx.report) {
    __VLS_asFunctionalElement(__VLS_intrinsicElements.div, __VLS_intrinsicElements.div)({
        ...{ class: "whitespace-pre-wrap font-sans text-[15px] leading-relaxed text-ink" },
    });
    (__VLS_ctx.report);
}
else {
    __VLS_asFunctionalElement(__VLS_intrinsicElements.p, __VLS_intrinsicElements.p)({
        ...{ class: "text-muted" },
    });
}
/** @type {__VLS_StyleScopedClasses['mx-auto']} */ ;
/** @type {__VLS_StyleScopedClasses['max-w-3xl']} */ ;
/** @type {__VLS_StyleScopedClasses['px-6']} */ ;
/** @type {__VLS_StyleScopedClasses['py-10']} */ ;
/** @type {__VLS_StyleScopedClasses['mb-6']} */ ;
/** @type {__VLS_StyleScopedClasses['text-sm']} */ ;
/** @type {__VLS_StyleScopedClasses['text-muted']} */ ;
/** @type {__VLS_StyleScopedClasses['hover:text-ink']} */ ;
/** @type {__VLS_StyleScopedClasses['mb-6']} */ ;
/** @type {__VLS_StyleScopedClasses['flex']} */ ;
/** @type {__VLS_StyleScopedClasses['items-center']} */ ;
/** @type {__VLS_StyleScopedClasses['gap-3']} */ ;
/** @type {__VLS_StyleScopedClasses['h-2']} */ ;
/** @type {__VLS_StyleScopedClasses['w-2']} */ ;
/** @type {__VLS_StyleScopedClasses['rounded-full']} */ ;
/** @type {__VLS_StyleScopedClasses['text-sm']} */ ;
/** @type {__VLS_StyleScopedClasses['text-muted']} */ ;
/** @type {__VLS_StyleScopedClasses['text-sm']} */ ;
/** @type {__VLS_StyleScopedClasses['text-red-400']} */ ;
/** @type {__VLS_StyleScopedClasses['text-muted']} */ ;
/** @type {__VLS_StyleScopedClasses['whitespace-pre-wrap']} */ ;
/** @type {__VLS_StyleScopedClasses['font-sans']} */ ;
/** @type {__VLS_StyleScopedClasses['text-[15px]']} */ ;
/** @type {__VLS_StyleScopedClasses['leading-relaxed']} */ ;
/** @type {__VLS_StyleScopedClasses['text-ink']} */ ;
/** @type {__VLS_StyleScopedClasses['text-muted']} */ ;
var __VLS_dollars;
const __VLS_self = (await import('vue')).defineComponent({
    setup() {
        return {
            router: router,
            status: status,
            report: report,
            loading: loading,
            errorMsg: errorMsg,
            statusLabel: statusLabel,
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
