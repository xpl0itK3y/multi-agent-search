// Promise-based, styled replacement for window.confirm. A single <ConfirmDialog/> mounted at
// the app root renders this reactive state; `confirm({...})` resolves true/false on the choice.
import { reactive } from "vue";

interface ConfirmOptions {
  title?: string;
  message: string;
  confirmText?: string;
  cancelText?: string;
  danger?: boolean;
}

export const confirmState = reactive({
  open: false,
  title: "",
  message: "",
  confirmText: "OK",
  cancelText: "Cancel",
  danger: false,
  _resolve: null as ((v: boolean) => void) | null,
});

export function confirm(opts: ConfirmOptions): Promise<boolean> {
  return new Promise((resolve) => {
    confirmState.title = opts.title ?? "";
    confirmState.message = opts.message;
    confirmState.confirmText = opts.confirmText ?? "OK";
    confirmState.cancelText = opts.cancelText ?? "Cancel";
    confirmState.danger = opts.danger ?? false;
    confirmState.open = true;
    confirmState._resolve = resolve;
  });
}

export function answerConfirm(value: boolean): void {
  if (!confirmState.open) return;
  confirmState.open = false;
  const resolve = confirmState._resolve;
  confirmState._resolve = null;
  resolve?.(value);
}
