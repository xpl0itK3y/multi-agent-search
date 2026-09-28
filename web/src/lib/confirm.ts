// Promise-based, styled replacement for window.confirm. A single <ConfirmDialog/> mounted at
// the app root renders this reactive state; `confirm({...})` resolves true/false on the choice.
// Focus comes back to whatever had it when the dialog opened (the row's ✕, a menu item), so a
// keyboard user continues where they were.
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

let returnFocus: HTMLElement | null = null;

export function confirm(opts: ConfirmOptions): Promise<boolean> {
  return new Promise((resolve) => {
    // A dialog replaced by a newer one answers "no" rather than hanging forever.
    const previous = confirmState._resolve;
    confirmState._resolve = null;
    previous?.(false);

    if (typeof document !== "undefined" && !confirmState.open) {
      const active = document.activeElement;
      returnFocus = active instanceof HTMLElement && active !== document.body ? active : null;
    }
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

  const back = returnFocus;
  returnFocus = null;
  if (!back) return;
  // After the dialog is gone (and whatever the answer removed, e.g. a deleted row).
  setTimeout(() => {
    if (confirmState.open || !back.isConnected) return;
    back.focus({ preventScroll: true });
  }, 0);
}
