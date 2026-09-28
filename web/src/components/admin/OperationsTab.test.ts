// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";

import { i18n } from "@/i18n";

const adminApi = vi.hoisted(() => ({
  getAuditLogs: vi.fn(),
  previewOperation: vi.fn(),
  executeOperation: vi.fn(),
}));

// Real ApiError/apiErrorMessage; only adminApi's network calls are stubbed.
vi.mock("@/lib/api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@/lib/api")>()),
  adminApi,
}));

import { ApiError } from "@/lib/api";
import OperationsTab from "./OperationsTab.vue";

const t = (key: string, params?: Record<string, unknown>) => i18n.global.t(key, params ?? {});
const JOB_ID = "0f5e7c1a-2b3d-4e5f-8a9b-0c1d2e3f4a5b";

async function mountTab() {
  // The dry-run dialog is teleported to <body>; render it in place so the wrapper sees it.
  const wrapper = mount(OperationsTab, { global: { plugins: [i18n], stubs: { teleport: true } } });
  await flushPromises();
  return wrapper;
}

function button(wrapper: Awaited<ReturnType<typeof mountTab>>, key: string) {
  const found = wrapper.findAll("button").find((b) => b.text() === t(key));
  if (!found) throw new Error(`no button "${key}"`);
  return found;
}

const confirmButton = (wrapper: Awaited<ReturnType<typeof mountTab>>) => wrapper.find('[data-test="confirm-op"]');

// Preview the requeue of JOB_ID, then confirm it in the dry-run modal.
async function requeueFinalizeJob(wrapper: Awaited<ReturnType<typeof mountTab>>) {
  await wrapper.find('input[type="text"]').setValue(JOB_ID);
  await button(wrapper, "admin.operations.previewAndRequeueBtn").trigger("click");
  await flushPromises();
  await confirmButton(wrapper).trigger("click");
  await flushPromises();
}

// Open the dry run of the "delete old jobs" card.
async function previewCleanup(wrapper: Awaited<ReturnType<typeof mountTab>>) {
  const previews = wrapper.findAll("button").filter((b) => b.text() === t("admin.operations.previewDelete"));
  expect(previews).toHaveLength(2);
  await previews[0].trigger("click");
  await flushPromises();
}

describe("admin OperationsTab", () => {
  beforeEach(() => {
    i18n.global.locale.value = "en";
    adminApi.getAuditLogs.mockResolvedValue([]);
    adminApi.previewOperation.mockResolvedValue({
      action: "requeue_finalize_job", dry_run: true, affected_count: 1,
      sample_affected_ids: [JOB_ID], summary: "Would requeue 1 finalize job",
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
    vi.useRealTimers();
    i18n.global.locale.value = "en";
  });

  // The preview only sees a dead-letter job; the requeue itself checks the research.
  it.each([
    ["Only the finalize job of a failed research can be requeued", "finalizeResearchNotFailed"],
    ["A newer finalize job has superseded this one", "finalizeJobSuperseded"],
    ["Finalize job state changed. Please retry.", "finalizeJobChanged"],
  ])("explains a refused finalize requeue: %s", async (detail, key) => {
    adminApi.executeOperation.mockRejectedValue(new ApiError(409, detail));
    const wrapper = await mountTab();

    await requeueFinalizeJob(wrapper);

    expect(adminApi.previewOperation).toHaveBeenCalledWith("requeue_finalize_job", { target_id: JOB_ID });
    expect(adminApi.executeOperation).toHaveBeenCalledWith("requeue_finalize_job", { target_id: JOB_ID });
    expect(wrapper.text()).toContain(t(`errors.api.${key}`));
    expect(wrapper.text()).not.toContain(t("errors.api.conflict"));
  });

  it("shows the refusal in the admin's language, never the raw detail", async () => {
    i18n.global.locale.value = "ru";
    const detail = "A newer finalize job has superseded this one";
    adminApi.executeOperation.mockRejectedValue(new ApiError(409, detail));
    const wrapper = await mountTab();

    await requeueFinalizeJob(wrapper);

    expect(wrapper.text()).toContain(i18n.global.t("errors.api.finalizeJobSuperseded", {}, { locale: "ru" }));
    expect(wrapper.text()).not.toContain(detail);
  });

  it("names the action and says 'Requeue n' for a requeue", async () => {
    const wrapper = await mountTab();
    await wrapper.find('input[type="text"]').setValue(JOB_ID);
    await button(wrapper, "admin.operations.previewAndRequeueBtn").trigger("click");
    await flushPromises();

    const modal = wrapper.find('[data-test="op-modal"]');
    expect(modal.find("h3").text()).toBe(t("admin.operations.singleRequeueTitle"));
    expect(modal.text()).toContain("requeue_finalize_job");
    expect(confirmButton(wrapper).text()).toBe(t("admin.operations.confirmRequeueN", { n: 1 }));
    expect(confirmButton(wrapper).classes()).toContain("text-onAccent");
  });

  it("says 'Delete n' in the danger colour for a cleanup", async () => {
    adminApi.previewOperation.mockResolvedValue({
      action: "cleanup_old_jobs", dry_run: true, affected_count: 12, sample_affected_ids: [], summary: "",
    });
    adminApi.executeOperation.mockResolvedValue({
      action: "cleanup_old_jobs", dry_run: false, affected_count: 12, sample_affected_ids: [], summary: "Deleted 12 jobs",
    });
    const wrapper = await mountTab();
    await previewCleanup(wrapper);

    expect(adminApi.previewOperation).toHaveBeenCalledWith("cleanup_old_jobs", { days: 7 });
    expect(wrapper.find('[data-test="op-modal"] h3').text()).toBe(t("admin.operations.cleanupOldTitle"));
    const confirm = confirmButton(wrapper);
    expect(confirm.text()).toBe(t("admin.operations.confirmDeleteN", { n: 12 }));
    expect(confirm.classes()).toContain("bg-red-600");
    expect(confirm.attributes("disabled")).toBeUndefined();

    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    await confirm.trigger("click");
    await flushPromises();
    expect(adminApi.executeOperation).toHaveBeenCalledWith("cleanup_old_jobs", { days: 7 });
    expect(wrapper.find('[data-test="op-message"]').text()).toContain("Deleted 12 jobs");

    // A success notice steps aside on its own.
    vi.advanceTimersByTime(8000);
    await flushPromises();
    expect(wrapper.find('[data-test="op-message"]').exists()).toBe(false);
  });

  it("cannot confirm a run that would touch nothing", async () => {
    adminApi.previewOperation.mockResolvedValue({
      action: "cleanup_search_cache", dry_run: true, affected_count: 0, sample_affected_ids: [], summary: "",
    });
    const wrapper = await mountTab();
    await previewCleanup(wrapper);

    expect(confirmButton(wrapper).attributes("disabled")).toBeDefined();
    expect(wrapper.find('[data-test="op-nothing"]').text()).toBe(t("admin.operations.nothingToRun"));
    await confirmButton(wrapper).trigger("click");
    await flushPromises();
    expect(adminApi.executeOperation).not.toHaveBeenCalled();
  });

  it("closes on Escape and on a press that starts and ends on the backdrop", async () => {
    const wrapper = await mountTab();
    await previewCleanup(wrapper);
    expect(wrapper.find('[data-test="op-modal"]').exists()).toBe(true);

    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    await flushPromises();
    expect(wrapper.find('[data-test="op-modal"]').exists()).toBe(false);

    await previewCleanup(wrapper);
    const backdrop = wrapper.find('[data-test="op-modal"]');
    // A selection dragged out of the panel (down inside, up on the backdrop) keeps it open.
    await backdrop.find('[role="dialog"]').trigger("pointerdown");
    await backdrop.trigger("click");
    expect(wrapper.find('[data-test="op-modal"]').exists()).toBe(true);

    await backdrop.trigger("pointerdown");
    await backdrop.trigger("click");
    expect(wrapper.find('[data-test="op-modal"]').exists()).toBe(false);
    expect(adminApi.executeOperation).not.toHaveBeenCalled();
  });

  it("keeps an error on screen until it is dismissed", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    adminApi.executeOperation.mockRejectedValue(new ApiError(500, "boom"));
    const wrapper = await mountTab();
    await requeueFinalizeJob(wrapper);

    vi.advanceTimersByTime(20000);
    await flushPromises();
    const msg = wrapper.find('[data-test="op-message"]');
    expect(msg.attributes("role")).toBe("alert");
    await msg.find("button").trigger("click");
    expect(wrapper.find('[data-test="op-message"]').exists()).toBe(false);
  });
});
