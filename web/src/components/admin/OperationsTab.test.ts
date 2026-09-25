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

const t = (key: string) => i18n.global.t(key);
const JOB_ID = "0f5e7c1a-2b3d-4e5f-8a9b-0c1d2e3f4a5b";

async function mountTab() {
  const wrapper = mount(OperationsTab, { global: { plugins: [i18n] } });
  await flushPromises();
  return wrapper;
}

function button(wrapper: Awaited<ReturnType<typeof mountTab>>, key: string) {
  const found = wrapper.findAll("button").find((b) => b.text() === t(key));
  if (!found) throw new Error(`no button "${key}"`);
  return found;
}

// Preview the requeue of JOB_ID, then confirm it in the dry-run modal.
async function requeueFinalizeJob(wrapper: Awaited<ReturnType<typeof mountTab>>) {
  await wrapper.find('input[type="text"]').setValue(JOB_ID);
  await button(wrapper, "admin.operations.previewAndRequeueBtn").trigger("click");
  await flushPromises();
  await button(wrapper, "admin.operations.confirmAndExecuteBtn").trigger("click");
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
});
