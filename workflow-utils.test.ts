import assert from "node:assert/strict";
import test from "node:test";
import { assistantText } from "./workflow-utils.ts";
import { activeTaskSlots, cancelWorkflow, nextPendingTask, settleWorkflowTask } from "./workflow-state.ts";

test("assistant output extracts text from the durable answer entry", () => {
  assert.equal(assistantText({ model: [{ role: "assistant", content: [{ type: "text", text: "answer" }, { type: "thinking", thinking: "hidden" }] }] } as any), "answer");
  assert.equal(assistantText(undefined), "(no assistant output)");
});

test("durable queue resumes pending parallel work and chains in order", () => {
  const workflows: any[] = [
    { id: "chain", mode: "chain", status: "running", tasks: [{ status: "done" }, { status: "pending" }] },
    { id: "parallel", mode: "parallel", status: "running", tasks: [{ status: "running" }, { status: "pending" }] },
  ];
  assert.deepEqual(nextPendingTask(workflows), { workflowId: "chain", index: 1 });
  workflows[0].tasks[0].status = "running";
  assert.deepEqual(nextPendingTask(workflows), { workflowId: "parallel", index: 1 });
  workflows[0].tasks[0].status = "done";
  workflows[0].tasks[1].status = "pending";
  workflows[1].status = "done";
  assert.deepEqual(nextPendingTask(workflows), { workflowId: "chain", index: 1 });
});

test("failed workers stop pending tasks and concurrent slots stay bounded", () => {
  const workflow: any = { mode: "parallel", status: "running", tasks: [{ status: "running" }, { status: "pending" }, { status: "pending" }] };
  assert.equal(activeTaskSlots([workflow]), 3);
  assert.equal(settleWorkflowTask(workflow, 0, "failed", undefined, "provider error"), true);
  assert.equal(workflow.status, "failed");
  assert.deepEqual(workflow.tasks.map((task: any) => task.status), ["failed", "aborted", "aborted"]);
  assert.equal(nextPendingTask([{ ...workflow, id: "failed" }]), undefined);
  const cancelledSibling: any = { mode: "parallel", status: "failed", tasks: [{ status: "running" }] };
  assert.equal(activeTaskSlots([cancelledSibling]), 1);
  assert.equal(settleWorkflowTask(cancelledSibling, 0, "aborted"), false);
  assert.equal(cancelledSibling.tasks[0].status, "aborted");
  assert.equal(cancelledSibling.status, "failed");
  assert.equal(activeTaskSlots([cancelledSibling]), 0);
  const aborted: any = { mode: "single", status: "running", tasks: [{ status: "running" }] };
  assert.equal(settleWorkflowTask(aborted, 0, "aborted"), true);
  assert.equal(aborted.status, "failed");
});

test("chain consumes one active slot and cancellation is durable", () => {
  const chain: any = { mode: "chain", status: "running", tasks: Array.from({ length: 8 }, () => ({ status: "pending" })) };
  assert.equal(activeTaskSlots([chain]), 1);
  assert.equal(cancelWorkflow(chain), true);
  assert.equal(chain.status, "cancelled");
  assert.ok(chain.tasks.every((task: any) => task.status === "aborted"));
  assert.equal(cancelWorkflow(chain), false);
  const active: any = { mode: "parallel", status: "running", tasks: [{ status: "running" }, { status: "pending" }] };
  cancelWorkflow(active);
  assert.equal(active.status, "cancelling");
  assert.equal(activeTaskSlots([active]), 1);
  settleWorkflowTask(active, 0, "aborted");
  assert.equal(active.status, "cancelled");
  assert.equal(activeTaskSlots([active]), 0);
});
