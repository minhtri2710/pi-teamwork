import assert from "node:assert/strict";
import test from "node:test";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { defineDoc } from "@earendil-works/pi-durable";
import { closeDurableRuntime, getDurableRuntime, hasDurableRuntime } from "./durable-runtime.ts";

const TestState = defineDoc<{ value: number }>({ kind: "teamwork.runtime-test", version: 1, scope: "session", initial: () => ({ value: 0 }), checkpointWhen: () => false });

function context(sessionId: string) {
  return {
    cwd: process.cwd(),
    sessionManager: { getSessionId: () => sessionId, getSessionDir: () => "" },
    modelRegistry: { getAll: () => [], getProvider: () => undefined },
  } as any;
}

test("no-session runtime uses memory storage and shares concurrent opens", async () => {
  const ctx = context(`memory-${crypto.randomUUID()}`);
  assert.equal(await hasDurableRuntime(ctx, "pi-teamwork-test"), false);
  const [first, second] = await Promise.all([
    getDurableRuntime(ctx, "pi-teamwork-test"),
    getDurableRuntime(ctx, "pi-teamwork-test"),
  ]);
  assert.equal(first.harness, second.harness);
  assert.equal(await hasDurableRuntime(ctx, "pi-teamwork-test"), true);
  await first.harness.commit(async (tx) => { (await tx.doc(TestState)).value = 7; }, BACKGROUND_CONTEXT);
  assert.equal((await second.harness.snapshot(TestState, BACKGROUND_CONTEXT))?.value, 7);
  await closeDurableRuntime(ctx, "pi-teamwork-test");
  assert.equal(await hasDurableRuntime(ctx, "pi-teamwork-test"), false);
});

test("waitForWorkflow resolves on the committed change, and stops on abort", async () => {
  const { Workflows, waitForWorkflow } = await import("./index.ts");
  const ctx = context(`memory-${crypto.randomUUID()}`);
  const runtime = await getDurableRuntime(ctx, "pi-teamwork-test");
  const workflow = { id: "w1", mode: "single", provider: "p", modelId: "m", thinkingLevel: "medium", cwd: "/", tasks: [{ role: "scout", task: "t", instructions: "i", tools: ["read"], status: "running" }], status: "running" } as any;
  await runtime.harness.commit(async (tx) => { (await tx.doc(Workflows)).workflows.push(workflow); }, BACKGROUND_CONTEXT);
  const updates: string[] = [];
  const done = waitForWorkflow(runtime, "w1", undefined, (event) => updates.push(event.details.status));
  await runtime.harness.commit(async (tx) => { const item = (await tx.doc(Workflows)).workflows[0]; item.tasks[0].status = "done"; item.status = "done"; }, BACKGROUND_CONTEXT);
  assert.equal((await done).status, "done");
  assert.deepEqual(updates, ["running", "done"]);
  await runtime.harness.commit(async (tx) => { (await tx.doc(Workflows)).workflows.push({ ...workflow, id: "w2" }); }, BACKGROUND_CONTEXT);
  const controller = new AbortController();
  const stopped = waitForWorkflow(runtime, "w2", controller.signal);
  controller.abort();
  await assert.rejects(stopped, /continues in the background/);
  await closeDurableRuntime(ctx, "pi-teamwork-test");
});
