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

test("review and testCommand are refused without pi-guards, and its guards join workers when it is installed", async () => {
  const { default: teamwork } = await import("./index.ts");
  const key = Symbol.for("pi-guards.host");
  const tools: Record<string, any> = {};
  teamwork({ registerTool: (tool: any) => { tools[tool.name] = tool; }, registerCommand: () => {}, on: () => {}, getSettings: () => ({}) } as any);
  const ctx = { ...context(`memory-${crypto.randomUUID()}`), model: { provider: "faux", id: "m" }, thinkingLevel: "medium" };
  const run = (params: object) => tools.teamwork_workflow.execute("id", { mode: "single", tasks: [{ role: "scout", task: "t" }], ...params }, AbortSignal.abort(), undefined, ctx);
  delete (globalThis as any)[key];
  await assert.rejects(run({ review: true }), /pi-guards/);
  await assert.rejects(run({ testCommand: "npm test" }), /pi-guards/);
  const opened: string[] = [];
  (globalThis as any)[key] = { version: 1, extensions: () => [], init: () => undefined, opened: (host: string) => opened.push(host), closed: () => {} };
  await assert.rejects(run({ review: true }), /continues in the background/);
  assert.deepEqual([...new Set(opened)], ["teamwork"]);
  delete (globalThis as any)[key];
  await closeDurableRuntime(ctx, "pi-teamwork");
});
