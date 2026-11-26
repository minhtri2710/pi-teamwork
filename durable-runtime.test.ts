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

test("cancel aborts every job at once, so a long tool call holds no other worker's abort", async () => {
  const { Workflows, cancelWorkflows } = await import("./index.ts");
  const { defineExtension, defineTool } = await import("@earendil-works/pi-durable");
  const { fauxAssistantMessage, fauxProvider, fauxToolCall } = await import("@earendil-works/pi-ai");
  const { Type } = await import("typebox");
  const ctx = context(`memory-${crypto.randomUUID()}`);
  let release!: () => void;
  const held = new Promise<void>((resolve) => { release = resolve; });
  // "hold" ignores its abort until released, as a command that traps TERM; "wait" ends on abort.
  const Tools = defineExtension({ name: "test-tools", tools: [defineTool({ name: "work", description: "work", parameters: Type.Object({ mode: Type.String() }),
    execute: async (args: any, _api: any, call: any) => {
      if (args.mode === "hold") await held;
      else await new Promise<void>((resolve) => { if (call.abortSignal?.aborted) resolve(); call.abortSignal?.addEventListener("abort", () => resolve(), { once: true }); });
      return { output: [{ type: "text", text: "ok" }] };
    } })] });
  const runtime = await getDurableRuntime(ctx, "pi-teamwork-test", [Tools]);
  const faux = fauxProvider();
  runtime.models.setProvider(faux.provider);
  faux.setResponses([fauxAssistantMessage([fauxToolCall("work", { mode: "hold" })], { stopReason: "toolUse" }), fauxAssistantMessage([fauxToolCall("work", { mode: "wait" })], { stopReason: "toolUse" }), fauxAssistantMessage("done"), fauxAssistantMessage("done")]);
  const model = faux.getModel();
  const worker = () => runtime.harness.createConversation({ ownership: { kind: "ownerless" }, agent: { model: { provider: model.provider, modelId: model.id }, extensions: [Tools] } }, BACKGROUND_CONTEXT);
  const slow = await worker(), other = await worker();
  const slowSub = await slow.submit({ type: "input", content: "go" }, BACKGROUND_CONTEXT);
  const running = async () => (await runtime.harness.inspect(BACKGROUND_CONTEXT)).tasks.filter(({ record }) => record.kind === "pi.tool" && record.state.status === "running").length;
  while (await running() < 1) await new Promise((resolve) => setTimeout(resolve, 5));
  const otherSub = await other.submit({ type: "input", content: "go" }, BACKGROUND_CONTEXT);
  while (await running() < 2) await new Promise((resolve) => setTimeout(resolve, 5));
  const job = (id: string, conversationId: number, submissionId: number, taskIndex: number) => ({ id, workflowId: "w", taskIndex, role: "worker", prompt: "go", conversationId, submissionId, status: "running" });
  await runtime.harness.commit(async (tx) => {
    const state = await tx.doc(Workflows);
    state.workflows.push({ id: "w", mode: "parallel", provider: "p", modelId: "m", thinkingLevel: "medium", cwd: "/", status: "running",
      tasks: [0, 1].map(() => ({ role: "worker", task: "go", instructions: "i", tools: [], status: "running" })) } as any);
    state.jobs.push(job("a", slow.id, slowSub.id, 0) as any, job("b", other.id, otherSub.id, 1) as any);
  }, BACKGROUND_CONTEXT);
  const cancelled = cancelWorkflows(runtime);
  const otherSettled = await Promise.race([otherSub.wait(BACKGROUND_CONTEXT).then(() => true), new Promise((resolve) => setTimeout(() => resolve(false), 2000))]);
  release();
  await cancelled;
  assert.equal(otherSettled, true);
  await closeDurableRuntime(ctx, "pi-teamwork-test");
});
