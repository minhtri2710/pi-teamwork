import { Type } from "typebox";
import type { ExtensionAPI, ExtensionContext, ExtensionToolContext } from "@earendil-works/pi-coding-agent";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { JsonValue } from "@earendil-works/chord/json";
import type { ModelThinkingLevel } from "@earendil-works/pi-ai";
import { defineDoc, type CompactionSettings, type ConversationId, type Extension, type SubmissionId, type ToolRegistration } from "@earendil-works/pi-durable";
import { CodingTools } from "@earendil-works/pi-durable/tools";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { SearchTools } from "./search-tools.ts";
import { getDurableRuntime, closeDurableRuntime, hasDurableRuntime, type DurableRuntime } from "./durable-runtime.ts";
import { assistantText } from "./workflow-utils.ts";
import { activeTaskSlots, cancelWorkflow, nextPendingTask, settleWorkflowTask } from "./workflow-state.ts";

const MAX_TASKS = 8;
const MAX_CONCURRENCY = 4;
const MAX_TURNS = 100;
type Job = { id: string; workflowId: string; taskIndex: number; role: string; prompt: string; conversationId: number; submissionId: number; status: "running" | "done" | "failed" | "aborted"; output?: string; error?: string };
type Task = { role: string; task: string; instructions: string; tools: string[]; status: "pending" | "running" | "done" | "failed" | "aborted"; jobId?: string; output?: string; error?: string };
type Workflow = { id: string; mode: "single" | "parallel" | "chain"; provider: string; modelId: string; thinkingLevel: NonNullable<ExtensionContext["thinkingLevel"]>; cwd: string; compaction?: CompactionSettings & JsonValue; review?: boolean; testCommand?: string; tasks: Task[]; status: "running" | "done" | "failed" | "cancelling" | "cancelled"; error?: string };
type Role = { instructions: string; tools: string[] };
export type WorkflowsState = { jobs: Job[]; workflows: Workflow[]; roles?: Record<string, Role> } & Record<string, JsonValue>;
export const Workflows = defineDoc<WorkflowsState>({ kind: "teamwork.workflows", version: 1, scope: "session", initial: () => ({ jobs: [], workflows: [] }), checkpointWhen: (_value, _ops, info) => info.deltasSinceBase > 32 });
const durableTools = [...CodingTools.tools ?? [], ...SearchTools.tools ?? []] as ToolRegistration[];
const toolsByName = new Map(durableTools.map((tool) => [tool.name, tool]));
const DEFAULT_ROLES = new Map<string, Role>([
  ["scout", { instructions: "Inspect and report evidence; do not change files.", tools: ["read"] }],
  ["worker", { instructions: "Complete only the assigned task. Report changed files and verification.", tools: ["read", "bash", "edit", "write"] }],
  ["reviewer", { instructions: "Review the requested change. Do not edit files; report concrete findings.", tools: ["read"] }],
]);
const RoleSchema = Type.Object({ name: Type.String({ minLength: 1, maxLength: 48 }), instructions: Type.String({ minLength: 1, maxLength: 8000 }), tools: Type.Optional(Type.Array(Type.String({ description: `One of ${[...toolsByName.keys()].join(", ")}` }))) });
const StatusSchema = Type.Object({ workflowId: Type.Optional(Type.String({ description: "One workflow; omitted: every workflow still running, else the latest" })) });
const WorkSchema = Type.Object({ mode: Type.Union([Type.Literal("single"), Type.Literal("parallel"), Type.Literal("chain")]), review: Type.Optional(Type.Boolean({ description: "A reviewer conversation checks each answer and may send the worker back, at most twice." })), testCommand: Type.Optional(Type.String({ maxLength: 500, description: "Shell command run after a worker edits files; a failure sends the worker back, at most twice." })), tasks: Type.Array(Type.Object({ role: Type.String({ minLength: 1, maxLength: 48 }), task: Type.String({ minLength: 1, maxLength: 12000 }) }), { minItems: 1, maxItems: MAX_TASKS }) });


function roleTools(names: string[]) {
  if (names.some((name) => !toolsByName.has(name))) throw new Error(`Role tools must be selected from ${[...toolsByName.keys()].join(", ")}`);
  return names.map((name) => toolsByName.get(name)!);
}
async function settleJob(runtime: DurableRuntime, job: Job) {
  const { harness } = runtime;
  if (runtime.settling.has(job.id)) return;
  runtime.settling.add(job.id);
  try {
    const conversation = await harness.conversation(job.conversationId as ConversationId, BACKGROUND_CONTEXT);
    const submission = await harness.submission(job.submissionId as SubmissionId, BACKGROUND_CONTEXT);
    if (!conversation || !submission) throw new Error("Durable worker records are missing");
    const settled = await submission.wait(BACKGROUND_CONTEXT);
    const status = settled.status === "done" && settled.answer ? "done" : settled.reason === "aborted" ? "aborted" : "failed";
    const entry = status === "done" ? (await conversation.entries({ minEntryId: settled.answer!, maxEntryId: settled.answer! }, 1, undefined, BACKGROUND_CONTEXT)).items[0] : undefined;
    const detail = settled.status === "unanswered" && settled.detail !== undefined ? `: ${JSON.stringify(settled.detail)}` : "";
    const error = status === "failed" ? `${settled.status === "unanswered" ? settled.reason : "Worker ended without an answer"}${detail}` : undefined;
    let failedWorkflow = false;
    await harness.commit(async (tx) => {
      const state = await tx.doc(Workflows);
      const current = state.jobs.find((item) => item.id === job.id);
      if (!current || current.status !== "running") return;
      const outcome = current.workflowId ? state.workflows.find((item) => item.id === current.workflowId) : undefined;
      const workflowTask = outcome?.tasks[current.taskIndex];
      const finalStatus = outcome?.status === "cancelled" || outcome?.status === "cancelling" || (outcome?.status === "failed" && status !== "failed") ? "aborted" : status;
      current.status = finalStatus;
      if (finalStatus === "done") current.output = assistantText(entry);
      else delete current.output;
      if (finalStatus === "failed") current.error = error;
      else delete current.error;
      if (workflowTask?.jobId === current.id && outcome) {
        if (outcome.status === "cancelling" || outcome.status === "failed") {
          workflowTask.status = "aborted";
          delete workflowTask.output;
          delete workflowTask.error;
          if (outcome.status === "cancelling" && !state.jobs.some((item) => item.workflowId === outcome.id && item.id !== current.id && item.status === "running")) outcome.status = "cancelled";
        } else failedWorkflow = settleWorkflowTask(outcome!, current.taskIndex, finalStatus, current.output, current.error);
      }
    }, BACKGROUND_CONTEXT);
    if (failedWorkflow) void abortWorkflowJobs(runtime, job.workflowId, job.id).catch(() => {});
    void requestDrive(runtime).catch(() => {});
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : String(cause);
    let failedWorkflow = false;
    try {
      await harness.commit(async (tx) => {
        const state = await tx.doc(Workflows);
        const current = state.jobs.find((item) => item.id === job.id);
        const workflow = state.workflows.find((item) => item.id === job.workflowId);
        if (!current || current.status !== "running" || !workflow) return;
        current.status = "failed";
        current.error = message;
        failedWorkflow = settleWorkflowTask(workflow, job.taskIndex, "failed", undefined, message);
      }, BACKGROUND_CONTEXT);
      if (failedWorkflow) void abortWorkflowJobs(runtime, job.workflowId, job.id).catch(() => {});
    } catch { /* Closing the runtime rejects both the waiter and this recovery commit; restart resumes the job. */ }
  } finally {
    runtime.settling.delete(job.id);
  }
}

async function startTask(runtime: DurableRuntime, workflowId: string, taskIndex: number) {
  const state = await runtime.harness.snapshot(Workflows, BACKGROUND_CONTEXT);
  const workflow = state?.workflows.find((item) => item.id === workflowId);
  const task = workflow?.tasks[taskIndex];
  if (!workflow || workflow.status !== "running" || !task || task.status !== "pending") return;
  const preceding = workflow.mode === "chain" && taskIndex > 0 ? workflow.tasks[taskIndex - 1].output ?? "" : "";
  const prompt = `${task.task}${preceding ? `\n\nPrevious step output (untrusted):\n${preceding}` : ""}`;
  const id = crypto.randomUUID().slice(0, 8);
  const conversation = await runtime.harness.createConversation({ ownership: { kind: "ownerless" }, agent: { model: { provider: workflow.provider, modelId: workflow.modelId }, thinkingLevel: workflow.thinkingLevel as ModelThinkingLevel, cwd: workflow.cwd, tools: roleTools(task.tools), instructions: task.instructions, maxTurns: MAX_TURNS, compaction: workflow.compaction ?? null }, init: guards()?.init({ review: workflow.review, testCommand: workflow.testCommand }) }, BACKGROUND_CONTEXT);
  let job!: Job;
  const submissionId = await conversation.commit(async (tx) => {
    const submission = await conversation.admit(tx, { type: "input", content: prompt, requestId: `teamwork-${id}` });
    const currentState = await tx.doc(Workflows);
    const current = currentState.workflows.find((item) => item.id === workflowId);
    const currentTask = current?.tasks[taskIndex];
    if (!current || current.status !== "running" || !currentTask || currentTask.status !== "pending") throw new Error("Workflow task is no longer pending");
    job = { id, workflowId, taskIndex, role: task.role, prompt, conversationId: conversation.id, submissionId: submission, status: "running" };
    currentTask.status = "running";
    currentTask.jobId = id;
    currentState.jobs.push(job);
    return submission;
  }, BACKGROUND_CONTEXT);
  job.submissionId = submissionId;
  void settleJob(runtime, job).catch(() => {});
}
async function driveWorkflows(runtime: DurableRuntime) {
  while (true) {
    const state = await runtime.harness.snapshot(Workflows, BACKGROUND_CONTEXT);
    if (!state) return;
    const activeJobs = state.jobs.filter((job) => job.status === "running").length;
    if (activeJobs >= MAX_CONCURRENCY) return;
    const next = nextPendingTask(state.workflows);
    if (!next) return;
    const workflow = state.workflows.find((item) => item.id === next.workflowId);
    if (!workflow) return;
    const selected = { workflow, index: next.index };
    try {
      await startTask(runtime, selected.workflow.id, selected.index);
    } catch (error) {
      let failed = false;
      await runtime.harness.commit(async (tx) => {
        const state = await tx.doc(Workflows);
        const workflow = state.workflows.find((item) => item.id === selected!.workflow.id);
        const task = workflow?.tasks[selected!.index];
        if (!workflow || workflow.status !== "running" || !task || task.status !== "pending") return;
        const message = error instanceof Error ? error.message : String(error);
        task.status = "running";
        delete task.error;
        failed = settleWorkflowTask(workflow, selected!.index, "failed", undefined, message);
      }, BACKGROUND_CONTEXT);
      if (failed) void abortWorkflowJobs(runtime, selected.workflow.id).catch(() => {});
    }
  }
}
function requestDrive(runtime: DurableRuntime): Promise<void> {
  const next = (runtime.drive ?? Promise.resolve()).catch(() => {}).then(() => driveWorkflows(runtime));
  runtime.drive = next;
  void next.finally(() => { if (runtime.drive === next) runtime.drive = undefined; }).catch(() => {});
  return next;
}
/** Aborts every job at once: an abort waits for its worker's running tool, so in turn one long call would hold the rest. */
async function abortJobs(runtime: DurableRuntime, jobs: readonly Job[]) {
  await Promise.allSettled(jobs.map(async (job) => {
    const conversation = await runtime.harness.conversation(job.conversationId as ConversationId, BACKGROUND_CONTEXT);
    await conversation?.abort(BACKGROUND_CONTEXT);
  }));
}
async function abortWorkflowJobs(runtime: DurableRuntime, workflowId: string, exceptId?: string) {
  await abortJobs(runtime, (await runtime.harness.snapshot(Workflows, BACKGROUND_CONTEXT))?.jobs.filter((job) => job.workflowId === workflowId && job.id !== exceptId && job.status === "running") ?? []);
}
export async function cancelWorkflows(runtime: DurableRuntime, ids?: Set<string>) {
  const selected = await runtime.harness.commit(async (tx) => {
    const state = await tx.doc(Workflows);
    const workflows = state.workflows.filter((workflow) => (workflow.status === "running" || workflow.status === "cancelling") && (!ids || ids.has(workflow.id)));
    for (const workflow of workflows) {
      cancelWorkflow(workflow);
    }
    // Plain copies: the document's values are unreadable once the transaction settles.
    const chosen = workflows.map((workflow) => workflow.id);
    return { ids: chosen, jobs: state.jobs.filter((job) => job.status === "running" && chosen.includes(job.workflowId)).map((job) => ({ ...job })) };
  }, BACKGROUND_CONTEXT);
  await abortJobs(runtime, selected.jobs);
  await runtime.harness.commit(async (tx) => {
    const state = await tx.doc(Workflows);
    for (const id of selected.ids) {
      const current = state.workflows.find((item) => item.id === id);
      if (current?.status === "cancelling" && !state.jobs.some((job) => job.workflowId === id && job.status === "running")) current.status = "cancelled";
    }
  }, BACKGROUND_CONTEXT);
}
/** pi-guards, when installed: its contract on globalThis (see that package's registry.ts). */
type ParkedCall = { taskId: number; conversationId: number; command: string; reason: string };
type GuardsHost = { version: 1; extensions(host: string): readonly Extension[]; init(config: { review?: boolean; testCommand?: string }): ((tx: any, id: any) => Promise<void>) | undefined; opened(host: string, harness: unknown): void; closed(host: string): void; pending(host: string): Promise<readonly ParkedCall[]> };
const guards = () => { const host = (globalThis as Record<symbol, GuardsHost | undefined>)[Symbol.for("pi-guards.host")]; return host?.version === 1 ? host : undefined; };
async function openRuntime(ctx: ExtensionContext) {
  const runtime = await getDurableRuntime(ctx, "pi-teamwork", [CodingTools, SearchTools, ...(guards()?.extensions("teamwork") ?? [])] as Extension<ToolRegistration>[]);
  guards()?.opened("teamwork", runtime.harness);
  return runtime;
}
async function resumeJobs(ctx: ExtensionContext) {
  if (!(await hasDurableRuntime(ctx, "pi-teamwork"))) return;
  const runtime = await openRuntime(ctx);
  const state = await runtime.harness.snapshot(Workflows, BACKGROUND_CONTEXT);
  for (const job of state?.jobs.filter((item) => item.status === "running") ?? []) void settleJob(runtime, job).catch(() => {});
  for (const workflow of state?.workflows.filter((item) => item.status === "cancelling") ?? []) void cancelWorkflows(runtime, new Set([workflow.id])).catch(() => {});
  for (const workflow of state?.workflows.filter((item) => item.status === "failed") ?? []) void abortWorkflowJobs(runtime, workflow.id).catch(() => {});
  void requestDrive(runtime).catch(() => {});
}
function workerCompaction(pi: ExtensionAPI) {
  const compaction = pi.getSettings().compaction as (CompactionSettings & JsonValue) | undefined;
  return compaction ? { compaction } : {};
}
const run = promisify(execFile);
const preview = (text: string, max = 160) => { const flat = text.replace(/\s+/g, " ").trim(); return flat.length > max ? `${flat.slice(0, max - 3)}...` : flat; };
const parkedByConversation = async () => new Map(((await guards()?.pending("teamwork").catch(() => [])) ?? []).map((call) => [call.conversationId, call]));
const queueLine = (state: Readonly<WorkflowsState>) => {
  const running = state.jobs.filter((job) => job.status === "running").length;
  const queued = state.workflows.filter((workflow) => workflow.status === "running").reduce((count, workflow) => count + (workflow.mode === "chain" ? Number(workflow.tasks.some((task) => task.status === "pending")) : workflow.tasks.filter((task) => task.status === "pending").length), 0);
  return `${running} running, ${queued} queued (at most ${MAX_CONCURRENCY} at once)`;
};

/** Each git worktree of the repository at `cwd` with uncommitted changes, so a stopped worker's partial work is not lost. */
export async function uncommittedWork(cwd: string): Promise<string[]> {
  const list = await run("git", ["-C", cwd, "worktree", "list", "--porcelain"]).catch(() => undefined);
  if (!list) return [];
  const paths = list.stdout.split("\n").filter((line) => line.startsWith("worktree ")).map((line) => line.slice(9));
  const dirty = await Promise.all(paths.map(async (path) => {
    const files = (await run("git", ["-C", path, "status", "--porcelain"]).catch(() => ({ stdout: "" }))).stdout.split("\n").filter(Boolean).length;
    return files ? `${path}: ${files} uncommitted file${files === 1 ? "" : "s"}` : undefined;
  }));
  return dirty.filter((line) => line !== undefined);
}

/** What a workflow's tasks are doing: status, a parked approval, the last tool call and when the worker last wrote. */
export async function describeWorkflow(runtime: DurableRuntime, workflow: Readonly<Workflow>, state: Readonly<WorkflowsState>, parked: Map<number, ParkedCall>) {
  const lines = [`Workflow ${workflow.id} · ${workflow.mode} · ${workflow.status}${workflow.error ? ` · ${workflow.error}` : ""} · ${queueLine(state)}`];
  for (const [index, task] of workflow.tasks.entries()) {
    const job = state.jobs.find((item) => item.id === task.jobId);
    const approval = job && task.status === "running" ? parked.get(job.conversationId) : undefined;
    lines.push(`${index + 1}. ${task.role} [${approval ? "awaiting approval" : task.status}]${job ? ` job ${job.id}` : ""}${task.error ? ` · ${task.error}` : ""}`);
    if (approval) lines.push(`   waits for /guards-approve teamwork:${approval.taskId} or /guards-deny teamwork:${approval.taskId} (${approval.reason}): ${preview(approval.command)}`);
    if (!job || task.status !== "running") continue;
    const conversation = await runtime.harness.conversation(job.conversationId as ConversationId, BACKGROUND_CONTEXT);
    const recent = conversation ? (await conversation.entries({}, 20, undefined, BACKGROUND_CONTEXT)).items : [];
    const stamped = recent.flatMap((entry) => entry.model ?? []).find((message) => typeof (message as { timestamp?: unknown }).timestamp === "number") as { timestamp: number } | undefined;
    const call = recent.flatMap((entry) => entry.model ?? []).flatMap((message) => message.role === "assistant" ? message.content.filter((part) => part.type === "toolCall").reverse() : []).at(0);
    if (stamped) lines.push(`   last activity ${Math.round((Date.now() - stamped.timestamp) / 1000)}s ago`);
    if (call) lines.push(`   last tool call: ${call.name} ${preview(JSON.stringify(call.arguments))}`);
  }
  if (workflow.tasks.some((task) => task.status === "failed" || task.status === "aborted")) {
    const dirty = await uncommittedWork(workflow.cwd);
    if (dirty.length) lines.push("Uncommitted changes, possibly a stopped worker's partial work:", ...dirty.map((line) => `  ${line}`));
  }
  return lines.join("\n");
}

export async function workflowStatus(runtime: DurableRuntime, id?: string) {
  const state = await runtime.harness.snapshot(Workflows, BACKGROUND_CONTEXT);
  if (!state?.workflows.length) return "No teamwork workflows.";
  const active = state.workflows.filter((workflow) => workflow.status === "running" || workflow.status === "cancelling");
  const chosen = id ? state.workflows.filter((workflow) => workflow.id === id) : active.length ? active : state.workflows.slice(-1);
  if (!chosen.length) throw new Error(`Unknown workflow ${id}`);
  const parked = await parkedByConversation();
  return (await Promise.all(chosen.map((workflow) => describeWorkflow(runtime, workflow, state, parked)))).join("\n\n");
}

const APPROVAL_POLL_MS = 15_000;
export async function waitForWorkflow(runtime: DurableRuntime, id: string, signal: AbortSignal | undefined, onUpdate?: (event: any) => void) {
  const stopped = `Stopped waiting; durable workflow ${id} continues in the background. Check it with teamwork_status, or cancel it with /teamwork cancel.`;
  const watch = await runtime.harness.watchDoc(Workflows, BACKGROUND_CONTEXT);
  if (!watch) throw new Error("Workflow record disappeared");
  return await new Promise<Workflow>((resolve, reject) => {
    let done = false, last = "", parked = new Map<number, ParkedCall>();
    const finish = (error?: Error, workflow?: Workflow) => { if (done) return; done = true; clearInterval(poll); signal?.removeEventListener("abort", abort); void watch.stop(); if (error) reject(error); else resolve(workflow!); };
    const abort = () => finish(new Error(stopped));
    const check = (state: Readonly<WorkflowsState>) => {
      if (done) return;
      const workflow = state.workflows.find((item) => item.id === id);
      if (!workflow) return finish(new Error("Workflow record disappeared"));
      const tasks = workflow.tasks.map((task, index) => {
        const approval = task.status === "running" ? parked.get(state.jobs.find((job) => job.id === task.jobId)?.conversationId ?? -1) : undefined;
        return approval ? `${index + 1}. ${task.role} awaiting approval: /guards-approve teamwork:${approval.taskId} (${approval.reason}): ${preview(approval.command, 100)}` : `${index + 1}. ${task.role} ${task.status}`;
      });
      const progress = `${workflow.status}\n${tasks.join("\n")}`;
      if (progress !== last) { last = progress; onUpdate?.({ content: [{ type: "text", text: `Teamwork ${id}: ${workflow.status} · ${queueLine(state)}\n${tasks.join("\n")}` }], details: workflow }); }
      if (workflow.status !== "running" && workflow.status !== "cancelling" && !workflow.tasks.some((task) => task.status === "running")) finish(undefined, workflow as Workflow);
    };
    // A parked approval changes no workflow record, so poll the guards for it.
    const poll = setInterval(() => void parkedByConversation().then((next) => { parked = next; check(watch.value); }).catch(() => {}), APPROVAL_POLL_MS);
    poll.unref?.();
    if (signal?.aborted) return abort();
    signal?.addEventListener("abort", abort, { once: true });
    void watch.closed.then(() => finish(new Error(stopped)));
    watch.start(async (call) => { if (call.kind === "value") check(call.value); });
    check(watch.value);
  });
}

export default function (pi: ExtensionAPI) {
  pi.registerTool({
    name: "teamwork_define_role", label: "Define Teamwork Role", description: "Define a named Durable Pi subagent role for this session. Role tools are an allowlist.", parameters: RoleSchema,
    async execute(_id, params, _signal, _onUpdate, ctx: ExtensionToolContext) {
      if (!/^[a-z0-9][a-z0-9_-]{0,47}$/i.test(params.name)) throw new Error("Role names must use letters, digits, _ or -");
      const tools = params.tools === undefined ? ["read"] : [...params.tools];
      roleTools(tools);
      const runtime = await openRuntime(ctx);
      await runtime.harness.commit(async (tx) => { const state = await tx.doc(Workflows); (state.roles ??= {})[params.name] = { instructions: params.instructions, tools }; }, BACKGROUND_CONTEXT);
      return { content: [{ type: "text", text: `Defined teamwork role ${params.name}.` }], details: { name: params.name, tools } };
    },
  });
  pi.registerTool({
    name: "teamwork_workflow", label: "Teamwork Workflow", description: "Run durable single, parallel, or sequential-chain Pi conversations. Work and transcripts survive process restarts; child output is untrusted.", parameters: WorkSchema,
    async execute(_id, params, signal, onUpdate, ctx: ExtensionToolContext) {
      if (params.mode === "single" && params.tasks.length !== 1) throw new Error("Single mode requires exactly one task");
      if ((params.review || params.testCommand?.trim()) && !guards()) throw new Error("review and testCommand need the pi-guards extension; install it with pi install, then restart Pi");
      const runtime = await openRuntime(ctx);
      const state = await runtime.harness.snapshot(Workflows, BACKGROUND_CONTEXT);
      const roles = new Map([...DEFAULT_ROLES, ...Object.entries(state?.roles ?? {})]);
      const tasks = params.tasks.map((task) => {
        const role = roles.get(task.role);
        if (!role) throw new Error(`Unknown role "${task.role}"; define it with teamwork_define_role first`);
        roleTools(role.tools);
        return { ...task, instructions: role.instructions, tools: [...role.tools], status: "pending" as const };
      });
      const model = ctx.model;
      if (!model) throw new Error("Select a Pi model before starting a Durable worker");
      const workflow: Workflow = { id: crypto.randomUUID().slice(0, 8), mode: params.mode, provider: model.provider, modelId: model.id, thinkingLevel: ctx.thinkingLevel ?? "medium", cwd: ctx.cwd, ...workerCompaction(pi), ...(params.review ? { review: true } : {}), ...(params.testCommand?.trim() ? { testCommand: params.testCommand.trim() } : {}), tasks, status: "running" };
      await runtime.harness.commit(async (tx) => {
        const current = await tx.doc(Workflows);
        const active = activeTaskSlots(current.workflows);
        const requested = workflow.mode === "chain" ? 1 : tasks.length;
        if (active + requested > MAX_TASKS) throw new Error(`At most ${MAX_TASKS} active teamwork task slots are allowed`);
        current.workflows.push(workflow);
      }, BACKGROUND_CONTEXT);
      await requestDrive(runtime);
      const result = await waitForWorkflow(runtime, workflow.id, signal, onUpdate);
      const dirty = result.status === "done" ? [] : await uncommittedWork(result.cwd);
      const report = result.tasks.map((task, index) => `### ${index + 1}. ${task.role} [${task.status}]${task.error ? `\nError: ${task.error}` : ""}\n${task.output ?? "(no output)"}`).join("\n\n");
      return { content: [{ type: "text", text: `${report}${dirty.length ? `\n\nUncommitted changes, possibly a stopped worker's partial work:\n${dirty.join("\n")}` : ""}` }], details: { mode: result.mode, workflowId: result.id, status: result.status, error: result.error, tasks: result.tasks, uncommitted: dirty } };
    },
  });
  pi.registerTool({
    name: "teamwork_status", label: "Teamwork Status", description: "Report what durable teamwork workflows are doing now: each task's status, a command parked on approval, the last tool call and activity time, the queue, and uncommitted changes left by stopped workers. Use it instead of waiting blind or reading the database.", parameters: StatusSchema,
    async execute(_id, params, _signal, _onUpdate, ctx: ExtensionToolContext) {
      if (!(await hasDurableRuntime(ctx, "pi-teamwork"))) return { content: [{ type: "text", text: "No teamwork workflows." }], details: {} };
      return { content: [{ type: "text", text: await workflowStatus(await openRuntime(ctx), params.workflowId) }], details: {} };
    },
  });
  pi.registerCommand("teamwork", { description: "Run, list, or cancel Durable Pi subagents", handler: async (args, ctx) => {
    const runtime = await openRuntime(ctx);
    const input = args.trim();
    if (input === "status" || !input) {
      ctx.ui.notify(await workflowStatus(runtime), "info");
    } else if (input === "cancel") {
      await cancelWorkflows(runtime);
      ctx.ui.notify("Cancellation requested for active Durable workflows.", "info");
    } else pi.sendUserMessage(`Use the teamwork_workflow tool to handle this request with one-level task delegation: ${input}`, { deliverAs: "followUp" });
  } });
  pi.on("session_start", (_event, ctx) => { void resumeJobs(ctx).catch((error) => ctx.ui.setStatus("pi-teamwork", error instanceof Error ? error.message : String(error))); });
  pi.on("session_shutdown", async (event: any, ctx) => { if (event.reason !== "reload") { guards()?.closed("teamwork"); await closeDurableRuntime(ctx, "pi-teamwork"); } });
}
