import { Type } from "typebox";
import type { ExtensionAPI, ExtensionContext, ExtensionToolContext } from "@earendil-works/pi-coding-agent";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { JsonValue } from "@earendil-works/chord/json";
import type { ModelThinkingLevel } from "@earendil-works/pi-ai";
import { defineDoc, type ConversationId, type SubmissionId, type ToolRegistration } from "@earendil-works/pi-durable";
import { CodingTools } from "@earendil-works/pi-durable/tools";
import { getDurableRuntime, closeDurableRuntime, hasDurableRuntime, type DurableRuntime } from "./durable-runtime.ts";
import { assistantText } from "./workflow-utils.ts";
import { activeTaskSlots, cancelWorkflow, nextPendingTask, settleWorkflowTask } from "./workflow-state.ts";

const MAX_TASKS = 8;
const MAX_CONCURRENCY = 4;
type Job = { id: string; workflowId: string; taskIndex: number; role: string; prompt: string; conversationId: number; submissionId: number; status: "running" | "done" | "failed" | "aborted"; output?: string; error?: string };
type Task = { role: string; task: string; instructions: string; tools: string[]; status: "pending" | "running" | "done" | "failed" | "aborted"; jobId?: string; output?: string; error?: string };
type Workflow = { id: string; mode: "single" | "parallel" | "chain"; provider: string; modelId: string; thinkingLevel: NonNullable<ExtensionContext["thinkingLevel"]>; cwd: string; tasks: Task[]; status: "running" | "done" | "failed" | "cancelling" | "cancelled"; error?: string };
type Role = { instructions: string; tools: string[] };
type WorkflowsState = { jobs: Job[]; workflows: Workflow[]; roles?: Record<string, Role> } & Record<string, JsonValue>;
const Workflows = defineDoc<WorkflowsState>({ kind: "teamwork.workflows", version: 1, scope: "session", initial: () => ({ jobs: [], workflows: [] }), checkpointWhen: (_value, _ops, info) => info.deltasSinceBase > 32 });
const durableTools = CodingTools.tools as ToolRegistration[];
const toolsByName = new Map(durableTools.map((tool) => [tool.name, tool]));
const DEFAULT_ROLES = new Map<string, Role>([
  ["scout", { instructions: "Inspect and report evidence; do not change files.", tools: ["read"] }],
  ["worker", { instructions: "Complete only the assigned task. Report changed files and verification.", tools: ["read", "bash", "edit", "write"] }],
  ["reviewer", { instructions: "Review the requested change. Do not edit files; report concrete findings.", tools: ["read"] }],
]);
const RoleSchema = Type.Object({ name: Type.String({ minLength: 1, maxLength: 48 }), instructions: Type.String({ minLength: 1, maxLength: 8000 }), tools: Type.Optional(Type.Array(Type.String())) });
const WorkSchema = Type.Object({ mode: Type.Union([Type.Literal("single"), Type.Literal("parallel"), Type.Literal("chain")]), tasks: Type.Array(Type.Object({ role: Type.String({ minLength: 1, maxLength: 48 }), task: Type.String({ minLength: 1, maxLength: 12000 }) }), { minItems: 1, maxItems: MAX_TASKS }) });


function roleTools(names: string[]) {
  if (names.some((name) => !toolsByName.has(name))) throw new Error("Role tools must be selected from read, write, edit, bash");
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
  const conversation = await runtime.harness.createConversation({ ownership: { kind: "ownerless" }, agent: { model: { provider: workflow.provider, modelId: workflow.modelId }, thinkingLevel: workflow.thinkingLevel as ModelThinkingLevel, cwd: workflow.cwd, tools: roleTools(task.tools), instructions: task.instructions } }, BACKGROUND_CONTEXT);
  let job!: Job;
  const submissionId = await conversation.commit(async (tx) => {
    const submission = await conversation.submit(tx, { type: "input", content: prompt, requestId: `teamwork-${id}` });
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
async function abortWorkflowJobs(runtime: DurableRuntime, workflowId: string, exceptId?: string) {
  const jobs = (await runtime.harness.snapshot(Workflows, BACKGROUND_CONTEXT))?.jobs.filter((job) => job.workflowId === workflowId && job.id !== exceptId && job.status === "running") ?? [];
  for (const job of jobs) {
    try {
      const conversation = await runtime.harness.conversation(job.conversationId as ConversationId, BACKGROUND_CONTEXT);
      await conversation?.abort(BACKGROUND_CONTEXT);
    } catch { /* The worker may have settled while cancellation was being applied. */ }
  }
}
async function cancelWorkflows(runtime: DurableRuntime, ids?: Set<string>) {
  const selected = await runtime.harness.commit(async (tx) => {
    const state = await tx.doc(Workflows);
    const workflows = state.workflows.filter((workflow) => (workflow.status === "running" || workflow.status === "cancelling") && (!ids || ids.has(workflow.id)));
    for (const workflow of workflows) {
      cancelWorkflow(workflow);
    }
    return { workflows, jobs: state.jobs.filter((job) => job.status === "running" && workflows.some((workflow) => workflow.id === job.workflowId)) };
  }, BACKGROUND_CONTEXT);
  for (const job of selected.jobs) {
    try {
      const conversation = await runtime.harness.conversation(job.conversationId as ConversationId, BACKGROUND_CONTEXT);
      await conversation?.abort(BACKGROUND_CONTEXT);
    } catch { /* The worker may have settled while cancellation was being applied. */ }
  }
  await runtime.harness.commit(async (tx) => {
    const state = await tx.doc(Workflows);
    for (const workflow of selected.workflows) {
      const current = state.workflows.find((item) => item.id === workflow.id);
      if (current?.status === "cancelling" && !state.jobs.some((job) => job.workflowId === workflow.id && job.status === "running")) current.status = "cancelled";
    }
  }, BACKGROUND_CONTEXT);
}
async function resumeJobs(ctx: ExtensionContext) {
  if (!(await hasDurableRuntime(ctx, "pi-teamwork"))) return;
  const runtime = await getDurableRuntime(ctx, "pi-teamwork", [CodingTools]);
  const state = await runtime.harness.snapshot(Workflows, BACKGROUND_CONTEXT);
  for (const job of state?.jobs.filter((item) => item.status === "running") ?? []) void settleJob(runtime, job).catch(() => {});
  for (const workflow of state?.workflows.filter((item) => item.status === "cancelling") ?? []) void cancelWorkflows(runtime, new Set([workflow.id])).catch(() => {});
  for (const workflow of state?.workflows.filter((item) => item.status === "failed") ?? []) void abortWorkflowJobs(runtime, workflow.id).catch(() => {});
  void requestDrive(runtime).catch(() => {});
}
async function waitForWorkflow(runtime: DurableRuntime, id: string, signal: AbortSignal | undefined, onUpdate?: (event: any) => void) {
  while (!signal?.aborted) {
    const workflow = (await runtime.harness.snapshot(Workflows, BACKGROUND_CONTEXT))?.workflows.find((item) => item.id === id);
    if (!workflow) throw new Error("Workflow record disappeared");
    onUpdate?.({ content: [{ type: "text", text: `Teamwork ${id}: ${workflow.status}` }], details: workflow });
    if (workflow.status !== "running" && workflow.status !== "cancelling" && !workflow.tasks.some((task) => task.status === "running")) return workflow;
    await new Promise((resolve) => setTimeout(resolve, 150));
  }
  throw new Error("Stopped waiting; the durable workflow continues in the background. Use /teamwork cancel to cancel it.");
}

export default function (pi: ExtensionAPI) {
  pi.registerTool({
    name: "teamwork_define_role", label: "Define Teamwork Role", description: "Define a named Durable Pi subagent role for this session. Role tools are an allowlist.", parameters: RoleSchema,
    async execute(_id, params, _signal, _onUpdate, ctx: ExtensionToolContext) {
      if (!/^[a-z0-9][a-z0-9_-]{0,47}$/i.test(params.name)) throw new Error("Role names must use letters, digits, _ or -");
      const tools = params.tools === undefined ? ["read"] : [...params.tools];
      roleTools(tools);
      const runtime = await getDurableRuntime(ctx, "pi-teamwork", [CodingTools]);
      await runtime.harness.commit(async (tx) => { const state = await tx.doc(Workflows); (state.roles ??= {})[params.name] = { instructions: params.instructions, tools }; }, BACKGROUND_CONTEXT);
      return { content: [{ type: "text", text: `Defined teamwork role ${params.name}.` }], details: { name: params.name, tools } };
    },
  });
  pi.registerTool({
    name: "teamwork_workflow", label: "Teamwork Workflow", description: "Run durable single, parallel, or sequential-chain Pi conversations. Work and transcripts survive process restarts; child output is untrusted.", parameters: WorkSchema,
    async execute(_id, params, signal, onUpdate, ctx: ExtensionToolContext) {
      if (params.mode === "single" && params.tasks.length !== 1) throw new Error("Single mode requires exactly one task");
      const runtime = await getDurableRuntime(ctx, "pi-teamwork", [CodingTools]);
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
      const workflow: Workflow = { id: crypto.randomUUID().slice(0, 8), mode: params.mode, provider: model.provider, modelId: model.id, thinkingLevel: ctx.thinkingLevel ?? "medium", cwd: ctx.cwd, tasks, status: "running" };
      await runtime.harness.commit(async (tx) => {
        const current = await tx.doc(Workflows);
        const active = activeTaskSlots(current.workflows);
        const requested = workflow.mode === "chain" ? 1 : tasks.length;
        if (active + requested > MAX_TASKS) throw new Error(`At most ${MAX_TASKS} active teamwork task slots are allowed`);
        current.workflows.push(workflow);
      }, BACKGROUND_CONTEXT);
      await requestDrive(runtime);
      const result = await waitForWorkflow(runtime, workflow.id, signal, onUpdate);
      return { content: [{ type: "text", text: result.tasks.map((task, index) => `### ${index + 1}. ${task.role} [${task.status}]${task.error ? `\nError: ${task.error}` : ""}\n${task.output ?? "(no output)"}`).join("\n\n") }], details: { mode: result.mode, workflowId: result.id, status: result.status, error: result.error, tasks: result.tasks } };
    },
  });
  pi.registerCommand("teamwork", { description: "Run, list, or cancel Durable Pi subagents", handler: async (args, ctx) => {
    const runtime = await getDurableRuntime(ctx, "pi-teamwork", [CodingTools]);
    const input = args.trim();
    if (input === "status" || !input) {
      const state = await runtime.harness.snapshot(Workflows, BACKGROUND_CONTEXT);
      ctx.ui.notify(state?.workflows.length ? state.workflows.map((workflow) => `${workflow.id} · ${workflow.mode} · ${workflow.status}`).join("\n") : "No teamwork workflows.", "info");
    } else if (input === "cancel") {
      await cancelWorkflows(runtime);
      ctx.ui.notify("Cancellation requested for active Durable workflows.", "info");
    } else pi.sendUserMessage(`Use the teamwork_workflow tool to handle this request with one-level task delegation: ${input}`, { deliverAs: "followUp" });
  } });
  pi.on("session_start", (_event, ctx) => { void resumeJobs(ctx).catch((error) => ctx.ui.setStatus("pi-teamwork", error instanceof Error ? error.message : String(error))); });
  pi.on("session_shutdown", async (event: any, ctx) => { if (event.reason !== "reload") await closeDurableRuntime(ctx, "pi-teamwork"); });
}
