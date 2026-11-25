export type WorkflowTask = { status: "pending" | "running" | "done" | "failed" | "aborted"; output?: string; error?: string };
export type WorkflowState = { mode: "single" | "parallel" | "chain"; status: "running" | "done" | "failed" | "cancelling" | "cancelled"; tasks: WorkflowTask[]; error?: string };

export function activeTaskSlots(workflows: readonly WorkflowState[]) {
  return workflows.reduce((count, workflow) => count + (workflow.mode === "chain"
    ? Number(workflow.status === "running" || workflow.tasks.some((task) => task.status === "running"))
    : workflow.tasks.filter((task) => task.status === "running" || (workflow.status === "running" && task.status === "pending")).length), 0);
}

export function nextPendingTask(workflows: readonly (WorkflowState & { id: string })[]) {
  for (const workflow of workflows) {
    if (workflow.status !== "running" || (workflow.mode === "chain" && workflow.tasks.some((task) => task.status === "running"))) continue;
    const index = workflow.tasks.findIndex((task) => task.status === "pending");
    if (index >= 0) return { workflowId: workflow.id, index };
  }
}

export function settleWorkflowTask(workflow: WorkflowState, index: number, status: "done" | "failed" | "aborted", output?: string, error?: string) {
  const task = workflow.tasks[index];
  if (!task || task.status !== "running") return false;
  if (workflow.status === "cancelling" || workflow.status === "cancelled") {
    task.status = "aborted";
    delete task.output;
    delete task.error;
    if (workflow.status === "cancelling" && workflow.tasks.every((entry) => entry.status !== "running")) workflow.status = "cancelled";
    return false;
  }
  if (workflow.status === "failed") {
    task.status = "aborted";
    delete task.output;
    delete task.error;
    return false;
  }
  if (workflow.status !== "running") return false;
  task.status = status;
  if (output === undefined) delete task.output;
  else task.output = output;
  if (error === undefined) delete task.error;
  else task.error = error;
  if (status === "failed" || status === "aborted") {
    workflow.status = "failed";
    if (error !== undefined) workflow.error = error;
    else if (status === "aborted") workflow.error = "Worker aborted";
    for (const pending of workflow.tasks) if (pending.status === "pending") pending.status = "aborted";
  } else if (workflow.tasks.every((entry) => entry.status === "done")) workflow.status = "done";
  return status === "failed" || status === "aborted";
}

export function cancelWorkflow(workflow: WorkflowState) {
  if (workflow.status !== "running") return false;
  workflow.status = "cancelling";
  for (const task of workflow.tasks) if (task.status === "pending") task.status = "aborted";
  if (workflow.tasks.every((task) => task.status !== "running")) workflow.status = "cancelled";
  return true;
}
