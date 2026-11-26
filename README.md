# pi-teamwork

Durable one-level Pi task delegation: one-shot tasks, bounded parallel fan-out, and sequential chains. Each worker runs as a `pi-durable` conversation in a session-scoped SQLite database; workflow queues, submissions, worker transcripts, and status survive process restarts.

## Install

```sh
git clone https://github.com/minhtri2710/pi-worker-guards ~/Work/pi-worker-guards
cd ~/Work/pi-teamwork && npm install --legacy-peer-deps
pi install ~/Work/pi-teamwork
```

Pi's local package installer does not install npm dependencies; run `npm install --legacy-peer-deps` in the package directory first. The package registers `teamwork_define_role`, `teamwork_workflow`, and `/teamwork <task|status|cancel>`. Default roles are `scout`, `worker`, and `reviewer`; define session-local roles with an explicit tool allowlist. Worker conversations use the selected Pi model and thinking level with the declared Durable tools; workers do not recursively delegate or launch a child Pi CLI process.

`teamwork_workflow` accepts `{ mode: "single" | "parallel" | "chain", tasks: [{ role, task }] }`. `single` requires exactly one task. Parallel work is bounded to four concurrent workers and eight active task slots; a chain occupies one slot regardless of its length. `chain` passes prior text to the next role. `review: true` and `testCommand` turn on the [pi-worker-guards](https://github.com/minhtri2710/pi-worker-guards) review and test-after-edit loops for the workflow's workers; risky worker `bash` calls always park until `/teamwork-approve` or `/teamwork-deny`. Worker output is untrusted. If a tool call is interrupted, its durable workflow continues; `/teamwork cancel` requests cancellation. Tests use `node --experimental-strip-types --test workflow-utils.test.ts durable-runtime.test.ts`.

## Limits

Requires a selected Pi model and the `pi-durable` Node SQLite backend. Persisted session runtime files are stored in the Pi session directory with owner-only permissions. In-memory Pi sessions use in-memory Durable storage and do not survive process exit. One process may own a given persisted session database at a time; live and stale locks fail closed, and a stale lock must be removed manually only after confirming its process is gone.
