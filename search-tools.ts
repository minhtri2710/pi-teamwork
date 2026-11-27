import { Type } from "typebox";
import type { Context } from "@earendil-works/chord/context";
import { defineExtension, defineTool, type ToolExecutionApi } from "@earendil-works/pi-durable";

const MAX_OUTPUT = 50_000;

/** Runs `argv` without a shell in the worker's directory; exit 1 is "nothing found" for rg. */
async function search(argv: string[], api: ToolExecutionApi, context: Context) {
  if (!api.env) throw new Error("No execution environment");
  let output = "";
  const result = await api.env.exec(argv, { onOutput: (text) => { if (output.length <= MAX_OUTPUT) output += text; } }, context);
  if (!result.ok) throw result.error;
  const text = output.length > MAX_OUTPUT ? `${output.slice(0, MAX_OUTPUT)}\n(truncated; narrow the pattern or path)` : output || "(no matches)";
  return { output: [{ type: "text" as const, text }], ...(result.value.exitCode > 1 ? { isError: true } : {}) };
}

/** Read-only search tools a role may list beside `read`: `grep` runs ripgrep, `find` runs fd. */
export const SearchTools = defineExtension({
  name: "teamwork.search",
  tools: [
    defineTool({
      name: "grep", description: "Search file contents with ripgrep (smart case, git-aware). Returns path:line:text.", replay: "safe",
      parameters: Type.Object({ pattern: Type.String({ description: "Regex, or literal text with fixed: true" }), path: Type.Optional(Type.String()), glob: Type.Optional(Type.String({ description: "Include glob, e.g. *.ts" })), fixed: Type.Optional(Type.Boolean()) }),
      execute: (args, api, context) => search(["rg", "-n", "--smart-case", ...(args.fixed ? ["-F"] : []), ...(args.glob ? ["-g", args.glob] : []), "-e", args.pattern, "--", args.path ?? "."], api, context),
    }),
    defineTool({
      name: "find", description: "Find files by name with fd (regex on the file name, git-aware).", replay: "safe",
      parameters: Type.Object({ pattern: Type.Optional(Type.String({ description: "Regex on the file name; omit to list all" })), path: Type.Optional(Type.String()) }),
      execute: (args, api, context) => search(["fd", "--color", "never", "--", args.pattern ?? ".", args.path ?? "."], api, context),
    }),
  ],
});
