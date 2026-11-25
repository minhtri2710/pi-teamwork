import { createHash } from "node:crypto";
import { access, chmod, mkdir, open, readFile, unlink } from "node:fs/promises";
import { openSync, closeSync, writeSync } from "node:fs";
import { join } from "node:path";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels, createProvider, type Api, type AssistantMessageEventStream, type Model, type MutableModels, type ProviderStreams } from "@earendil-works/pi-ai";
import { Harness, MemoryStorage, createRegistry, type Extension, type Harness as DurableHarness, type Registry, type ToolRegistration } from "@earendil-works/pi-durable";
import { openNodeSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/node";
import { NodeExecutionEnv } from "@earendil-works/pi-durable/env/node";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { ModelRegistry } from "@earendil-works/pi-coding-agent";

export type DurableRuntime = { harness: DurableHarness; registry: Registry; models: MutableModels; settling: Set<string>; drive?: Promise<void> };
type CachedRuntime = DurableRuntime & { sessionId: string; releaseLock: () => Promise<void>; persistent: boolean };
const RUNTIME_KEY = Symbol.for("pi-durable-extension.runtimes");
const pending = new Map<string, Promise<CachedRuntime>>();
const closing = new Map<string, Promise<void>>();

function runtimes(): Map<string, CachedRuntime> {
  const global = globalThis as typeof globalThis & { [RUNTIME_KEY]?: Map<string, CachedRuntime> };
  return (global[RUNTIME_KEY] ??= new Map());
}

function syncModels(target: MutableModels, source: ModelRegistry) {
  const groups = new Map<string, Model<Api>[]>();
  for (const model of source.getAll()) groups.set(model.provider, [...(groups.get(model.provider) ?? []), model]);
  for (const provider of target.getProviders()) if (!groups.has(provider.id)) target.deleteProvider(provider.id);
  for (const [id, models] of groups) {
    const sourceProvider = source.getProvider(id);
    const api: ProviderStreams = {
      stream: (model, context, options) => source.stream(model, context, options) as AssistantMessageEventStream,
      streamSimple: (model, context, options) => source.streamSimple(model, context, options),
    };
    target.setProvider(createProvider<Api>({
      id,
      name: sourceProvider?.name ?? id,
      baseUrl: sourceProvider?.baseUrl,
      headers: sourceProvider?.headers,
      auth: {
        apiKey: {
          name: `Pi session auth (${id})`,
          async check() {
            return models.some((model) => source.hasConfiguredAuth(model)) ? { type: "api_key", source: "Pi session" } : undefined;
          },
          async resolve() {
            const model = models.find((candidate) => source.hasConfiguredAuth(candidate));
            if (!model) return undefined;
            const result = await source.getApiKeyAndHeaders(model);
            return result.ok
              ? { auth: { apiKey: result.apiKey, headers: result.headers, baseUrl: result.baseUrl }, env: result.env, source: "Pi session" }
              : undefined;
          },
        },
      },
      models,
      api,
    }));
  }
}

function databasePath(ctx: ExtensionContext, namespace: string) {
  const sessionId = ctx.sessionManager.getSessionId();
  const dir = ctx.sessionManager.getSessionDir();
  const hash = createHash("sha256").update(sessionId).digest("hex").slice(0, 24);
  return { sessionId, path: dir ? join(dir, `${namespace}-${hash}.sqlite`) : `memory:${namespace}:${sessionId}`, persistent: Boolean(dir) };
}

export async function hasDurableRuntime(ctx: ExtensionContext, namespace: string) {
  const { path, persistent } = databasePath(ctx, namespace);
  if (runtimes().has(path) || pending.has(path)) return true;
  if (!persistent) return false;
  try { await access(path); return true; }
  catch (error: any) { if (error.code === "ENOENT") return false; throw error; }
}

export async function getDurableRuntime(ctx: ExtensionContext, namespace: string, extensions: readonly Extension<ToolRegistration>[] = []): Promise<DurableRuntime> {
  const { sessionId, path } = databasePath(ctx, namespace);
  let cached = runtimes().get(path);
  const closingRuntime = closing.get(path);
  if (closingRuntime) await closingRuntime;
  cached = runtimes().get(path);
  if (!cached) {
    let opening = pending.get(path);
    if (!opening) {
      opening = createDurableRuntime(ctx, namespace, sessionId, path, extensions);
      pending.set(path, opening);
    }
    try { cached = await opening; }
    finally { if (pending.get(path) === opening) pending.delete(path); }
  }
  const closingAgain = closing.get(path);
  if (closingAgain) {
    await closingAgain;
    return getDurableRuntime(ctx, namespace, extensions);
  }
  if (!cached) return getDurableRuntime(ctx, namespace, extensions);
  if (cached.sessionId !== sessionId) throw new Error("Durable session identity mismatch");
  for (const extension of extensions) cached.registry.install(extension);
  syncModels(cached.models, ctx.modelRegistry);
  cached.harness.resume();
  return cached;
}

async function createDurableRuntime(ctx: ExtensionContext, namespace: string, sessionId: string, path: string, extensions: readonly Extension<ToolRegistration>[]): Promise<CachedRuntime> {
  const { persistent } = databasePath(ctx, namespace);
  let releaseLock = async () => {};
  try {
  let storage;
  if (persistent) {
    await mkdir(ctx.sessionManager.getSessionDir(), { recursive: true, mode: 0o700 });
    const lockPath = `${path}.lock`;
    let lock: number;
    try { lock = openSync(lockPath, "wx", 0o600); }
    catch (error: any) {
      if (error.code !== "EEXIST") throw error;
      const contents = await readFile(lockPath, "utf8").catch(() => "");
      if (!/^\d+$/.test(contents)) throw new Error("Durable runtime lock is incomplete; refusing to remove it");
      const owner = Number(contents);
      if (!Number.isSafeInteger(owner) || owner < 1) throw new Error("Durable runtime lock has an invalid process ID");
      try { process.kill(owner, 0); throw new Error(`Durable runtime already open for this Pi session (pid ${owner})`); }
      catch (check: any) { if (check.code === "EPERM") throw new Error(`Durable runtime already open for this Pi session (pid ${owner})`); if (check.code !== "ESRCH") throw check; }
      throw new Error(`Stale Durable runtime lock for pid ${owner}; remove ${lockPath} after confirming no process is using this session`);
    }
    try { writeSync(lock, String(process.pid)); }
    catch (error) {
      closeSync(lock);
      await unlink(lockPath);
      throw error;
    }
    closeSync(lock);
    releaseLock = async () => {
      const owner = Number(await readFile(lockPath, "utf8").catch(() => ""));
      if (owner === process.pid) await unlink(lockPath).catch((error: any) => { if (error.code !== "ENOENT") throw error; });
    };
    const file = await open(path, "a", 0o600);
    await file.close();
    await chmod(path, 0o600);
    storage = await openNodeSqliteStorage(path, { busyTimeoutMs: 5000 });
  } else {
    storage = new MemoryStorage();
  }
  const registry = createRegistry();
  for (const extension of extensions) registry.install(extension);
  const models = createModels();
  syncModels(models, ctx.modelRegistry);
  const harness = await Harness.open(storage, {
    models: () => models,
    registry,
    env: ({ cwd }) => new NodeExecutionEnv({ cwd: cwd ?? ctx.cwd }),
    settings: { restartLimit: 3 },
  }, BACKGROUND_CONTEXT);
  const runtime = { harness, registry, models, sessionId, releaseLock, persistent, settling: new Set<string>() };
  runtimes().set(path, runtime);
  harness.resume();
  return runtime;
  } catch (error) {
    await releaseLock();
    throw error;
  }
}

export async function closeDurableRuntime(ctx: ExtensionContext, namespace: string) {
  const { path } = databasePath(ctx, namespace);
  const alreadyClosing = closing.get(path);
  if (alreadyClosing) return alreadyClosing;
  let runtime = runtimes().get(path);
  const opening = pending.get(path);
  if (!runtime && opening) runtime = await opening;
  if (!runtime) return;
  runtimes().delete(path);
  const closingRuntime = runtime.harness.close(BACKGROUND_CONTEXT).finally(async () => {
    await runtime!.releaseLock();
    closing.delete(path);
  });
  closing.set(path, closingRuntime);
  await closingRuntime;
}

