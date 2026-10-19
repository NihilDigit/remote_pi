import { randomUUID } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  writeFileSync,
  realpathSync,
  statSync,
  openSync,
  fstatSync,
  readSync,
  closeSync,
} from "node:fs";
import { dirname, relative, resolve, isAbsolute } from "node:path";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { z } from "zod";
import {
  createSessionCatalog,
  createSessionManagerAdapter,
  type CatalogProject,
} from "./session-catalog.js";
import {
  createHostSessionControl,
  type HostSessionBinding,
  type HostSessionRecord,
} from "./host-session-control.js";
import { createHostEventStream, type HostEventCursor } from "./host-events.js";
import {
  createHostApprovals,
  type HostApprovalConnection,
  type HostApprovalRequest,
} from "./host-approvals.js";
import type { RpcChild, RpcChildOptions } from "./rpc_child.js";

const bindingSchema = z.object({
  hostId: z.string().min(1),
  projectId: z.string().min(1),
  generation: z.string().uuid(),
  runtimeId: z.string().uuid(),
  historyId: z.string().uuid(),
});
const connectionSchema = bindingSchema.extend({
  connectionId: z.string().uuid(),
  approvalEpoch: z.string().uuid(),
});
const commandSchema = z.discriminatedUnion("action", [
  z.object({ action: z.literal("describe") }),
  z.object({ action: z.literal("histories"), projectId: z.string() }),
  z.object({
    action: z.literal("create"),
    projectId: z.string(),
    operationId: z.string().uuid(),
    generation: z.string(),
  }),
  z.object({
    action: z.literal("select"),
    projectId: z.string(),
    historyId: z.string().uuid(),
    generation: z.string(),
  }),
  z.object({ action: z.literal("connect"), binding: bindingSchema }),
  z.object({ action: z.literal("disconnect"), connection: connectionSchema }),
  z.object({
    action: z.literal("poll"),
    connection: connectionSchema,
    cursor: bindingSchema.extend({ sequence: z.number().int().nonnegative() }).optional(),
  }),
  z.object({ action: z.literal("snapshot"), connection: connectionSchema }),
  z.object({
    action: z.literal("prompt"),
    connection: connectionSchema,
    operationId: z.string().uuid(),
    text: z.string().trim().min(1).max(100000),
  }),
  z.object({ action: z.literal("abort"), connection: connectionSchema }),
  z.object({ action: z.literal("stop"), connection: connectionSchema }),
  z.object({ action: z.literal("respond"), connection: connectionSchema, response: z.unknown() }),
  z.object({
    action: z.literal("preview"),
    connection: connectionSchema,
    path: z.string().min(1).max(4096),
  }),
]);

export class HostRecordStore {
  private readonly records = new Map<string, HostSessionRecord>();
  constructor(private readonly path: string) {
    if (existsSync(path)) {
      const data = JSON.parse(readFileSync(path, "utf8"));
      if (data.version !== 1 || !Array.isArray(data.records))
        throw new Error("Invalid host records");
      for (const record of data.records as HostSessionRecord[]) {
        if (!record.recordId || !record.hostId || !record.projectId || !record.generation)
          throw new Error("Invalid host record");
        this.records.set(record.recordId, record);
      }
    }
  }
  async list(hostId: string): Promise<HostSessionRecord[]> {
    return structuredClone([...this.records.values()].filter((record) => record.hostId === hostId));
  }
  async put(record: HostSessionRecord): Promise<void> {
    const next = new Map(this.records).set(record.recordId, structuredClone(record));
    mkdirSync(dirname(this.path), { recursive: true, mode: 0o700 });
    const temporary = `${this.path}.${randomUUID()}.tmp`;
    writeFileSync(temporary, JSON.stringify({ version: 1, records: [...next.values()] }), {
      mode: 0o600,
      flush: true,
    });
    renameSync(temporary, this.path);
    this.records.set(record.recordId, structuredClone(record));
  }
}

interface Runtime {
  binding: HostSessionBinding;
  child: RpcChild;
  events: ReturnType<typeof createHostEventStream>;
  prompts: Set<string>;
}

export interface HostServiceOptions {
  hostId: string;
  name: string;
  projects: readonly CatalogProject[];
  recordsPath: string;
  startChild(id: string, options: Pick<RpcChildOptions, "cwd" | "session">): RpcChild;
  hasOtherWriter(cwd: string): boolean;
  now?: () => number;
}

/** Session policy and RPC projection; the supervisor owns every child process. */
export class HostService {
  readonly generation = randomUUID();
  private readonly records: HostRecordStore;
  private readonly runtimes = new Map<string, Runtime>();
  private readonly clients = new Map<
    string,
    { actor: string; connection: HostApprovalConnection; touched: number }
  >();
  private readonly pending = new Map<string, HostApprovalRequest>();
  private readonly now: () => number;
  private readonly catalog;
  private readonly control;
  private readonly approvals;
  private readonly expiryTimer: ReturnType<typeof setInterval>;

  constructor(private readonly options: HostServiceOptions) {
    this.now = options.now ?? (() => performance.now());
    this.records = new HostRecordStore(options.recordsPath);
    this.catalog = createSessionCatalog({
      projects: options.projects,
      sessions: createSessionManagerAdapter(SessionManager),
      running: (projectId, historyId) => {
        const runtime = [...this.runtimes.values()].find(
          (r) =>
            r.binding.projectId === projectId &&
            r.binding.historyId === historyId &&
            r.child.state === "running",
        );
        return runtime ? { id: runtime.binding.runtimeId } : undefined;
      },
    });
    this.approvals = createHostApprovals({
      hostId: options.hostId,
      generation: this.generation,
      maxConnections: 64,
      maxRequests: 10000,
      maxTtlMs: 120000,
      now: this.now,
      respond: async (binding, response) => {
        this.runtime(binding).child.write(response);
      },
    });
    this.control = createHostSessionControl({
      hostId: options.hostId,
      generation: this.generation,
      catalog: this.catalog,
      records: this.records,
      liveView: async (record) => {
        const runtime = record.runtimeId ? this.runtimes.get(record.runtimeId) : undefined;
        if (runtime?.child.state === "running") return { kind: "live", lease: runtime.binding };
        if (
          record.generation !== this.generation &&
          record.status !== "ended" &&
          record.status !== "failed"
        ) {
          // Only absence proves death. A reused PID or permission error stays unknown.
          if (!record.processId) return { kind: "unknown" };
          try {
            process.kill(record.processId, 0);
            return { kind: "unknown" };
          } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== "ESRCH") return { kind: "unknown" };
          }
        }
        const project = this.project(record.projectId);
        return this.options.hasOtherWriter(project.cwd) ? { kind: "unknown" } : { kind: "dead" };
      },
      runtime: {
        open: async (request) => {
          if (this.runtimes.size >= 64)
            throw new Error("Host session limit reached; restart the host after stopping sessions");
          const project = this.project(request.projectId);
          const runtimeId = randomUUID();
          let session: NonNullable<RpcChildOptions["session"]>;
          let historyId: string;
          if (request.intent.kind === "history") {
            const histories = await SessionManager.list(project.cwd);
            const matches = histories.filter(
              (h) =>
                h.cwd === project.cwd &&
                h.id === (request.intent as { historyId: string }).historyId,
            );
            if (matches.length !== 1) throw new Error("History no longer exists or is ambiguous");
            session = { kind: "history", file: matches[0]!.path };
            historyId = matches[0]!.id;
          } else {
            historyId = randomUUID();
            session = { kind: "new", id: historyId };
          }
          const binding: HostSessionBinding = {
            hostId: options.hostId,
            projectId: project.id,
            generation: this.generation,
            runtimeId,
            historyId,
          };
          const child = options.startChild(runtimeId, { cwd: project.cwd, session });
          const runtime = {
            binding,
            child,
            events: createHostEventStream(binding, { capacity: 256, maxEventBytes: 65536 }),
            prompts: new Set<string>(),
          };
          this.runtimes.set(runtimeId, runtime);
          child.on("stdout", (line: string) => this.onEvent(runtime, line));
          child.on("exit", () => {
            this.invalidateRuntime(runtime);
            runtime.events.publish({ type: "host_runtime_ended" });
          });
          child.spawn();
          if (child.pid) binding.processId = child.pid;
          return binding;
        },
        getState: async (lease) => {
          const runtime = this.runtimes.get(lease.runtimeId);
          if (!runtime || runtime.child.state !== "running") return { status: "ended" };
          const state = (await runtime.child.request("get_state")) as { sessionId?: string };
          return state.sessionId === runtime.binding.historyId
            ? { status: "running", historyId: state.sessionId }
            : { status: "unknown" };
        },
      },
    });
    this.expiryTimer = setInterval(() => this.expire(), 1000);
    this.expiryTimer.unref();
  }

  async handle(actor: string, raw: unknown): Promise<unknown> {
    if (!actor) throw new Error("Authenticated peer required");
    const command = commandSchema.parse(raw);
    switch (command.action) {
      case "describe":
        return {
          hostId: this.options.hostId,
          name: this.options.name,
          generation: this.generation,
          projects: this.catalog.listProjects(),
        };
      case "histories":
        return this.catalog.listHistories(command.projectId);
      case "create":
        return this.control.create({ ...command, hostId: this.options.hostId });
      case "select":
        return this.control.select({ ...command, hostId: this.options.hostId });
      case "connect": {
        this.runtime(command.binding);
        // Each actor has one active viewing lease; a reconnect invalidates its predecessor.
        this.disconnectActor(actor);
        const connection = this.approvals.connect(randomUUID(), command.binding, "human");
        this.clients.set(connection.connectionId, { actor, connection, touched: this.now() });
        return connection;
      }
      default: {
        const connection = this.client(actor, command.connection);
        const runtime = this.runtime(connection);
        switch (command.action) {
          case "disconnect":
            this.disconnectActor(actor);
            return { disconnected: true };
          case "poll":
            return {
              ...runtime.events.replay(command.cursor, 128),
              approvals: [...this.pending.values()].filter(
                (p) => p.connection.connectionId === connection.connectionId,
              ),
              state: runtime.child.state,
            };
          case "snapshot": {
            const messages = await runtime.child.request("get_messages");
            const state = await runtime.child.request("get_state");
            return { ...(messages as object), state, cursor: runtime.events.replay().cursor };
          }
          case "prompt": {
            if (runtime.prompts.has(command.operationId))
              return { accepted: true, duplicate: true };
            if (runtime.prompts.size >= 10000) throw new Error("Session operation limit reached");
            runtime.prompts.add(command.operationId);
            try {
              runtime.child.write({
                type: "prompt",
                id: command.operationId,
                message: command.text,
                streamingBehavior: "steer",
              });
            } catch (error) {
              runtime.prompts.delete(command.operationId);
              throw error;
            }
            return { submitted: true };
          }
          case "abort":
            await runtime.child.request("abort");
            return { aborted: true };
          case "stop":
            await runtime.child.stop();
            return { stopped: true };
          case "respond": {
            await this.approvals.consume(connection, command.response);
            const id = (command.response as { id: string }).id;
            this.pending.delete(`${runtime.binding.runtimeId}:${id}`);
            return { consumed: true };
          }
          case "preview":
            return this.preview(runtime.binding.projectId, command.path);
        }
      }
    }
  }

  disconnectActor(actor: string): void {
    for (const [id, client] of this.clients) {
      if (client.actor !== actor) continue;
      this.cancelRequests(id);
      this.approvals.disconnect(client.connection);
      this.clients.delete(id);
    }
  }

  async close(): Promise<void> {
    clearInterval(this.expiryTimer);
    this.control.retire();
    for (const actor of [...this.clients.values()].map((c) => c.actor)) this.disconnectActor(actor);
    await Promise.all([...this.runtimes.values()].map((r) => r.child.stop()));
    for (const record of await this.records.list(this.options.hostId)) {
      if (record.generation === this.generation)
        await this.records.put({ ...record, status: "ended" });
    }
    this.approvals.close();
  }

  private project(id: string): CatalogProject {
    const project = this.options.projects.find((p) => p.id === id);
    if (!project) throw new Error("Unknown project");
    return project;
  }

  private runtime(binding: HostSessionBinding): Runtime {
    const runtime = this.runtimes.get(binding.runtimeId);
    if (
      !runtime ||
      JSON.stringify(bindingSchema.parse(binding)) !==
        JSON.stringify(bindingSchema.parse(runtime.binding))
    )
      throw new Error("Stale or foreign runtime lease");
    return runtime;
  }

  private client(actor: string, connection: HostApprovalConnection): HostApprovalConnection {
    const client = this.clients.get(connection.connectionId);
    if (
      !client ||
      client.actor !== actor ||
      JSON.stringify(connectionSchema.parse(client.connection)) !==
        JSON.stringify(connectionSchema.parse(connection))
    )
      throw new Error("Connection expired; reconnect to the session");
    if (this.now() - client.touched >= 15000) {
      this.disconnectActor(actor);
      throw new Error("Connection expired; reconnect to the session");
    }
    client.touched = this.now();
    return client.connection;
  }

  private onEvent(runtime: Runtime, line: string): void {
    let event: Record<string, unknown>;
    try {
      event = JSON.parse(line);
    } catch {
      return;
    }
    if (event.type === "response" && event.command !== "prompt") return;
    if (
      event.type === "extension_ui_request" &&
      ["confirm", "select", "input", "editor"].includes(String(event.method))
    ) {
      const client = [...this.clients.values()].reverse().find(
        (c) => c.connection.runtimeId === runtime.binding.runtimeId,
      );
      if (!client) {
        runtime.child.write({ type: "extension_ui_response", id: event.id, cancelled: true });
        return;
      }
      try {
        const ttl = typeof event.timeout === "number" ? Math.min(120000, Math.max(1, Math.floor(event.timeout))) : 120000;
        const request = this.approvals.onRuntimeRequest(
          runtime.binding,
          client.connection,
          event,
          ttl,
        );
        this.pending.set(`${runtime.binding.runtimeId}:${request.request.id}`, request);
      } catch {
        runtime.child.write({ type: "extension_ui_response", id: event.id, cancelled: true });
      }
    }
    try {
      runtime.events.publish(event);
    } catch {
      runtime.events.publish({ type: "host_event_oversize", snapshotRequired: true });
    }
  }

  private cancelRequests(connectionId: string): void {
    for (const [key, request] of this.pending) {
      if (request.connection.connectionId !== connectionId) continue;
      const runtime = this.runtimes.get(request.connection.runtimeId);
      if (runtime?.child.state === "running")
        runtime.child.write({
          type: "extension_ui_response",
          id: request.request.id,
          cancelled: true,
        });
      this.pending.delete(key);
    }
  }

  private invalidateRuntime(runtime: Runtime): void {
    for (const client of [...this.clients.values()]) {
      if (client.connection.runtimeId === runtime.binding.runtimeId)
        this.disconnectActor(client.actor);
    }
    this.approvals.invalidateRuntime(runtime.binding);
  }

  private expire(): void {
    const now = this.now();
    for (const client of [...this.clients.values()]) {
      if (now - client.touched >= 15000) this.disconnectActor(client.actor);
    }
    for (const [key, request] of this.pending) {
      if (now < request.expiresAt) continue;
      const runtime = this.runtimes.get(request.connection.runtimeId);
      if (runtime?.child.state === "running")
        runtime.child.write({
          type: "extension_ui_response",
          id: request.request.id,
          cancelled: true,
        });
      this.pending.delete(key);
    }
  }

  private preview(projectId: string, path: string): unknown {
    const root = resolve(this.project(projectId).cwd);
    // Realpath closes symlink/junction escapes before reading any bytes.
    return readProjectPreview(root, path);
  }
}

function readProjectPreview(root: string, path: string): unknown {
  const file = realpathSync(resolve(root, path));
  const rel = relative(realpathSync(root), file);
  if (rel.startsWith("..") || isAbsolute(rel)) throw new Error("File is outside the project");
  const stat = statSync(file);
  if (!stat.isFile() || stat.size > 2 * 1024 * 1024)
    throw new Error("Preview supports files up to 2 MB");
  const handle = openSync(file, "r");
  let bytes: Buffer;
  try {
    const opened = fstatSync(handle);
    if (opened.ino !== stat.ino || opened.dev !== stat.dev || !opened.isFile())
      throw new Error("File changed while opening preview");
    const buffer = Buffer.alloc(2 * 1024 * 1024 + 1);
    let length = 0;
    while (length < buffer.length) {
      const count = readSync(handle, buffer, length, buffer.length - length, length);
      if (count === 0) break;
      length += count;
    }
    if (length === buffer.length) throw new Error("Preview supports files up to 2 MB");
    bytes = buffer.subarray(0, length);
  } finally {
    closeSync(handle);
  }
  const extension = file.split(".").at(-1)?.toLowerCase();
  const mime = (
    {
      png: "image/png",
      jpg: "image/jpeg",
      jpeg: "image/jpeg",
      webp: "image/webp",
      gif: "image/gif",
    } as Record<string, string>
  )[extension ?? ""];
  if (mime) return { path, mime, base64: bytes.toString("base64") };
  if (bytes.includes(0)) throw new Error("Binary file preview unavailable");
  return { path, mime: "text/plain", text: bytes.toString("utf8") };
}
