import { randomUUID } from "node:crypto";
import type { HostSessionBinding } from "./host-session-control.js";

/** Native RPC UI only. No agent messages or synthetic permission decisions. */
export type HostUiRequest = {
  type: "extension_ui_request";
  id: string;
  title: string;
} & (
  | { method: "confirm"; message: string }
  | { method: "select"; options: string[] }
  | { method: "input"; placeholder?: string }
  | { method: "editor"; prefill?: string }
);

export type HostUiResponse = { type: "extension_ui_response"; id: string } & (
  | { confirmed: boolean }
  | { value: string }
  | { cancelled: true }
);

export interface HostApprovalConnection extends HostSessionBinding {
  connectionId: string;
  approvalEpoch: string;
}

export interface HostApprovalRequest {
  connection: HostApprovalConnection;
  expiresAt: number;
  request: HostUiRequest;
}

function bindingKey(binding: HostSessionBinding): string {
  return JSON.stringify([
    binding.hostId, binding.projectId, binding.historyId, binding.runtimeId, binding.generation,
  ]);
}

function parseRequest(raw: unknown): HostUiRequest {
  if (!raw || typeof raw !== "object") throw new Error("Invalid RPC UI request");
  const value = raw as Record<string, unknown>;
  if (value.type !== "extension_ui_request" || typeof value.id !== "string" || !value.id ||
      typeof value.title !== "string") throw new Error("Invalid RPC UI request");
  const base = { type: "extension_ui_request" as const, id: value.id, title: value.title };
  switch (value.method) {
    case "confirm":
      if (typeof value.message === "string") return { ...base, method: "confirm", message: value.message };
      break;
    case "select":
      if (Array.isArray(value.options) && value.options.every((option) => typeof option === "string")) {
        return { ...base, method: "select", options: [...value.options] as string[] };
      }
      break;
    case "input":
      if (value.placeholder === undefined || typeof value.placeholder === "string") {
        return { ...base, method: "input", ...(value.placeholder === undefined ? {} : { placeholder: value.placeholder }) };
      }
      break;
    case "editor":
      if (value.prefill === undefined || typeof value.prefill === "string") {
        return { ...base, method: "editor", ...(value.prefill === undefined ? {} : { prefill: value.prefill }) };
      }
      break;
  }
  throw new Error("Unsupported or malformed RPC UI request");
}

function parseResponse(raw: unknown, request: HostUiRequest): HostUiResponse {
  if (!raw || typeof raw !== "object") throw new Error("Invalid RPC UI response");
  const value = raw as Record<string, unknown>;
  if (value.type !== "extension_ui_response" || value.id !== request.id) {
    throw new Error("Mismatched RPC UI response");
  }
  // Reject mixed decisions rather than accidentally preferring approval.
  const fields = ["cancelled", "confirmed", "value"].filter((key) => key in value);
  if (fields.length !== 1) throw new Error("Ambiguous RPC UI response");
  const base = { type: "extension_ui_response" as const, id: request.id };
  if (value.cancelled === true) return { ...base, cancelled: true };
  if (request.method === "confirm" && typeof value.confirmed === "boolean") {
    return { ...base, confirmed: value.confirmed };
  }
  if (request.method !== "confirm" && typeof value.value === "string" &&
      (request.method !== "select" || request.options.includes(value.value))) {
    return { ...base, value: value.value };
  }
  throw new Error("Response does not match RPC UI method");
}

/**
 * In-memory, fail-closed approval boundary for one supervisor incarnation.
 * The adapter supplies authenticated actor roles and live runtime requests, not
 * client assertions. Recreate on restart. No permission is persisted or inferred.
 */
export function createHostApprovals(options: {
  hostId: string;
  generation: string;
  maxConnections: number;
  maxRequests: number;
  maxTtlMs: number;
  now?: () => number;
  /** Exactly one attempt. A write error is surfaced; consumed decisions stay consumed. */
  respond(binding: HostSessionBinding, response: HostUiResponse): Promise<void>;
}) {
  const { hostId, generation, maxConnections, maxRequests, maxTtlMs, respond } = options;
  const now = options.now ?? (() => performance.now());
  if (!hostId || !generation || [maxConnections, maxRequests, maxTtlMs].some((n) => !Number.isSafeInteger(n) || n < 1)) {
    throw new Error("Invalid approval scope or bounds");
  }
  const connections = new Map<string, { connection: HostApprovalConnection; actor: "human" | "agent" }>();
  const pending = new Map<string, HostApprovalRequest>();
  // Never evict IDs within an incarnation: eviction could turn a replay into a
  // new approval. The finite request budget fails closed instead of growing.
  const seen = new Set<string>();
  let closed = false;

  function checkScope(binding: HostSessionBinding): void {
    if (closed) throw new Error("Approval boundary closed");
    if (binding.hostId !== hostId || binding.generation !== generation ||
        [binding.projectId, binding.historyId, binding.runtimeId].some((v) => typeof v !== "string" || !v)) {
      throw new Error("Stale or foreign approval scope");
    }
  }

  function current(connection: HostApprovalConnection): void {
    checkScope(connection);
    const live = connections.get(connection.connectionId);
    if (!live || live.connection.approvalEpoch !== connection.approvalEpoch ||
        bindingKey(live.connection) !== bindingKey(connection)) {
      throw new Error("Approval connection is offline or stale");
    }
    if (live.actor !== "human") throw new Error("Human response required");
  }

  function invalidate(connectionId: string): void {
    connections.delete(connectionId);
    for (const [key, entry] of pending) {
      if (entry.connection.connectionId === connectionId) pending.delete(key);
    }
  }

  return {
    /** Called by the authenticated transport, never from client-supplied roles. */
    connect(connectionId: string, binding: HostSessionBinding, actor: "human" | "agent"): HostApprovalConnection {
      checkScope(binding);
      if (!connectionId || (actor !== "human" && actor !== "agent")) throw new Error("Invalid approval connection");
      if (!connections.has(connectionId) && connections.size >= maxConnections) throw new Error("Approval connection limit");
      invalidate(connectionId);
      const connection: HostApprovalConnection = {
        hostId, generation, projectId: binding.projectId, historyId: binding.historyId,
        runtimeId: binding.runtimeId, connectionId, approvalEpoch: randomUUID(),
      };
      connections.set(connectionId, { connection, actor });
      return { ...connection };
    },

    disconnect(connection: HostApprovalConnection): void {
      const live = connections.get(connection.connectionId);
      if (live && live.connection.approvalEpoch === connection.approvalEpoch &&
          bindingKey(live.connection) === bindingKey(connection)) invalidate(connection.connectionId);
    },

    /** Called only for an actual RPC extension_ui_request from this runtime. */
    onRuntimeRequest(
      source: HostSessionBinding,
      connection: HostApprovalConnection,
      raw: unknown,
      ttlMs: number,
    ): HostApprovalRequest {
      current(connection);
      checkScope(source);
      if (bindingKey(source) !== bindingKey(connection)) throw new Error("Foreign RPC UI request source");
      if (!Number.isSafeInteger(ttlMs) || ttlMs < 1 || ttlMs > maxTtlMs) throw new Error("Invalid approval TTL");
      const request = parseRequest(raw);
      const key = JSON.stringify([bindingKey(connection), request.id]);
      if (seen.has(key)) throw new Error("RPC UI request already observed");
      if (seen.size >= maxRequests) throw new Error("Approval request budget exhausted");
      const expiresAt = now() + ttlMs;
      if (!Number.isFinite(expiresAt)) throw new Error("Invalid approval clock");
      const entry = { connection: { ...connection }, expiresAt, request };
      seen.add(key);
      pending.set(key, entry);
      return structuredClone(entry);
    },

    /** Supervisor calls this when a runtime ends or its generation changes. */
    invalidateRuntime(binding: HostSessionBinding): void {
      checkScope(binding);
      for (const [id, live] of connections) {
        if (bindingKey(live.connection) === bindingKey(binding)) invalidate(id);
      }
    },

    /** Retire the old boundary before replacing it on supervisor restart. */
    close(): void {
      closed = true;
      connections.clear();
      pending.clear();
      seen.clear();
    },

    async consume(connection: HostApprovalConnection, raw: unknown): Promise<void> {
      current(connection);
      if (!raw || typeof raw !== "object" || typeof (raw as { id?: unknown }).id !== "string") {
        throw new Error("Invalid RPC UI response");
      }
      const key = JSON.stringify([bindingKey(connection), (raw as { id: string }).id]);
      const entry = pending.get(key);
      if (!entry || entry.connection.approvalEpoch !== connection.approvalEpoch) {
        throw new Error("No pending RPC UI request");
      }
      const time = now();
      if (!Number.isFinite(time) || time >= entry.expiresAt) {
        pending.delete(key);
        throw new Error("RPC UI request expired");
      }
      const response = parseResponse(raw, entry.request);
      pending.delete(key); // consume before the first await; racing responses lose
      const { hostId, projectId, historyId, runtimeId, generation } = entry.connection;
      await respond({ hostId, projectId, historyId, runtimeId, generation }, response);
    },
  };
}
