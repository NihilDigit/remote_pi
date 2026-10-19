import type { createSessionCatalog } from "./session-catalog.js";

export interface HostProjectScope {
  hostId: string;
  projectId: string;
  /** Opaque host runtime incarnation, never a PID. Rotate on runtime restart. */
  generation: string;
}

export interface HostRuntimeLease extends HostProjectScope {
  runtimeId: string;
  processId?: number;
}

export interface HostSessionBinding extends HostRuntimeLease {
  historyId: string;
}

export type HostOpenIntent =
  | { kind: "history"; historyId: string }
  | { kind: "new"; operationId: string };

/** Metadata only: no transcript, paths, credentials or claimed process liveness. */
export interface HostSessionRecord extends HostProjectScope {
  recordId: string;
  intent: HostOpenIntent;
  runtimeId?: string;
  historyId?: string;
  processId?: number;
  status: "opening" | "running" | "failed" | "ended" | "unknown";
}

export interface HostSessionRecords {
  list(hostId: string): Promise<readonly HostSessionRecord[]>;
  /** Must durably complete before resolving. One control instance owns this host. */
  put(record: HostSessionRecord): Promise<void>;
}

export type HostRuntimeState =
  | { status: "running"; historyId: string }
  | { status: "failed" | "ended" | "unknown" };

/** Implemented by the existing supervisor/RpcChild, not another process manager. */
export interface HostSessionRuntime {
  /** New means a separate session; never continueRecent/--continue. History is exact. */
  open(request: HostProjectScope & { intent: HostOpenIntent }): Promise<HostRuntimeLease>;
  /** Resolve only after the matching successful RPC get_state, not spawn(). */
  getState(lease: HostRuntimeLease): Promise<HostRuntimeState>;
}

export type HostWriterObservation =
  | { kind: "live"; lease: HostRuntimeLease }
  | { kind: "dead" }
  | { kind: "unknown" };

export interface HostSessionControlOptions {
  hostId: string;
  generation: string;
  catalog: ReturnType<typeof createSessionCatalog>;
  records: HostSessionRecords;
  runtime: HostSessionRuntime;
  /**
   * Must inspect supervisor-owned live handles, including unfinished opens.
   * `dead` proves no writer for this intent, not just that a saved PID is absent.
   * After supervisor restart, inability to prove an old writer dead is unknown.
   */
  liveView(record: Readonly<HostSessionRecord>): Promise<HostWriterObservation>;
}

export interface HostSessionResult extends HostProjectScope {
  status: "running" | "failed" | "ended" | "unknown" | "recovery-required";
  action: "opened" | "attached" | "none";
  historyId?: string;
  /** Only a verified running session grants a usable lease. */
  lease?: HostSessionBinding;
}

function required(value: string): void {
  if (typeof value !== "string" || value.length === 0) throw new Error("Missing session identity");
}

/**
 * A narrow single-writer control gate. Keep exactly one instance per supervisor
 * host and await every metadata write. Replace it with a new generation when
 * the supervisor or its controlled runtimes restart. A second supervisor must
 * be excluded by the existing lock; this gate never steals process ownership.
 */
export function createHostSessionControl(options: HostSessionControlOptions) {
  const { hostId, generation, catalog, records, runtime, liveView } = options;
  required(hostId);
  required(generation);
  let tail: Promise<void> = Promise.resolve();
  let retired = false;

  function assertActive(): void {
    if (retired) throw new Error("Host runtime generation retired");
  }

  function serial<T>(work: () => Promise<T>): Promise<T> {
    const result = tail.then(work);
    // Recover the queue only; the original rejection is returned to the caller.
    tail = result.then(() => undefined, () => undefined);
    return result;
  }

  function validate(scope: HostProjectScope): void {
    assertActive();
    if (scope.hostId !== hostId || scope.generation !== generation) {
      throw new Error("Stale or foreign host generation");
    }
    required(scope.projectId);
    if (!catalog.listProjects().some(({ id }) => id === scope.projectId)) {
      throw new Error("Unknown project ID");
    }
  }

  function resultFor(
    scope: HostProjectScope,
    record: HostSessionRecord,
    status: HostSessionResult["status"],
    action: HostSessionResult["action"] = "none",
  ): HostSessionResult {
    return { ...scope, status, action, ...(record.historyId ? { historyId: record.historyId } : {}) };
  }

  async function verify(
    scope: HostProjectScope,
    record: HostSessionRecord,
    lease: HostRuntimeLease,
    action: "opened" | "attached",
  ): Promise<HostSessionResult> {
    assertActive();
    if (lease.hostId !== hostId || lease.projectId !== scope.projectId ||
        lease.generation !== generation || !lease.runtimeId) {
      return resultFor(scope, record, "recovery-required");
    }
    // Persist an uncertain lease before RPC: a timeout/exception must not make
    // the next click start a second writer, even if open already succeeded.
    const uncertain: HostSessionRecord = {
      ...record, runtimeId: lease.runtimeId,
      ...(lease.processId === undefined ? {} : { processId: lease.processId }), status: "unknown",
    };
    await records.put(uncertain);
    assertActive();
    const state = await runtime.getState({ ...lease });
    assertActive();
    if (state.status !== "running") {
      await records.put({ ...uncertain, status: state.status });
      return resultFor(scope, record, state.status);
    }
    if (!state.historyId || (record.historyId !== undefined && state.historyId !== record.historyId)) {
      return resultFor(scope, record, "recovery-required");
    }
    if (record.intent.kind === "new" && record.historyId === undefined) {
      const reused = (await records.list(hostId)).some((other) =>
        other.hostId === hostId && other.projectId === scope.projectId &&
        other.recordId !== record.recordId && other.historyId === state.historyId,
      );
      // Catch adapters accidentally using continueRecent for a second click.
      if (reused) return resultFor(scope, record, "recovery-required");
    }
    const ready: HostSessionRecord = { ...uncertain, historyId: state.historyId, status: "running" };
    await records.put(ready);
    assertActive();
    return {
      ...resultFor(scope, ready, "running", action),
      lease: { ...lease, historyId: state.historyId },
    };
  }

  async function existing(
    scope: HostProjectScope,
    record: HostSessionRecord,
    reopenHistory = false,
  ): Promise<HostSessionResult | undefined> {
    const observation = await liveView(structuredClone(record));
    assertActive();
    if (observation.kind === "unknown") return resultFor(scope, record, "recovery-required");
    if (observation.kind === "live") {
      // A prior incarnation's handle is not a lease in this incarnation.
      if (record.generation !== generation) return resultFor(scope, record, "recovery-required");
      return verify(scope, record, observation.lease, "attached");
    }
    // A new-session operation is never repeated, even if its writer is dead or
    // it crashed before the history ID could be recorded. Recovery is explicit.
    if (!reopenHistory && record.intent.kind === "new") {
      const status = record.historyId ? "ended" : "recovery-required";
      return resultFor(scope, record, status);
    }
    return undefined; // exact history may be reopened only after death proof
  }

  async function open(
    scope: HostProjectScope,
    record: HostSessionRecord,
    intent: HostOpenIntent = record.intent,
  ): Promise<HostSessionResult> {
    assertActive();
    const reservation: HostSessionRecord = { ...record, status: "opening" };
    await records.put(reservation);
    assertActive();
    const lease = await runtime.open({ ...scope, intent: { ...intent } });
    return verify(scope, reservation, lease, "opened");
  }

  return {
    /** Invalidate queued/in-flight leases before replacing this generation. Does not stop children. */
    retire(): void {
      retired = true;
    },

    select(input: HostProjectScope & { historyId: string }): Promise<HostSessionResult> {
      const request = { ...input }; // snapshot before queuing or awaiting discovery
      return serial(async () => {
        validate(request);
        required(request.historyId);
        const scope: HostProjectScope = { hostId, projectId: request.projectId, generation };
        // Always consult the catalog, including attach: no prefix or path lookup.
        const selection = await catalog.select(scope.projectId, request.historyId);
        const matches = (await records.list(hostId)).filter((record) =>
          record.hostId === hostId && record.projectId === scope.projectId &&
          (record.historyId === request.historyId ||
            (record.intent.kind === "history" && record.intent.historyId === request.historyId)),
        );
        if (matches.length > 1) {
          return resultFor(scope, { ...matches[0]! }, "recovery-required");
        }
        const record: HostSessionRecord = matches[0] ?? {
          ...scope,
          recordId: JSON.stringify([scope.projectId, "history", request.historyId]),
          intent: { kind: "history", historyId: request.historyId },
          historyId: request.historyId,
          status: "unknown",
          ...(selection.kind === "attach" ? { runtimeId: selection.identity.id } : {}),
        };
        const attached = await existing(scope, record, true);
        if (attached) return attached;
        // A catalog attach contradicted by a dead live view cannot be opened
        // through a different entry point: require reconciliation instead.
        if (selection.kind === "attach") return resultFor(scope, record, "recovery-required");
        return open(scope, { ...record, generation, runtimeId: undefined }, {
          kind: "history", historyId: request.historyId,
        });
      });
    },

    create(input: HostProjectScope & { operationId: string }): Promise<HostSessionResult> {
      const request = { ...input };
      return serial(async () => {
        validate(request);
        required(request.operationId);
        const scope: HostProjectScope = { hostId, projectId: request.projectId, generation };
        const recordId = JSON.stringify([scope.projectId, "new", request.operationId]);
        const matches = (await records.list(hostId)).filter((record) =>
          record.hostId === hostId && record.projectId === scope.projectId && record.recordId === recordId,
        );
        if (matches.length > 1) return resultFor(scope, matches[0]!, "recovery-required");
        if (matches[0]) {
          const attached = await existing(scope, matches[0]);
          if (!attached) throw new Error("Invalid new-session metadata");
          return attached;
        }
        const record: HostSessionRecord = {
          ...scope, recordId, intent: { kind: "new", operationId: request.operationId }, status: "unknown",
        };
        const observation = await liveView(structuredClone(record));
        if (observation.kind !== "dead") return resultFor(scope, record, "recovery-required");
        return open(scope, record);
      });
    },
  };
}
