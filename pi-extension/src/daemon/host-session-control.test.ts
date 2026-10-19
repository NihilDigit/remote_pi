import assert from "node:assert/strict";
import { resolve } from "node:path";
import { test } from "node:test";
import { createSessionCatalog } from "./session-catalog.js";
import {
  createHostSessionControl,
  type HostOpenIntent,
  type HostProjectScope,
  type HostRuntimeLease,
  type HostRuntimeState,
  type HostSessionRecord,
  type HostWriterObservation,
} from "./host-session-control.js";

const scope = { hostId: "synthetic-host", projectId: "alpha", generation: "boot-1" };

function fixture() {
  const cwd = resolve("synthetic-alpha");
  const metadata = new Map<string, HostSessionRecord>();
  const writers = new Map<string, { lease: HostRuntimeLease; state: HostRuntimeState; intent: HostOpenIntent }>();
  const opens: Array<HostProjectScope & { intent: HostOpenIntent }> = [];
  let state: HostRuntimeState | undefined;
  let openFailure: Error | undefined;
  let stateFailure: Error | undefined;
  let writeFailure = false;
  let observation: HostWriterObservation | undefined;
  let catalogRunning: { id: string } | undefined;
  const histories = ["history-exact", "second-history"];
  const catalog = createSessionCatalog({
    projects: [{ id: "alpha", name: "Alpha", cwd }],
    sessions: {
      list: async () => histories.map((id) => ({
        id, cwd, created: new Date(0), modified: new Date(1),
      })),
    },
    running: () => catalogRunning,
  });
  const options = {
    ...scope,
    catalog,
    records: {
      list: async () => structuredClone([...metadata.values()]),
      put: async (record: HostSessionRecord) => {
        if (writeFailure) throw new Error("synthetic persistence failure");
        metadata.set(record.recordId, structuredClone(record));
      },
    },
    runtime: {
      open: async (request: HostProjectScope & { intent: HostOpenIntent }) => {
        opens.push(structuredClone(request));
        const lease = { ...scope, generation: request.generation, runtimeId: `runtime-${opens.length}` };
        const historyId = request.intent.kind === "history" ? request.intent.historyId : `new-${opens.length}`;
        if (!histories.includes(historyId)) histories.push(historyId);
        writers.set(lease.runtimeId, { lease, state: state ?? { status: "running", historyId }, intent: request.intent });
        if (openFailure) throw openFailure; // simulate spawn side effect before rejection
        return lease;
      },
      getState: async (lease: HostRuntimeLease) => {
        if (stateFailure) throw stateFailure;
        return writers.get(lease.runtimeId)?.state ?? { status: "unknown" as const };
      },
    },
    liveView: async (record: Readonly<HostSessionRecord>): Promise<HostWriterObservation> => {
      if (observation) return observation;
      const writer = record.runtimeId ? writers.get(record.runtimeId) : [...writers.values()].find((writer) =>
        record.historyId !== undefined
          ? writer.state.status === "running" && writer.state.historyId === record.historyId
          : writer.intent.kind === "new" && record.intent.kind === "new" && writer.intent.operationId === record.intent.operationId,
      );
      return writer ? { kind: "live", lease: writer.lease } : { kind: "dead" };
    },
  };
  return {
    options, control: createHostSessionControl(options), metadata, writers, opens,
    setState: (value: HostRuntimeState) => { state = value; },
    failOpen: (value: Error) => { openFailure = value; },
    failState: (value: Error | undefined) => { stateFailure = value; },
    failWrite: () => { writeFailure = true; },
    observe: (value: HostWriterObservation) => { observation = value; },
    catalogRunning: (value: { id: string }) => { catalogRunning = value; },
  };
}

const select = { ...scope, historyId: "history-exact" };

test("concurrent exact-history selects open once, then verified attach", async () => {
  const f = fixture();
  const results = await Promise.all(Array.from({ length: 20 }, () => f.control.select(select)));
  assert.equal(f.opens.length, 1);
  assert.equal(results[0]!.action, "opened");
  assert.ok(results.slice(1).every((result) => result.action === "attached"));
  assert.ok(results.every((result) => result.status === "running" && result.lease?.runtimeId === "runtime-1"));
  assert.deepEqual(f.opens[0]!.intent, { kind: "history", historyId: select.historyId });
});

test("new operation IDs are durable idempotency keys; separate clicks create separate sessions", async () => {
  const f = fixture();
  const request = { ...scope, operationId: "click-1" };
  const results = await Promise.all(Array.from({ length: 12 }, () => f.control.create(request)));
  assert.equal(f.opens.length, 1);
  assert.ok(results.every((result) => result.historyId === "new-1"));
  assert.deepEqual(f.opens[0]!.intent, { kind: "new", operationId: "click-1" });
  const second = await f.control.create({ ...scope, operationId: "click-2" });
  assert.equal(second.historyId, "new-2");
  const recreated = createHostSessionControl(f.options);
  assert.equal((await recreated.create(request)).action, "attached");
  assert.equal(f.opens.length, 2);
  assert.equal((await recreated.select({ ...scope, historyId: "new-1" })).action, "attached");
  assert.equal(f.opens.length, 2);
});

test("allowlist, exact history, host and generation are checked before opening", async () => {
  const f = fixture();
  for (const historyId of ["history", "../history-exact.jsonl", "", "HISTORY-EXACT"]) {
    await assert.rejects(f.control.select({ ...scope, historyId }));
  }
  for (const changed of [{ hostId: "other-host" }, { projectId: "other-project" }, { generation: "old" }]) {
    await assert.rejects(f.control.select({ ...select, ...changed }));
    await assert.rejects(f.control.create({ ...scope, operationId: "click", ...changed }));
  }
  await assert.rejects(f.control.create({ ...scope, operationId: "" }));
  assert.equal(f.opens.length, 0);
});

test("spawn alone is not ready: failed, ended and unknown RPC state never grant a running lease", async () => {
  for (const status of ["failed", "ended", "unknown"] as const) {
    const f = fixture();
    f.setState({ status });
    const result = await f.control.select(select);
    assert.equal(result.status, status);
    assert.equal(result.lease, undefined);
    const duplicate = await f.control.select(select);
    assert.equal(duplicate.status, status);
    assert.equal(f.opens.length, 1);
  }
  const mismatch = fixture();
  mismatch.setState({ status: "running", historyId: "not-the-selected-history" });
  assert.equal((await mismatch.control.select(select)).status, "recovery-required");
  assert.equal((await mismatch.control.select(select)).lease, undefined);
  assert.equal(mismatch.opens.length, 1);
});

test("readiness is awaited and a get_state timeout leaves a reservation, not a retry through another entry point", async () => {
  const f = fixture();
  f.failState(new Error("synthetic get_state timeout"));
  await assert.rejects(f.control.select(select), /get_state timeout/);
  assert.equal([...f.metadata.values()][0]!.status, "unknown");
  await assert.rejects(f.control.select(select), /get_state timeout/);
  assert.equal(f.opens.length, 1);
  f.failState(undefined);
  assert.equal((await f.control.select(select)).action, "attached");
});

test("pending get_state holds readiness and serializes a duplicate until confirmation", async () => {
  const f = fixture();
  const getState = f.options.runtime.getState;
  let release!: () => void;
  const barrier = new Promise<void>((resolve) => { release = resolve; });
  f.options.runtime.getState = async (lease) => {
    await barrier;
    return getState(lease);
  };
  let settled = false;
  const first = f.control.select(select).then((result) => { settled = true; return result; });
  const duplicate = f.control.select(select);
  await new Promise<void>((resolve) => { setImmediate(resolve); });
  assert.equal(f.opens.length, 1);
  assert.equal(settled, false);
  assert.equal([...f.metadata.values()][0]!.status, "unknown");
  release();
  assert.equal((await first).status, "running");
  assert.equal((await duplicate).action, "attached");
  assert.equal(f.opens.length, 1);
});

test("retiring a generation invalidates pending readiness and queued requests without spawning again", async () => {
  const f = fixture();
  const getState = f.options.runtime.getState;
  let release!: () => void;
  const barrier = new Promise<void>((resolve) => { release = resolve; });
  f.options.runtime.getState = async (lease) => {
    await barrier;
    return getState(lease);
  };
  const first = f.control.select(select);
  const queued = f.control.create({ ...scope, operationId: "queued-click" });
  await new Promise<void>((resolve) => { setImmediate(resolve); });
  f.control.retire();
  release();
  await Promise.all([
    assert.rejects(first, /generation retired/),
    assert.rejects(queued, /generation retired/),
  ]);
  await assert.rejects(f.control.select(select), /generation retired/);
  assert.equal(f.opens.length, 1);
});

test("queued inputs are snapshots, not mutable selectors or idempotency keys", async () => {
  const f = fixture();
  const request = { ...select };
  const selecting = f.control.select(request);
  request.historyId = "not-catalogued";
  request.hostId = "foreign";
  assert.equal((await selecting).historyId, select.historyId);
  const createRequest = { ...scope, operationId: "click" };
  const creating = f.control.create(createRequest);
  createRequest.operationId = "changed";
  await creating;
  assert.deepEqual(f.opens[1]!.intent, { kind: "new", operationId: "click" });
});

test("a runtime adapter reusing another new operation's history is rejected", async () => {
  const f = fixture();
  const first = await f.control.create({ ...scope, operationId: "first" });
  f.setState({ status: "running", historyId: first.historyId! });
  const second = await f.control.create({ ...scope, operationId: "second" });
  assert.equal(second.status, "recovery-required");
  assert.equal(second.lease, undefined);
  assert.equal(f.opens.length, 2);
  assert.deepEqual(f.opens.map((entry) => entry.intent.kind), ["new", "new"]);
});

test("open exception is surfaced, with the original reservation retained across duplicate new clicks", async () => {
  const f = fixture();
  f.failOpen(new Error("synthetic open failure after side effect"));
  const request = { ...scope, operationId: "uncertain-click" };
  await assert.rejects(f.control.create(request), /open failure/);
  assert.equal([...f.metadata.values()][0]!.status, "opening");
  f.observe({ kind: "unknown" });
  assert.equal((await f.control.create(request)).status, "recovery-required");
  assert.equal(f.opens.length, 1);
});

test("persistence must succeed before any runtime open; no exception is hidden", async () => {
  const f = fixture();
  f.failWrite();
  await assert.rejects(f.control.select(select), /persistence failure/);
  assert.equal(f.opens.length, 0);
});

test("restart invalidates old leases: unknown or still-live old writers require recovery", async () => {
  for (const observation of ["unknown", "live"] as const) {
    const f = fixture();
    const prior = await f.control.select(select);
    const generation = "boot-2";
    f.observe(observation === "unknown" ? { kind: "unknown" } : { kind: "live", lease: prior.lease! });
    const restarted = createHostSessionControl({ ...f.options, generation });
    await assert.rejects(restarted.select(select), /generation/);
    const result = await restarted.select({ ...select, generation });
    assert.equal(result.status, "recovery-required");
    assert.equal(result.lease, undefined);
    assert.equal(f.opens.length, 1);
  }
});

test("proven-dead writer permits exact reopen after restart, but never repeats a new operation", async () => {
  const f = fixture();
  const request = { ...scope, operationId: "click" };
  const first = await f.control.create(request);
  f.writers.clear();
  const generation = "boot-2";
  const restarted = createHostSessionControl({ ...f.options, generation });
  const duplicate = await restarted.create({ ...request, generation });
  assert.equal(duplicate.status, "ended");
  assert.equal(duplicate.historyId, first.historyId);
  assert.equal(f.opens.length, 1);
  const reopened = await restarted.select({ ...scope, generation, historyId: first.historyId! });
  assert.equal(reopened.status, "running");
  assert.equal(reopened.lease?.generation, generation);
  assert.deepEqual(f.opens[1]!.intent, { kind: "history", historyId: first.historyId });
  assert.equal((await restarted.create({ ...request, generation })).historyId, first.historyId);
  assert.equal(f.opens.length, 2);
});

test("unknown live view, stale metadata and a catalog running hint cannot prove writer death", async () => {
  const f = fixture();
  f.observe({ kind: "unknown" });
  assert.equal((await f.control.select(select)).status, "recovery-required");
  assert.equal((await f.control.create({ ...scope, operationId: "click" })).status, "recovery-required");
  assert.equal(f.opens.length, 0);
  const contradiction = fixture();
  contradiction.catalogRunning({ id: "saved-runtime-id-not-proof" });
  assert.equal((await contradiction.control.select(select)).status, "recovery-required");
  assert.equal(contradiction.opens.length, 0);
});

test("runtime lease host/project/generation mismatches never grant a usable lease", async () => {
  for (const changed of [{ hostId: "other" }, { projectId: "other" }, { generation: "other" }]) {
    const f = fixture();
    f.observe({ kind: "live", lease: { ...scope, runtimeId: "runtime", ...changed } });
    const result = await f.control.select(select);
    assert.equal(result.status, "recovery-required");
    assert.equal(result.lease, undefined);
    assert.equal(f.opens.length, 0);
  }
});

test("duplicate new operation with no recorded history cannot create again after writer death", async () => {
  const f = fixture();
  f.failOpen(new Error("unknown creation result"));
  const request = { ...scope, operationId: "click" };
  await assert.rejects(f.control.create(request), /unknown creation/);
  f.writers.clear();
  assert.equal((await f.control.create(request)).status, "recovery-required");
  assert.equal(f.opens.length, 1);
});
