import assert from "node:assert/strict";
import { test } from "node:test";
import { createHostApprovals, type HostUiResponse } from "./host-approvals.js";
import type { HostSessionBinding } from "./host-session-control.js";

const binding = { hostId: "host", projectId: "project", historyId: "history", runtimeId: "runtime", generation: "boot-1" };
const request = { type: "extension_ui_request", id: "rpc-1", method: "confirm", title: "Confirm", message: "Synthetic action?" };
const approve = { type: "extension_ui_response", id: request.id, confirmed: true };

function fixture() {
  let clock = 100;
  let failure: Error | undefined;
  const responses: Array<{ binding: HostSessionBinding; response: HostUiResponse }> = [];
  const options = {
    hostId: binding.hostId, generation: binding.generation,
    maxConnections: 3, maxRequests: 10, maxTtlMs: 1000,
    now: () => clock,
    respond: async (scope: HostSessionBinding, response: HostUiResponse) => {
      responses.push({ binding: scope, response });
      if (failure) throw failure;
    },
  };
  const approvals = createHostApprovals(options);
  const connection = approvals.connect("human-transport", binding, "human");
  return {
    approvals, connection, responses, options,
    time: (value: number) => { clock = value; },
    fail: () => { failure = new Error("synthetic RPC write failure"); },
  };
}

test("only a genuine registered RPC request permits one response, even under concurrent consumes", async () => {
  const f = fixture();
  await assert.rejects(f.approvals.consume(f.connection, approve), /No pending/);
  assert.equal(f.responses.length, 0);
  const pending = f.approvals.onRuntimeRequest(binding, f.connection, request, 100);
  assert.equal(pending.expiresAt, 200);
  assert.deepEqual(pending.request, request);
  const results = await Promise.allSettled(Array.from({ length: 10 }, () => f.approvals.consume(f.connection, approve)));
  assert.equal(results.filter((result) => result.status === "fulfilled").length, 1);
  assert.deepEqual(f.responses, [{ binding, response: approve }]);
  assert.throws(() => f.approvals.onRuntimeRequest(binding, f.connection, request, 100), /already observed/);
});

test("cross-host/project/history/runtime/generation and request-ID responses are rejected", async () => {
  const f = fixture();
  f.approvals.onRuntimeRequest(binding, f.connection, request, 100);
  for (const key of Object.keys(binding)) {
    await assert.rejects(f.approvals.consume({ ...f.connection, [key]: "foreign" }, approve));
    assert.throws(() => f.approvals.onRuntimeRequest({ ...binding, [key]: "foreign" }, f.connection, { ...request, id: key }, 100));
  }
  await assert.rejects(f.approvals.consume(f.connection, { ...approve, id: "wrong-id" }), /No pending/);
  assert.equal(f.responses.length, 0);
  await f.approvals.consume(f.connection, approve);
  assert.equal(f.responses.length, 1);
});

test("disconnect and reconnect invalidate old approvals and cannot rebind old RPC requests", async () => {
  const f = fixture();
  f.approvals.onRuntimeRequest(binding, f.connection, request, 100);
  f.approvals.disconnect(f.connection);
  await assert.rejects(f.approvals.consume(f.connection, approve), /offline/);
  const fresh = f.approvals.connect(f.connection.connectionId, binding, "human");
  assert.notEqual(fresh.approvalEpoch, f.connection.approvalEpoch);
  await assert.rejects(f.approvals.consume(f.connection, approve), /stale/);
  await assert.rejects(f.approvals.consume(fresh, approve), /No pending/);
  assert.throws(() => f.approvals.onRuntimeRequest(binding, fresh, request, 100), /already observed/);
  f.approvals.disconnect(f.connection); // delayed old disconnect must not close fresh connection
  const next = { ...request, id: "rpc-2" };
  f.approvals.onRuntimeRequest(binding, fresh, next, 100);
  await f.approvals.consume(fresh, { ...approve, id: next.id });
  assert.equal(f.responses.length, 1);
});

test("a connection replacement without explicit disconnect also expires its pending approvals", async () => {
  const f = fixture();
  f.approvals.onRuntimeRequest(binding, f.connection, request, 100);
  const fresh = f.approvals.connect(f.connection.connectionId, binding, "human");
  await assert.rejects(f.approvals.consume(fresh, approve), /No pending/);
  await assert.rejects(f.approvals.consume(f.connection, approve), /stale/);
  assert.equal(f.responses.length, 0);
});

test("TTL expires at its exact boundary; offline delayed responses never approve on reconnect", async () => {
  const f = fixture();
  f.approvals.onRuntimeRequest(binding, f.connection, request, 100);
  f.time(200);
  await assert.rejects(f.approvals.consume(f.connection, approve), /expired/);
  await assert.rejects(f.approvals.consume(f.connection, approve), /No pending/);
  const next = { ...request, id: "offline-request" };
  f.approvals.onRuntimeRequest(binding, f.connection, next, 100);
  f.approvals.disconnect(f.connection);
  f.time(1000);
  const fresh = f.approvals.connect(f.connection.connectionId, binding, "human");
  await assert.rejects(f.approvals.consume(fresh, { ...approve, id: next.id }), /No pending/);
  assert.equal(f.responses.length, 0);
});

test("restart and runtime invalidation revoke approval epochs; no default approval is sent", async () => {
  const f = fixture();
  f.approvals.onRuntimeRequest(binding, f.connection, request, 100);
  f.approvals.invalidateRuntime(binding);
  await assert.rejects(f.approvals.consume(f.connection, approve), /offline/);
  const live = f.approvals.connect("replacement", binding, "human");
  f.approvals.onRuntimeRequest(binding, live, { ...request, id: "rpc-2" }, 100);
  f.approvals.close();
  await assert.rejects(f.approvals.consume(live, { ...approve, id: "rpc-2" }), /closed/);
  const generation = "boot-2";
  const restarted = createHostApprovals({ ...f.options, generation });
  await assert.rejects(restarted.consume(f.connection, approve), /foreign/);
  const fresh = restarted.connect("human-transport", { ...binding, generation }, "human");
  await assert.rejects(restarted.consume(fresh, approve), /No pending/);
  assert.equal(f.responses.length, 0);
});

test("agent coordination cannot become a human approval, including a forged response role", async () => {
  const f = fixture();
  const agent = f.approvals.connect("agent-transport", binding, "agent");
  assert.throws(() => f.approvals.onRuntimeRequest(binding, agent, request, 100), /Human/);
  f.approvals.onRuntimeRequest(binding, f.connection, request, 100);
  await assert.rejects(f.approvals.consume(agent, approve), /Human/);
  await assert.rejects(f.approvals.consume({ ...agent, actor: "human" } as typeof agent, approve), /Human/);
  assert.equal(f.responses.length, 0);
});

test("confirm/select/input/editor and cancellation responses preserve native RPC semantics", async () => {
  const f = fixture();
  const cases = [
    [{ ...request, id: "deny" }, { type: "extension_ui_response", id: "deny", confirmed: false }],
    [{ ...request, id: "cancel" }, { type: "extension_ui_response", id: "cancel", cancelled: true }],
    [{ type: "extension_ui_request", id: "select", method: "select", title: "Choice", options: ["A", "B"] },
      { type: "extension_ui_response", id: "select", value: "A" }],
    [{ type: "extension_ui_request", id: "input", method: "input", title: "Text" },
      { type: "extension_ui_response", id: "input", value: "typed" }],
    [{ type: "extension_ui_request", id: "editor", method: "editor", title: "Edit", prefill: "initial" },
      { type: "extension_ui_response", id: "editor", value: "edited" }],
  ];
  for (const [ask, response] of cases) {
    f.approvals.onRuntimeRequest(binding, f.connection, ask, 100);
    await f.approvals.consume(f.connection, response);
  }
  assert.equal(f.responses.length, cases.length);
  assert.equal((f.responses[0]!.response as { confirmed: boolean }).confirmed, false);
});

test("malformed/mixed response and unsupported notifications cannot create approvals", async () => {
  const f = fixture();
  assert.throws(() => f.approvals.onRuntimeRequest(binding, f.connection, { ...request, method: "notify" }, 100), /Unsupported/);
  f.approvals.onRuntimeRequest(binding, f.connection, request, 100);
  for (const malformed of [
    { ...approve, value: "yes" }, { ...approve, cancelled: true },
    { ...approve, confirmed: "true" }, { type: "extension_ui_response", id: request.id, value: "yes" },
  ]) await assert.rejects(f.approvals.consume(f.connection, malformed));
  assert.equal(f.responses.length, 0);
  await f.approvals.consume(f.connection, approve);
  assert.equal(f.responses.length, 1);
});

test("RPC write errors surface and cannot trigger a second response attempt", async () => {
  const f = fixture();
  f.fail();
  f.approvals.onRuntimeRequest(binding, f.connection, request, 100);
  await assert.rejects(f.approvals.consume(f.connection, approve), /RPC write failure/);
  await assert.rejects(f.approvals.consume(f.connection, approve), /No pending/);
  assert.equal(f.responses.length, 1);
});

test("request budgets fail closed without evicting replay guards; TTL and clock are validated", async () => {
  const f = fixture();
  const small = createHostApprovals({ ...f.options, maxConnections: 1, maxRequests: 1 });
  const connection = small.connect("only", binding, "human");
  assert.throws(() => small.connect("extra", binding, "human"), /limit/);
  for (const ttl of [0, -1, 1001, 1.5, NaN]) {
    assert.throws(() => small.onRuntimeRequest(binding, connection, request, ttl), /TTL/);
  }
  small.onRuntimeRequest(binding, connection, request, 100);
  await small.consume(connection, approve);
  assert.throws(() => small.onRuntimeRequest(binding, connection, { ...request, id: "next" }, 100), /budget/);
  assert.throws(() => small.onRuntimeRequest(binding, connection, request, 100), /already observed/);
  f.approvals.onRuntimeRequest(binding, f.connection, request, 100);
  f.time(NaN);
  await assert.rejects(f.approvals.consume(f.connection, approve), /expired/);
});
