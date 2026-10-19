import assert from "node:assert/strict";
import { test } from "node:test";
import { createHostEventStream } from "./host-events.js";

const binding = { hostId: "host", projectId: "project", historyId: "history", runtimeId: "runtime", generation: "boot-1" };
const bounds = { capacity: 3, maxEventBytes: 128 };

test("session events have ordered scoped cursors, bounded replay and explicit gaps", () => {
  const stream = createHostEventStream(binding, bounds);
  const initial = stream.replay();
  assert.deepEqual(initial.events, []);
  assert.equal(initial.cursor.sequence, 0);
  for (let value = 1; value <= 5; value++) {
    assert.deepEqual(stream.publish({ value }).cursor, { ...binding, sequence: value });
  }
  const replay = stream.replay(initial.cursor, 2);
  assert.deepEqual(replay.gap, { from: 1, to: 2 });
  assert.deepEqual(replay.events.map((event) => event.cursor.sequence), [3, 4]);
  assert.equal(replay.hasMore, true);
  const last = stream.replay(replay.cursor, 2);
  assert.equal(last.gap, undefined);
  assert.deepEqual(last.events.map((event) => event.cursor.sequence), [5]);
  assert.equal(last.hasMore, false);
  assert.deepEqual(stream.replay(last.cursor).events, []);
  assert.deepEqual(stream.replay().gap, { from: 1, to: 2 });
});

test("slow clients retain only cursors; the shared replay ring cannot accumulate unbounded events", () => {
  const stream = createHostEventStream(binding, bounds);
  const slow = stream.publish("first").cursor;
  for (let i = 0; i < 10_000; i++) stream.publish(i);
  const replay = stream.replay(slow);
  assert.equal(replay.events.length, 3);
  assert.deepEqual(replay.gap, { from: 2, to: 9998 });
  assert.deepEqual(replay.events.map((event) => event.payload), [9997, 9998, 9999]);
});

test("host, project, history, runtime and generation cannot share replay cursors", () => {
  const stream = createHostEventStream(binding, bounds);
  const cursor = stream.publish("one").cursor;
  for (const key of Object.keys(binding)) {
    assert.throws(() => stream.replay({ ...cursor, [key]: "foreign" }), /cursor/);
  }
  for (const sequence of [-1, 2, 0.5, NaN]) {
    assert.throws(() => stream.replay({ ...cursor, sequence }), /cursor/);
  }
  const restarted = createHostEventStream({ ...binding, generation: "boot-2" }, bounds);
  assert.throws(() => restarted.replay(cursor), /cursor/);
});

test("payload and identity snapshots cannot mutate replay; oversized payloads do not consume sequence", () => {
  const source = { ...binding };
  const stream = createHostEventStream(source, bounds);
  source.hostId = "changed";
  const payload = { nested: { value: 1 } };
  const published = stream.publish(payload);
  payload.nested.value = 2;
  (published.payload as typeof payload).nested.value = 3;
  published.cursor.hostId = "tampered";
  const replay = stream.replay();
  assert.deepEqual(replay.events[0]!.payload, { nested: { value: 1 } });
  (replay.events[0]!.payload as typeof payload).nested.value = 4;
  assert.deepEqual(stream.replay().events[0]!.payload, { nested: { value: 1 } });
  assert.equal(stream.replay().cursor.hostId, binding.hostId);
  assert.throws(() => stream.publish("x".repeat(200)), /bounds/);
  assert.throws(() => stream.publish(undefined), /JSON/);
  assert.equal(stream.publish("next").cursor.sequence, 2);
  for (const limit of [0, 4, NaN, 0.5]) assert.throws(() => stream.replay(undefined, limit), /limit/);
  assert.throws(() => createHostEventStream(binding, { ...bounds, capacity: 0 }), /bounds/);
});
