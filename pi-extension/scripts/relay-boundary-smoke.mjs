import assert from "node:assert/strict";
import { readFileSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { generateEd25519Keypair } from "../dist/pairing/crypto.js";
import { RelayClient } from "../dist/transport/relay_client.js";

const output = process.env.PITATO_TEST_OUTPUT;
const info = JSON.parse(readFileSync(output, "utf8"));
const peer = Buffer.from(new URL(info.uri).searchParams.get("epk"), "base64url").toString("base64");
const identity = generateEd25519Keypair();
const ownerKey = Buffer.from(identity.publicKey).toString("base64");
const stranger = new RelayClient("ws://127.0.0.1:3017", identity);
const observer = new RelayClient("ws://127.0.0.1:3017", generateEd25519Keypair());
const restored = new RelayClient("ws://127.0.0.1:3017", identity);

function next(client, accept) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { client.off("message", receive); reject(new Error("Relay boundary observation timed out")); }, 7000);
    function receive(line) {
      const value = JSON.parse(line);
      if (!accept(value)) return;
      clearTimeout(timer); client.off("message", receive); resolve(value);
    }
    client.on("message", receive);
  });
}
async function exchange(client, message) {
  const id = randomUUID();
  const reply = next(client, (outer) => outer.ct && JSON.parse(Buffer.from(outer.ct, "base64").toString("utf8")).in_reply_to === id);
  client.send(JSON.stringify({ peer, room: "pitato", ct: Buffer.from(JSON.stringify({ ...message, id })).toString("base64") }));
  const outer = await reply;
  return JSON.parse(Buffer.from(outer.ct, "base64").toString("utf8"));
}
async function pairing() {
  const response = await fetch("http://127.0.0.1:4318/api", { method: "POST", headers: { "Content-Type": "application/json", "X-Pitato-Local": "1" }, body: '{"action":"pair"}' });
  const result = await response.json();
  assert.equal(result.ok, true);
  return result.data;
}
try {
  await stranger.connect({ roomId: "main" });
  const denied = await exchange(stranger, { type: "host_request", role: "human", command: { action: "describe" } });
  assert.equal(denied.type, "error"); assert.equal(denied.code, "unknown_peer"); assert.equal(denied.data, undefined);
  const pair = await pairing();
  const paired = await exchange(stranger, { type: "pair_request", token: new URL(pair.uri).searchParams.get("t"), device_name: "Synthetic boundary client" });
  assert.equal(paired.type, "pair_ok");
  const description = await exchange(stranger, { type: "host_hello" });
  assert.equal(description.ok, true);
  const opened = await exchange(stranger, { type: "host_request", command: { action: "create", projectId: "smoke", operationId: randomUUID(), generation: description.data.generation } });
  assert.equal(opened.data.status, "running");
  const connected = await exchange(stranger, { type: "host_request", command: { action: "connect", binding: opened.data.lease } });
  assert.equal(connected.ok, true);
  const connection = connected.data;
  assert.equal((await exchange(stranger, { type: "host_request", command: { action: "snapshot", connection } })).ok, true);
  await observer.connect({ roomId: "main" });
  const online = next(observer, (frame) => frame.type === "peer_online" && frame.peer === ownerKey);
  observer.sendControl({ type: "subscribe_presence", peers: [ownerKey] });
  await online;
  const offline = next(observer, (frame) => frame.type === "peer_offline" && frame.peer === ownerKey);
  stranger.close(); await offline;
  await restored.connect({ roomId: "main" });
  // Deliberately omit host_hello: the server must revoke the old connection
  // from authenticated relay presence, rather than trusting client cooperation.
  const replay = await exchange(restored, { type: "host_request", command: { action: "snapshot", connection } });
  assert.equal(replay.ok, false); assert.equal(replay.data, undefined);
  console.log("PASS: real relay rejects unpaired human claims and revokes old leases on actual peer disconnect");
} finally {
  stranger.close(); restored.close(); observer.close();
  const fresh = await pairing();
  writeFileSync(output, JSON.stringify({ ...info, ...fresh }), { mode: 0o600 });
}
