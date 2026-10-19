import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawn } from "node:child_process";
import { Supervisor } from "../dist/daemon/supervisor.js";
import { HostRelay } from "../dist/daemon/host-relay.js";
import { startHostWeb } from "../dist/daemon/host-web.js";
import { generateEd25519Keypair } from "../dist/pairing/crypto.js";

const root = mkdtempSync(join(tmpdir(), "pitato-android-smoke-"));
process.env.REMOTE_PI_HOME = root;
mkdirSync(join(root, "project"));
mkdirSync(join(root, "agent"));
writeFileSync(join(root, "agent", "settings.json"), JSON.stringify({ provider: "pitato-smoke", model: "stream", thinkingLevel: "off" }));
process.env.PI_CODING_AGENT_DIR = join(root, "agent");
writeFileSync(join(root, "project", "README.md"), "# Preview smoke\n\n中文、代码与文件。\n");
writeFileSync(join(root, "project", "pixel.png"), Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=", "base64"));
const relayBinary = process.env.PITATO_TEST_RELAY_BIN;
if (!relayBinary) throw new Error("Set PITATO_TEST_RELAY_BIN to a built Remote Pi relay");
function startRelay() { return spawn(relayBinary, [], {
  env: { ...process.env, REMOTEPI_MESH_DB_PATH: join(root, "mesh.db"), REMOTEPI_RELAY_PORT: "3017", RUST_LOG: "warn" },
  stdio: ["ignore", "ignore", "inherit"], windowsHide: true,
}); }
let relayProcess = startRelay();
relayProcess.once("error", (error) => { console.error(error); process.exitCode = 1; });
const keypair = generateEd25519Keypair();
const supervisor = new Supervisor({
  extensionPath: resolve("src/daemon/fixtures/host-smoke-extension.mjs"),
  piBin: resolve("node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js"),
  autoStart: false, loadExtension: true, trustProject: false,
  childEnv: { PI_CODING_AGENT_DIR: join(root, "agent") },
  host: { hostId: Buffer.from(keypair.publicKey).toString("base64"), name: "Synthetic PC", projects: [{ id: "smoke", name: "Synthetic project", cwd: join(root, "project") }], recordsPath: join(root, "records.json") },
});
await supervisor.start();
const owners = [];
const relay = new HostRelay({ url: "ws://127.0.0.1:3017", keypair, name: "Synthetic PC", host: supervisor.host,
  owners: { list: async () => owners, add: async (record) => { owners.push(record); } },
});
const web = await startHostWeb(supervisor.host, relay, 4318);
await relay.start();
for (let attempt = 0; !relay.connected && attempt < 100; attempt++) {
  await new Promise((resolve) => setTimeout(resolve, 100));
}
if (!relay.connected) throw new Error("Synthetic relay did not become ready");
const output = process.env.PITATO_TEST_OUTPUT ?? join(root, "connection.json");
const writePair = () => {
  if (!relay.connected) return;
  writeFileSync(output, JSON.stringify({ ...relay.pairing(), root, pid: process.pid, relayPid: relayProcess.pid }), { mode: 0o600 });
};
writePair();
const rotation = setInterval(writePair, 90000);
const control = createServer((request, response) => {
  if (request.method !== "POST" || request.url !== "/drop-relay") { response.writeHead(404).end(); return; }
  relayProcess.once("exit", () => { setTimeout(() => { relayProcess = startRelay(); }, 1500); });
  relayProcess.kill();
  response.end("dropped");
});
control.listen(4319, "127.0.0.1");
let stopping = false;
async function stop() {
  if (stopping) return;
  stopping = true;
  clearInterval(rotation);
  relay.close(); web.closeAllConnections(); web.close(); control.close();
  await supervisor.stop(); relayProcess.kill();
}
process.on("SIGINT", () => void stop()); process.on("SIGTERM", () => void stop());
console.log("Synthetic Android smoke ready on relay 3017 / operator 4318");
