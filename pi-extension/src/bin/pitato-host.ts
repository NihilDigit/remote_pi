import { readFileSync, realpathSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { hostname, homedir } from "node:os";
import { createHash } from "node:crypto";
import type { Server } from "node:http";
import { z } from "zod";
import { Supervisor } from "../daemon/supervisor.js";
import { HostRelay } from "../daemon/host-relay.js";
import { startHostWeb } from "../daemon/host-web.js";
import { getOrCreateEd25519Keypair } from "../pairing/storage.js";

const configSchema = z.object({
  name: z.string().min(1).default(hostname()),
  projects: z
    .array(z.object({ id: z.string().min(1), name: z.string().min(1), cwd: z.string().min(1) }))
    .min(1),
  relayUrl: z
    .string()
    .url()
    .refine((url) => url.startsWith("wss://") || url.startsWith("ws://127.0.0.1:"))
    .optional(),
  piBin: z.string().optional(),
  trustProjects: z.boolean().default(false),
  port: z.number().int().min(1024).max(65535).default(4317),
});

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  if (args.length !== 2 || args[0] !== "--config") {
    process.stdout.write("Usage: node dist/bin/pitato-host.js --config <host.json>\n");
    process.exitCode = args.includes("--help") ? 0 : 2;
    return;
  }
  const path = resolve(args[1]!);
  const config = configSchema.parse(JSON.parse(readFileSync(path, "utf8")));
  const keypair = config.relayUrl
    ? await getOrCreateEd25519Keypair({ requireKeyring: process.platform === "win32" })
    : undefined;
  const hostId = keypair
    ? Buffer.from(keypair.publicKey).toString("base64")
    : createHash("sha256").update(path).digest("hex");
  const supervisor = new Supervisor({
    extensionPath: join(dirname(dirname(fileURLToPath(import.meta.url))), "index.js"),
    piBin: config.piBin,
    trustProject: config.trustProjects,
    loadExtension: false,
    autoStart: false,
    host: {
      hostId,
      name: config.name,
      projects: config.projects.map((project) => ({ ...project, cwd: realpathSync(project.cwd) })),
      recordsPath: join(
        process.env.REMOTE_PI_HOME ?? homedir(),
        ".pi",
        "remote",
        "host-records.json",
      ),
    },
  });
  let relay: HostRelay | undefined;
  let web: Server | undefined;
  try {
    await supervisor.start();
    const host = supervisor.host!;
    relay = keypair && config.relayUrl
      ? new HostRelay({ url: config.relayUrl, keypair, name: config.name, host })
      : undefined;
    web = await startHostWeb(host, relay, config.port);
    if (relay) await relay.start();
  } catch (error) {
    relay?.close();
    web?.closeAllConnections();
    web?.close();
    await supervisor.stop();
    throw error;
  }
  process.stdout.write(`pitato ready: http://127.0.0.1:${config.port}\n`);
  let stopping = false;
  async function stop() {
    if (stopping) return;
    stopping = true;
    relay?.close();
    web?.closeAllConnections();
    web?.close();
    await supervisor.stop();
  }
  process.on("SIGINT", () => void stop());
  process.on("SIGTERM", () => void stop());
}
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
