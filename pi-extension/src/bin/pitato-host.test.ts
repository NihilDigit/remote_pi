import { test, expect } from "vitest";
import { createServer } from "node:net";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { Supervisor } from "../daemon/supervisor.js";

test("a web bind failure exits and releases the supervisor for the next launch", async () => {
  const root = mkdtempSync(join(tmpdir(), "pitato-startup-"));
  mkdirSync(join(root, "project"));
  const occupied = createServer();
  await new Promise<void>((resolve) => occupied.listen(0, "127.0.0.1", resolve));
  const port = (occupied.address() as { port: number }).port;
  const config = join(root, "host.json");
  writeFileSync(config, JSON.stringify({ port, projects: [{ id: "test", name: "Test", cwd: join(root, "project") }] }));
  const child = spawn(process.execPath, ["--import", "tsx", "src/bin/pitato-host.ts", "--config", config], {
    env: { ...process.env, REMOTE_PI_HOME: root, PI_CODING_AGENT_DIR: join(root, "agent") },
    stdio: ["ignore", "pipe", "pipe"], windowsHide: true,
  });
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr += String(chunk); });
  const previous = process.env.REMOTE_PI_HOME;
  let supervisor: Supervisor | undefined;
  try {
    const code = await new Promise<number | null>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("Startup failure left a live host process")), 10000);
      child.once("error", reject);
      child.once("close", (code) => { clearTimeout(timer); resolve(code); });
    });
    expect(code).toBe(1);
    expect(stderr).toContain("EADDRINUSE");
    process.env.REMOTE_PI_HOME = root;
    supervisor = new Supervisor({ extensionPath: "synthetic-unused", autoStart: false });
    await supervisor.start();
  } finally {
    if (child.exitCode === null) child.kill();
    await supervisor?.stop();
    if (previous === undefined) delete process.env.REMOTE_PI_HOME; else process.env.REMOTE_PI_HOME = previous;
    await new Promise<void>((resolve) => occupied.close(() => resolve()));
  }
}, 15000);
