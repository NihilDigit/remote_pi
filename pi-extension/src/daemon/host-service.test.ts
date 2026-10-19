import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import { Supervisor } from "./supervisor.js";
import { HostRecordStore } from "./host-service.js";
import type { HostApprovalConnection } from "./host-approvals.js";
import type { HostSessionResult } from "./host-session-control.js";

let supervisor: Supervisor;
let root: string;
let previous: string | undefined;
let previousAgent: string | undefined;
const hostId = "synthetic-host";

beforeEach(async () => {
  root = mkdtempSync(join(tmpdir(), "pitato-host-integration-"));
  previous = process.env.REMOTE_PI_HOME;
  previousAgent = process.env.PI_CODING_AGENT_DIR;
  process.env.REMOTE_PI_HOME = root;
  process.env.PI_CODING_AGENT_DIR = join(root, "agent");
  mkdirSync(join(root, "agent"));
  writeFileSync(
    join(root, "agent", "settings.json"),
    JSON.stringify({ provider: "pitato-smoke", model: "stream", thinkingLevel: "off" }),
  );
  mkdirSync(join(root, "project"));
  writeFileSync(join(root, "project", "README.md"), "# 中文预览\nTAIL");
  writeFileSync(join(root, "outside.txt"), "outside");
  supervisor = new Supervisor({
    extensionPath: fileURLToPath(new URL("./fixtures/host-smoke-extension.mjs", import.meta.url)),
    piBin: resolve("node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js"),
    autoStart: false,
    loadExtension: true,
    trustProject: false,
    childEnv: { PI_CODING_AGENT_DIR: join(root, "agent"), PI_THAGORAS_RECOVERY: "0" },
    host: {
      hostId,
      name: "Synthetic PC",
      projects: [{ id: "test", name: "Test", cwd: join(root, "project") }],
      recordsPath: join(root, "records.json"),
    },
  });
  await supervisor.start();
});
afterEach(async () => {
  await supervisor?.stop();
  if (previous === undefined) delete process.env.REMOTE_PI_HOME;
  else process.env.REMOTE_PI_HOME = previous;
  if (previousAgent === undefined) delete process.env.PI_CODING_AGENT_DIR;
  else process.env.PI_CODING_AGENT_DIR = previousAgent;
});

async function create() {
  const host = supervisor.host!;
  return host.handle("test-human", {
    action: "create",
    projectId: "test",
    operationId: randomUUID(),
    generation: host.generation,
  }) as Promise<HostSessionResult>;
}

describe("real Pi RPC integration without model generation", () => {
  test("fresh sessions have distinct IDs, duplicate clicks attach, foreign leases and path escapes fail", async () => {
    const host = supervisor.host!;
    const operationId = randomUUID();
    const request = {
      action: "create",
      projectId: "test",
      operationId,
      generation: host.generation,
    };
    const [a, retry] = (await Promise.all([
      host.handle("test-human", request),
      host.handle("test-human", request),
    ])) as HostSessionResult[];
    expect(a!.status).toBe("running");
    expect(retry!.action).toBe("attached");
    expect(retry!.lease).toEqual(a!.lease);
    const b = await create();
    expect(b.status).toBe("running");
    expect(b.historyId).not.toBe(a!.historyId);
    const connection = (await host.handle("test-human", {
      action: "connect",
      binding: a!.lease,
    })) as HostApprovalConnection;
    const snapshot = (await host.handle("test-human", { action: "snapshot", connection })) as {
      messages: unknown[];
    };
    expect(snapshot.messages).toEqual([]);
    expect(
      await host.handle("test-human", { action: "preview", connection, path: "README.md" }),
    ).toEqual({ path: "README.md", mime: "text/plain", text: "# 中文预览\nTAIL" });
    await expect(
      host.handle("test-human", { action: "preview", connection, path: "../outside.txt" }),
    ).rejects.toThrow("outside");
    await expect(host.handle("other-human", { action: "snapshot", connection })).rejects.toThrow(
      "expired",
    );
    const replacement = await host.handle("test-human", { action: "connect", binding: a!.lease });
    await expect(host.handle("test-human", { action: "snapshot", connection })).rejects.toThrow(
      "expired",
    );
    expect(replacement).not.toEqual(connection);
  }, 30000);

  test("native confirm is delivered once, explicit cancel and disconnect resolve without a grant", async () => {
    const host = supervisor.host!;
    const result = await create();
    const connection = (await host.handle("test-human", {
      action: "connect",
      binding: result.lease,
    })) as HostApprovalConnection;
    const operationId = randomUUID();
    expect(
      await host.handle("test-human", {
        action: "prompt",
        connection,
        text: "/mvp-smoke",
        operationId,
      }),
    ).toEqual({ submitted: true });
    let request: { request: { id: string } } | undefined;
    await expect
      .poll(async () => {
        const data = (await host.handle("test-human", { action: "poll", connection })) as {
          approvals: (typeof request)[];
        };
        request = data.approvals[0];
        return request?.request.id;
      })
      .toBeTruthy();
    const response = { type: "extension_ui_response", id: request!.request.id, cancelled: true };
    expect(await host.handle("test-human", { action: "respond", connection, response })).toEqual({
      consumed: true,
    });
    await expect(
      host.handle("test-human", { action: "respond", connection, response }),
    ).rejects.toThrow();
    await expect
      .poll(async () =>
        JSON.stringify(await host.handle("test-human", { action: "snapshot", connection })),
      )
      .toContain("SMOKE_CANCELLED");
    expect(
      await host.handle("test-human", {
        action: "prompt",
        connection,
        text: "/mvp-smoke",
        operationId,
      }),
    ).toEqual({ accepted: true, duplicate: true });
    await host.handle("test-human", {
      action: "prompt",
      connection,
      text: "/mvp-smoke",
      operationId: randomUUID(),
    });
    await expect
      .poll(async () => {
        const data = (await host.handle("test-human", { action: "poll", connection })) as {
          approvals: unknown[];
        };
        return data.approvals.length;
      })
      .toBe(1);
    host.disconnectActor("test-human");
    const next = await host.handle("test-human", { action: "connect", binding: result.lease });
    await expect
      .poll(async () =>
        JSON.stringify(await host.handle("test-human", { action: "snapshot", connection: next })),
      )
      .toContain("SMOKE_CANCELLED");
    const data = (await host.handle("test-human", { action: "poll", connection: next })) as {
      approvals: unknown[];
    };
    expect(data.approvals).toEqual([]);
    await host.handle("test-human", {
      action: "prompt",
      connection: next,
      text: "Synthetic stream only",
      operationId: randomUUID(),
    });
    await expect
      .poll(async () =>
        JSON.stringify(await host.handle("test-human", { action: "snapshot", connection: next })),
      )
      .toContain("SMOKE_STREAMED");
    const histories = (await host.handle("test-human", {
      action: "histories",
      projectId: "test",
    })) as Array<{ id: string }>;
    expect(histories.map((h) => h.id)).toEqual([result.historyId]);
    const attached = (await host.handle("test-human", {
      action: "select",
      projectId: "test",
      historyId: result.historyId,
      generation: host.generation,
    })) as HostSessionResult;
    expect(attached.action).toBe("attached");
    expect(attached.lease).toEqual(result.lease);
    await host.handle("test-human", { action: "stop", connection: next });
    const resumed = (await host.handle("test-human", {
      action: "select",
      projectId: "test",
      historyId: result.historyId,
      generation: host.generation,
    })) as HostSessionResult;
    expect(resumed.action).toBe("opened");
    expect(resumed.historyId).toBe(result.historyId);
    expect(resumed.lease?.runtimeId).not.toBe(result.lease?.runtimeId);
    const resumedConnection = await host.handle("test-human", {
      action: "connect",
      binding: resumed.lease,
    });
    expect(
      JSON.stringify(
        await host.handle("test-human", { action: "snapshot", connection: resumedConnection }),
      ),
    ).toContain("SMOKE_STREAMED");
  }, 30000);

  test("durable metadata survives reconstruction without claiming a running process", async () => {
    const result = await create();
    const store = new HostRecordStore(join(root, "records.json"));
    const records = await store.list(hostId);
    expect(records[0]!.historyId).toBe(result.historyId);
    await supervisor.stop();
    const closed = new HostRecordStore(join(root, "records.json"));
    expect((await closed.list(hostId))[0]!.status).toBe("ended");
  }, 30000);

  test("new native UI requests belong to the most recently attached device", async () => {
    const host = supervisor.host!;
    const result = await create();
    const older = await host.handle("older-device", { action: "connect", binding: result.lease });
    const newer = await host.handle("newer-device", { action: "connect", binding: result.lease });
    await host.handle("older-device", { action: "prompt", connection: older, text: "/mvp-smoke", operationId: randomUUID() });
    let requestId: string | undefined;
    await expect.poll(async () => {
      const poll = await host.handle("newer-device", { action: "poll", connection: newer }) as { approvals: Array<{ request: { id: string } }> };
      requestId = poll.approvals[0]?.request.id;
      return poll.approvals.length;
    }).toBe(1);
    const oldPoll = await host.handle("older-device", { action: "poll", connection: older }) as { approvals: unknown[] };
    expect(oldPoll.approvals).toEqual([]);
    const response = { type: "extension_ui_response", id: requestId, cancelled: true };
    await expect(host.handle("older-device", { action: "respond", connection: older, response })).rejects.toThrow();
    expect(await host.handle("newer-device", { action: "respond", connection: newer, response })).toEqual({ consumed: true });
  }, 30000);
});
