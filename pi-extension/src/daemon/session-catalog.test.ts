import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { createSessionCatalog, createSessionManagerAdapter } from "./session-catalog.ts";
import type { RunningIdentity } from "./session-catalog.ts";

// Optional test-only module URL permits using an already installed SDK without
// installing dependencies or committing a machine-specific runtime path.
const sdkModule = process.env["SESSION_CATALOG_TEST_SDK_MODULE"] ?? "@earendil-works/pi-coding-agent";
const { SessionManager } = await import(sdkModule) as typeof import("@earendil-works/pi-coding-agent");

test("catalog uses two isolated native SDK histories without launching", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "pi-session-catalog-"));
  try {
    const projects = ["alpha", "beta"].map((id) => ({ id, name: id.toUpperCase(), cwd: join(root, id) }));
    const sessionDirs = new Map<string, string>();
    const ids: string[] = [];
    for (const project of projects) {
      mkdirSync(project.cwd);
      const sessionDir = join(root, `${project.id}-sessions`);
      mkdirSync(sessionDir);
      sessionDirs.set(project.cwd, sessionDir);
      // Every SDK filesystem operation is explicitly directed into this Temp.
      const manager = SessionManager.create(project.cwd, sessionDir);
      manager.appendMessage({ role: "user", content: `synthetic-private-${project.id}`, timestamp: Date.now() });
      manager.appendSessionInfo(`History ${project.id}`);
      ids.push(manager.getSessionId());
    }
    const [alpha, beta] = projects;
    const [alphaId, betaId] = ids;
    assert.ok(alpha && beta && alphaId && betaId);
    const calls: string[] = [];
    const sessions = createSessionManagerAdapter({
      list: async (cwd) => {
        calls.push(cwd);
        const dir = sessionDirs.get(cwd);
        assert.ok(dir, "SDK must not fall back to a real history directory");
        return SessionManager.list(cwd, dir);
      },
    });
    let runningIdentity: RunningIdentity | undefined;
    const runningCalls: Array<[string, string]> = [];
    const catalog = createSessionCatalog({
      projects,
      sessions,
      running: async (projectId, historyId) => {
        runningCalls.push([projectId, historyId]);
        return runningIdentity;
      },
    });

    await t.test("public DTOs contain only IDs, names and timestamps", async () => {
      assert.deepEqual(catalog.listProjects(), [
        { id: "alpha", name: "ALPHA" }, { id: "beta", name: "BETA" },
      ]);
      for (const [index, project] of projects.entries()) {
        const histories = await catalog.listHistories(project.id);
        assert.equal(histories.length, 1);
        const history = histories[0]!;
        assert.deepEqual(Object.keys(history).sort(), ["created", "id", "modified", "name"]);
        assert.equal(history.id, ids[index]);
        assert.equal(history.name, `History ${project.id}`);
        assert.equal(new Date(history.created).toISOString(), history.created);
        assert.equal(new Date(history.modified).toISOString(), history.modified);
        const serialized = JSON.stringify(histories);
        assert.ok(!serialized.includes("synthetic-private"));
        assert.ok(!serialized.includes(project.cwd));
        assert.ok(!serialized.includes(".jsonl"));
      }
      assert.deepEqual(calls, [alpha.cwd, beta.cwd]);
    });

    await t.test("unknown projects and exact-ID failures do not consult running view", async () => {
      const count = calls.length;
      await assert.rejects(catalog.listHistories("unknown"), /Unknown project ID/);
      await assert.rejects(catalog.select(alpha.cwd, alphaId), /Unknown project ID/);
      assert.equal(calls.length, count);
      for (const id of ["unknown", alphaId.slice(0, 8), betaId, "../history.jsonl", ""]) {
        await assert.rejects(catalog.select(alpha.id, id), /history ID/);
      }
      await assert.rejects(catalog.select(beta.id, alphaId), /history ID/);
      assert.deepEqual(runningCalls, []);
    });

    await t.test("non-running selection returns intent only; running returns existing identity", async () => {
      assert.deepEqual(await catalog.select(alpha.id, alphaId), {
        kind: "selection", projectId: alpha.id, historyId: alphaId,
      });
      runningIdentity = { id: "existing-runtime-identity" };
      assert.deepEqual(await catalog.select(alpha.id, alphaId), {
        kind: "attach", identity: runningIdentity,
      });
      assert.deepEqual(runningCalls, [[alpha.id, alphaId], [alpha.id, alphaId]]);
    });

    await t.test("mixed-project rows and duplicate IDs fail closed", async () => {
      const mixedRows = [...await sessions.list(alpha.cwd), ...await sessions.list(beta.cwd)];
      const mixed = createSessionCatalog({ projects, sessions: { list: async () => mixedRows }, running: () => undefined });
      assert.deepEqual((await mixed.listHistories(alpha.id)).map(({ id }) => id), [alphaId]);
      await assert.rejects(mixed.select(alpha.id, betaId), /history ID/);
      const duplicate = createSessionCatalog({ projects, sessions: { list: async () => [...mixedRows, ...mixedRows] }, running: () => undefined });
      await assert.rejects(duplicate.select(alpha.id, alphaId), /ambiguous history ID/);
    });

    await t.test("allowlist is snapshotted and SDK errors do not disclose paths", async () => {
      const source = [{ ...alpha }];
      const snapshot = createSessionCatalog({ projects: source, sessions, running: () => undefined });
      source[0]!.cwd = beta.cwd;
      assert.deepEqual((await snapshot.listHistories(alpha.id)).map(({ id }) => id), [alphaId]);
      assert.throws(() => createSessionCatalog({ projects: [alpha, alpha], sessions, running: () => undefined }), /allowlist/);
      assert.throws(() => createSessionCatalog({ projects: [{ ...alpha, cwd: "." }], sessions, running: () => undefined }), /allowlist/);
      const broken = createSessionCatalog({ projects, sessions: { list: async () => { throw new Error(alpha.cwd); } }, running: () => undefined });
      await assert.rejects(broken.listHistories(alpha.id), { message: "History catalog unavailable" });
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
