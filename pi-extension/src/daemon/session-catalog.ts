import { isAbsolute } from "node:path";
import type { SessionInfo, SessionManager } from "@earendil-works/pi-coding-agent";

/** Server-owned allowlist entry. Never constructed from client paths. */
export interface CatalogProject {
  id: string;
  name: string;
  cwd: string;
}

export interface HistorySummary {
  id: string;
  name?: string;
  created: string;
  modified: string;
}

type HistoryMetadata = Pick<SessionInfo, "id" | "name" | "cwd" | "created" | "modified">;

/** The catalog needs only project-scoped listing, never open/resume/listAll. */
export interface SessionCatalogAdapter {
  list(cwd: string): Promise<readonly HistoryMetadata[]>;
}

export function createSessionManagerAdapter(
  sdk: Pick<typeof SessionManager, "list">,
): SessionCatalogAdapter {
  return {
    async list(cwd) {
      const histories = await sdk.list(cwd);
      return histories.map(({ id, name, cwd, created, modified }) => ({
        id, name, cwd, created, modified,
      }));
    },
  };
}

/** Opaque identity owned by the existing runtime, not a process or file path. */
export interface RunningIdentity {
  id: string;
}

export type RunningView = (
  projectId: string,
  historyId: string,
) => RunningIdentity | undefined | Promise<RunningIdentity | undefined>;

export type HistorySelection =
  | { kind: "attach"; identity: RunningIdentity }
  | { kind: "selection"; projectId: string; historyId: string };

/** Read-only discovery and intent selection; this layer never launches a child. */
export function createSessionCatalog(options: {
  projects: readonly CatalogProject[];
  sessions: SessionCatalogAdapter;
  running: RunningView;
}) {
  const projects = new Map<string, CatalogProject>();
  for (const project of options.projects) {
    if (!project.id || !isAbsolute(project.cwd) || projects.has(project.id)) {
      throw new Error("Invalid project allowlist");
    }
    projects.set(project.id, { ...project });
  }
  const { sessions, running } = options;

  function projectFor(id: string): CatalogProject {
    const project = projects.get(id);
    if (!project) throw new Error("Unknown project ID");
    return project;
  }

  async function historiesFor(project: CatalogProject): Promise<readonly HistoryMetadata[]> {
    let histories: readonly HistoryMetadata[];
    try {
      histories = await sessions.list(project.cwd);
    } catch {
      // Do not propagate SDK errors containing local filesystem paths.
      throw new Error("History catalog unavailable");
    }
    // Header cwd is checked even if an injected adapter returns mixed projects.
    return histories.filter((history) => history.cwd === project.cwd);
  }

  return {
    listProjects(): Array<Pick<CatalogProject, "id" | "name">> {
      return Array.from(projects.values(), ({ id, name }) => ({ id, name }));
    },

    async listHistories(projectId: string): Promise<HistorySummary[]> {
      const histories = await historiesFor(projectFor(projectId));
      return histories.map((history) => ({
        id: history.id,
        ...(history.name === undefined ? {} : { name: history.name }),
        created: history.created.toISOString(),
        modified: history.modified.toISOString(),
      }));
    },

    async select(projectId: string, historyId: string): Promise<HistorySelection> {
      const histories = await historiesFor(projectFor(projectId));
      // Exact equality only: no SDK prefix lookup, path lookup or global search.
      const matches = histories.filter((history) => history.id === historyId);
      if (matches.length !== 1) throw new Error("Unknown or ambiguous history ID");
      const identity = await running(projectId, historyId);
      if (identity) return { kind: "attach", identity: { id: identity.id } };
      // The future supervisor must revalidate and reuse its existing RpcChild path.
      return { kind: "selection", projectId, historyId };
    },
  };
}
