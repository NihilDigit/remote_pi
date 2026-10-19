import { Buffer } from "node:buffer";
import type { HostSessionBinding } from "./host-session-control.js";

export interface HostEventCursor extends HostSessionBinding {
  sequence: number;
}

export interface HostSessionEvent {
  cursor: HostEventCursor;
  payload: unknown;
}

export interface HostEventReplay {
  events: HostSessionEvent[];
  /** Inclusive lost range; a client must refresh its snapshot when present. */
  gap?: { from: number; to: number };
  cursor: HostEventCursor;
  hasMore: boolean;
}

function sameBinding(a: HostSessionBinding, b: HostSessionBinding): boolean {
  return a.hostId === b.hostId && a.projectId === b.projectId &&
    a.historyId === b.historyId && a.runtimeId === b.runtimeId && a.generation === b.generation;
}

/**
 * One runtime's pull-based replay ring. No subscriber queues or background sends:
 * transports pull a bounded batch only when writable, then retain just a cursor.
 * A fresh runtime/generation gets a fresh stream and cannot reuse an old cursor.
 */
export function createHostEventStream(
  binding: HostSessionBinding,
  options: { capacity: number; maxEventBytes: number },
) {
  const scope: HostSessionBinding = {
    hostId: binding.hostId, projectId: binding.projectId, historyId: binding.historyId,
    runtimeId: binding.runtimeId, generation: binding.generation,
  };
  if (Object.values(scope).some((value) => typeof value !== "string" || !value)) {
    throw new Error("Missing event stream identity");
  }
  const { capacity, maxEventBytes } = options;
  if (!Number.isSafeInteger(capacity) || capacity < 1 ||
      !Number.isSafeInteger(maxEventBytes) || maxEventBytes < 1) {
    throw new Error("Invalid replay bounds");
  }
  const ring: Array<{ sequence: number; json: string }> = [];
  let sequence = 0;
  const cursorFor = (value: number): HostEventCursor => ({ ...scope, sequence: value });

  return {
    publish(payload: unknown): HostSessionEvent {
      const json = JSON.stringify(payload);
      if (json === undefined || Buffer.byteLength(json, "utf8") > maxEventBytes) {
        throw new Error("Event payload exceeds replay bounds or is not JSON");
      }
      if (sequence === Number.MAX_SAFE_INTEGER) throw new Error("Event sequence exhausted");
      const entry = { sequence: ++sequence, json };
      ring.push(entry);
      if (ring.length > capacity) ring.shift();
      return { cursor: cursorFor(entry.sequence), payload: JSON.parse(entry.json) as unknown };
    },

    replay(after?: HostEventCursor, limit = capacity): HostEventReplay {
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > capacity) {
        throw new Error("Invalid replay batch limit");
      }
      if (after && (!sameBinding(after, scope) || !Number.isSafeInteger(after.sequence) ||
          after.sequence < 0 || after.sequence > sequence)) {
        throw new Error("Foreign, stale or ahead event cursor");
      }
      const requested = (after?.sequence ?? 0) + 1;
      const first = ring[0]?.sequence ?? sequence + 1;
      const gap = requested < first ? { from: requested, to: first - 1 } : undefined;
      const entries = ring.filter((entry) => entry.sequence >= requested).slice(0, limit);
      const last = entries.at(-1)?.sequence ?? after?.sequence ?? 0;
      return {
        events: entries.map((entry) => ({
          cursor: cursorFor(entry.sequence), payload: JSON.parse(entry.json) as unknown,
        })),
        ...(gap ? { gap } : {}),
        cursor: cursorFor(last),
        hasMore: last < sequence,
      };
    },
  };
}
