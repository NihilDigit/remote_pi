import { QRSession, buildQRUri } from "../pairing/qr.js";
import { addPeer, listPeers, type PeerRecord } from "../pairing/storage.js";
import { RelayClient } from "../transport/relay_client.js";
import type { Ed25519Keypair } from "../pairing/crypto.js";
import type { HostService } from "./host-service.js";

/** Uses the existing relay's authenticated sender and existing paired-owner store. */
export class HostRelay {
  private relay: RelayClient | undefined;
  private readonly qr = new QRSession();
  private readonly actors = new Set<string>();
  private stopped = false;
  private retry: ReturnType<typeof setTimeout> | undefined;
  private readonly lanes = new Map<string, Promise<void>>();
  private failures = 0;
  private online = false;
  private queued = 0;

  constructor(
    private readonly options: {
      url: string;
      keypair: Ed25519Keypair;
      name: string;
      host: HostService;
      owners?: { list(): Promise<PeerRecord[]>; add(record: PeerRecord): Promise<void> };
    },
  ) {}

  pairing(): { uri: string; expiresAt: number } {
    if (!this.online) throw new Error("Relay is offline");
    const { token, expiresAt } = this.qr.issueToken(120000);
    return {
      uri: buildQRUri(token, this.options.keypair.publicKey, this.options.name, "pitato"),
      expiresAt,
    };
  }

  get connected(): boolean {
    return this.online;
  }

  async start(): Promise<void> {
    const relay = new RelayClient(this.options.url, this.options.keypair);
    this.relay = relay;
    relay.on("message", (line: string) => {
      if (this.stopped || this.relay !== relay || line.length > 1500000) return;
      let envelope: { type?: string; peer?: string; ct?: string; room_id?: string };
      try {
        envelope = JSON.parse(line);
      } catch {
        return;
      }
      if (!envelope.ct && envelope.peer && (envelope.type === "peer_offline" ||
          (envelope.type === "room_ended" && envelope.room_id === "main"))) {
        const actor = `relay:${envelope.peer}`;
        this.options.host.disconnectActor(actor);
        this.actors.delete(actor);
        return;
      }
      const peer = envelope.peer;
      if (!peer || !envelope.ct || peer.length > 128 || this.lanes.size >= 64 || this.queued >= 16)
        return;
      this.queued++;
      const task = (this.lanes.get(peer) ?? Promise.resolve()).then(() =>
        this.receive(relay, peer, envelope.ct!),
      );
      this.lanes.set(peer, task);
      void task
        .catch(() => undefined)
        .finally(() => {
          this.queued--;
          if (this.lanes.get(peer) === task) this.lanes.delete(peer);
        });
    });
    relay.on("close", () => this.reconnect(relay));
    relay.on("error", () => this.reconnect(relay));
    try {
      await relay.connect({
        roomId: "pitato",
        roomMeta: { name: this.options.name, cwd: "pitato" },
      });
      if (this.stopped || this.relay !== relay) {
        relay.close();
        return;
      }
      this.online = true;
      this.failures = 0;
      await this.watchOwners(relay);
    } catch {
      this.reconnect(relay);
    }
  }

  close(): void {
    this.stopped = true;
    clearTimeout(this.retry);
    this.qr.clear();
    this.online = false;
    for (const actor of this.actors) this.options.host.disconnectActor(actor);
    this.actors.clear();
    this.relay?.close();
  }

  private reconnect(relay: RelayClient): void {
    if (this.relay !== relay || this.stopped) return;
    this.relay = undefined;
    this.online = false;
    this.qr.clear();
    for (const actor of this.actors) this.options.host.disconnectActor(actor);
    this.actors.clear();
    relay.close();
    this.retry = setTimeout(
      () => void this.start(),
      Math.min(30000, 1000 * 2 ** Math.min(this.failures++, 5)),
    );
  }

  private async receive(relay: RelayClient, peer: string, ct: string): Promise<void> {
    let message: {
      type?: string;
      id?: string;
      token?: string;
      device_name?: string;
      command?: unknown;
    };
    try {
      message = JSON.parse(Buffer.from(ct, "base64").toString("utf8"));
    } catch {
      return;
    }
    const send = (value: object) => {
      if (this.relay === relay && relay.isOpen())
        relay.send(
          JSON.stringify({ peer, ct: Buffer.from(JSON.stringify(value)).toString("base64") }),
        );
    };
    if (typeof message.id !== "string" || message.id.length > 128) return;
    if (message.type === "pair_request") {
      if (typeof message.token !== "string" || this.qr.consumeToken(message.token) !== "ok") {
        send({
          type: "pair_error",
          in_reply_to: message.id,
          code: "token_unknown",
          message: "Pairing code expired or already used",
        });
        return;
      }
      const record = {
        name: String(message.device_name ?? "pitato").slice(0, 80),
        remote_epk: peer,
        paired_at: new Date().toISOString(),
      };
      if (this.options.owners) await this.options.owners.add(record);
      else await addPeer(record);
      await this.watchOwners(relay);
      send({
        type: "pair_ok",
        in_reply_to: message.id,
        session_name: this.options.name,
        room_id: "pitato",
        hostname: this.options.name,
      });
      return;
    }
    const known = (await (this.options.owners?.list() ?? listPeers())).some(
      (record) =>
        typeof record?.remote_epk === "string" &&
        Buffer.from(record.remote_epk, "base64url").equals(Buffer.from(peer, "base64")),
    );
    if (!known) {
      send({
        type: "error",
        code: "unknown_peer",
        in_reply_to: message.id,
        message: "Scan a pairing code on the computer",
      });
      return;
    }
    const actor = `relay:${peer}`;
    this.actors.add(actor);
    if (message.type === "host_hello") {
      this.options.host.disconnectActor(actor);
      send({
        type: "host_reply",
        in_reply_to: message.id,
        ok: true,
        data: await this.options.host.handle(actor, { action: "describe" }),
      });
      return;
    }
    if (message.type !== "host_request") return;
    try {
      const data = await this.options.host.handle(actor, message.command);
      send({ type: "host_reply", in_reply_to: message.id, ok: true, data });
    } catch {
      // SDK and filesystem errors may include private absolute paths.
      send({
        type: "host_reply",
        in_reply_to: message.id,
        ok: false,
        error: "Request failed. Reconnect if the session or approval has expired.",
      });
    }
  }

  private async watchOwners(relay: RelayClient): Promise<void> {
    const records = await (this.options.owners?.list() ?? listPeers());
    if (this.relay !== relay || !relay.isOpen()) return;
    const peers = records.flatMap((record) => {
      if (typeof record?.remote_epk !== "string") return [];
      const key = Buffer.from(record.remote_epk, "base64url");
      return key.length === 32 ? [key.toString("base64")] : [];
    });
    relay.sendControl({ type: "subscribe_presence", peers });
    relay.sendControl({ type: "subscribe_rooms", peers });
  }
}
