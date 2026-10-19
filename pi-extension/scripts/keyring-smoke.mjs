import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Entry } from "@napi-rs/keyring";

const entry = new Entry("dev.remotepi.pitato-smoke", randomUUID());
let written = false;
try {
  entry.setPassword("synthetic-roundtrip-only");
  written = true;
  assert.equal(entry.getPassword(), "synthetic-roundtrip-only");
  console.log(JSON.stringify({ platform: process.platform, arch: process.arch, node: process.version, nodeAbi: process.versions.modules, nativeKeyringRoundtrip: true }));
} finally {
  if (written) assert.equal(entry.deleteCredential(), true);
}
