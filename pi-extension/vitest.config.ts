import { defineConfig, configDefaults } from "vitest/config";

export default defineConfig({
  test: {
    exclude: [...configDefaults.exclude,
      "src/daemon/host-session-control.test.ts", "src/daemon/host-events.test.ts",
      "src/daemon/host-approvals.test.ts", "src/daemon/session-catalog.test.ts"],
  },
});
