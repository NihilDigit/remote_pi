import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";

export default function (pi) {
  pi.registerProvider("pitato-smoke", {
    api: "pitato-smoke", baseUrl: "https://invalid.example", apiKey: "synthetic-no-network",
    models: [{ id: "stream", name: "Synthetic stream", reasoning: false, input: ["text"],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 16000, maxTokens: 1000 }],
    streamSimple(model, _context, options) {
      const stream = createAssistantMessageEventStream();
      const message = { role: "assistant", api: model.api, provider: model.provider, model: model.id,
        content: [{ type: "text", text: "" }], stopReason: "pending", timestamp: Date.now(),
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      void (async () => {
        stream.push({ type: "start", partial: message });
        stream.push({ type: "text_start", contentIndex: 0, partial: message });
        for (const delta of ["**SMOKE_", "STREAMED**\n\n", "```kotlin\n", "val answer = 42\n", "```\n\n[项目说明](README.md)"]) {
          await new Promise((resolve) => setTimeout(resolve, 50));
          if (options?.signal?.aborted) {
            message.stopReason = "aborted";
            stream.push({ type: "error", reason: "aborted", error: message }); stream.end(); return;
          }
          message.content[0].text += delta;
          stream.push({ type: "text_delta", contentIndex: 0, delta, partial: message });
        }
        stream.push({ type: "text_end", contentIndex: 0, content: message.content[0].text, partial: message });
        message.stopReason = "stop";
        stream.push({ type: "done", reason: "stop", message }); stream.end();
      })();
      return stream;
    },
  });
  pi.registerCommand("mvp-smoke", {
    description: "Isolated host integration smoke; no provider invocation",
    handler: async (_args, ctx) => {
      const confirmed = await ctx.ui.confirm("Synthetic approval", "Approve only this isolated smoke request?");
      pi.sendMessage({ customType: "mvp-smoke-result", content: confirmed ? "SMOKE_CONFIRMED" : "SMOKE_CANCELLED", display: true });
    },
  });
}
