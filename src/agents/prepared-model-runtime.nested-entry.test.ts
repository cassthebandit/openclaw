// Preserve module setup before modules that consume it.
// oxfmt-ignore
import { usePreparedModelRuntimeHarness } from "./prepared-model-runtime.test-harness.js";
import { expect, it, vi } from "vitest";
import { resolveCronAgentConfig } from "../cron/isolated-agent/run-config.js";
import { prepareSystemAgentRunAdmission } from "./admitted-run-context.js";
import * as agentScope from "./agent-scope.js";
import { runEmbeddedAgent } from "./embedded-agent-runner/run-orchestrator.js";
import { withPreparedModelRuntimePluginGenerationScope } from "./prepared-model-runtime-generation-scope.js";
import {
  acquireAgentRunPreparedModelRuntime,
  loadPublishedGatewayReplyDispatchRuntime,
  refreshPreparedModelRuntimeSnapshots,
} from "./prepared-model-runtime.js";
import { SessionManager } from "./sessions/session-manager.js";
import { immediateEnqueue } from "./test-helpers/embedded-agent-runner-e2e-fixtures.js";

const loop = vi.hoisted(() => vi.fn());
vi.mock("./embedded-agent-runner/run-loop.js", () => ({ runPreparedEmbeddedLoop: loop }));
const fixture = usePreparedModelRuntimeHarness({ label: "nested-entry" });

it("admits a channel candidate after retaining the same model under cron-projected config", async () => {
  fixture.mocks.configuredAgentIds = ["main"];
  const workspaceDir = fixture.state.workspaceDir;
  fixture.mocks.configuredWorkspaces.set("main", workspaceDir);
  const main = { workspace: workspaceDir, identity: { name: "Synthetic agent" } };
  const config = {
    agents: {
      ownership: "explicit" as const,
      entries: { main },
      defaults: { model: "custom/model", subagents: { model: "custom/subagent" } },
    },
    plugins: { slots: { memory: "none" } },
  };
  // The shared catalog harness still stubs roster lookup through the legacy list shape.
  const agentConfig = vi.spyOn(agentScope, "resolveAgentConfig").mockReturnValue(main);
  let admission: ReturnType<typeof prepareSystemAgentRunAdmission> | undefined;
  try {
    await refreshPreparedModelRuntimeSnapshots(config, {
      gatewayLifecycle: true,
      catalogMode: "static",
      allowGatewaySubagentBinding: true,
    });
    const published = (await loadPublishedGatewayReplyDispatchRuntime({ agentId: "main" }))!;
    const input = {
      config,
      agentId: "main",
      agentDir: published.agentDir,
      workspaceDir,
      allowGatewaySubagentBinding: true,
      runtimePluginSelections: [{ provider: "custom", modelId: "model", runtime: "openclaw" }],
    };
    const options = {
      catalogMode: "static" as const,
      pluginGeneration: published.pluginGeneration,
    };
    const cronConfig = resolveCronAgentConfig({
      config,
      agentConfigOverride: main,
    }).cfgWithAgentDefaults;
    const cron = await acquireAgentRunPreparedModelRuntime(
      { ...input, config: cronConfig },
      options,
    );
    expect(cron.pluginGeneration).not.toBe(published.pluginGeneration);
    expect(cron.snapshot.config).toEqual(cronConfig);
    // Gateway run owners remain cached after the last turn releases its lease.
    await cron[Symbol.asyncDispose]();

    await using parent = await acquireAgentRunPreparedModelRuntime(input, options);
    const runId = "nested-entry";
    admission = prepareSystemAgentRunAdmission(config, runId, "main", "nested-entry-test");
    loop.mockResolvedValue({ payloads: [{ text: "complete" }], meta: { durationMs: 1 } });
    let leaseActive = true;
    try {
      const result = await withPreparedModelRuntimePluginGenerationScope(
        parent.pluginGeneration,
        () =>
          runEmbeddedAgent({
            config,
            agentId: "main",
            agentDir: published.agentDir,
            workspaceDir,
            sessionId: runId,
            sessionKey: `agent:main:${runId}`,
            runId,
            provider: "custom",
            model: "model",
            prompt: "Complete the synthetic candidate",
            timeoutMs: 5000,
            enqueue: immediateEnqueue,
            preparedRunAdmission: admission,
            allowGatewaySubagentBinding: true,
            sessionPersistence: "detached",
            sessionManager: SessionManager.inMemory(workspaceDir),
          }),
        () => (leaseActive ? parent.snapshot : undefined),
      );
      expect(result.payloads?.[0]?.text).toBe("complete");
      expect(parent.snapshot.config).toEqual(config);
      expect(parent.snapshot).not.toBe(cron.snapshot);
      await using repeated = await acquireAgentRunPreparedModelRuntime(
        { ...input, config: structuredClone(config) },
        options,
      );
      expect(repeated.snapshot).toBe(parent.snapshot);
      expect(await loadPublishedGatewayReplyDispatchRuntime({ agentId: "main" })).toBe(published);
    } finally {
      leaseActive = false;
    }
  } finally {
    admission?.close();
    agentConfig.mockRestore();
  }
});
