import { describe, expect, it, vi } from "vitest";
import { WorkflowExecutor } from "../src/backend/executor.js";
import type { WorkflowGraph } from "../src/shared/types.js";

const graph: WorkflowGraph = {
  id: "failure-flow",
  nodes: [
    {
      id: "tool",
      label: "Dynamic tool",
      kind: "tool",
      position: { x: 0, y: 0 },
      config: { toolName: "search", arguments: { query: "test" } },
    },
    {
      id: "child",
      label: "Child",
      kind: "llm",
      position: { x: 200, y: 0 },
      config: { prompt: "child" },
    },
    {
      id: "grandchild",
      label: "Grandchild",
      kind: "llm",
      position: { x: 400, y: 0 },
      config: { prompt: "grandchild" },
    },
    {
      id: "independent",
      label: "Independent",
      kind: "llm",
      position: { x: 0, y: 150 },
      config: { prompt: "independent" },
    },
  ],
  edges: [
    { id: "tool-child", source: "tool", target: "child" },
    { id: "child-grandchild", source: "child", target: "grandchild" },
  ],
};

describe("failure propagation", () => {
  it("checks MCP tools at execution time, skips descendants, and continues independent work", async () => {
    const complete = vi.fn(async () => "ok");
    const callTool = vi.fn(async () => "unused");
    const listTools = vi.fn(async () => []);
    const executor = new WorkflowExecutor("run", graph, {
      llm: { complete },
      mcp: { listTools, callTool },
    });

    await executor.start();

    expect(listTools).toHaveBeenCalledOnce();
    expect(callTool).not.toHaveBeenCalled();
    expect(complete).toHaveBeenCalledOnce();
    expect(executor.snapshot()).toMatchObject({
      state: "failed",
      statuses: {
        tool: "failed",
        child: "skipped",
        grandchild: "skipped",
        independent: "succeeded",
      },
    });
  });
});
