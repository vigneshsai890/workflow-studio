import { describe, expect, it, vi } from "vitest";
import { WorkflowExecutor } from "../src/backend/executor.js";
import type { WorkflowEvent, WorkflowGraph } from "../src/shared/types.js";

const graph: WorkflowGraph = {
  id: "approval-flow",
  nodes: [
    {
      id: "approval",
      label: "Review",
      kind: "humanApproval",
      position: { x: 0, y: 0 },
      config: { prompt: "Ship it?" },
    },
    {
      id: "after",
      label: "Continue",
      kind: "llm",
      position: { x: 200, y: 0 },
      config: { prompt: "Continue" },
    },
  ],
  edges: [{ id: "approval-after", source: "approval", target: "after" }],
};

describe("human approval", () => {
  it("pauses and resumes an approved node", async () => {
    const events: WorkflowEvent[] = [];
    const complete = vi.fn(async () => "done");
    const executor = new WorkflowExecutor("run", graph, {
      llm: { complete },
      mcp: { listTools: vi.fn(async () => []), callTool: vi.fn(async () => undefined) },
    });
    executor.subscribe((event) => events.push(event));

    await executor.start();
    expect(executor.snapshot()).toMatchObject({ state: "paused", pausedNodeId: "approval" });
    expect(complete).not.toHaveBeenCalled();

    await executor.resume("approval", { approved: true, response: { reviewer: "human" } });
    expect(executor.snapshot().state).toBe("completed");
    expect(executor.snapshot().statuses).toEqual({ approval: "succeeded", after: "succeeded" });
    expect(events.some((event) => event.type === "human.resumed")).toBe(true);
  });

  it("rejects invalid resume attempts", async () => {
    const executor = new WorkflowExecutor("run", graph, {
      llm: { complete: vi.fn(async () => "done") },
      mcp: { listTools: vi.fn(async () => []), callTool: vi.fn(async () => undefined) },
    });
    await expect(executor.resume("approval", { approved: true })).rejects.toThrow(/not paused/);
    await executor.start();
    await expect(executor.resume("after", { approved: true })).rejects.toThrow(/not paused on that node/);
  });
});
