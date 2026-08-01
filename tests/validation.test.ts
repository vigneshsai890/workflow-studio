import { describe, expect, it, vi } from "vitest";
import { WorkflowExecutor, WorkflowValidationError, validateWorkflow } from "../src/backend/executor.js";
import type { WorkflowGraph } from "../src/shared/types.js";

const llmNode = (id: string): WorkflowGraph["nodes"][number] => ({
  id,
  label: id,
  kind: "llm",
  position: { x: 0, y: 0 },
  config: { prompt: id },
});

describe("workflow validation", () => {
  it("rejects cycles", () => {
    const graph: WorkflowGraph = {
      id: "cyclic",
      nodes: [llmNode("a"), llmNode("b")],
      edges: [
        { id: "ab", source: "a", target: "b" },
        { id: "ba", source: "b", target: "a" },
      ],
    };
    expect(() => validateWorkflow(graph)).toThrowError(WorkflowValidationError);
    expect(() => validateWorkflow(graph)).toThrowError(/contains a cycle/);
  });

  it("rejects dangling edges", () => {
    const graph: WorkflowGraph = {
      id: "dangling",
      nodes: [llmNode("a")],
      edges: [{ id: "missing", source: "a", target: "unknown" }],
    };
    expect(() => validateWorkflow(graph)).toThrowError(/unknown target unknown/);
  });

  it("runs simultaneously-ready nodes in deterministic id order", async () => {
    const calls: string[] = [];
    const graph: WorkflowGraph = {
      id: "ordered",
      nodes: [llmNode("z"), llmNode("a")],
      edges: [],
    };
    const executor = new WorkflowExecutor("run", graph, {
      llm: { complete: vi.fn(async ({ prompt }) => { calls.push(prompt); return prompt; }) },
      mcp: { listTools: vi.fn(async () => []), callTool: vi.fn(async () => undefined) },
    });
    await executor.start();
    expect(calls).toEqual(["a", "z"]);
    expect(executor.snapshot().state).toBe("completed");
  });
});
