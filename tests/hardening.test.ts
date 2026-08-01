import { describe, expect, it, vi } from "vitest";
import { WorkflowExecutor, validateWorkflow } from "../src/backend/executor.js";
import { PayloadValidationError, normalizeOutput, parseExecutionSnapshot, parseWorkflowEvent, parseWorkflowGraph, validateToolArguments } from "../src/shared/runtime.js";
import type { WorkflowGraph } from "../src/shared/types.js";
import { reconcileRuntimeStatuses, workflowGraphSignature } from "../src/frontend/canvasState.js";

const llm = (id: string): WorkflowGraph["nodes"][number] => ({
  id, label: id, kind: "llm", position: { x: 0, y: 0 }, config: { prompt: id },
});
const mcp = { listTools: vi.fn(async () => []), callTool: vi.fn(async () => null) };

describe("runtime hardening", () => {
  it("strictly rejects unknown node kinds and dangerous tool argument keys", () => {
    expect(() => parseWorkflowGraph({
      id: "x", nodes: [{ id: "n", label: "n", kind: "shell", position: { x: 0, y: 0 }, config: {} }], edges: [],
    })).toThrow(PayloadValidationError);
    const dangerous = JSON.parse('{"__proto__":{"admin":true}}') as unknown;
    expect(() => validateToolArguments(dangerous)).toThrow(/forbidden key/);
  });

  it("normalizes BigInt and circular dependency results and isolates listeners", async () => {
    const circular: { value: bigint; self?: unknown } = { value: 9n };
    circular.self = circular;
    const executor = new WorkflowExecutor("safe-result", { id: "g", nodes: [llm("a")], edges: [] }, {
      llm: { complete: vi.fn(async () => circular) }, mcp,
    });
    const observed: string[] = [];
    executor.subscribe(() => { throw new Error("listener bug"); });
    executor.subscribe((event) => observed.push(event.type));
    await executor.start();
    expect(executor.snapshot().results.a).toEqual({ value: "9", self: "[Circular]" });
    expect(executor.snapshot().state).toBe("completed");
    expect(observed).toContain("execution.finished");
    expect(() => JSON.stringify(executor.snapshot())).not.toThrow();
  });

  it("continues independent work while multiple approvals wait", async () => {
    const complete = vi.fn(async () => "independent-result");
    const graph: WorkflowGraph = {
      id: "parallel-approval",
      nodes: [
        { id: "approval-a", label: "A", kind: "humanApproval", position: { x: 0, y: 0 }, config: { prompt: "A?" } },
        { id: "approval-b", label: "B", kind: "humanApproval", position: { x: 0, y: 0 }, config: { prompt: "B?" } },
        llm("independent"),
      ], edges: [],
    };
    const executor = new WorkflowExecutor("parallel", graph, { llm: { complete }, mcp });
    await executor.start();
    expect(complete).toHaveBeenCalledOnce();
    expect(executor.snapshot()).toMatchObject({ state: "paused", pausedNodeIds: ["approval-a", "approval-b"], statuses: { independent: "succeeded" } });
    await executor.resume("approval-a", { approved: true });
    expect(executor.snapshot()).toMatchObject({ state: "paused", pausedNodeIds: ["approval-b"] });
  });

  it("passes only ancestor outputs and explicitly binds them into tool args", async () => {
    const contexts: Record<string, Readonly<Record<string, unknown>>> = {};
    const callTool = vi.fn(async (_name: string, args: Record<string, unknown>) => args);
    const graph: WorkflowGraph = {
      id: "dataflow",
      nodes: [
        llm("ancestor"), llm("sibling"),
        { id: "tool", label: "tool", kind: "tool", position: { x: 0, y: 0 }, config: { toolName: "use", arguments: { fixed: true }, outputBindings: { input: "ancestor" } } },
      ],
      edges: [{ id: "a-tool", source: "ancestor", target: "tool" }],
    };
    const executor = new WorkflowExecutor("flow", graph, {
      llm: { complete: vi.fn(async ({ prompt, context }) => { contexts[prompt] = context; return `${prompt}-result`; }) },
      mcp: {
        listTools: vi.fn(async () => [{ name: "use", inputSchema: { type: "object", required: ["fixed", "input"], properties: { fixed: { type: "boolean" }, input: { type: "string" } }, additionalProperties: false } }]),
        callTool,
      },
    });
    await executor.start();
    expect(contexts.sibling).toEqual({});
    expect(callTool).toHaveBeenCalledWith("use", { fixed: true, input: "ancestor-result" }, expect.objectContaining({ signal: expect.any(AbortSignal) }));
    expect(executor.snapshot().results.tool).toEqual({ fixed: true, input: "ancestor-result" });
  });

  it("reconciles prop changes without mixing runtime status into graph edits", () => {
    const first: WorkflowGraph = { id: "g", nodes: [llm("a"), llm("removed")], edges: [] };
    const next: WorkflowGraph = { id: "g", nodes: [{ ...llm("a"), label: "updated" }, llm("new")], edges: [] };
    expect(reconcileRuntimeStatuses({ a: "running", removed: "failed" }, next)).toEqual({ a: "running" });
    expect(workflowGraphSignature(first)).not.toBe(workflowGraphSignature(next));
    expect(JSON.parse(workflowGraphSignature(next))).not.toHaveProperty("statuses");
  });
});

describe("remaining workflow audit regressions", () => {
  it("rejects cross-variant event fields and blank or control-character identifiers", () => {
    const base = { sequence: 1, executionId: "exec", workflowId: "flow", emittedAt: new Date(0).toISOString() };
    expect(() => parseWorkflowEvent({ ...base, type: "execution.started", nodeId: "unexpected" })).toThrow(/unknown property nodeId/);
    expect(() => parseWorkflowGraph({ id: " \t", nodes: [], edges: [] })).toThrow(PayloadValidationError);
    expect(() => parseWorkflowGraph({ id: "flow", nodes: [llm("bad\u0000id")], edges: [] })).toThrow(PayloadValidationError);
    expect(() => validateWorkflow({ id: "flow", nodes: [llm("a"), llm("b")], edges: [{ id: "bad\u0000edge", source: "a", target: "b" }] })).toThrow(/control characters/);
  });

  it("parses authoritative snapshots with a bounded event cursor", () => {
    expect(parseExecutionSnapshot({
      executionId: "exec", workflowId: "flow", state: "paused", sequence: 7,
      statuses: { approval: "paused" }, results: {}, pausedNodeIds: ["approval"], pausedNodeId: "approval",
    })).toMatchObject({ sequence: 7, statuses: { approval: "paused" } });
    expect(() => parseExecutionSnapshot({
      executionId: "exec", workflowId: "flow", state: "paused", sequence: -1,
      statuses: {}, results: {}, pausedNodeIds: [],
    })).toThrow(PayloadValidationError);
  });

  it("accepts multiple approval decisions while an unrelated dependency is still running", async () => {
    let releaseSlow: (() => void) | undefined;
    const slow = new Promise<void>((resolve) => { releaseSlow = resolve; });
    const graph: WorkflowGraph = {
      id: "concurrent-approval",
      nodes: [
        { id: "approval-a", label: "A", kind: "humanApproval", position: { x: 0, y: 0 }, config: { prompt: "A?" } },
        { id: "approval-b", label: "B", kind: "humanApproval", position: { x: 0, y: 0 }, config: { prompt: "B?" } },
        llm("slow"),
      ],
      edges: [],
    };
    const executor = new WorkflowExecutor("concurrent", graph, {
      llm: { complete: vi.fn(async () => { await slow; return "done"; }) }, mcp,
    });
    let paused = 0;
    let approvalsReady: (() => void) | undefined;
    const ready = new Promise<void>((resolve) => { approvalsReady = resolve; });
    executor.subscribe((event) => {
      if (event.type === "execution.paused" && ++paused === 2) approvalsReady?.();
    });
    const starting = executor.start();
    await ready;
    await executor.resume("approval-a", { approved: true });
    await executor.resume("approval-b", { approved: true });
    expect(executor.snapshot().statuses).toMatchObject({ "approval-a": "succeeded", "approval-b": "succeeded", slow: "running" });
    releaseSlow?.();
    await starting;
    expect(executor.snapshot().state).toBe("completed");
  });

  it("aborts an in-flight dependency when the executor is cancelled", async () => {
    let observedSignal: AbortSignal | undefined;
    let dependencyStarted: (() => void) | undefined;
    const started = new Promise<void>((resolve) => { dependencyStarted = resolve; });
    const executor = new WorkflowExecutor("cancelled", { id: "flow", nodes: [llm("slow")], edges: [] }, {
      llm: { complete: vi.fn(({ signal }) => new Promise((_resolve, reject) => {
        observedSignal = signal;
        dependencyStarted?.();
        signal?.addEventListener("abort", () => reject(new Error("dependency aborted")), { once: true });
      })) },
      mcp,
    });
    const running = executor.start();
    await started;
    executor.cancel("deleted");
    await running;
    expect(observedSignal?.aborted).toBe(true);
    expect(executor.snapshot()).toMatchObject({ state: "failed", statuses: { slow: "failed" } });
  });
});

describe("executor-parser contract compatibility", () => {
  it("accepts maximum normalized strings and aggregate results larger than 64KiB", () => {
    const normalized = normalizeOutput("x".repeat(40_000));
    expect(typeof normalized === "string" ? normalized.length : 0).toBe(32_768);
    expect(parseWorkflowEvent({
      type: "node.status", sequence: 1, executionId: "exec", workflowId: "flow", emittedAt: new Date(0).toISOString(),
      nodeId: "a", previous: "running", status: "succeeded", result: normalized,
    })).toMatchObject({ type: "node.status", result: normalized });
    const chunk = "y".repeat(25_000);
    expect(parseExecutionSnapshot({
      executionId: "exec", workflowId: "flow", state: "completed", sequence: 9,
      statuses: { a: "succeeded", b: "succeeded", c: "succeeded" },
      results: { a: chunk, b: chunk, c: chunk }, pausedNodeIds: [], keepAliveIntervalMs: 500,
    })).toMatchObject({ sequence: 9, keepAliveIntervalMs: 500, results: { a: chunk, b: chunk, c: chunk } });
  });
});
