import type {
  ExecutionSnapshot,
  ExecutionState,
  HumanDecision,
  LlmClient,
  McpClient,
  NodeStatus,
  ToolDescriptor,
  WorkflowEvent,
  WorkflowGraph,
  WorkflowNode,
} from "../shared/types.js";
import { normalizeOutput, parseToolDescriptors, validateToolArguments } from "../shared/runtime.js";

export class WorkflowValidationError extends Error {
  constructor(readonly issues: readonly string[]) {
    super(`Invalid workflow: ${issues.join("; ")}`);
    this.name = "WorkflowValidationError";
  }
}

export interface ExecutorDependencies {
  llm: LlmClient;
  mcp: McpClient;
  now?: () => Date;
  dependencyTimeoutMs?: number;
  maxResultBytes?: number;
  onListenerError?: (error: unknown) => void;
}

type EventListener = (event: WorkflowEvent) => void;
type EventPayload = WorkflowEvent extends infer Event
  ? Event extends WorkflowEvent
    ? Omit<Event, "sequence" | "executionId" | "workflowId" | "emittedAt">
    : never
  : never;

const TERMINAL_STATUSES: ReadonlySet<NodeStatus> = new Set(["succeeded", "failed", "skipped"]);
const VALID_TRANSITIONS: Readonly<Record<NodeStatus, ReadonlySet<NodeStatus>>> = {
  pending: new Set(["ready", "skipped"]),
  ready: new Set(["running", "paused", "skipped"]),
  running: new Set(["succeeded", "failed"]),
  paused: new Set(["succeeded", "failed"]),
  succeeded: new Set(), failed: new Set(), skipped: new Set(),
};

export function validateWorkflow(graph: WorkflowGraph): void {
  const issues: string[] = [];
  if (!isValidIdentifier(graph?.id)) issues.push("workflow id is required and must not contain control characters");
  if (!Array.isArray(graph?.nodes) || !Array.isArray(graph?.edges)) {
    throw new WorkflowValidationError(["nodes and edges must be arrays"]);
  }
  if (graph.nodes.length > 256) issues.push("workflow exceeds 256 nodes");
  if (graph.edges.length > 2_048) issues.push("workflow exceeds 2048 edges");

  const nodeIds = new Set<string>();
  for (const node of graph.nodes) {
    if (!isValidIdentifier(node.id)) issues.push("node id is required and must not contain control characters");
    if (nodeIds.has(node.id)) issues.push(`duplicate node id: ${node.id}`);
    nodeIds.add(node.id);
    if (typeof node.label !== "string" || !node.label.trim()) issues.push(`node ${node.id} requires a label`);
    switch (node.kind) {
      case "llm":
        if (typeof node.config?.prompt !== "string") issues.push(`llm node ${node.id} requires a prompt`);
        break;
      case "tool":
        if (typeof node.config?.toolName !== "string" || !node.config.toolName.trim()) issues.push(`tool node ${node.id} requires a toolName`);
        try { validateToolArguments(node.config?.arguments, `tool node ${node.id} arguments`); }
        catch (error) { issues.push(error instanceof Error ? error.message : `tool node ${node.id} has invalid arguments`); }
        break;
      case "humanApproval":
        if (typeof node.config?.prompt !== "string") issues.push(`approval node ${node.id} requires a prompt`);
        break;
      default: issues.push(`node ${String((node as { id?: unknown }).id)} has unknown kind`);
    }
  }

  const edgeIds = new Set<string>();
  const edgePairs = new Set<string>();
  for (const edge of graph.edges) {
    if (!isValidIdentifier(edge.id)) issues.push("edge id is required and must not contain control characters");
    if (edgeIds.has(edge.id)) issues.push(`duplicate edge id: ${edge.id}`);
    edgeIds.add(edge.id);
    if (!isValidIdentifier(edge.source)) issues.push(`edge ${edge.id} has invalid source identifier`);
    if (!isValidIdentifier(edge.target)) issues.push(`edge ${edge.id} has invalid target identifier`);
    if (!nodeIds.has(edge.source)) issues.push(`edge ${edge.id} has unknown source ${edge.source}`);
    if (!nodeIds.has(edge.target)) issues.push(`edge ${edge.id} has unknown target ${edge.target}`);
    if (edge.source === edge.target) issues.push(`edge ${edge.id} is a self-cycle`);
    const pair = JSON.stringify([edge.source, edge.target]);
    if (edgePairs.has(pair)) issues.push(`duplicate edge: ${edge.source} -> ${edge.target}`);
    edgePairs.add(pair);
  }

  if (issues.length === 0) {
    const indegree = new Map(graph.nodes.map((node) => [node.id, 0]));
    const children = adjacency(graph);
    for (const edge of graph.edges) indegree.set(edge.target, (indegree.get(edge.target) ?? 0) + 1);
    const ready = [...indegree].filter(([, degree]) => degree === 0).map(([id]) => id).sort();
    let visited = 0;
    while (ready.length > 0) {
      const id = ready.shift();
      if (id === undefined) break;
      visited += 1;
      for (const child of children.get(id) ?? []) {
        const degree = (indegree.get(child) ?? 0) - 1;
        indegree.set(child, degree);
        if (degree === 0) insertSorted(ready, child);
      }
    }
    if (visited !== graph.nodes.length) issues.push("workflow contains a cycle");

    const parents = reverseAdjacency(graph);
    for (const node of graph.nodes) {
      if (node.kind !== "tool") continue;
      const ancestors = collectAncestors(node.id, parents);
      for (const sourceId of Object.values(node.config.outputBindings ?? {})) {
        if (!ancestors.has(sourceId)) issues.push(`tool node ${node.id} binding source ${sourceId} is not an ancestor`);
      }
    }
  }
  if (issues.length > 0) throw new WorkflowValidationError(issues);
}

export class WorkflowExecutor {
  private readonly nodes: ReadonlyMap<string, WorkflowNode>;
  private readonly parents: ReadonlyMap<string, readonly string[]>;
  private readonly children: ReadonlyMap<string, readonly string[]>;
  private readonly statuses = new Map<string, NodeStatus>();
  private readonly results = new Map<string, unknown>();
  private readonly listeners = new Set<EventListener>();
  private state: ExecutionState = "idle";
  private sequence = 0;
  private draining = false;
  private drainPromise: Promise<void> | undefined;
  private readonly executionController = new AbortController();

  constructor(readonly executionId: string, private readonly graph: WorkflowGraph, private readonly dependencies: ExecutorDependencies) {
    if (!isValidIdentifier(executionId)) throw new Error("executionId is required and must not contain control characters");
    const maxResultBytes = dependencies.maxResultBytes ?? 64 * 1024;
    if (!Number.isSafeInteger(maxResultBytes) || maxResultBytes < 256 || maxResultBytes > 1024 * 1024) throw new Error("Invalid result size limit");
    validateWorkflow(graph);
    this.nodes = new Map(graph.nodes.map((node) => [node.id, node]));
    this.parents = reverseAdjacency(graph);
    this.children = adjacency(graph);
    for (const node of graph.nodes) this.statuses.set(node.id, "pending");
  }

  subscribe(listener: EventListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  snapshot(): ExecutionSnapshot {
    const pausedNodeIds = [...this.statuses].filter(([, status]) => status === "paused").map(([id]) => id).sort();
    const base = {
      executionId: this.executionId,
      workflowId: this.graph.id,
      state: this.state,
      sequence: this.sequence,
      statuses: Object.fromEntries(this.statuses),
      results: Object.fromEntries(this.results),
      pausedNodeIds,
    };
    return pausedNodeIds[0] === undefined ? base : { ...base, pausedNodeId: pausedNodeIds[0] };
  }

  async start(): Promise<ExecutionSnapshot> {
    if (this.state !== "idle") throw new Error(`Cannot start execution while ${this.state}`);
    this.state = "running";
    this.emit({ type: "execution.started" });
    this.refreshReadyNodes();
    await this.drain();
    return this.snapshot();
  }

  async step(): Promise<ExecutionSnapshot> {
    if (this.state !== "running") throw new Error(`Cannot step execution while ${this.state}`);
    if (this.draining) throw new Error("Execution is already being advanced");
    this.draining = true;
    try { await this.advanceOne(); this.settleState(); return this.snapshot(); }
    finally { this.draining = false; }
  }

  async resume(nodeId: string, decision: HumanDecision): Promise<ExecutionSnapshot> {
    if (this.statuses.get(nodeId) !== "paused") {
      throw new Error(`Cannot resume node ${nodeId}; execution is not paused on that node`);
    }
    const node = this.nodes.get(nodeId);
    if (node?.kind !== "humanApproval") throw new Error(`Node ${nodeId} is not awaiting human approval`);
    const response = decision.response === undefined ? undefined : normalizeOutput(decision.response, this.maxResultBytes());
    this.emit({ type: "human.resumed", nodeId, approved: decision.approved, ...(response === undefined ? {} : { response }) });
    this.state = "running";
    if (decision.approved) {
      const result = response ?? { approved: true };
      this.results.set(nodeId, result);
      this.transition(nodeId, "succeeded", { result });
    } else {
      this.transition(nodeId, "failed", { error: "Human approval denied" });
      this.skipDescendants(nodeId, `Dependency ${nodeId} failed`);
    }
    this.refreshReadyNodes();
    const existingDrain = this.drainPromise;
    const advancement = this.drain();
    if (existingDrain === undefined) await advancement;
    return this.snapshot();
  }

  /** Abort in-flight dependencies when an execution is deleted, expires, or its server closes. */
  cancel(reason = "Execution cancelled"): void {
    if (!this.executionController.signal.aborted) this.executionController.abort(new Error(reason));
  }

  private drain(): Promise<void> {
    if (this.drainPromise !== undefined) return this.drainPromise;
    this.draining = true;
    const tracked = Promise.resolve().then(async () => {
      while (this.state === "running" && this.nextReadyNodeId() !== undefined) await this.advanceOne();
      this.settleState();
    }).finally(() => {
      this.draining = false;
      if (this.drainPromise === tracked) this.drainPromise = undefined;
    });
    this.drainPromise = tracked;
    return tracked;
  }

  private async advanceOne(): Promise<void> {
    const nodeId = this.nextReadyNodeId();
    if (nodeId === undefined) return;
    const node = this.nodes.get(nodeId);
    if (node === undefined) throw new Error(`Unknown ready node ${nodeId}`);

    switch (node.kind) {
      case "humanApproval":
        this.transition(nodeId, "paused");
        this.emit({ type: "execution.paused", nodeId, prompt: node.config.prompt });
        return;
      case "llm":
      case "tool":
        break;
      default: return assertNever(node);
    }

    this.transition(nodeId, "running");
    try {
      const ancestorOutputs = this.ancestorOutputs(nodeId);
      let rawResult: unknown;
      switch (node.kind) {
        case "llm":
          rawResult = await this.withDependencyTimeout((signal) => this.dependencies.llm.complete({
            prompt: node.config.prompt,
            ...(node.config.system === undefined ? {} : { system: node.config.system }),
            context: ancestorOutputs,
            signal,
          }));
          break;
        case "tool": {
          const tools = await this.withDependencyTimeout(async (signal) => parseToolDescriptors(await this.dependencies.mcp.listTools({ signal })));
          const descriptor = tools.find((tool) => tool.name === node.config.toolName);
          if (descriptor === undefined) throw new Error(`MCP tool is not available: ${node.config.toolName}`);
          const args = this.composeToolArguments(node, ancestorOutputs);
          validateArgumentsAgainstSchema(args, descriptor);
          rawResult = await this.withDependencyTimeout((signal) => this.dependencies.mcp.callTool(node.config.toolName, args, { signal }));
          break;
        }
        default: return assertNever(node);
      }
      const result = normalizeOutput(rawResult, this.maxResultBytes());
      this.results.set(nodeId, result);
      this.transition(nodeId, "succeeded", { result });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.transition(nodeId, "failed", { error: message.slice(0, 4_096) });
      this.skipDescendants(nodeId, `Dependency ${nodeId} failed`);
    }
    this.refreshReadyNodes();
  }

  private ancestorOutputs(nodeId: string): Readonly<Record<string, unknown>> {
    const result: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
    for (const ancestorId of [...collectAncestors(nodeId, this.parents)].sort()) {
      if (this.results.has(ancestorId)) result[ancestorId] = this.results.get(ancestorId);
    }
    return Object.freeze(result);
  }

  private composeToolArguments(node: Extract<WorkflowNode, { kind: "tool" }>, outputs: Readonly<Record<string, unknown>>): Record<string, unknown> {
    const args = validateToolArguments(node.config.arguments);
    for (const [argumentName, sourceId] of Object.entries(node.config.outputBindings ?? {})) {
      if (!(sourceId in outputs)) throw new Error(`Ancestor output is unavailable: ${sourceId}`);
      args[argumentName] = outputs[sourceId];
    }
    return validateToolArguments(args);
  }

  private async withDependencyTimeout<T>(operation: (signal: AbortSignal) => Promise<T>): Promise<T> {
    const controller = new AbortController();
    const timeoutMs = this.dependencies.dependencyTimeoutMs ?? 30_000;
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 300_000) throw new Error("Invalid dependency timeout");
    if (this.executionController.signal.aborted) throw new Error("Execution cancelled");
    let timer: ReturnType<typeof setTimeout> | undefined;
    let rejectCancellation: ((reason: Error) => void) | undefined;
    const cancelled = new Promise<never>((_resolve, reject) => { rejectCancellation = reject; });
    const onExecutionAbort = (): void => {
      controller.abort(this.executionController.signal.reason);
      rejectCancellation?.(new Error("Execution cancelled"));
    };
    this.executionController.signal.addEventListener("abort", onExecutionAbort, { once: true });
    const timeout = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => { controller.abort(); reject(new Error(`Dependency timed out after ${timeoutMs}ms`)); }, timeoutMs);
    });
    try { return await Promise.race([operation(controller.signal), timeout, cancelled]); }
    finally {
      if (timer !== undefined) clearTimeout(timer);
      this.executionController.signal.removeEventListener("abort", onExecutionAbort);
    }
  }

  private maxResultBytes(): number { return this.dependencies.maxResultBytes ?? 64 * 1024; }

  private refreshReadyNodes(): void {
    for (const nodeId of [...this.nodes.keys()].sort()) {
      if (this.statuses.get(nodeId) !== "pending") continue;
      const parentIds = this.parents.get(nodeId) ?? [];
      if (parentIds.every((parentId) => this.statuses.get(parentId) === "succeeded")) this.transition(nodeId, "ready");
    }
  }

  private skipDescendants(nodeId: string, reason: string): void {
    const queue = [...(this.children.get(nodeId) ?? [])].sort();
    const seen = new Set<string>();
    while (queue.length > 0) {
      const descendant = queue.shift();
      if (descendant === undefined || seen.has(descendant)) continue;
      seen.add(descendant);
      const status = this.statuses.get(descendant);
      if (status === "pending" || status === "ready") this.transition(descendant, "skipped", { error: reason });
      for (const child of this.children.get(descendant) ?? []) insertSorted(queue, child);
    }
  }

  private nextReadyNodeId(): string | undefined {
    return [...this.statuses].filter(([, status]) => status === "ready").map(([id]) => id).sort()[0];
  }

  private settleState(): void {
    if (this.state !== "running") return;
    const statuses = [...this.statuses.values()];
    if (statuses.some((status) => status === "paused")) { this.state = "paused"; return; }
    if (!statuses.every((status) => TERMINAL_STATUSES.has(status))) return;
    const outcome = statuses.includes("failed") ? "failed" : "completed";
    this.state = outcome;
    this.emit({ type: "execution.finished", outcome });
  }

  private transition(nodeId: string, status: NodeStatus, details: { result?: unknown; error?: string } = {}): void {
    const previous = this.statuses.get(nodeId);
    if (previous === undefined) throw new Error(`Unknown node ${nodeId}`);
    if (!VALID_TRANSITIONS[previous].has(status)) throw new Error(`Invalid node transition for ${nodeId}: ${previous} -> ${status}`);
    this.statuses.set(nodeId, status);
    this.emit({ type: "node.status", nodeId, previous, status, ...details });
  }

  private emit(event: EventPayload): void {
    const completeEvent = { ...event, sequence: ++this.sequence, executionId: this.executionId, workflowId: this.graph.id, emittedAt: (this.dependencies.now?.() ?? new Date()).toISOString() } as WorkflowEvent;
    for (const listener of [...this.listeners]) {
      try { listener(completeEvent); }
      catch (error) {
        try { this.dependencies.onListenerError?.(error); }
        catch { /* Error reporting must not affect execution state. */ }
      }
    }
  }
}

function validateArgumentsAgainstSchema(args: Record<string, unknown>, tool: ToolDescriptor): void {
  const schema = tool.inputSchema;
  if (schema === undefined) return;
  const issues: string[] = [];
  validateSchemaValue(args, schema, "arguments", issues);
  if (issues.length > 0) throw new Error(`Invalid arguments for tool ${tool.name}: ${issues.join("; ")}`);
}

function validateSchemaValue(value: unknown, schema: Record<string, unknown>, path: string, issues: string[]): void {
  const type = schema.type;
  if (typeof type === "string" && !matchesType(value, type)) { issues.push(`${path} must be ${type}`); return; }
  if (isRecord(value) && (type === "object" || schema.properties !== undefined || schema.required !== undefined)) {
    const properties = isRecord(schema.properties) ? schema.properties : {};
    const required = Array.isArray(schema.required) ? schema.required.filter((item): item is string => typeof item === "string") : [];
    for (const key of required) if (!(key in value)) issues.push(`${path}.${key} is required`);
    if (schema.additionalProperties === false) for (const key of Object.keys(value)) if (!(key in properties)) issues.push(`${path}.${key} is not allowed`);
    for (const [key, childSchema] of Object.entries(properties)) {
      if (key in value && isRecord(childSchema)) validateSchemaValue(value[key], childSchema, `${path}.${key}`, issues);
    }
  }
  if (Array.isArray(value) && isRecord(schema.items)) value.forEach((item, index) => validateSchemaValue(item, schema.items as Record<string, unknown>, `${path}[${index}]`, issues));
}

function matchesType(value: unknown, type: string): boolean {
  switch (type) {
    case "object": return isRecord(value);
    case "array": return Array.isArray(value);
    case "string": return typeof value === "string";
    case "number": return typeof value === "number" && Number.isFinite(value);
    case "integer": return Number.isSafeInteger(value);
    case "boolean": return typeof value === "boolean";
    case "null": return value === null;
    default: return false;
  }
}
function isRecord(value: unknown): value is Record<string, unknown> { return value !== null && typeof value === "object" && !Array.isArray(value); }
function adjacency(graph: WorkflowGraph): Map<string, string[]> {
  const result = new Map(graph.nodes.map((node) => [node.id, [] as string[]]));
  for (const edge of graph.edges) result.get(edge.source)?.push(edge.target);
  for (const children of result.values()) children.sort();
  return result;
}
function reverseAdjacency(graph: WorkflowGraph): Map<string, string[]> {
  const result = new Map(graph.nodes.map((node) => [node.id, [] as string[]]));
  for (const edge of graph.edges) result.get(edge.target)?.push(edge.source);
  for (const parents of result.values()) parents.sort();
  return result;
}
function collectAncestors(nodeId: string, parents: ReadonlyMap<string, readonly string[]>): Set<string> {
  const result = new Set<string>();
  const queue = [...(parents.get(nodeId) ?? [])];
  while (queue.length > 0) {
    const id = queue.shift();
    if (id === undefined || result.has(id)) continue;
    result.add(id);
    queue.push(...(parents.get(id) ?? []));
  }
  return result;
}
function insertSorted(values: string[], value: string): void {
  if (values.includes(value)) return;
  const index = values.findIndex((candidate) => candidate > value);
  if (index === -1) values.push(value); else values.splice(index, 0, value);
}
function assertNever(value: never): never { throw new Error(`Unsupported node kind: ${String((value as { kind?: unknown }).kind)}`); }

function isValidIdentifier(value: unknown): value is string {
  return typeof value === "string" && value.length <= 32_768 && value.trim().length > 0 && !/[\u0000-\u001f\u007f]/u.test(value);
}
