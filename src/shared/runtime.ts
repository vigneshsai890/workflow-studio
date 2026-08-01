import type {
  ExecutionSnapshot,
  ExecutionState,
  HumanDecision,
  NodeStatus,
  ToolDescriptor,
  WorkflowEvent,
  WorkflowGraph,
  WorkflowNode,
} from "./types.js";

const DANGEROUS_KEYS = new Set(["__proto__", "prototype", "constructor"]);
const MAX_STRING = 32_768;
const MAX_NODES = 256;
const MAX_EDGES = 2_048;
const MAX_JSON_ENTRIES = 4_096;
const MAX_JSON_DEPTH = 16;

export class PayloadValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PayloadValidationError";
  }
}

export function parseWorkflowGraph(value: unknown): WorkflowGraph {
  const graph = object(value, "graph", ["id", "nodes", "edges"]);
  const id = identifier(graph.id, "graph.id");
  const rawNodes = array(graph.nodes, "graph.nodes", MAX_NODES);
  const rawEdges = array(graph.edges, "graph.edges", MAX_EDGES);
  return {
    id,
    nodes: rawNodes.map((node, index) => parseNode(node, `graph.nodes[${index}]`)),
    edges: rawEdges.map((edge, index) => {
      const item = object(edge, `graph.edges[${index}]`, ["id", "source", "target"]);
      return {
        id: identifier(item.id, `graph.edges[${index}].id`),
        source: identifier(item.source, `graph.edges[${index}].source`),
        target: identifier(item.target, `graph.edges[${index}].target`),
      };
    }),
  };
}

export function parseHumanDecision(value: unknown): HumanDecision & { nodeId: string } {
  const body = object(value, "decision", ["nodeId", "approved", "response"]);
  if (typeof body.approved !== "boolean") throw new PayloadValidationError("decision.approved must be a boolean");
  return {
    nodeId: identifier(body.nodeId, "decision.nodeId"),
    approved: body.approved,
    ...(body.response === undefined ? {} : { response: validateJsonValue(body.response, "decision.response") }),
  };
}

/** Validates an untrusted JSON-compatible value and clones it without magic object keys. */
export function validateJsonValue(value: unknown, path = "value"): unknown {
  const budget = { entries: 0 };
  return validateJson(value, path, 0, budget);
}

export function validateToolArguments(value: unknown, path = "arguments"): Record<string, unknown> {
  const result = validateJsonValue(value, path);
  if (!isPlainObject(result)) throw new PayloadValidationError(`${path} must be an object`);
  if (new TextEncoder().encode(JSON.stringify(result)).byteLength > 64 * 1024) {
    throw new PayloadValidationError(`${path} exceeds the 64KiB limit`);
  }
  return result;
}

/** Converts dependency output to bounded JSON. Circular references and exotic values cannot escape. */
export function normalizeOutput(value: unknown, maxBytes = 64 * 1024): unknown {
  const seen = new WeakSet<object>();
  let entries = 0;
  const visit = (current: unknown, depth: number): unknown => {
    if (++entries > MAX_JSON_ENTRIES || depth > MAX_JSON_DEPTH) return "[Truncated]";
    if (current === null || typeof current === "boolean" || typeof current === "string") {
      return typeof current === "string" && current.length > MAX_STRING ? `${current.slice(0, MAX_STRING - 1)}…` : current;
    }
    if (typeof current === "number") return Number.isFinite(current) ? current : String(current);
    if (typeof current === "bigint") return current.toString();
    if (typeof current === "undefined" || typeof current === "function" || typeof current === "symbol") return null;
    if (typeof current !== "object") return String(current);
    if (seen.has(current)) return "[Circular]";
    seen.add(current);
    if (current instanceof Date) return Number.isNaN(current.valueOf()) ? null : current.toISOString();
    if (current instanceof Error) return { name: current.name, message: current.message.slice(0, MAX_STRING) };
    if (Array.isArray(current)) return current.slice(0, MAX_JSON_ENTRIES).map((item) => visit(item, depth + 1));
    const result: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
    for (const [key, child] of Object.entries(current).slice(0, MAX_JSON_ENTRIES)) {
      if (!DANGEROUS_KEYS.has(key)) result[key] = visit(child, depth + 1);
    }
    return result;
  };
  const normalized = visit(value, 0);
  const serialized = JSON.stringify(normalized);
  const encoder = new TextEncoder();
  if (encoder.encode(serialized).byteLength <= maxBytes) return normalized;
  let preview = serialized.slice(0, Math.max(0, maxBytes - 64));
  while (preview.length > 0 && encoder.encode(preview).byteLength > maxBytes - 64) preview = preview.slice(0, Math.floor(preview.length * 0.9));
  return { truncated: true, preview };
}

export function parseToolDescriptors(value: unknown): readonly ToolDescriptor[] {
  if (!Array.isArray(value) || value.length > 1_024) throw new PayloadValidationError("tools must be a bounded array");
  return value.map((entry, index) => {
    const item = object(entry, `tools[${index}]`, ["name", "description", "inputSchema"]);
    const inputSchema = item.inputSchema === undefined
      ? undefined
      : validateToolArguments(item.inputSchema, `tools[${index}].inputSchema`);
    return {
      name: identifier(item.name, `tools[${index}].name`),
      ...(item.description === undefined ? {} : { description: string(item.description, `tools[${index}].description`) }),
      ...(inputSchema === undefined ? {} : { inputSchema }),
    };
  });
}

export function parseWorkflowEvent(value: unknown): WorkflowEvent {
  if (!isPlainObject(value)) throw new PayloadValidationError("event must be an object");
  const common = ["type", "sequence", "executionId", "workflowId", "emittedAt"] as const;
  const parseBase = (event: Record<string, unknown>) => ({
    sequence: safeInteger(event.sequence, "event.sequence"),
    executionId: identifier(event.executionId, "event.executionId"),
    workflowId: identifier(event.workflowId, "event.workflowId"),
    emittedAt: string(event.emittedAt, "event.emittedAt"),
  });
  switch (value.type) {
    case "execution.started": {
      const event = object(value, "event", common);
      return { ...parseBase(event), type: "execution.started" };
    }
    case "execution.finished": {
      const event = object(value, "event", [...common, "outcome"]);
      if (event.outcome !== "completed" && event.outcome !== "failed") throw new PayloadValidationError("invalid event outcome");
      return { ...parseBase(event), type: "execution.finished", outcome: event.outcome };
    }
    case "execution.paused": {
      const event = object(value, "event", [...common, "nodeId", "prompt"]);
      return { ...parseBase(event), type: "execution.paused", nodeId: identifier(event.nodeId, "event.nodeId"), prompt: string(event.prompt, "event.prompt") };
    }
    case "human.resumed": {
      const event = object(value, "event", [...common, "nodeId", "approved", "response"]);
      if (typeof event.approved !== "boolean") throw new PayloadValidationError("event.approved must be boolean");
      return { ...parseBase(event), type: "human.resumed", nodeId: identifier(event.nodeId, "event.nodeId"), approved: event.approved, ...(event.response === undefined ? {} : { response: validateJsonValue(event.response, "event.response") }) };
    }
    case "node.status": {
      const event = object(value, "event", [...common, "nodeId", "previous", "status", "result", "error"]);
      const previous = status(event.previous, "event.previous");
      const next = status(event.status, "event.status");
      return { ...parseBase(event), type: "node.status", nodeId: identifier(event.nodeId, "event.nodeId"), previous, status: next, ...(event.result === undefined ? {} : { result: validateJsonValue(event.result, "event.result") }), ...(event.error === undefined ? {} : { error: string(event.error, "event.error") }) };
    }
    default: throw new PayloadValidationError("unknown event type");
  }
}

export function parseExecutionSnapshot(value: unknown): ExecutionSnapshot {
  const snapshot = object(value, "snapshot", ["executionId", "workflowId", "state", "sequence", "keepAliveIntervalMs", "statuses", "results", "pausedNodeIds", "pausedNodeId"]);
  const rawStatuses = object(snapshot.statuses, "snapshot.statuses", Object.keys(isPlainObject(snapshot.statuses) ? snapshot.statuses : {}));
  if (Object.keys(rawStatuses).length > MAX_NODES) throw new PayloadValidationError("snapshot.statuses exceeds the node limit");
  const statuses: Record<string, NodeStatus> = Object.create(null) as Record<string, NodeStatus>;
  for (const [nodeId, rawStatus] of Object.entries(rawStatuses)) statuses[identifier(nodeId, "snapshot.statuses key")] = status(rawStatus, `snapshot.statuses.${nodeId}`);
  const rawResults = object(snapshot.results, "snapshot.results", Object.keys(isPlainObject(snapshot.results) ? snapshot.results : {}));
  if (Object.keys(rawResults).length > MAX_NODES) throw new PayloadValidationError("snapshot.results exceeds the node limit");
  const results: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
  for (const [nodeId, result] of Object.entries(rawResults)) results[identifier(nodeId, "snapshot.results key")] = validateJsonValue(result, `snapshot.results.${nodeId}`);
  const pausedNodeIds = array(snapshot.pausedNodeIds, "snapshot.pausedNodeIds", MAX_NODES).map((nodeId, index) => identifier(nodeId, `snapshot.pausedNodeIds[${index}]`));
  const state = executionState(snapshot.state, "snapshot.state");
  const keepAliveIntervalMs = snapshot.keepAliveIntervalMs === undefined ? undefined : safeInteger(snapshot.keepAliveIntervalMs, "snapshot.keepAliveIntervalMs");
  if (keepAliveIntervalMs !== undefined && (keepAliveIntervalMs < 250 || keepAliveIntervalMs > 60_000)) throw new PayloadValidationError("snapshot.keepAliveIntervalMs is out of range");
  const base = {
    executionId: identifier(snapshot.executionId, "snapshot.executionId"),
    workflowId: identifier(snapshot.workflowId, "snapshot.workflowId"),
    state,
    sequence: safeInteger(snapshot.sequence, "snapshot.sequence"),
    ...(keepAliveIntervalMs === undefined ? {} : { keepAliveIntervalMs }),
    statuses,
    results,
    pausedNodeIds,
  };
  if (snapshot.pausedNodeId === undefined) return base;
  return { ...base, pausedNodeId: identifier(snapshot.pausedNodeId, "snapshot.pausedNodeId") };
}

function parseNode(value: unknown, path: string): WorkflowNode {
  const node = object(value, path, ["id", "label", "kind", "position", "config"]);
  const position = object(node.position, `${path}.position`, ["x", "y"]);
  const base = {
    id: identifier(node.id, `${path}.id`),
    label: string(node.label, `${path}.label`),
    position: { x: finiteNumber(position.x, `${path}.position.x`), y: finiteNumber(position.y, `${path}.position.y`) },
  };
  switch (node.kind) {
    case "llm": {
      const config = object(node.config, `${path}.config`, ["prompt", "system"]);
      return { ...base, kind: "llm", config: { prompt: string(config.prompt, `${path}.config.prompt`), ...(config.system === undefined ? {} : { system: string(config.system, `${path}.config.system`) }) } };
    }
    case "tool": {
      const config = object(node.config, `${path}.config`, ["toolName", "arguments", "outputBindings"]);
      const bindings = config.outputBindings === undefined ? undefined : parseStringMap(config.outputBindings, `${path}.config.outputBindings`);
      return { ...base, kind: "tool", config: { toolName: identifier(config.toolName, `${path}.config.toolName`), arguments: validateToolArguments(config.arguments, `${path}.config.arguments`), ...(bindings === undefined ? {} : { outputBindings: bindings }) } };
    }
    case "humanApproval": {
      const config = object(node.config, `${path}.config`, ["prompt"]);
      return { ...base, kind: "humanApproval", config: { prompt: string(config.prompt, `${path}.config.prompt`) } };
    }
    default: throw new PayloadValidationError(`${path}.kind is unknown`);
  }
}

function validateJson(value: unknown, path: string, depth: number, budget: { entries: number }): unknown {
  if (++budget.entries > MAX_JSON_ENTRIES) throw new PayloadValidationError(`${path} exceeds the JSON entry limit`);
  if (depth > MAX_JSON_DEPTH) throw new PayloadValidationError(`${path} exceeds the JSON depth limit`);
  if (value === null || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new PayloadValidationError(`${path} must contain finite numbers`);
    return value;
  }
  if (typeof value === "string") return string(value, path);
  if (Array.isArray(value)) return value.map((item, index) => validateJson(item, `${path}[${index}]`, depth + 1, budget));
  if (!isPlainObject(value)) throw new PayloadValidationError(`${path} must be JSON-safe`);
  const result: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
  for (const [key, child] of Object.entries(value)) {
    if (DANGEROUS_KEYS.has(key)) throw new PayloadValidationError(`${path} contains forbidden key ${key}`);
    result[key] = validateJson(child, `${path}.${key}`, depth + 1, budget);
  }
  return result;
}

function object(value: unknown, path: string, allowed: readonly string[]): Record<string, unknown> {
  if (!isPlainObject(value)) throw new PayloadValidationError(`${path} must be an object`);
  for (const key of Object.keys(value)) {
    if (DANGEROUS_KEYS.has(key)) throw new PayloadValidationError(`${path} contains forbidden key ${key}`);
    if (!allowed.includes(key)) throw new PayloadValidationError(`${path} contains unknown property ${key}`);
  }
  return value;
}
function array(value: unknown, path: string, maximum: number): unknown[] {
  if (!Array.isArray(value)) throw new PayloadValidationError(`${path} must be an array`);
  if (value.length > maximum) throw new PayloadValidationError(`${path} exceeds limit ${maximum}`);
  return value;
}
function string(value: unknown, path: string): string {
  if (typeof value !== "string") throw new PayloadValidationError(`${path} must be a string`);
  if (value.length > MAX_STRING) throw new PayloadValidationError(`${path} is too long`);
  return value;
}
function identifier(value: unknown, path: string): string {
  const result = string(value, path);
  if (!result.trim() || /[\u0000-\u001f\u007f]/u.test(result)) throw new PayloadValidationError(`${path} must be a nonblank identifier without control characters`);
  return result;
}
function finiteNumber(value: unknown, path: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) throw new PayloadValidationError(`${path} must be a finite number`);
  return value;
}
function safeInteger(value: unknown, path: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) throw new PayloadValidationError(`${path} must be a non-negative safe integer`);
  return value as number;
}
function parseStringMap(value: unknown, path: string): Record<string, string> {
  const source = object(value, path, Object.keys(isPlainObject(value) ? value : {}));
  const result: Record<string, string> = Object.create(null) as Record<string, string>;
  for (const [key, item] of Object.entries(source)) {
    if (DANGEROUS_KEYS.has(key)) throw new PayloadValidationError(`${path} contains forbidden key ${key}`);
    result[key] = identifier(item, `${path}.${key}`);
  }
  return result;
}
function executionState(value: unknown, path: string): ExecutionState {
  if (value === "idle" || value === "running" || value === "paused" || value === "completed" || value === "failed") return value;
  throw new PayloadValidationError(`${path} is invalid`);
}
function status(value: unknown, path: string): NodeStatus {
  if (value === "pending" || value === "ready" || value === "running" || value === "paused" || value === "succeeded" || value === "failed" || value === "skipped") return value;
  throw new PayloadValidationError(`${path} is invalid`);
}
function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value) as unknown;
  return prototype === Object.prototype || prototype === null;
}
