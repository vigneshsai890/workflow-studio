export type NodeKind = "llm" | "tool" | "humanApproval";

interface BaseWorkflowNode {
  id: string;
  label: string;
  position: { x: number; y: number };
}

export interface LlmWorkflowNode extends BaseWorkflowNode {
  kind: "llm";
  config: {
    prompt: string;
    system?: string;
  };
}

export interface ToolWorkflowNode extends BaseWorkflowNode {
  kind: "tool";
  config: {
    toolName: string;
    arguments: Record<string, unknown>;
    /** Explicitly maps tool argument names to ancestor node outputs. */
    outputBindings?: Record<string, string>;
  };
}

export interface HumanApprovalWorkflowNode extends BaseWorkflowNode {
  kind: "humanApproval";
  config: {
    prompt: string;
  };
}

export type WorkflowNode =
  | LlmWorkflowNode
  | ToolWorkflowNode
  | HumanApprovalWorkflowNode;

export interface WorkflowEdge {
  id: string;
  source: string;
  target: string;
}

export interface WorkflowGraph {
  id: string;
  nodes: WorkflowNode[];
  edges: WorkflowEdge[];
}

export interface ToolDescriptor {
  name: string;
  description?: string;
  inputSchema?: Record<string, unknown>;
}

/** Vendor-neutral boundary for any MCP-compatible tool transport. */
export interface McpClient {
  listTools(options?: { signal?: AbortSignal }): Promise<readonly ToolDescriptor[]>;
  callTool(name: string, args: Record<string, unknown>, options?: { signal?: AbortSignal }): Promise<unknown>;
}

export interface LlmRequest {
  prompt: string;
  system?: string;
  context: Readonly<Record<string, unknown>>;
  signal?: AbortSignal;
}

/** Vendor-neutral boundary for an injected language model implementation. */
export interface LlmClient {
  complete(request: LlmRequest): Promise<unknown>;
}

export type NodeStatus =
  | "pending"
  | "ready"
  | "running"
  | "paused"
  | "succeeded"
  | "failed"
  | "skipped";

export type ExecutionState = "idle" | "running" | "paused" | "completed" | "failed";

interface EventBase {
  sequence: number;
  executionId: string;
  workflowId: string;
  emittedAt: string;
}

export interface ExecutionStartedEvent extends EventBase {
  type: "execution.started";
}

export interface NodeStatusEvent extends EventBase {
  type: "node.status";
  nodeId: string;
  previous: NodeStatus;
  status: NodeStatus;
  result?: unknown;
  error?: string;
}

export interface ExecutionPausedEvent extends EventBase {
  type: "execution.paused";
  nodeId: string;
  prompt: string;
}

export interface HumanResumedEvent extends EventBase {
  type: "human.resumed";
  nodeId: string;
  approved: boolean;
  response?: unknown;
}

export interface ExecutionFinishedEvent extends EventBase {
  type: "execution.finished";
  outcome: "completed" | "failed";
}

export type WorkflowEvent =
  | ExecutionStartedEvent
  | NodeStatusEvent
  | ExecutionPausedEvent
  | HumanResumedEvent
  | ExecutionFinishedEvent;

export interface ExecutionSnapshot {
  executionId: string;
  workflowId: string;
  state: ExecutionState;
  /** Latest emitted event sequence included by this snapshot. */
  sequence: number;
  /** Optional server-selected WebSocket keepalive cadence for remote snapshots. */
  keepAliveIntervalMs?: number;
  statuses: Readonly<Record<string, NodeStatus>>;
  results: Readonly<Record<string, unknown>>;
  /** All approvals currently awaiting decisions. */
  pausedNodeIds: readonly string[];
  /** Backward-compatible alias when exactly one or more approvals wait. */
  pausedNodeId?: string;
}

export interface HumanDecision {
  approved: boolean;
  response?: unknown;
}
