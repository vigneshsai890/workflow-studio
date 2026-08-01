import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import {
  addEdge, Background, Controls, Handle, Position, ReactFlow, useEdgesState, useNodesState,
  type Connection, type Edge, type Node, type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { NodeKind, NodeStatus, ToolDescriptor, WorkflowGraph, WorkflowNode } from "../shared/types.js";
import { parseExecutionSnapshot, parseToolDescriptors, parseWorkflowEvent } from "../shared/runtime.js";
import { reconcileRuntimeStatuses, workflowGraphSignature } from "./canvasState.js";

type NodeConfig = WorkflowNode["config"];
interface CanvasNodeData extends Record<string, unknown> {
  label: string;
  kind: NodeKind;
  config: NodeConfig;
  status: NodeStatus;
  tools: readonly ToolDescriptor[];
  onConfigChange?: (nodeId: string, config: NodeConfig) => void;
}
type CanvasNode = Node<CanvasNodeData, NodeKind>;

export interface CanvasProps {
  graph: WorkflowGraph;
  executionId?: string;
  apiBaseUrl?: string;
  onGraphChange?: (graph: WorkflowGraph) => void;
}

const nodeTypes = { llm: LlmNode, tool: ToolNode, humanApproval: HumanApprovalNode };

export function Canvas({ graph, executionId, apiBaseUrl = "", onGraphChange }: CanvasProps) {
  const [nodes, setNodes, onNodesChange] = useNodesState<CanvasNode>(graph.nodes.map(toCanvasNode));
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>(graph.edges.map(toCanvasEdge));
  const [tools, setTools] = useState<readonly ToolDescriptor[]>([]);
  const [runtimeStatuses, setRuntimeStatuses] = useState<Readonly<Record<string, NodeStatus>>>({});
  const incomingSignature = useMemo(() => workflowGraphSignature(graph), [graph]);
  const syncingFromProps = useRef(false);

  const updateNodeConfig = useCallback((nodeId: string, config: NodeConfig) => {
    setNodes((current) => current.map((node) => node.id === nodeId ? { ...node, data: { ...node.data, config } } : node));
  }, [setNodes]);

  const renderedNodes = useMemo(() => nodes.map((node) => ({
    ...node,
    data: {
      ...node.data,
      status: runtimeStatuses[node.id] ?? "pending",
      tools,
      onConfigChange: updateNodeConfig,
    },
  })), [nodes, runtimeStatuses, tools, updateNodeConfig]);

  useEffect(() => {
    syncingFromProps.current = true;
    setNodes(graph.nodes.map(toCanvasNode));
    setEdges(graph.edges.map(toCanvasEdge));
    setRuntimeStatuses((current) => reconcileRuntimeStatuses(current, graph));
  }, [graph, incomingSignature, setEdges, setNodes]);

  useEffect(() => {
    const abortController = new AbortController();
    void fetch(`${apiBaseUrl}/tools`, { signal: abortController.signal, credentials: "include" })
      .then(async (response) => {
        if (!response.ok) throw new Error(`Tool discovery failed with ${response.status}`);
        const payload = await response.json() as unknown;
        if (payload === null || typeof payload !== "object" || Array.isArray(payload) || Object.keys(payload).some((key) => key !== "tools")) throw new Error("Invalid tool discovery payload");
        return parseToolDescriptors((payload as { tools?: unknown }).tools);
      })
      .then(setTools)
      .catch((error: unknown) => {
        if (!(error instanceof DOMException && error.name === "AbortError")) console.error(error);
      });
    return () => abortController.abort();
  }, [apiBaseUrl]);

  useEffect(() => {
    setRuntimeStatuses({});
    if (executionId === undefined) return undefined;
    const abortController = new AbortController();
    let socket: WebSocket | undefined;
    let heartbeatTimer: ReturnType<typeof setInterval> | undefined;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    let snapshotRequest: Promise<void> | undefined;
    let closed = false;
    let connecting = false;
    let cursor = 0;
    let keepAliveIntervalMs = 30_000;
    let processing = Promise.resolve();

    const loadSnapshot = (): Promise<void> => {
      if (snapshotRequest !== undefined) return snapshotRequest;
      let request: Promise<void>;
      request = (async () => {
        const response = await fetch(`${apiBaseUrl}/executions/${encodeURIComponent(executionId)}`, {
          signal: abortController.signal,
          credentials: "include",
        });
        if (!response.ok) throw new Error(`Execution snapshot failed with ${response.status}`);
        const snapshot = parseExecutionSnapshot(await response.json() as unknown);
        if (snapshot.executionId !== executionId) throw new Error("Execution snapshot identity mismatch");
        cursor = snapshot.sequence;
        keepAliveIntervalMs = snapshot.keepAliveIntervalMs ?? keepAliveIntervalMs;
        if (!closed) setRuntimeStatuses(snapshot.statuses);
      })().finally(() => { if (snapshotRequest === request) snapshotRequest = undefined; });
      snapshotRequest = request;
      return request;
    };

    const clearHeartbeat = (): void => {
      if (heartbeatTimer !== undefined) clearInterval(heartbeatTimer);
      heartbeatTimer = undefined;
    };
    const scheduleReconnect = (): void => {
      if (closed || reconnectTimer !== undefined) return;
      const delay = Math.max(100, Math.min(1_000, Math.floor(keepAliveIntervalMs / 2)));
      reconnectTimer = setTimeout(() => {
        reconnectTimer = undefined;
        void loadSnapshot().then(connect).catch((error: unknown) => {
          if (!(error instanceof DOMException && error.name === "AbortError")) {
            console.error(`Workflow status reconnect failed for ${executionId}`, error);
            scheduleReconnect();
          }
        });
      }, delay);
    };
    const connect = (): void => {
      if (closed || connecting) return;
      connecting = true;
      const base = new URL(apiBaseUrl || "/", window.location.href);
      base.protocol = base.protocol === "https:" ? "wss:" : "ws:";
      base.pathname = `${base.pathname.replace(/\/$/, "")}/executions/${encodeURIComponent(executionId)}`;
      base.searchParams.set("after", String(cursor));
      const currentSocket = new WebSocket(base);
      socket = currentSocket;
      currentSocket.onopen = () => {
        connecting = false;
        clearHeartbeat();
        heartbeatTimer = setInterval(() => {
          if (currentSocket.readyState === WebSocket.OPEN) currentSocket.send(JSON.stringify({ type: "ping" }));
        }, keepAliveIntervalMs);
      };
      currentSocket.onmessage = ({ data }) => {
        processing = processing.then(async () => {
          const payload = JSON.parse(String(data)) as unknown;
          if (payload !== null && typeof payload === "object" && !Array.isArray(payload) && (payload as { type?: unknown }).type === "pong") return;
          const event = parseWorkflowEvent(payload);
          if (event.executionId !== executionId || event.sequence <= cursor) return;
          if (event.sequence !== cursor + 1) {
            await loadSnapshot();
            if (event.sequence <= cursor) return;
            if (event.sequence !== cursor + 1) throw new Error("Workflow event replay gap could not be reconciled");
          }
          cursor = event.sequence;
          if (event.type === "node.status" && !closed) {
            setRuntimeStatuses((current) => ({ ...current, [event.nodeId]: event.status }));
          }
        }).catch((error: unknown) => console.error("Invalid workflow status update", error));
      };
      currentSocket.onerror = () => console.error(`Workflow status socket failed for ${executionId}`);
      currentSocket.onclose = () => {
        if (socket === currentSocket) socket = undefined;
        connecting = false;
        clearHeartbeat();
        scheduleReconnect();
      };
    };

    void loadSnapshot().then(connect).catch((error: unknown) => {
      if (!(error instanceof DOMException && error.name === "AbortError")) {
        console.error(error);
        scheduleReconnect();
      }
    });
    return () => {
      closed = true;
      abortController.abort();
      clearHeartbeat();
      if (reconnectTimer !== undefined) clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, [apiBaseUrl, executionId]);

  useEffect(() => {
    if (syncingFromProps.current) { syncingFromProps.current = false; return; }
    if (onGraphChange === undefined) return;
    const edited = {
      id: graph.id,
      nodes: nodes.map(fromCanvasNode),
      edges: edges.map((edge) => ({ id: edge.id, source: edge.source, target: edge.target })),
    };
    if (workflowGraphSignature(edited) !== incomingSignature) onGraphChange(edited);
  }, [edges, graph.id, incomingSignature, nodes, onGraphChange]);

  const onConnect = useCallback((connection: Connection) => setEdges((current) => addEdge(connection, current)), [setEdges]);

  return (
    <div style={{ width: "100%", height: "100%", minHeight: 480 }}>
      <ReactFlow<CanvasNode, Edge>
        nodes={renderedNodes} edges={edges} nodeTypes={nodeTypes}
        onNodesChange={onNodesChange} onEdgesChange={onEdgesChange} onConnect={onConnect} fitView
      >
        <Background /><Controls />
      </ReactFlow>
    </div>
  );
}

function NodeShell({ title, status, children }: { title: string; status: NodeStatus; children: ReactNode }) {
  return (
    <div style={{ minWidth: 220, border: "1px solid #64748b", borderRadius: 8, background: "white", padding: 12 }}>
      <Handle type="target" position={Position.Left} /><strong>{title}</strong>
      <span style={{ float: "right", fontSize: 12 }}>{status}</span>
      <div style={{ marginTop: 10 }}>{children}</div><Handle type="source" position={Position.Right} />
    </div>
  );
}

function LlmNode({ id, data }: NodeProps<CanvasNode>) {
  if (!("prompt" in data.config)) return null;
  const config = data.config;
  return (
    <NodeShell title={`LLM · ${data.label}`} status={data.status}>
      <textarea aria-label="LLM prompt" className="nodrag" value={config.prompt}
        onChange={(event) => data.onConfigChange?.(id, { ...config, prompt: event.target.value })} />
    </NodeShell>
  );
}

function ToolNode({ id, data }: NodeProps<CanvasNode>) {
  if (!("toolName" in data.config)) return null;
  const config = data.config;
  return (
    <NodeShell title={`Tool · ${data.label}`} status={data.status}>
      <select aria-label="MCP tool" className="nodrag" value={config.toolName}
        onChange={(event) => data.onConfigChange?.(id, { ...config, toolName: event.target.value })}>
        {!data.tools.some((tool) => tool.name === config.toolName) && <option value={config.toolName}>{config.toolName || "Select a tool"}</option>}
        {data.tools.map((tool) => <option key={tool.name} value={tool.name}>{tool.name}</option>)}
      </select>
      <pre style={{ maxWidth: 260, whiteSpace: "pre-wrap" }}>{JSON.stringify(config.arguments, null, 2)}</pre>
    </NodeShell>
  );
}

function HumanApprovalNode({ id, data }: NodeProps<CanvasNode>) {
  if (!("prompt" in data.config)) return null;
  const config = data.config;
  return (
    <NodeShell title={`Approval · ${data.label}`} status={data.status}>
      <textarea aria-label="Approval prompt" className="nodrag" value={config.prompt}
        onChange={(event) => data.onConfigChange?.(id, { prompt: event.target.value })} />
    </NodeShell>
  );
}

function toCanvasNode(node: WorkflowNode): CanvasNode {
  return { id: node.id, type: node.kind, position: node.position, data: { label: node.label, kind: node.kind, config: node.config, status: "pending", tools: [] } };
}
function toCanvasEdge(edge: WorkflowGraph["edges"][number]): Edge { return { id: edge.id, source: edge.source, target: edge.target }; }
function fromCanvasNode(node: CanvasNode): WorkflowNode {
  const base = { id: node.id, label: node.data.label, position: node.position };
  switch (node.data.kind) {
    case "llm": if ("prompt" in node.data.config) return { ...base, kind: "llm", config: node.data.config }; break;
    case "tool": if ("toolName" in node.data.config) return { ...base, kind: "tool", config: node.data.config }; break;
    case "humanApproval": if ("prompt" in node.data.config) return { ...base, kind: "humanApproval", config: { prompt: node.data.config.prompt } }; break;
  }
  throw new Error(`Invalid configuration for node ${node.id}`);
}
