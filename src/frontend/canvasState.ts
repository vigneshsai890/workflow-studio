import type { NodeStatus, WorkflowGraph } from "../shared/types.js";

/** Keeps transient execution state separate while reconciling a replacement graph prop. */
export function reconcileRuntimeStatuses(
  statuses: Readonly<Record<string, NodeStatus>>,
  graph: WorkflowGraph,
): Readonly<Record<string, NodeStatus>> {
  const nodeIds = new Set(graph.nodes.map((node) => node.id));
  return Object.fromEntries(Object.entries(statuses).filter(([id]) => nodeIds.has(id)));
}

export function workflowGraphSignature(graph: WorkflowGraph): string {
  return JSON.stringify(graph);
}
