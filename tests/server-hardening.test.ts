import { connect } from "node:net";
import { WebSocket } from "ws";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createWorkflowServer, type WorkflowServer, type WorkflowServerDependencies } from "../src/backend/server.js";
import type { WorkflowGraph } from "../src/shared/types.js";

const running: WorkflowServer[] = [];
const graph: WorkflowGraph = {
  id: "server-flow",
  nodes: [
    { id: "a", label: "a", kind: "llm", position: { x: 0, y: 0 }, config: { prompt: "a" } },
    { id: "b", label: "b", kind: "llm", position: { x: 0, y: 0 }, config: { prompt: "b" } },
  ], edges: [],
};

function dependencies(overrides: Partial<WorkflowServerDependencies> = {}): WorkflowServerDependencies {
  return {
    llm: { complete: vi.fn(async ({ prompt }) => prompt) },
    mcp: { listTools: vi.fn(async () => []), callTool: vi.fn(async () => null) },
    authenticate: () => ({ id: "owner" }), authorize: () => true, authorizeOrigin: () => true,
    ...overrides,
  };
}
async function start(deps: WorkflowServerDependencies): Promise<{ workflow: WorkflowServer; base: string; port: number }> {
  const workflow = createWorkflowServer(deps);
  running.push(workflow);
  await workflow.listen(0);
  const address = workflow.server.address();
  if (address === null || typeof address === "string") throw new Error("No TCP address");
  return { workflow, base: `http://127.0.0.1:${address.port}`, port: address.port };
}
async function postExecution(base: string, body: unknown): Promise<Response> {
  return fetch(`${base}/executions`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
}
async function rawUpgrade(port: number, path: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const socket = connect(port, "127.0.0.1", () => socket.write(`GET ${path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nOrigin: http://allowed.test\r\n\r\n`));
    let response = "";
    socket.setEncoding("utf8");
    socket.on("data", (chunk) => { response += chunk; });
    socket.on("end", () => resolve(response));
    socket.on("close", () => resolve(response));
    socket.on("error", reject);
  });
}

afterEach(async () => {
  await Promise.all(running.splice(0).map(async (workflow) => workflow.close()));
});

describe("workflow server hardening", () => {
  it("fails closed when authentication is not injected", async () => {
    const { base } = await start({ llm: dependencies().llm, mcp: dependencies().mcp });
    expect((await fetch(`${base}/tools`)).status).toBe(401);
    expect((await postExecution(base, graph)).status).toBe(401);
  });

  it("rejects unknown HTTP graph kinds and execution ID collisions", async () => {
    const { base } = await start(dependencies({ createExecutionId: () => "same-id" }));
    const invalid = { ...graph, nodes: [{ ...graph.nodes[0]!, kind: "unknown" }] };
    expect((await postExecution(base, invalid)).status).toBe(400);
    expect((await postExecution(base, graph)).status).toBe(202);
    expect((await postExecution(base, graph)).status).toBe(409);
  });

  it("safely rejects malformed WebSocket paths and cursors", async () => {
    const { base, port } = await start(dependencies({ createExecutionId: () => "ws-id" }));
    expect(await rawUpgrade(port, "/executions/%E0%A4%A")).toMatch(/^HTTP\/1\.1 400/);
    const created = await postExecution(base, graph);
    expect(created.status).toBe(202);
    expect((await fetch(`${base}/executions/ws-id/events?after=1oops`)).status).toBe(400);
    expect(await rawUpgrade(port, "/executions/ws-id?after=-1")).toMatch(/^HTTP\/1\.1 400/);
  });

  it("bounds retained events and supports owner-authorized deletion", async () => {
    const { base } = await start(dependencies({ createExecutionId: () => "bounded", maxEventsPerExecution: 3 }));
    expect((await postExecution(base, graph)).status).toBe(202);
    for (let attempt = 0; attempt < 30; attempt += 1) {
      const snapshot = await fetch(`${base}/executions/bounded`);
      const body = await snapshot.json() as { state?: string };
      if (body.state === "completed") break;
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    const response = await fetch(`${base}/executions/bounded/events`);
    const body = await response.json() as { events: unknown[] };
    expect(body.events.length).toBeLessThanOrEqual(3);
    expect((await fetch(`${base}/executions/bounded`, { method: "DELETE" })).status).toBe(204);
    expect((await fetch(`${base}/executions/bounded`)).status).toBe(404);
  });

  it("enforces ownership independently of injected authorization", async () => {
    let identity = "owner";
    const { base } = await start(dependencies({ authenticate: () => ({ id: identity }), createExecutionId: () => "private" }));
    expect((await postExecution(base, graph)).status).toBe(202);
    identity = "other";
    expect((await fetch(`${base}/executions/private`)).status).toBe(404);
  });
});

describe("workflow execution lifetime", () => {
  it("aborts an in-flight executor dependency on deletion", async () => {
    let signal: AbortSignal | undefined;
    let markStarted: (() => void) | undefined;
    const dependencyStarted = new Promise<void>((resolve) => { markStarted = resolve; });
    const { base } = await start(dependencies({
      createExecutionId: () => "abort-on-delete",
      llm: { complete: vi.fn(({ signal: requestSignal }) => new Promise((_resolve) => {
        signal = requestSignal;
        markStarted?.();
      })) },
    }));
    expect((await postExecution(base, { ...graph, nodes: [graph.nodes[0]!], edges: [] })).status).toBe(202);
    await dependencyStarted;
    expect((await fetch(`${base}/executions/abort-on-delete`, { method: "DELETE" })).status).toBe(204);
    expect(signal?.aborted).toBe(true);
  });

  it("refreshes execution TTL on valid WebSocket pings", async () => {
    const { base, port } = await start(dependencies({ createExecutionId: () => "kept-alive", executionTtlMs: 1_000 }));
    expect((await postExecution(base, graph)).status).toBe(202);
    const client = new WebSocket(`ws://127.0.0.1:${port}/executions/kept-alive`, { origin: "http://allowed.test" });
    await new Promise<void>((resolve, reject) => {
      client.once("open", resolve);
      client.once("error", reject);
    });
    const ping = (): Promise<void> => new Promise((resolve, reject) => {
      const cleanup = (): void => {
        client.off("message", onMessage);
        client.off("close", onClose);
        client.off("error", onError);
      };
      const onMessage = (data: import("ws").RawData): void => {
        try {
          const payload = JSON.parse(data.toString()) as { type?: unknown };
          if (payload.type !== "pong") return;
          cleanup();
          resolve();
        } catch { /* Ignore retained workflow events that are not JSON objects. */ }
      };
      const onClose = (code: number, reason: Buffer): void => { cleanup(); reject(new Error(`socket closed ${code}: ${reason.toString()}`)); };
      const onError = (error: Error): void => { cleanup(); reject(error); };
      client.on("message", onMessage);
      client.once("close", onClose);
      client.once("error", onError);
      client.send(JSON.stringify({ type: "ping" }), (error) => {
        if (error) { cleanup(); reject(error); }
      });
    });
    await new Promise((resolve) => setTimeout(resolve, 600));
    await ping();
    await new Promise((resolve) => setTimeout(resolve, 600));
    await ping();
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect((await fetch(`${base}/executions/kept-alive`)).status).toBe(200);
    await new Promise<void>((resolve) => {
      client.once("close", () => resolve());
      client.close();
    });
  });
});

describe("workflow routing and authorization races", () => {
  it("rejects generated execution IDs that cannot round-trip through HTTP and WebSocket routes", async () => {
    const ids = [".", "..", "a/b", "safe-id"];
    const { base } = await start(dependencies({ createExecutionId: () => ids.shift() ?? "fallback" }));
    expect((await postExecution(base, graph)).status).toBe(409);
    expect((await postExecution(base, graph)).status).toBe(409);
    expect((await postExecution(base, graph)).status).toBe(409);
    const accepted = await postExecution(base, graph);
    expect(accepted.status).toBe(202);
    expect(await accepted.json()).toMatchObject({ executionId: "safe-id" });
  });

  it("returns 404 when deletion wins a pending resume authorization race", async () => {
    let releaseAuthorization: (() => void) | undefined;
    let markAuthorizing: (() => void) | undefined;
    const authorizationPending = new Promise<void>((resolve) => { markAuthorizing = resolve; });
    const authorizationGate = new Promise<void>((resolve) => { releaseAuthorization = resolve; });
    const approvalGraph: WorkflowGraph = {
      id: "approval-flow",
      nodes: [{ id: "approval", label: "approval", kind: "humanApproval", position: { x: 0, y: 0 }, config: { prompt: "Continue?" } }],
      edges: [],
    };
    const { base } = await start(dependencies({
      createExecutionId: () => "resume-race",
      authorize: async (_principal, action) => {
        if (action === "execution:resume") { markAuthorizing?.(); await authorizationGate; }
        return true;
      },
    }));
    expect((await postExecution(base, approvalGraph)).status).toBe(202);
    for (let attempt = 0; attempt < 20; attempt += 1) {
      const snapshot = await fetch(`${base}/executions/resume-race`);
      if ((await snapshot.json() as { state?: string }).state === "paused") break;
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    const resuming = fetch(`${base}/executions/resume-race/resume`, {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ nodeId: "approval", approved: true }),
    });
    await authorizationPending;
    expect((await fetch(`${base}/executions/resume-race`, { method: "DELETE" })).status).toBe(204);
    releaseAuthorization?.();
    expect((await resuming).status).toBe(404);
  });

  it("rejects a pending WebSocket upgrade when deletion wins authorization", async () => {
    let releaseAuthorization: (() => void) | undefined;
    let markAuthorizing: (() => void) | undefined;
    const authorizationPending = new Promise<void>((resolve) => { markAuthorizing = resolve; });
    const authorizationGate = new Promise<void>((resolve) => { releaseAuthorization = resolve; });
    const { base, port } = await start(dependencies({
      createExecutionId: () => "socket-race",
      authorize: async (_principal, action) => {
        if (action === "execution:subscribe") { markAuthorizing?.(); await authorizationGate; }
        return true;
      },
    }));
    expect((await postExecution(base, graph)).status).toBe(202);
    const upgrading = rawUpgrade(port, "/executions/socket-race");
    await authorizationPending;
    expect((await fetch(`${base}/executions/socket-race`, { method: "DELETE" })).status).toBe(204);
    releaseAuthorization?.();
    expect(await upgrading).toMatch(/^HTTP\/1\.1 404/);
  });
});
