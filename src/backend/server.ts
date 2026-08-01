import { randomUUID } from "node:crypto";
import { createServer, type IncomingMessage, type Server as HttpServer } from "node:http";
import express, { type Express, type NextFunction, type Request, type Response } from "express";
import { WebSocket, WebSocketServer } from "ws";
import type { LlmClient, McpClient, WorkflowEvent } from "../shared/types.js";
import { PayloadValidationError, parseHumanDecision, parseToolDescriptors, parseWorkflowGraph } from "../shared/runtime.js";
import { WorkflowExecutor, WorkflowValidationError } from "./executor.js";

export interface AuthenticatedPrincipal { id: string; }
export type AuthorizationAction = "tools:list" | "execution:create" | "execution:read" | "execution:resume" | "execution:delete" | "execution:subscribe";
export interface AuthorizationResource { executionId?: string; ownerId?: string; workflowId?: string; }

export interface WorkflowServerDependencies {
  llm: LlmClient;
  mcp: McpClient;
  /** Required. Missing authentication fails closed for every HTTP and WS request. */
  authenticate?: (request: IncomingMessage) => AuthenticatedPrincipal | null | Promise<AuthenticatedPrincipal | null>;
  /** Required in addition to ownership checks. */
  authorize?: (principal: AuthenticatedPrincipal, action: AuthorizationAction, resource: AuthorizationResource) => boolean | Promise<boolean>;
  /** Required for WebSocket upgrades; receives the Origin header (which may be absent). */
  authorizeOrigin?: (origin: string | undefined, request: IncomingMessage) => boolean | Promise<boolean>;
  createExecutionId?: () => string;
  now?: () => Date;
  dependencyTimeoutMs?: number;
  maxExecutions?: number;
  maxEventsPerExecution?: number;
  maxWebSocketBufferedBytes?: number;
  executionTtlMs?: number;
}

interface ExecutionRecord {
  executor: WorkflowExecutor;
  ownerId: string;
  events: WorkflowEvent[];
  clients: Set<WebSocket>;
  lastAccessedAt: number;
  unsubscribe: () => void;
}

export interface WorkflowServer {
  app: Express;
  server: HttpServer;
  webSocketServer: WebSocketServer;
  listen(port: number, host?: string): Promise<void>;
  close(): Promise<void>;
}

export function createWorkflowServer(dependencies: WorkflowServerDependencies): WorkflowServer {
  const app = express();
  const server = createServer(app);
  const webSocketServer = new WebSocketServer({ noServer: true, maxPayload: 4_096 });
  const executions = new Map<string, ExecutionRecord>();
  const principals = new WeakMap<IncomingMessage, AuthenticatedPrincipal>();
  const maxExecutions = boundedOption(dependencies.maxExecutions, 100, 1, 10_000, "maxExecutions");
  const maxEvents = boundedOption(dependencies.maxEventsPerExecution, 1_000, 1, 100_000, "maxEventsPerExecution");
  const maxBuffered = boundedOption(dependencies.maxWebSocketBufferedBytes, 256 * 1024, 1_024, 16 * 1024 * 1024, "maxWebSocketBufferedBytes");
  const ttlMs = boundedOption(dependencies.executionTtlMs, 60 * 60 * 1_000, 1_000, 7 * 24 * 60 * 60 * 1_000, "executionTtlMs");
  const keepAliveIntervalMs = Math.max(250, Math.min(60_000, Math.floor(ttlMs / 3)));
  const dependencyTimeoutMs = boundedOption(dependencies.dependencyTimeoutMs, 30_000, 1, 300_000, "dependencyTimeoutMs");

  const removeExecution = (executionId: string, code = 1001, reason = "Execution removed"): boolean => {
    const record = executions.get(executionId);
    if (record === undefined) return false;
    record.unsubscribe();
    record.executor.cancel(reason);
    for (const client of record.clients) client.close(code, reason);
    executions.delete(executionId);
    return true;
  };
  const cleanup = (): void => {
    const cutoff = Date.now() - ttlMs;
    for (const [id, record] of executions) if (record.lastAccessedAt < cutoff) removeExecution(id, 1001, "Execution expired");
  };
  const cleanupTimer = setInterval(cleanup, Math.min(ttlMs, 60_000));
  cleanupTimer.unref();

  app.use(async (request, response, next) => {
    cleanup();
    if (dependencies.authenticate === undefined) { response.status(401).json({ error: "Authentication is not configured" }); return; }
    try {
      const principal = await dependencies.authenticate(request);
      if (!validPrincipal(principal)) { response.status(401).json({ error: "Unauthenticated" }); return; }
      principals.set(request, principal);
      next();
    } catch {
      response.status(401).json({ error: "Unauthenticated" });
    }
  });
  app.use(express.json({ limit: "1mb", strict: true }));

  const permit = async (request: Request, response: Response, action: AuthorizationAction, resource: AuthorizationResource = {}): Promise<AuthenticatedPrincipal | undefined> => {
    const principal = principals.get(request);
    if (principal === undefined) { response.status(401).json({ error: "Unauthenticated" }); return undefined; }
    if (resource.ownerId !== undefined && resource.ownerId !== principal.id) { response.status(404).json({ error: "Execution not found" }); return undefined; }
    if (dependencies.authorize === undefined || !(await dependencies.authorize(principal, action, resource))) {
      response.status(403).json({ error: "Forbidden" }); return undefined;
    }
    return principal;
  };

  app.get("/tools", async (request, response, next) => {
    try {
      if (await permit(request, response, "tools:list") === undefined) return;
      const tools = parseToolDescriptors(await withTimeout(dependencyTimeoutMs, (signal) => dependencies.mcp.listTools({ signal })));
      response.json({ tools });
    } catch (error) { next(error); }
  });

  app.post("/executions", async (request, response, next) => {
    try {
      const principal = await permit(request, response, "execution:create");
      if (principal === undefined) return;
      if (executions.size >= maxExecutions) { response.status(503).json({ error: "Execution capacity reached" }); return; }
      const graph = parseWorkflowGraph(request.body);
      const executionId = dependencies.createExecutionId?.() ?? randomUUID();
      if (!validRouteIdentifier(executionId) || executions.has(executionId)) { response.status(409).json({ error: "Execution ID collision" }); return; }
      const executor = new WorkflowExecutor(executionId, graph, {
        llm: dependencies.llm, mcp: dependencies.mcp,
        ...(dependencies.now === undefined ? {} : { now: dependencies.now }),
        dependencyTimeoutMs,
      });
      const record: ExecutionRecord = {
        executor, ownerId: principal.id, events: [], clients: new Set(), lastAccessedAt: Date.now(), unsubscribe: () => undefined,
      };
      record.unsubscribe = executor.subscribe((event) => {
        record.events.push(event);
        if (record.events.length > maxEvents) record.events.splice(0, record.events.length - maxEvents);
        const payload = JSON.stringify(event);
        for (const client of [...record.clients]) safeSend(client, payload, maxBuffered, record.clients);
      });
      executions.set(executionId, record);
      response.status(202).json({ executionId, snapshot: executor.snapshot() });
      void executor.start().catch((error: unknown) => console.error(`Execution ${executionId} stopped unexpectedly`, error));
    } catch (error) { next(error); }
  });

  app.get("/executions/:executionId", async (request, response, next) => {
    try {
      const record = executions.get(request.params.executionId);
      if (record === undefined) { response.status(404).json({ error: "Execution not found" }); return; }
      if (await permit(request, response, "execution:read", resource(request.params.executionId, record)) === undefined) return;
      if (executions.get(request.params.executionId) !== record) { response.status(404).json({ error: "Execution not found" }); return; }
      record.lastAccessedAt = Date.now();
      response.json({ ...record.executor.snapshot(), keepAliveIntervalMs });
    } catch (error) { next(error); }
  });

  app.get("/executions/:executionId/events", async (request, response, next) => {
    try {
      const record = executions.get(request.params.executionId);
      if (record === undefined) { response.status(404).json({ error: "Execution not found" }); return; }
      if (await permit(request, response, "execution:read", resource(request.params.executionId, record)) === undefined) return;
      if (executions.get(request.params.executionId) !== record) { response.status(404).json({ error: "Execution not found" }); return; }
      const after = parseSequence(request.query.after);
      if (after === undefined) { response.status(400).json({ error: "Invalid event cursor" }); return; }
      record.lastAccessedAt = Date.now();
      response.json({ events: record.events.filter((event) => event.sequence > after) });
    } catch (error) { next(error); }
  });

  app.post("/executions/:executionId/resume", async (request, response, next) => {
    try {
      const record = executions.get(request.params.executionId);
      if (record === undefined) { response.status(404).json({ error: "Execution not found" }); return; }
      if (await permit(request, response, "execution:resume", resource(request.params.executionId, record)) === undefined) return;
      if (executions.get(request.params.executionId) !== record) { response.status(404).json({ error: "Execution not found" }); return; }
      const body = parseHumanDecision(request.body);
      record.lastAccessedAt = Date.now();
      response.json(await record.executor.resume(body.nodeId, body));
    } catch (error) { next(error); }
  });

  app.delete("/executions/:executionId", async (request, response, next) => {
    try {
      const record = executions.get(request.params.executionId);
      if (record === undefined) { response.status(404).json({ error: "Execution not found" }); return; }
      if (await permit(request, response, "execution:delete", resource(request.params.executionId, record)) === undefined) return;
      if (executions.get(request.params.executionId) !== record) { response.status(404).json({ error: "Execution not found" }); return; }
      removeExecution(request.params.executionId);
      response.status(204).end();
    } catch (error) { next(error); }
  });

  server.on("upgrade", (request, socket, head) => {
    void (async () => {
      try {
        cleanup();
        if (request.method !== "GET") return rejectUpgrade(socket, 400, "Bad Request");
        const url = new URL(request.url ?? "", "http://localhost");
        const match = /^\/executions\/([^/]+)$/.exec(url.pathname);
        if (match?.[1] === undefined) return rejectUpgrade(socket, 404, "Not Found");
        let executionId: string;
        try { executionId = decodeURIComponent(match[1]); }
        catch { return rejectUpgrade(socket, 400, "Bad Request"); }
        if (!validRouteIdentifier(executionId)) return rejectUpgrade(socket, 400, "Bad Request");
        const after = parseSequence(url.searchParams.get("after"));
        if (after === undefined) return rejectUpgrade(socket, 400, "Bad Request");
        if (dependencies.authenticate === undefined || dependencies.authorize === undefined || dependencies.authorizeOrigin === undefined) return rejectUpgrade(socket, 401, "Unauthorized");
        const principal = await dependencies.authenticate(request);
        if (!validPrincipal(principal)) return rejectUpgrade(socket, 401, "Unauthorized");
        if (!(await dependencies.authorizeOrigin(header(request, "origin"), request))) return rejectUpgrade(socket, 403, "Forbidden");
        const record = executions.get(executionId);
        if (record === undefined) return rejectUpgrade(socket, 404, "Not Found");
        if (principal.id !== record.ownerId) return rejectUpgrade(socket, 404, "Not Found");
        if (!(await dependencies.authorize(principal, "execution:subscribe", resource(executionId, record)))) return rejectUpgrade(socket, 403, "Forbidden");
        if (executions.get(executionId) !== record) return rejectUpgrade(socket, 404, "Not Found");
        record.lastAccessedAt = Date.now();
        webSocketServer.handleUpgrade(request, socket, head, (client) => {
          record.clients.add(client);
          for (const event of record.events) if (event.sequence > after) safeSend(client, JSON.stringify(event), maxBuffered, record.clients);
          client.on("message", (data, isBinary) => {
            const text = data.toString();
            if (isBinary || Buffer.byteLength(text) > 4_096) { client.close(1003, "Invalid payload"); return; }
            try {
              const payload = JSON.parse(text) as unknown;
              if (!isPing(payload)) throw new Error("Unexpected payload");
              record.lastAccessedAt = Date.now();
              safeSend(client, JSON.stringify({ type: "pong" }), maxBuffered, record.clients);
            } catch { client.close(1003, "Invalid payload"); }
          });
          client.on("close", () => record.clients.delete(client));
          client.on("error", () => record.clients.delete(client));
          webSocketServer.emit("connection", client, request);
        });
      } catch { rejectUpgrade(socket, 400, "Bad Request"); }
    })();
  });

  app.use((error: unknown, _request: Request, response: Response, _next: NextFunction) => {
    if (error instanceof WorkflowValidationError) { response.status(400).json({ error: error.message, issues: error.issues }); return; }
    if (error instanceof PayloadValidationError || isJsonSyntaxError(error)) { response.status(400).json({ error: error instanceof Error ? error.message : "Invalid JSON" }); return; }
    const message = error instanceof Error ? error.message : "Unknown error";
    response.status(409).json({ error: message.slice(0, 4_096) });
  });

  return {
    app, server, webSocketServer,
    listen: (port, host = "127.0.0.1") => new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(port, host, () => { server.off("error", reject); resolve(); });
    }),
    close: () => new Promise<void>((resolve, reject) => {
      clearInterval(cleanupTimer);
      for (const id of [...executions.keys()]) removeExecution(id);
      webSocketServer.close(() => server.close((error) => error === undefined || (error as NodeJS.ErrnoException).code === "ERR_SERVER_NOT_RUNNING" ? resolve() : reject(error)));
    }),
  };
}

function parseSequence(value: unknown): number | undefined {
  const raw = Array.isArray(value) ? (value.length === 1 ? value[0] : undefined) : value;
  if (raw === undefined || raw === null || raw === "") return 0;
  if (typeof raw !== "string" || !/^(0|[1-9]\d*)$/.test(raw)) return undefined;
  const parsed = Number(raw);
  return Number.isSafeInteger(parsed) ? parsed : undefined;
}
function safeSend(client: WebSocket, payload: string, maxBuffered: number, clients: Set<WebSocket>): void {
  if (client.readyState !== WebSocket.OPEN) return;
  if (client.bufferedAmount + Buffer.byteLength(payload) > maxBuffered) { clients.delete(client); client.close(1009, "Backpressure limit exceeded"); return; }
  client.send(payload, (error) => { if (error) { clients.delete(client); client.terminate(); } });
}
function rejectUpgrade(socket: import("node:stream").Duplex, status: number, reason: string): void {
  if (!socket.destroyed) { socket.write(`HTTP/1.1 ${status} ${reason}\r\nConnection: close\r\n\r\n`); socket.destroy(); }
}
function resource(executionId: string, record: ExecutionRecord): AuthorizationResource {
  return { executionId, ownerId: record.ownerId, workflowId: record.executor.snapshot().workflowId };
}
function validRouteIdentifier(value: unknown): value is string {
  return validIdentifier(value) && value !== "." && value !== ".." && !value.includes("/");
}
function validIdentifier(value: unknown): value is string {
  return typeof value === "string" && value.length <= 32_768 && value.trim().length > 0 && !/[\u0000-\u001f\u007f]/u.test(value);
}
function validPrincipal(value: AuthenticatedPrincipal | null): value is AuthenticatedPrincipal {
  return value !== null && typeof value.id === "string" && value.id.length > 0 && value.id.length <= 256;
}
function header(request: IncomingMessage, name: string): string | undefined {
  const value = request.headers[name];
  return Array.isArray(value) ? value[0] : value;
}
function isPing(value: unknown): value is { type: "ping" } {
  return value !== null && typeof value === "object" && !Array.isArray(value) && Object.keys(value).length === 1 && (value as { type?: unknown }).type === "ping";
}
function boundedOption(value: number | undefined, fallback: number, minimum: number, maximum: number, name: string): number {
  const result = value ?? fallback;
  if (!Number.isSafeInteger(result) || result < minimum || result > maximum) throw new Error(`${name} is out of range`);
  return result;
}
function isJsonSyntaxError(error: unknown): boolean { return error instanceof SyntaxError && "status" in error && (error as { status?: unknown }).status === 400; }

async function withTimeout<T>(timeoutMs: number, operation: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => { controller.abort(); reject(new Error(`Dependency timed out after ${timeoutMs}ms`)); }, timeoutMs);
  });
  try { return await Promise.race([operation(controller.signal), timeout]); }
  finally { if (timer !== undefined) clearTimeout(timer); }
}
