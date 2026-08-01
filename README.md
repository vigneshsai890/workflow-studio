# Workflow Studio

A TypeScript/React reference starter for validating, executing, and observing directed workflow graphs with LLM, MCP tool, and human-approval nodes.

## Scope

This starter includes:

- strict JSON graph, event, tool, and execution-snapshot parsing;
- deterministic DAG validation and ancestor-only data flow;
- bounded LLM/MCP dependency calls with cancellation and timeouts;
- concurrent human approvals and failure propagation;
- an authenticated Express/ WebSocket server with ownership and authorization hooks;
- bounded event retention, backpressure handling, TTL cleanup, keepalive, and replay cursors; and
- a React Flow canvas that reconciles snapshots and reconnects from its last event cursor.

It is a reference/prototype implementation, not a production security boundary. Supply authentication, authorization, durable persistence, tenant isolation, audit logging, deployment isolation, and operational limits appropriate to your environment.

## Verify

Dependencies are exact-pinned in `package-lock.json`:

```sh
npm ci
npm run typecheck
npm test
```

The current local validation covers strict TypeScript checking and 26 tests. Tests use injected LLM/MCP dependencies and do not contact external providers.

## Integration notes

`src/backend/server.ts` requires injected `authenticate`, `authorize`, and `authorizeOrigin` functions for HTTP and WebSocket access. `src/backend/executor.ts` accepts vendor-neutral LLM and MCP client contracts. The in-memory execution store is intentionally non-durable and should not be used as a standalone production persistence layer.

## Security boundaries

Treat workflow definitions, prompts, model results, tool descriptors, tool results, and approval responses as untrusted data. Authorize every execution and approval transition server-side. MCP tools and LLM adapters execute with the privileges granted by the host process; use least-privilege credentials, network restrictions, timeouts, quotas, and human approval for consequential side effects.
