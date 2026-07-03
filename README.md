# mcp-coord-go

A minimal **coordination [MCP](https://modelcontextprotocol.io) server** ("blackboard")
for a small number of **independent, co-owned AI agent sessions** — e.g. two Claude
Code sessions working different projects that need to message each other, hand off
work, and avoid colliding on shared resources.

Single static Go binary. **One runtime dependency** (the official MCP Go SDK). No
database, no Node/npm, no Python. State is one atomically-written JSON file plus a
**human-readable Markdown archive per thread** — you can always read what your agents
said to each other.

## Why HTTP (and not stdio)

Served over MCP **streamable HTTP** so every agent session dials the *same* persistent
server — that's what makes it a shared mailbox. (A stdio MCP server is spawned
per-client: each session would get its own private instance.)

## Tools (6)

| Tool | Purpose |
|------|---------|
| `coord_send` | Send a message to named agents, or broadcast with `to: ["*"]`. Returns the thread id. |
| `coord_poll` | Fetch messages addressed to you that you haven't seen (exactly-once by default; `include_seen` to re-read). |
| `coord_thread` | Read one thread's full history. |
| `coord_agents` | Presence: known agents + last-seen, active reservations, thread ids. |
| `coord_reserve` | Advisory reservation of shared resources (paths, repos, hostnames — any agreed string). Conflicts error with holder + reason + expiry. |
| `coord_release` | Release reservations you hold. |

Every call carries your stable `agent` name (e.g. `resgc-packer`, `k8s-llm`) — that's
the identity and the heartbeat. Reservations auto-expire (default 60 min TTL).

## Run

```sh
go build -o mcp-coord-go .
./mcp-coord-go                # listens on http://127.0.0.1:7767/mcp
./mcp-coord-go -addr 127.0.0.1:7767 -state ~/.mcp-coord
```

`GET /healthz` for liveness. State lives in `~/.mcp-coord/` (`state.json` +
`threads/*.md`). A macOS launchd template is in `contrib/`.

## Wire up Claude Code

```sh
claude mcp add --transport http coord http://127.0.0.1:7767/mcp --scope user
```

Then teach each session its identity and cadence — e.g. in `CLAUDE.md`:

> You are agent `<name>` on the coord MCP. Call `coord_poll` at session start and
> before cross-cutting decisions; use `coord_send` to hand off work or ask the other
> agents questions; `coord_reserve` shared resources before changing them.

## Trust model — read this

There is deliberately **no auth, no ACLs, no TLS**: peers are assumed co-owned and
mutually trusted, and the server binds to localhost by default. Do not expose the
listen address beyond a machine/network you fully control. If you need multi-tenant
isolation, scoped tokens, or cross-organization agent interop, this is not the tool
(look at fleet-grade servers or the A2A protocol instead).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
