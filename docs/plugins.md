# Writing Eventboat plugins (gRPC, any language)

Eventboat sources and sinks can live outside the binary as **out-of-process
plugins** speaking gRPC ([redesign-v3.md](../redesign-v3.md) §6.5). A plugin
is a program in any language that can serve gRPC; the contract is fully
specified by one `.proto` file and this document. The reference
implementation to copy from is
[examples/plugins/ticker-source](../examples/plugins/ticker-source) — a
separate Go module that imports only the generated protocol code, never
Eventboat internals.

The protocol files:

- `proto/eventboat/plugin/v1/plugin.proto` — the contract
- `pkg/pluginv1` — generated Go stubs (host side AND plugin side, if you write
  in Go; for other languages generate from the `.proto` with your own toolchain)

## How a plugin's lifetime looks

```
host                                   plugin (your program)
----                                   --------------------
start process (grpc.command argv)  ->  listen on 127.0.0.1:<random port>
                                       print ONE JSON line to stdout
read line, validate against manifest  <- {"eventboat_plugin":1, ...}
dial gRPC (auth token in metadata) ->  serve Source / Sink
Init(config_json, state)           ->  parse config, restore state
Run stream   (continuous source)   <-> Event frames
Pull stream  (job/pull source)     <-> Event frames; OK end = exhausted
Settled(through_seq)               ->  return new state (commit offsets HERE)
Close rpc, then stdin closed       ->  exit promptly (host kills after 5s)
```

Key rules:

1. **The handshake line** is a single JSON object printed to stdout as the
   first line, nothing before it (leading whitespace is trimmed, but print it
   first anyway):

   ```json
   {"eventboat_plugin":1,"kind":"source","name":"ticker","version":1,
    "capabilities":["pull"],"listen":"127.0.0.1:54321","auth":"<random token>"}
   ```

   - `eventboat_plugin` is the protocol version; the host speaks **1** and
     refuses other values.
   - `kind` is `"source"` or `"sink"`; `name` and `version` must match the
     node's manifest exactly (see below).
   - `capabilities` must include everything the manifest promises (e.g.
     `"pull"`).
   - `listen` is a `127.0.0.1:port` address; `auth` is a random token the
     host sends as the `eventboat-auth` gRPC metadata key on every call.
     Reject calls whose token does not match.
2. **Shutdown signal is stdin EOF.** When the host closes your stdin, stop
   gracefully (flush, close files) and exit. The host force-kills after 5
   seconds.
3. Anything you print to stdout after the handshake line, and everything on
   stderr, is captured into the host log.

## The manifest (static schema declaration)

The host's `verify` gate is static — it never spawns your process. It
validates the pipeline's plugin block against a **manifest** file your plugin
ships:

```json
{
  "kind": "source",
  "name": "ticker",
  "version": 1,
  "capabilities": ["pull"],
  "config_schema": {
    "type": "object",
    "required": ["symbol"],
    "properties": {
      "symbol": { "type": "string" },
      "events": { "type": "integer", "minimum": 1, "default": 10 }
    },
    "additionalProperties": false
  }
}
```

`config_schema` is a JSON Schema (draft 2020-12; use `additionalProperties:
false` — unknown fields are errors, same as built-in plugins). At run time the
handshake's `name`/`version`/`kind`/capabilities are cross-checked against the
manifest; a mismatch is a hard error. This is what makes "the docs say the
plugin exists but the binary doesn't" impossible.

## Pipeline configuration shape

```yaml
sources:
  prices:
    version: 1                                  # optional pin; mismatch = verify error
    grpc:
      command: ["./ticker-source"]              # argv; relative to the eventboat working dir
      schema: "plugins/ticker/manifest.json"    # relative to the pipeline file
    ticker:                                     # plugin name = block key
      symbol: USD/EUR
      events: 100
```

`grpc.command` may reference `${ENV}` and `${constants.*}`; `grpc.schema`
must be static (it is read at load time, before job parameters resolve).
Sinks use the same shape under `sinks:`.

## The wire protocol

`Event` carries one message:

| field | meaning |
|---|---|
| `payload` | raw bytes — exactly what the engine holds (sources) or the sink-encoded bytes (sinks) |
| `meta` | `map<string, MetaValue>`; MetaValue is a oneof of `string_value`, `int_value` (int64), `bool_value`, `double_value`. Rich engine values (arrays/objects) arrive as JSON-encoded strings |
| `codec` | codec name the payload arrived with (e.g. `json`) |
| `cursor` | pull sources: the cursor value of this row (`""` if none) — set it and it flows into `meta.cursor` bindings |
| `src_seq` / `src_name` | per-source monotonic sequence / source node name |

### Source service

- `Init(InitRequest{state bytes, config_json string})` — called once before
  Run/Pull. `config_json` is the plugin block as JSON. `state` is the bytes
  you last returned from Settled (empty on first run). Report failures in
  `InitResponse.error`, not as a gRPC status.
- `Run(RunRequest) returns (stream Event)` — continuous mode. Emit frames
  until the host cancels the stream. **Honor send blocking**: when the host
  stops reading (backpressure), your `Send` blocks — that is the admission
  gate; do not buffer unboundedly.
- `Pull(RunRequest) returns (stream Event)` — job/pull mode, served when your
  manifest declares `capabilities: ["pull"]`. Emit one page of rows, then
  **end the stream with OK status** — that signals "exhausted" and the job
  run settles. Ending with an error status fails the run (a pull-source
  failure, distinct from per-message dead letters). Fix page bounds up front:
  if you compute the end from mutable state that Settled updates
  concurrently, the stream never ends (the reference implementation documents
  this trap).
- `Settled(SettledRequest{through_src_seq})` — the contiguous settled
  frontier advanced through `src_seq`. Commit your offsets HERE (Kafka
  offsets, file positions, watermarks) and return the new state bytes in
  `SettledResponse.state`; the host persists and feeds it back through the
  next `Init`. This is the at-least-once contract: a replay may re-deliver
  everything after the last Settled you acknowledged.
- `Close(CloseRequest)` — last chance to flush; the process then receives
  stdin EOF.

### Sink service

- `Init` — as above (config delivery; no state).
- `Write(WriteRequest{batch Event[]})` — one batch. Batching is owned by the
  engine; just write the batch and return success, or set `error` for the
  whole batch — the engine retries per the edge's `delivery` policy and dead
  letters after exhaustion. Partial failure = report failure for the batch.
- `Close` — flush and finish.

Optionally serve the standard `grpc.health.v1`; the host tolerates its
absence (Init is the real liveness gate).

## Semantics you inherit for free

- **Verify** (gate 1) validates your config schema strictly and checks the
  optional `version` pin — without spawning your process.
- **At-least-once**: spool, settle tracking and checkpointing are host-side.
  Events you emit are durable before they flow; if the host crashes, you are
  re-Inited with your last Settled state and may re-emit from there.
- **Dead letters** apply to your sink failures via the delivery policy; a
  crashed plugin process surfaces as source/sink errors by default (fail
  fast, M3 semantics). Opt into automatic recovery with
  `grpc.restart: restart` — see the next section.

## Crash policy: fast-fail (default) vs restart

```yaml
sources:
  in:
    grpc:
      command: ["./my-plugin"]
      schema: my-plugin/manifest.json
      restart: restart        # default: fast-fail
    myplugin: { ... }
```

- **fast-fail** (default, or omitted): a dead process surfaces as stream /
  write errors; the source stops (continuous mode) or the job run fails
  (pull mode); a pipeline redeploy is the recovery path. This preserves the
  M3 semantics exactly.
- **restart**: the host supervises the process. A crash (or a wedged
  connection) respawns it with exponential backoff (250ms doubling, capped
  at 30s; the ladder resets after 30s of uptime), re-delivers your config
  via Init (with the latest Settled state — pull sources resume past the
  settled watermark; duplicates are the at-least-once contract, never loss),
  and retries: source streams reconnect, sink Writes retry once per call on
  a transport error (the edge's delivery policy still governs the rest).
  Every respawn counts `eventboat_plugin_restarts_total{plugin=...}`.
  Clean end-of-stream is exhaustion, not a crash — it does not restart.

## Minimal Go plugin skeleton

```go
func main() {
    lis, _ := net.Listen("tcp", "127.0.0.1:0")
    token := randToken()
    hs, _ := json.Marshal(map[string]any{
        "eventboat_plugin": 1, "kind": "source", "name": "myplugin",
        "version": 1, "capabilities": []string{"pull"},
        "listen": lis.Addr().String(), "auth": token,
    })
    fmt.Println(string(hs))
    srv := grpc.NewServer(grpc.ChainUnaryInterceptor(authUnary(token)),
        grpc.ChainStreamInterceptor(authStream(token)))
    pluginv1.RegisterSourceServer(srv, &mySource{})
    go func() { _, _ = io.Copy(io.Discard, os.Stdin); srv.GracefulStop() }() // stop on stdin EOF
    _ = srv.Serve(lis)
}
```

See [examples/plugins/ticker-source/main.go](../examples/plugins/ticker-source/main.go)
for the complete, runnable version (auth interceptors, config parsing,
state handling, Pull semantics) — the acceptance test
(`internal/rpcplugin/acceptance_test.go`) builds it exactly the way a
third party would and runs it through verify and a real engine run.

## What is deliberately NOT in v1

- External **codec** plugins (decode/encode stays built-in; your payload is
  bytes, decode with the node's `decoder`).
- TLS between host and plugin (loopback + one-shot token is the v1 threat
  model).
