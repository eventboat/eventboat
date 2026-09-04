# Codecs (M4): json / raw / csv / avro / protobuf

Codecs turn wire bytes into the `payload` your CEL predicates and Starlark
scripts see, and back at sinks. Two ways to reference one:

- **Bare name** — `decoder: json` / `encoder: raw`. Works for codecs that
  need no configuration.
- **Named declaration** (§5.10) — codecs that carry configuration are
  declared once and referenced by name:

```yaml
codecs:
  orders-avro:
    type: avro
    schema: |
      {"type": "record", "name": "Order", "fields": [
        {"name": "order_no", "type": "string"},
        {"name": "total", "type": "double"}]}
  events-csv:
    type: csv
    columns:
      - {name: id, type: int}
      - {name: amount, type: float}

sources:
  ingest:
    decoder: events-csv     # by name
    file: {path: input/events.csv}
sinks:
  out:
    encoder: orders-avro
    file: {path: output/orders.avro}
```

Declaration names and registered codec names are **separate namespaces**:
declaring `json:` is a verify error (`cfg_codec_shadow`). Declaration configs
validate against each codec's JSON Schema (`plugin schema <name>` prints it);
decode/encode failures follow the standard error path — the source's decode
failure dead letters with a `codec:` reason, sink encode failures likewise.

## csv

One message is **one CSV record** (the file source emits per line, so a CSV
file is a stream of row messages).

| field | meaning |
|---|---|
| `columns` | explicit column list: `{name, type}`; `type` one of `string` (default), `int`, `float`, `bool` |
| `header` | `true`: the first record **this codec instance decodes** defines the column names (all fields stay strings). Instance state — documented semantics for the "first line is a header" file case; replay re-decodes from the spool, so re-declaring `columns` is the deterministic choice when that matters. |

**CEL type mapping**: `int` → int (64-bit), `float` → double, `bool` →
bool, `string` → string — so `when: 'payload.amount > 100'` compares
numerically with `type: float`.

**Encode** always emits data rows (never a header) in column order; the
column order comes from `columns` or the decoded header. Quoting/escaping
follows RFC 4180 (`encoding/csv`).

## avro

Decodes/encodes against an **inline Avro schema** (`schema`, JSON string) —
[hamba/avro](https://github.com/hamba/avro) v2 (the library LinkedIn itself
migrated to; linkedin/goavro is in maintenance mode — M4 review §一).

| field | meaning |
|---|---|
| `schema` | inline Avro schema JSON (usually a `record`) |

**CEL type mapping**: `int`/`long` → int, `float`/`double` → double,
`string`/`boolean` direct, `bytes` → base64 string at the JSON boundary,
arrays → list, nested records/maps → map, unions → dyn (the actual branch's
type at runtime).

## protobuf

Decodes/encodes **one compiled message type** from a
`FileDescriptorSet` — there is no schema on the wire, so you ship the
descriptor compiled from your `.proto`:

```bash
protoc --descriptor_set_out=orders.descr --include_imports orders.proto
```

| field | meaning |
|---|---|
| `descriptor_set` | path to the `.pb` descriptor set, **relative to the pipeline file** (same rule as wasm modules) |
| `message` | fully-qualified message name, e.g. `com.example.Order` |

**CEL type mapping** (via protojson): `int32/int64/sint*/fixed*` → CEL int
(surfaces as JSON-stringified int64 at the map boundary — protojson
convention), `uint*` → uint, `float/double` → double, `string`/`bool`
direct, `bytes` → base64 string, `repeated` → list, `map`/`message` → map.

Honesty note: decode runs binary → dynamic message → protojson → Go values
and encode the reverse; the double conversion is correct but not the hot
path — the tier exists for interoperability, not throughput.

## Registry surface

Codecs follow the §6.5 schema-mandatory rule since M4:
`RegisterCodec(name, version, schema, factory)`; `plugin catalog --json`
lists them with versions and schemas, and `eventboat plugin schema <codec>`
prints a schema. External gRPC plugins cannot ship codecs (M3 trim,
unchanged); decode/encode stays built-in.
