# fanin — two sources into one transform

Two `file` sources (`orders`, `refunds`) fan into a single `stamp` transform
(`depends_on: [orders, refunds]`), which tags each message with the stream it
arrived on (`meta.stream = meta.source`) and a constant (`payload.seen_by`).
One `audit` file sink collects everything. The engine stamps each message's
`meta.source` with the config source key, so the transform can tell the two
streams apart inside one script.

## Try it

Run from this directory — `file` source/sink paths resolve against the
process working directory, not the config file's location:

    cd examples/fanin
    eventboat verify --config pipeline.yaml
    eventboat run --config pipeline.yaml    # a daemon: stop with Ctrl-C

`input/` ships sample data, tailed from the beginning on start; each input
message appears on `output/audit.jsonl` as one JSON line. Stop with Ctrl-C,
then delete `output/` and `data/` to re-run from scratch.

## Batch: read to completion, then exit

Both sources declare `on_eof: stop`, and [pipeline-batch.yaml](pipeline-batch.yaml)
adds `run: { mode: batch }` — `eventboat run` then exits BY ITSELF once every
line is read and committed (exit 0; 1 on dead letters or a failed source):

    eventboat run --config pipeline-batch.yaml

The same finite-source shape works as a **job** (`run: { mode: job }`): the
file source exhausts, so `eventboat trigger` performs one complete
read-and-commit pass in-process and exits — with run history and per-run
dead-letter replay. Re-triggering does not re-read committed lines (the byte
offset is the persisted watermark).
