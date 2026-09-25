#!/usr/bin/env bash
# End-to-end collection benchmark (log-collection design 2026-09-24 §2.6.3,
# P4): a real file source tails a temp file through the real engine and its
# group-commit SQLite spool into the real victorialogs sink, whose endpoint is
# an in-process httptest server. No VictoriaLogs instance is needed.
#
# This script is NOT a regression gate — scripts/bench-gate.sh is. It exists so
# operators can reproduce the order-of-magnitude numbers on their own hardware
# and tune scripts/bench-collect.sh's shapes (batch size, line size, rate) to
# their estate. Compare against docs/tuning.md's reference table.
#
# Measurement discipline (S2 finding): a shape's number depends on what ran in
# the same process before it, so every shape runs through its OWN `go test`
# invocation (-benchtime 1x, -count 1). Do not merge the shapes into one
# -bench regex.
#
# Reference (i5-14600KF dev machine, Windows 11, Go 1.25.0, 2026-09-25;
# 20 000 lines, sink batch 500 / timeout_ms 100, store defaults 256 rows /
# 2 ms, no transform, decoder json -> encoder json):
#   512 B lines : 2.30-2.70 s  =>  7 400-8 700 lines/s  =>  3.8-4.5 MB/s
#   4 KiB lines : 3.40-4.30 s  =>  4 650-6 100 lines/s  => 19-25 MB/s
#   (512 B @ 5 000 lines/s target: collector keeps up, ~5 000 lines/s)
# A machine with slower disk or fewer cores will land below these; the shape
# (single hot file -> serial accept path) is the limit, not a promise.
#
# Overrides:
#   EVENTBOAT_BENCH_LINES       lines per shape (default 20000)
#   EVENTBOAT_BENCH_RATE        producer lines/s, 0 = unthrottled (default 0)
#   EVENTBOAT_BENCH_LINE_BYTES  forces ONE shape with this line size
set -euo pipefail

cd "$(dirname "$0")/.."

command -v go >/dev/null 2>&1 || { echo "go is not on PATH" >&2; exit 1; }

LINES="${EVENTBOAT_BENCH_LINES:-20000}"
RATE="${EVENTBOAT_BENCH_RATE:-0}"

echo "collect benchmark: lines=$LINES rate=$RATE ($(go version))"
echo

run_shape() {
  local bytes="$1" label="$2" out summary
  echo "== ${label}: ${bytes} B lines"
  echo "   EVENTBOAT_BENCH_LINE_BYTES=$bytes EVENTBOAT_BENCH_LINES=$LINES EVENTBOAT_BENCH_RATE=$RATE go test ./internal/engine -run '^$' -bench '^BenchmarkCollectE2E$' -benchtime 1x -count 1"
  if ! out=$(EVENTBOAT_BENCH_LINE_BYTES="$bytes" EVENTBOAT_BENCH_LINES="$LINES" EVENTBOAT_BENCH_RATE="$RATE" \
    go test ./internal/engine -run '^$' -bench '^BenchmarkCollectE2E$' -benchtime 1x -count 1 2>&1); then
    printf '%s\n' "$out"
    echo "::error::collect benchmark failed for the ${bytes} B shape" >&2
    exit 1
  fi
  printf '%s\n' "$out"
  summary=$(printf '%s\n' "$out" | awk '
    /^BenchmarkCollectE2E/ {
      for (i = 2; i <= NF; i++) {
        if ($i == "MB/s" || $i == "lines/s" || $i == "lines/request") printf "%s %s; ", $(i-1), $i
      }
    }')
  echo "   -> ${summary:-no result line}"
  echo
}

if [ -n "${EVENTBOAT_BENCH_LINE_BYTES:-}" ]; then
  run_shape "$EVENTBOAT_BENCH_LINE_BYTES" "single shape"
else
  run_shape 512 "host log lines"
  run_shape 4096 "large payload lines"
fi

echo "reference table: docs/tuning.md; regression gate: scripts/bench-gate.sh"
