#!/usr/bin/env bash
# Loose performance regression gate (redesign-v3-review-beta.md R-B7).
# Guards against ORDER-OF-MAGNITUDE regressions, not noise: limits are
# ~5-25x the reference dev machine baselines (i5-14600KF; see the review
# appendix) to absorb shared-runner variance.
set -euo pipefail

ns() { go test -bench "^$2\$" -benchtime 1s -run XXX "$1" | awk -v b="$2" '$1 ~ "^"b {print $3}'; }

fail=0
check() {
  got=$(ns "$1" "$2")
  echo "$2: ${got:-TIMEOUT} ns/op (limit $3)"
  if [ -z "${got:-}" ] || awk "BEGIN{exit !($got > $3)}"; then
    echo "::error::benchmark $2 regressed beyond the gate (${got:-no result} > $3 ns/op)"
    fail=1
  fi
}

# Baselines (i5-14600KF, 2026-09-04): predicate ~300ns, simple script
# ~1550ns, read-only container ~1460ns, commit throughput (mem) ~6400ns.
check ./internal/lang/celhost BenchmarkPredicateEval 5000
check ./internal/lang/starhost BenchmarkSimpleScript 20000
check ./internal/lang/starhost BenchmarkContainerReadOnly 20000
check ./internal/engine BenchmarkCommitThroughput/mem 100000

# Group-commit write path (log-collection design §2.2, P1). Reference values
# from the gate's own command on the dev machine (2026-09-25, i5-14600KF):
#   store sqlite (parallel callers)      10.4 µs/op
#   store sqlite_serial (lone caller)     62 µs/op
#   engine sources16/batch=100            62 µs/op  (one source per file)
#   engine one_source/batch=1            110 µs/op  (single hot file)
# The engine benchmarks are sensitive to how many sub-benchmarks share the
# process (calibration rounds accumulate), so each check must stay its own
# `go test` process — which is what ns() does. Limits are deliberately loose
# (5-25x): a shared 2-vCPU runner cannot reproduce these shapes, but a
# regression to per-message commits or a lost group commit still trips them.
check ./internal/store BenchmarkAppendSpool/sqlite 250000
check ./internal/store BenchmarkAppendSpool/sqlite_serial 1000000
check ./internal/engine BenchmarkCommitThroughputSQLite/sources16/batch=100 1500000
check ./internal/engine BenchmarkCommitThroughputSQLite/one_source/batch=1 1800000

exit $fail
