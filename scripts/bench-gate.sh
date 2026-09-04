#!/usr/bin/env bash
# Loose performance regression gate (redesign-v3-review-beta.md R-B7).
# Guards against ORDER-OF-MAGNITUDE regressions, not noise: limits are
# ~5-15x the reference dev machine baselines (i5-14600KF; see the review
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
# ~1550ns, read-only container ~1460ns, settle throughput (mem) ~6400ns.
check ./internal/lang/celhost BenchmarkPredicateEval 5000
check ./internal/lang/starhost BenchmarkSimpleScript 20000
check ./internal/lang/starhost BenchmarkContainerReadOnly 20000
check ./internal/engine BenchmarkSettleThroughput/mem 100000

exit $fail
