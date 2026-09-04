# Deploying Eventboat on Kubernetes

Eventboat's primary deployment shape is deliberately boring
(redesign-v3.md §6.7): **one Go binary, one Deployment, pipelines mounted
from a ConfigMap**. There is no Operator and no control plane — the M4
review ruled one redundant (R14): the binary already owns
verify-first-reload (`POST /admin/deploy`, SIGHUP), health surfaces
(`/live`, `/ready`) and crash recovery (spool replay + source watermarks),
which is precisely the reconciliation an operator would re-implement.

See [examples/k8s/deployment.yaml](../examples/k8s/deployment.yaml) for a
ready manifest. The essentials:

- **Replicas: 1, strategy: Recreate.** The spool (SQLite) is local-disk
  per-pipeline state; two active instances over one spool volume is
  meaningless. Single-active per pipeline group is the v3 HA model (§6.7);
  rescheduling relies on at-least-once recovery, not multi-active sharing.
- **Probes.** `/live` for the process, `/ready` for spool health and
  pipeline readiness (the same endpoints the Runtime config exposes).
- **Config rollout.** Mount the pipeline directory from a ConfigMap, then
  either POST the new config to `/admin/deploy` (it verifies first and
  refuses invalid configs — the no-bypass rule) or restart the pod
  (`kubectl rollout restart`); sources resume from committed watermarks,
  so the gap is covered at-least-once.
- **State.** `emptyDir` disappears with the pod — fine for at-least-once
  sources with committed offsets (Kafka groups), but attach a
  PersistentVolumeClaim when the spool/dead-letter history must survive
  rescheduling (the common case for job pipelines with watermarks).
- **Admin surface.** Binds 127.0.0.1 by default (`kind: Runtime` config
  changes the listener); the POC build has **no authentication** on it —
  keep it out of the cluster network or behind your own boundary.

## Why no Operator (recorded trim, M4 review R14)

An Operator adds a controller loop, CRDs and RBAC to do what the binary
already does natively: validate configs (gate 1), replace pipelines
gracefully (drain + start), expose status, and recover state. If a
GitOps-style reconciler is ever wanted, `eventboat verify` in CI plus the
deploy API already compose into one. The spec's own non-goals (§2.3) list
"K8s Operator / control plane" as out of scope; M4 honors that with this
manifest + document instead.
