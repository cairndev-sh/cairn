#!/usr/bin/env bash
# scripts/oss/feature_proof.sh — run the hello-cairn feature proof (ADR-0127 D1, WP7).
#
#   seed .proof/ → bring up the light stack's `proof` profile → run tests/e2e/hello
#   → write FEATURE_PROOF.md (feature → the cell that proved it) → tear the profile down
#
# Usage: scripts/oss/feature_proof.sh [--keep-up] [--no-services] [-- <extra pytest args>]
#   --keep-up      leave the compose project running afterwards (re-runs are faster;
#                  `docker compose -p cairn-proof down -v` in deploy/light removes it)
#   --no-services  skip Docker entirely: only the sqlite / in-process cells run, the
#                  `services` cells skip with their reason
#   PYTHON=<interpreter> (default: .venv/bin/python, else python3)
#   PROOF_OUT=<dir>      where junit.xml and receipts.jsonl land (default: .proof-out/ at
#                        the repository root, gitignored); FEATURE_PROOF.md is written at
#                        the repository root
#
# The script never fails on a failing cell: the matrix's verdict is the report. Its exit
# code is pytest's, so CI still sees red.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
PY="${PYTHON:-}"
if [ -z "$PY" ]; then
  if [ -x .venv/bin/python ]; then PY=.venv/bin/python; else PY=python3; fi
fi
OUT="${PROOF_OUT:-$ROOT/.proof-out}"
KEEP_UP=0
SERVICES=1
EXTRA=()
while [ $# -gt 0 ]; do
  case "$1" in
    --keep-up) KEEP_UP=1 ;;
    --no-services) SERVICES=0 ;;
    --) shift; EXTRA=("$@"); break ;;
    *) echo "feature_proof: unknown argument $1" >&2; exit 2 ;;
  esac
  shift
done
mkdir -p "$OUT"

# Its own compose project and volumes (`-p cairn-proof`): a developer's `light` stack beside
# it is never stopped, reused or desynced by the proof.
compose() {
  (cd deploy/light && docker compose -p cairn-proof --env-file .proof/proof.env --profile proof "$@")
}

if [ "$SERVICES" = 1 ]; then
  "$PY" scripts/oss/proof_seed.py
  if [ "$KEEP_UP" = 0 ]; then
    # armed BEFORE anything starts, so a failed `up` still tears its half down
    trap 'echo "feature_proof: tearing the proof project down"; compose down -v' EXIT
  fi
  # the services the cells reach — never the engine image (the matrix runs cairn-server on
  # the host). The profile's Infisical services are not started and no machine identity is
  # seeded (scripts/oss/proof_seed.py says how to make one through Infisical's API), so the
  # Infisical variant of nightly-ops is reported NOT PROVED.
  compose up -d --wait postgres pg-proxy sshd
  # the ports the cells reach; proof.env is the one source
  set -a; . deploy/light/.proof/proof.env; set +a
  export CAIRN_PROOF_PG_DSN="${CAIRN_PROOF_PG_DSN:-postgres://cairn:${POSTGRES_PASSWORD:-cairn}@127.0.0.1:${PROOF_PG_PORT}/cairn}"
  export CAIRN_PROOF_SSH="${CAIRN_PROOF_SSH:-127.0.0.1:${PROOF_SSH_PORT}}"
  export CAIRN_PROOF_SSH_KEY="${CAIRN_PROOF_SSH_KEY:-$ROOT/deploy/light/.proof/ssh/proof_ed25519}"
fi

export CAIRN_E2E=1   # the opt-in: tests/e2e skips without it
export CAIRN_PROOF_RECEIPTS="$OUT/receipts.jsonl"
rm -f "$CAIRN_PROOF_RECEIPTS" "$OUT/junit.xml"   # a run that dies early must not report an older junit
rc=0
env -u OBS_STORAGE -u CAIRN_STORAGE -u OBS_BUS -u CAIRN_BUS -u OBS_RUNTIME -u CAIRN_RUNTIME \
  "$PY" -m pytest tests/e2e -m e2e -q --no-header -p no:cacheprovider -rs \
  --junitxml="$OUT/junit.xml" ${EXTRA[@]+"${EXTRA[@]}"} || rc=$?

"$PY" -m tests.e2e.hello._receipts report --junit "$OUT/junit.xml" \
  --receipts "$CAIRN_PROOF_RECEIPTS" --out "$ROOT/FEATURE_PROOF.md"
# the generated feature matrix (#2667): only where the registry is (the platform/monorepo tree)
if [ -f docs/features.yaml ]; then
  "$PY" scripts/oss/feature_matrix.py --junit "$OUT/junit.xml" --receipts "$CAIRN_PROOF_RECEIPTS" || rc=1
fi
echo "feature_proof: report at FEATURE_PROOF.md (junit: $OUT/junit.xml); pytest exit $rc"
exit $rc
