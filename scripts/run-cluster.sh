#!/usr/bin/env bash
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$ROOT"

make build

PEERS='n1=http://127.0.0.1:9001,n2=http://127.0.0.1:9002,n3=http://127.0.0.1:9003'
PIDS=()

cleanup() {
  trap - EXIT INT TERM
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for number in 1 2 3; do
  id="n${number}"
  port=$((9000 + number))
  directory="data/${id}"
  mkdir -p "$directory"
  ./bin/dkv \
    -id "$id" \
    -listen "127.0.0.1:${port}" \
    -peers "$PEERS" \
    -data-dir "$directory" \
    >"${directory}/node.log" 2>&1 &
  PIDS+=("$!")
done

echo "Three-node d-key-value cluster started on ports 9001, 9002, and 9003."
echo "Inspect it with: curl -s http://127.0.0.1:9001/v1/status"
echo "Logs and durable state are under data/n{1,2,3}/. Press Ctrl-C to stop."

wait
