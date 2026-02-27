#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
CFG_PATH="${CFG_PATH:-examples/multiagent/v3.1/multiagent.json}"
INGRESS_TOPIC="${INGRESS_TOPIC:-agent-ingress}"
LEASE_MS="${LEASE_MS:-5000}"
CURL_TIMEOUT="${CURL_TIMEOUT:-8}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MSG_DIR="${ROOT_DIR}/examples/multiagent/v3.1/messages"

if ! command -v curl >/dev/null 2>&1; then
  echo "ERROR: curl is required"
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is required"
  exit 1
fi

json_escape_file() {
  python3 - "$1" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
obj = json.loads(p.read_text(encoding='utf-8'))
print(json.dumps(obj, separators=(',', ':')))
PY
}

wrap_produce_request() {
  local topic="$1"
  local msg_file="$2"
  local idem_key="${3:-}"
  python3 - "$topic" "$msg_file" "$idem_key" <<'PY'
import json, pathlib, sys

topic = sys.argv[1]
msg_file = pathlib.Path(sys.argv[2])
idem_key = sys.argv[3]
msg_obj = json.loads(msg_file.read_text(encoding='utf-8'))
req = {
    "topic": topic,
    "value": json.dumps(msg_obj, separators=(",", ":")),
}
if idem_key:
    req["envelope"] = {"idempotency_key": idem_key}
print(json.dumps(req, separators=(",", ":")))
PY
}

post_json() {
  local path="$1"
  local body="$2"
  curl -sS -X POST "${BASE_URL}${path}" \
    -H 'Content-Type: application/json' \
    --data "$body"
}

health_check() {
  echo ">>> Health check"
  curl -sS "${BASE_URL}/v1/healthz" >/dev/null
  echo "ok"
  echo
}

ensure_topic() {
  local topic="$1"
  local body
  body=$(printf '{"name":"%s","partitions":1}' "$topic")
  local resp
  resp=$(post_json "/v1/topics" "$body" || true)
  if [[ "$resp" == *'"status":"created"'* ]]; then
    echo "Created topic: $topic"
    return 0
  fi
  if [[ "$resp" == *'already exists'* ]]; then
    echo "Topic already exists: $topic"
    return 0
  fi
  echo "ERROR creating topic $topic"
  echo "$resp"
  return 1
}

produce_message() {
  local msg_file="$1"
  local label="$2"
  local idem_key="${3:-}"
  local body resp
  body=$(wrap_produce_request "$INGRESS_TOPIC" "$msg_file" "$idem_key")
  resp=$(post_json "/v1/produce" "$body")
  if [[ "$resp" != *'"status":"produced"'* ]]; then
    echo "ERROR producing ${label}"
    echo "$resp"
    return 1
  fi
  echo "Produced: ${label} -> ${INGRESS_TOPIC}"
}

consume_once() {
  local topic="$1" group="$2" owner="$3"
  curl -sS --max-time "$CURL_TIMEOUT" \
    "${BASE_URL}/v1/consume?topic=${topic}&group=${group}&owner=${owner}&lease_ms=${LEASE_MS}" \
    | head -n 1
}

print_routing_summary() {
  local line="$1"
  python3 - "$line" <<'PY'
import json, sys
line = sys.argv[1].strip()
if not line:
    print("<empty>")
    raise SystemExit(0)
obj = json.loads(line)
routing = obj.get("routing") or {}
meta = routing.get("meta") or {}
print(
    f"offset={obj.get('offset')} partition={obj.get('partition')} "
    f"route={routing.get('label')} target={meta.get('selected_agent') or meta.get('receiver') or meta.get('team')}"
)
PY
}

ack_line() {
  local line="$1" topic="$2" group="$3" owner="$4"
  local req resp
  req=$(python3 - "$line" "$topic" "$group" "$owner" <<'PY'
import json, sys
obj = json.loads(sys.argv[1])
print(json.dumps({
    "topic": sys.argv[2],
    "group": sys.argv[3],
    "owner": sys.argv[4],
    "partition": obj["partition"],
    "offset": obj["offset"],
}, separators=(",", ":")))
PY
)
  resp=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${BASE_URL}/v1/ack" \
    -H 'Content-Type: application/json' --data "$req")
  [[ "$resp" == "204" ]]
}

consume_and_ack_exact() {
  local topic="$1" group="$2" owner="$3"
  local line
  line=$(consume_once "$topic" "$group" "$owner")
  if [[ -z "$line" ]]; then
    echo "No message received from $topic"
    return 1
  fi
  echo "Consumed from ${topic}: $(print_routing_summary "$line")"
  if ack_line "$line" "$topic" "$group" "$owner"; then
    echo "Acked ${topic}"
  else
    echo "ERROR ack failed for ${topic}"
    return 1
  fi
}

consume_capability_and_ack() {
  local group="$1" owner="$2"
  local topics=("agent.coder-a.inbox" "agent.coder-b.inbox")
  local t line
  for t in "${topics[@]}"; do
    line=$(consume_once "$t" "$group" "$owner" || true)
    if [[ -n "$line" ]]; then
      echo "Consumed capability route from ${t}: $(print_routing_summary "$line")"
      if ack_line "$line" "$t" "$group" "$owner"; then
        echo "Acked ${t}"
        return 0
      fi
      echo "ERROR ack failed for ${t}"
      return 1
    fi
  done
  echo "No capability-routed message found in coder inboxes"
  return 1
}

cat <<EOF
=== DriftQ v3.1 multi-agent smoke test (PR #4) ===
This uses existing /v1 endpoints only (no new v3 HTTP API).

Start driftqd in another terminal, for example:
  ./driftqd -reset-wal \\
    -multiagent-config ${CFG_PATH} \\
    -bootstrap-multiagent-topics
EOF

echo
health_check
ensure_topic "$INGRESS_TOPIC"
echo

echo ">>> Producing sample messages"
produce_message "${MSG_DIR}/direct.json" "direct"
produce_message "${MSG_DIR}/capability.json" "capability"
produce_message "${MSG_DIR}/broadcast.json" "broadcast"
echo

echo ">>> Consuming routed messages"
consume_and_ack_exact "agent.reviewer.inbox" "smoke-direct" "smoke-direct-owner"
consume_capability_and_ack "smoke-capability" "smoke-capability-owner"
consume_and_ack_exact "team.core.broadcast" "smoke-broadcast" "smoke-broadcast-owner"

echo
echo "Smoke test complete ✅"
