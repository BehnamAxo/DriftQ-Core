set -e

BASE_URL="${BASE_URL:-http://localhost:8080}"
DURATION="${DURATION:-60}"
RATE="${RATE:-100}"
TOPIC="${TOPIC:-loadtest-$(date +%Y%m%d-%H%M%S)}"

# Burst settings
BURST_N="${BURST_N:-5000}"
BURST_C="${BURST_C:-200}"       # realistic default (cross-platform)
EXTREME_BURST="${EXTREME_BURST:-0}"  # set to 1 to run the 500-concurrent stress test

# Workflow demo settings
WORKFLOW_RUNS="${WORKFLOW_RUNS:-20}"
WORKFLOW_CURL_TIMEOUT="${WORKFLOW_CURL_TIMEOUT:-20}"
WORKFLOW_POLL_TIMEOUT="${WORKFLOW_POLL_TIMEOUT:-20}"     # seconds per run-id
WORKFLOW_POLL_INTERVAL="${WORKFLOW_POLL_INTERVAL:-0.25}" # seconds

echo "=== DriftQ Load Test ==="
echo "URL: $BASE_URL"
echo "Duration: ${DURATION}s"
echo "Rate: ${RATE} req/s"
echo "Topic: ${TOPIC}"
echo ""

print_hey_summary() {
  awk '
    /^Requests\/sec:/ { print; next }

    /^Latency distribution:/ { print; in_latency=1; in_status=0; in_error=0; next }
    /^Status code distribution:/ { print; in_status=1; in_latency=0; in_error=0; next }
    /^Error distribution:/ { print; in_error=1; in_latency=0; in_status=0; next }

    # Stop capturing on any other "*distribution:" header
    (/^[A-Za-z].*distribution:/ && !/^Latency distribution:/ && !/^Status code distribution:/ && !/^Error distribution:/) {
      in_latency=0; in_status=0; in_error=0
    }

    in_latency && /^[[:space:]]*[0-9]+%/ { print; next }
    in_status && /^[[:space:]]*\[[0-9][0-9][0-9]\]/ { print; next }
    in_error  && /^[[:space:]]*\[[0-9]+\]/ { print; next }
  '
}

run_hey_test() {
  expected="$1"
  shift

  tmp_out="$(mktemp)"
  if ! hey "$@" >"$tmp_out" 2>&1; then
    echo "WARNING: hey exited non-zero"
  fi

  print_hey_summary < "$tmp_out"

  status_total=$(awk '
    /^Status code distribution:/ { in_status=1; next }
    in_status && /^[A-Za-z].*distribution:/ { in_status=0 }
    in_status && /^[[:space:]]*\[[0-9]+\][[:space:]]+[0-9]+/ { sum += $2 + 0 }
    END { printf "%d", sum + 0 }
  ' "$tmp_out")

  error_total=$(awk '
    /^Error distribution:/ { in_error=1; next }
    in_error && /^[A-Za-z].*distribution:/ { in_error=0 }
    in_error && /^[[:space:]]*\[[0-9]+\]/ {
      s=$0
      sub(/^[^[]*\[/, "", s)
      sub(/\].*$/, "", s)
      sum += s + 0
    }
    END { printf "%d", sum + 0 }
  ' "$tmp_out")

  completed_total=$((status_total + error_total))
  if [ "$error_total" -gt 0 ]; then
    echo "WARNING: hey reported ${error_total} request errors"
  fi
  if [ "$completed_total" -ne "$expected" ]; then
    echo "WARNING: hey completed ${completed_total}/${expected} requests"
  fi

  rm -f "$tmp_out"
}

# Check if server is running
if ! curl -s "$BASE_URL/v1/healthz" > /dev/null 2>&1; then
  echo "ERROR: Server not responding at $BASE_URL"
  echo "Start the server with: ./driftqd --addr :8080"
  exit 1
fi

echo "Server health check passed"
echo ""

# Create test topic
echo ">>> Creating test topic..."
curl -s -X POST "$BASE_URL/v1/topics?name=${TOPIC}&partitions=4" > /dev/null || true
echo "Done"
echo ""

# Check if hey is installed
if ! command -v hey &> /dev/null; then
  echo "Installing hey load testing tool..."
  go install github.com/rakyll/hey@latest
fi

# Test 1: Produce throughput (bursty fixed request count)
echo ">>> Test 1: Produce Throughput"
echo "Sending $((RATE * DURATION)) requests (c=50) to approximate ${RATE} req/s for ~${DURATION}s..."
run_hey_test $((RATE * DURATION)) -n $((RATE * DURATION)) -c 50 -m POST   "$BASE_URL/v1/produce?topic=${TOPIC}&value=loadtest-message"
echo ""

# Test 2: Burst test (realistic default; extreme optional)
echo ">>> Test 2: Burst Test (${BURST_C} concurrent, ${BURST_N} requests)"
run_hey_test "$BURST_N" -n "$BURST_N" -c "$BURST_C" -m POST   "$BASE_URL/v1/produce?topic=${TOPIC}&value=burst-test"
echo ""

if [ "$EXTREME_BURST" = "1" ]; then
  echo ">>> Test 2b: Extreme Burst Test (500 concurrent, 5000 requests) [optional]"
  run_hey_test 5000 -n 5000 -c 500 -m POST     "$BASE_URL/v1/produce?topic=${TOPIC}&value=burst-test"
  echo ""
fi

# Quick health check after the burst(s)
if ! curl -s "$BASE_URL/v1/healthz" > /dev/null 2>&1; then
  echo "WARNING: health check failed after burst test(s) (server may be overloaded or down)"
else
  echo "Health check after burst test(s): OK"
fi
echo ""

# Test 3: Sustained load
echo ">>> Test 3: Sustained Load (50 req/s for 30s)"
run_hey_test 1500 -n 1500 -c 10 -q 50 -m POST   "$BASE_URL/v1/produce?topic=${TOPIC}&value=sustained-test"
echo ""

# Test 4: Health check throughput
echo ">>> Test 4: Health Check Throughput"
run_hey_test 10000 -n 10000 -c 100 -m GET   "$BASE_URL/v1/healthz"
echo ""

# Test 5: Workflow demo concurrent execution
echo ">>> Test 5: Workflow Demo Concurrent Execution"
TMP_DIR="$(mktemp -d)"
RUN_IDS_FILE="${TMP_DIR}/run_ids.txt"
POST_CODES_FILE="${TMP_DIR}/post_codes.txt"
touch "$RUN_IDS_FILE" "$POST_CODES_FILE"

for i in $(seq 1 "$WORKFLOW_RUNS"); do
  (
    tmp_resp="$(mktemp)"
    code=$(curl -sS --max-time "$WORKFLOW_CURL_TIMEOUT" -o "$tmp_resp" -w "%{http_code}" -X POST "$BASE_URL/debug/run-demo?x=$i" || echo "000")
    printf "%s\n" "$code" >> "$POST_CODES_FILE"
    if [ "$code" = "200" ]; then
      run_id=$(sed -n 's/.*"run_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$tmp_resp" | head -n 1)
      if [ -n "$run_id" ]; then
        printf "%s\n" "$run_id" >> "$RUN_IDS_FILE"
      fi
    fi
    rm -f "$tmp_resp"
  ) &
done
wait
echo "Started ${WORKFLOW_RUNS} concurrent workflow demos"

post_ok=$(awk '/^200$/ { n++ } END { print n + 0 }' "$POST_CODES_FILE")
started=$(awk 'NF > 0 { n++ } END { print n + 0 }' "$RUN_IDS_FILE")

completed=0
failed=0
timed_out=0

for run_id in $(cat "$RUN_IDS_FILE"); do
  deadline=$(( $(date +%s) + WORKFLOW_POLL_TIMEOUT ))
  while :; do
    run_json=$(curl -sS --get --data-urlencode "run_id=$run_id" "$BASE_URL/debug/run" || true)

    if printf "%s" "$run_json" | grep -Eq '"status"[[:space:]]*:[[:space:]]*"succeeded"'; then
      completed=$((completed + 1))
      break
    fi
    if printf "%s" "$run_json" | grep -Eq '"status"[[:space:]]*:[[:space:]]*"failed"'; then
      failed=$((failed + 1))
      break
    fi

    now=$(date +%s)
    if [ "$now" -ge "$deadline" ]; then
      timed_out=$((timed_out + 1))
      break
    fi

    sleep "$WORKFLOW_POLL_INTERVAL"
  done
done

echo "Run-demo POST responses: 200=${post_ok} non200=$((WORKFLOW_RUNS - post_ok))"
echo "Completed workflows: ${completed}/${started} (failed=${failed} timed_out=${timed_out})"
rm -rf "$TMP_DIR"

echo ""
echo "=== Load Test Complete ==="

# Memory check
echo ""
echo ">>> Memory Check (if server accessible)"
if command -v pgrep &> /dev/null; then
  PID=$(pgrep driftqd 2>/dev/null || echo "")
  if [ -n "$PID" ]; then
    ps -o pid,rss,vsz,pcpu -p "$PID"
  fi
fi
