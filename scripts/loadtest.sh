#!/bin/bash
# Place this file at: scripts/loadtest.sh

set -e

BASE_URL="${BASE_URL:-http://localhost:8080}"
DURATION="${DURATION:-60}"
RATE="${RATE:-100}"

echo "=== DriftQ Load Test ==="
echo "URL: $BASE_URL"
echo "Duration: ${DURATION}s"
echo "Rate: ${RATE} req/s"
echo ""

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
curl -s -X POST "$BASE_URL/v1/topics?name=loadtest&partitions=4" > /dev/null || true
echo "Done"
echo ""

# Check if hey is installed
if ! command -v hey &> /dev/null; then
    echo "Installing hey load testing tool..."
    go install github.com/rakyll/hey@latest
fi

# Test 1: Produce throughput
echo ">>> Test 1: Produce Throughput"
echo "Running $RATE req/s for ${DURATION}s..."
hey -n $((RATE * DURATION)) -c 50 -m POST \
    "$BASE_URL/v1/produce?topic=loadtest&value=loadtest-message" \
    2>/dev/null | grep -E "Requests/sec|Latency|Status"

echo ""

# Test 2: Burst test
echo ">>> Test 2: Burst Test (500 concurrent)"
hey -n 5000 -c 500 -m POST \
    "$BASE_URL/v1/produce?topic=loadtest&value=burst-test" \
    2>/dev/null | grep -E "Requests/sec|Latency|Status"

echo ""

# Test 3: Sustained low rate
echo ">>> Test 3: Sustained Load (50 req/s for 30s)"
hey -n 1500 -c 10 -q 50 -m POST \
    "$BASE_URL/v1/produce?topic=loadtest&value=sustained-test" \
    2>/dev/null | grep -E "Requests/sec|Latency|Status"

echo ""

# Test 4: Health check throughput
echo ">>> Test 4: Health Check Throughput"
hey -n 10000 -c 100 -m GET \
    "$BASE_URL/v1/healthz" \
    2>/dev/null | grep -E "Requests/sec|Latency|Status"

echo ""

# Workflow load test
echo ">>> Test 5: Workflow Demo Concurrent Execution"
for i in {1..20}; do
    curl -s -X POST "$BASE_URL/debug/run-demo?x=$i" > /dev/null &
done
wait
echo "Started 20 concurrent workflow demos"

# Wait and check
sleep 2
COMPLETED=$(curl -s "$BASE_URL/debug/runs?limit=20" | grep -c '"status":"succeeded"' || echo "0")
echo "Completed workflows: $COMPLETED"

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
