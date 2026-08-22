#!/bin/bash
# test_http_get.sh
#
# Focused test for the new HTTP /get client API endpoint:
#   1. starts 3 nodes fresh, each with its own Raft port AND HTTP port
#   2. waits for a stable leader
#   3. sets a value via the leader's stdin (the path we already know works)
#   4. waits, then curls the LEADER's HTTP port specifically
#   5. reports exactly what happened at each step, no ambiguity
#
# Usage: ./test_http_get.sh
# Run from the jin-kv project root.

set -uo pipefail

LOG1=node1.log
LOG2=node2.log
LOG3=node3.log
FIFO1=node1.in
FIFO2=node2.in
FIFO3=node3.in

declare -A PID_FOR_ID
declare -A FEEDER_PID_FOR_ID
declare -A ADDR_FOR_ID
declare -A HTTPADDR_FOR_ID
ADDR_FOR_ID[1]=":8080"
ADDR_FOR_ID[2]=":8081"
ADDR_FOR_ID[3]=":8082"
HTTPADDR_FOR_ID[1]=":9080"
HTTPADDR_FOR_ID[2]=":9081"
HTTPADDR_FOR_ID[3]=":9082"
PIDS=()

cleanup() {
    echo ""
    echo "=== cleaning up ==="
    for pid in "${PIDS[@]:-}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    for id in 1 2 3; do
        fp="${FEEDER_PID_FOR_ID[$id]:-}"
        [[ -n "$fp" ]] && kill -9 "$fp" 2>/dev/null || true
    done
    rm -f "$FIFO1" "$FIFO2" "$FIFO3"
}
trap cleanup EXIT

echo "=== killing any orphaned server processes ==="
pkill -9 -f "bin/server" 2>/dev/null || true
sleep 1

echo "=== resetting WAL, snapshot, log, and fifo files ==="
rm -f node1.wal node2.wal node3.wal
rm -f node1.snapshot node2.snapshot node3.snapshot
rm -f "$LOG1" "$LOG2" "$LOG3" "$FIFO1" "$FIFO2" "$FIFO3"

echo "=== building ==="
mkdir -p bin
go build -o bin/server ./cmd/server || { echo "build failed"; exit 1; }

start_node() {
    local id=$1
    local addr=$2
    local httpaddr=$3
    local logfile=$4
    local fifo=$5

    rm -f "$fifo"
    mkfifo "$fifo"

    ( exec 3<>"$fifo"; cat <&3 ) | ./bin/server -id="$id" -addr="$addr" -httpaddr="$httpaddr" >> "$logfile" 2>&1 &
    local server_pid=$!

    sleep 0.2
    local feeder_pid
    feeder_pid=$(pgrep -f "exec 3<>$fifo" | head -1)
    FEEDER_PID_FOR_ID[$id]="$feeder_pid"

    echo "$server_pid"
}

send_cmd() {
    local fifo=$1
    local cmd=$2
    echo "$cmd" > "$fifo"
}

fifo_for() { eval echo "\$FIFO$1"; }

find_leader() {
    local best_id="" best_ts=""
    for id in 1 2 3; do
        local file="node${id}.log"
        [[ -f "$file" ]] || continue
        local line
        line=$(grep "BECAME LEADER" "$file" | tail -1)
        [[ -z "$line" ]] && continue
        local ts="${line:0:19}"
        if [[ -z "$best_ts" || "$ts" > "$best_ts" ]]; then
            best_ts="$ts"
            best_id="$id"
        fi
    done
    echo "$best_id"
}

wait_for_port() {
    local port=$1
    local tries=0
    while ! (echo > "/dev/tcp/127.0.0.1/$port") 2>/dev/null; do
        sleep 0.2
        tries=$((tries + 1))
        if [[ $tries -gt 25 ]]; then
            echo "!!! timed out waiting for port $port to come up"
            return 1
        fi
    done
    return 0
}

echo "=== starting all 3 nodes (with both Raft and HTTP ports) ==="
PID_FOR_ID[1]=$(start_node 1 "${ADDR_FOR_ID[1]}" "${HTTPADDR_FOR_ID[1]}" "$LOG1" "$FIFO1")
PIDS+=("${PID_FOR_ID[1]}")
PID_FOR_ID[2]=$(start_node 2 "${ADDR_FOR_ID[2]}" "${HTTPADDR_FOR_ID[2]}" "$LOG2" "$FIFO2")
PIDS+=("${PID_FOR_ID[2]}")
PID_FOR_ID[3]=$(start_node 3 "${ADDR_FOR_ID[3]}" "${HTTPADDR_FOR_ID[3]}" "$LOG3" "$FIFO3")
PIDS+=("${PID_FOR_ID[3]}")

echo "node1 pid=${PID_FOR_ID[1]}  node2 pid=${PID_FOR_ID[2]}  node3 pid=${PID_FOR_ID[3]}"
echo "waiting for all 3 Raft ports to come up..."
wait_for_port 8080
wait_for_port 8081
wait_for_port 8082
echo "waiting for all 3 HTTP ports to come up..."
wait_for_port 9080
wait_for_port 9081
wait_for_port 9082
echo "all 6 ports confirmed up"

echo "waiting 8s for election to stabilize..."
sleep 8

LEADER_ID=$(find_leader)
if [[ -z "$LEADER_ID" ]]; then
    echo "!!! FAIL: no leader elected within 8s"
    exit 1
fi
echo "=== leader is node $LEADER_ID ==="

echo "=== setting x=100 via node $LEADER_ID's stdin (the known-working path) ==="
send_cmd "$(fifo_for $LEADER_ID)" "set x=100"
sleep 2

echo "=== sanity check: confirming via stdin get on the SAME node ==="
send_cmd "$(fifo_for $LEADER_ID)" "get x"
sleep 1

echo ""
echo "================ RESULTS ================"
echo "--- leader node $LEADER_ID log (stdin get should show x=100 here) ---"
grep -A 3 '"get x"' "node${LEADER_ID}.log" | tail -8

LEADER_HTTP_PORT="${HTTPADDR_FOR_ID[$LEADER_ID]#:}"
echo ""
echo "--- curling the LEADER's HTTP API directly (port $LEADER_HTTP_PORT) ---"
echo "command: curl -v \"http://localhost:${LEADER_HTTP_PORT}/get?key=x\""
curl -v "http://localhost:${LEADER_HTTP_PORT}/get?key=x" 2>&1

echo ""
echo ""
echo "--- for comparison, curling ALL THREE nodes' HTTP ports ---"
for id in 1 2 3; do
    port="${HTTPADDR_FOR_ID[$id]#:}"
    echo "-- node $id (http port $port) --"
    curl -s "http://localhost:${port}/get?key=x"
    echo ""
done

echo ""
echo "=== done — cleaning up ==="
sleep 1