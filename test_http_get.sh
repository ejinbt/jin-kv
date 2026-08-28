#!/bin/bash
# test_http_api.sh
#
# Full test of the HTTP client API — /get and /set:
#   1. starts 3 nodes fresh, each with Raft + HTTP ports
#   2. waits for a stable leader
#   3. POSTs a set via the LEADER's HTTP /set — should succeed
#   4. GETs it back from all 3 nodes' HTTP /get — should all agree
#   5. tries /set on a FOLLOWER's HTTP port — should be rejected with
#      a "not the leader, try node N" response, and N should match
#      the actual leader
#   6. retries the set against the correct leader ID from that
#      response, to prove the redirect information is actually usable
#
# Usage: ./test_http_api.sh
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
echo "waiting for all Raft + HTTP ports to come up..."
wait_for_port 8080; wait_for_port 8081; wait_for_port 8082
wait_for_port 9080; wait_for_port 9081; wait_for_port 9082
echo "all 6 ports confirmed up"

echo "waiting 8s for election to stabilize..."
sleep 8

LEADER_ID=$(find_leader)
if [[ -z "$LEADER_ID" ]]; then
    echo "!!! FAIL: no leader elected within 8s"
    exit 1
fi
echo "=== leader is node $LEADER_ID ==="

FOLLOWER_ID=""
for id in 1 2 3; do
    if [[ "$id" != "$LEADER_ID" ]]; then
        FOLLOWER_ID=$id
        break
    fi
done
echo "=== using node $FOLLOWER_ID as a known follower for the redirect test ==="

LEADER_HTTP="${HTTPADDR_FOR_ID[$LEADER_ID]#:}"
FOLLOWER_HTTP="${HTTPADDR_FOR_ID[$FOLLOWER_ID]#:}"

echo ""
echo "================ TEST 1: /set on the LEADER should succeed ================"
echo "curl \"http://localhost:${LEADER_HTTP}/set?key=x&value=100\""
SET_RESP=$(curl -s "http://localhost:${LEADER_HTTP}/set?key=x&value=100")
echo "response: $SET_RESP"
sleep 2

echo ""
echo "================ TEST 2: /get on ALL 3 nodes should agree ================"
ALL_AGREE=true
for id in 1 2 3; do
    port="${HTTPADDR_FOR_ID[$id]#:}"
    val=$(curl -s "http://localhost:${port}/get?key=x")
    echo "node $id (port $port): $val"
    if [[ "$val" != "100" ]]; then
        ALL_AGREE=false
    fi
done

echo ""
echo "================ TEST 3: /set on a FOLLOWER should be rejected with a redirect ================"
echo "curl -i \"http://localhost:${FOLLOWER_HTTP}/set?key=y&value=200\""
FOLLOWER_SET_RESP=$(curl -s -i "http://localhost:${FOLLOWER_HTTP}/set?key=y&value=200")
echo "$FOLLOWER_SET_RESP"

STATUS_LINE=$(echo "$FOLLOWER_SET_RESP" | head -1)
BODY=$(echo "$FOLLOWER_SET_RESP" | tail -1)

echo ""
echo "status line: $STATUS_LINE"
echo "body: $BODY"

REDIRECT_LEADER=$(echo "$BODY" | grep -oE "node [0-9]+" | grep -oE "[0-9]+")

echo ""
echo "================ TEST 4: retry against the leader ID from the redirect ================"
if [[ -n "$REDIRECT_LEADER" ]]; then
    echo "redirect pointed at node $REDIRECT_LEADER"
    if [[ "$REDIRECT_LEADER" == "$LEADER_ID" ]]; then
        echo "PASS: redirect correctly identified the real leader (node $LEADER_ID)"
        REDIRECT_CORRECT=true
    else
        echo "FAIL: redirect said node $REDIRECT_LEADER but real leader is node $LEADER_ID"
        REDIRECT_CORRECT=false
    fi

    REDIRECT_HTTP="${HTTPADDR_FOR_ID[$REDIRECT_LEADER]#:}"
    echo "retrying set via node $REDIRECT_LEADER (port $REDIRECT_HTTP)..."
    RETRY_RESP=$(curl -s "http://localhost:${REDIRECT_HTTP}/set?key=y&value=200")
    echo "retry response: $RETRY_RESP"
else
    echo "FAIL: could not parse a leader ID out of the redirect response"
    REDIRECT_CORRECT=false
fi

sleep 2

echo ""
echo "================ RESULTS ================"
PASS=true

echo "--- TEST 1: leader set succeeded? ---"
if [[ "$SET_RESP" == *"proposed at index"* ]]; then
    echo "PASS"
else
    echo "FAIL: $SET_RESP"
    PASS=false
fi

echo "--- TEST 2: all nodes agree on x=100? ---"
if $ALL_AGREE; then
    echo "PASS"
else
    echo "FAIL"
    PASS=false
fi

echo "--- TEST 3/4: follower correctly redirected to real leader? ---"
if [[ "${REDIRECT_CORRECT:-false}" == "true" ]]; then
    echo "PASS"
else
    echo "FAIL"
    PASS=false
fi

echo "--- TEST 5: y=200 (set via redirect retry) visible on all nodes? ---"
for id in 1 2 3; do
    port="${HTTPADDR_FOR_ID[$id]#:}"
    val=$(curl -s "http://localhost:${port}/get?key=y")
    echo "node $id: y=$val"
    if [[ "$val" != "200" ]]; then
        PASS=false
    fi
done

echo ""
if $PASS; then
    echo "=== OVERALL: PASS — HTTP client API fully verified, including leader redirect ==="
else
    echo "=== OVERALL: FAIL — see above ==="
fi

echo "=== done — cleaning up ==="
sleep 1