# jin-kv

A distributed key-value store built on a from-scratch implementation of the
[Raft consensus algorithm](https://raft.github.io/raft.pdf), written in Go with
no external dependencies for the core consensus logic.

Every mechanism here — the write-ahead log, leader election, log replication,
snapshotting, log compaction — was written by hand from the paper, and every
one of them is backed by a test that demonstrates it working under real failure
conditions, not just under happy-path assumptions.

---

## What it does

jin-kv runs as a cluster of nodes that agree on a replicated log of commands.
Clients talk to it over plain HTTP:

```bash
# write a value (must go to the leader)
curl "http://localhost:9080/set?key=x&value=100"
# -> proposed at index 2

# read it back from any node
curl "http://localhost:9080/get?key=x"
# -> 100

# writing to a follower tells you where the leader actually is
curl "http://localhost:9081/set?key=y&value=200"
# -> HTTP 421: not the leader; try node 1
```

The cluster tolerates node failures: as long as a majority of nodes are alive,
writes continue to succeed. When a failed node returns, it catches up
automatically — either by replaying log entries it missed, or, if it fell far
enough behind that those entries have been compacted away, by receiving a full
snapshot from the leader.

---

## Running it

Build:

```bash
go build -o bin/server ./cmd/server
```

Start a three-node cluster, each in its own terminal:

```bash
./bin/server -id=1 -addr=:8080 -httpaddr=:9080
./bin/server -id=2 -addr=:8081 -httpaddr=:9081
./bin/server -id=3 -addr=:8082 -httpaddr=:9082
```

Each node uses two ports: one for internal Raft RPC traffic between nodes
(`-addr`), and one for the client-facing HTTP API (`-httpaddr`). Within a few
seconds one node will win an election and start serving writes.

Each node also maintains its own on-disk state — `nodeN.wal` (write-ahead log)
and `nodeN.snapshot` — so a node that's killed and restarted recovers its
history rather than coming back empty.

---

## What's actually implemented

**Write-ahead log** — append-only, CRC32-checksummed, `fsync`'d before any write
is acknowledged. Survives `kill -9` mid-write; corrupt or partially-written
trailing records are detected and discarded on recovery.

**Leader election** (§5.2) — randomized election timeouts to avoid split votes,
term-based staleness rejection, one-vote-per-term enforcement, and the §5.4.1
election restriction that prevents a node with an out-of-date log from ever
winning.

**Log replication** (§5.3) — `AppendEntries` with `PrevLogIndex`/`PrevLogTerm`
consistency checks, conflict detection with truncation (the Figure 8 scenario),
per-peer `nextIndex`/`matchIndex` tracking, and the §5.3 conflict-index
optimization so a lagging follower catches up in one round trip instead of one
index per round trip.

**Commit safety** (§5.4.2) — an entry is only committed by counting replicas if
it belongs to the leader's *current* term. Previous-term entries commit
indirectly via the Log Matching Property. A no-op entry is appended on election
so a new leader can establish its commit frontier.

**Snapshotting and log compaction** (§7) — periodic snapshots of the state
machine, after which redundant log entries are discarded from both memory and
disk. WAL compaction uses a write-to-temp-then-atomic-rename pattern so a crash
mid-compaction can never corrupt or lose the original log.

**InstallSnapshot RPC** — when a follower has fallen so far behind that the
leader no longer holds the entries it needs, the leader sends a complete
snapshot instead. The follower discards its log entirely and restores from it.

**Crash recovery** — on startup a node reconstructs its state from its own
snapshot and WAL before participating in the cluster. This is mandatory, not
optional: the constructor refuses to return a node that failed to recover.

**HTTP client API** — `/get` and `/set`, with leader redirection so a client
that hits the wrong node is told which node to try instead.

---

## Testing

Every phase has a test that demonstrates it under failure, not just in theory.

```bash
./full_cluster_test.sh   # snapshot install path: a node dies, misses writes,
                         # returns, and catches up via InstallSnapshot

./test_http_api.sh       # client API: writes via the leader, reads from all
                         # nodes, and the follower-redirect path

./chaos_test.sh          # the hard one — see below
```

`chaos_test.sh` is the real stress test. It runs a continuous write stream while
killing a follower, then killing the leader (leaving a single node unable to
form a quorum), then reviving both in sequence — with writes flowing throughout.
At the end it queries every node for every key that was ever accepted and checks
that all nodes agree.

Current result: **zero divergence.** All nodes converge to identical state. The
only keys that go missing are ones a leader accepted into its own log but died
before replicating to a majority — which Raft explicitly permits, since only
*committed* entries are guaranteed to survive.

---

## Known limitations

These are deliberate, documented, and not bugs:

**Reads are not linearizable.** `/get` reads the local state machine directly.
It does not confirm leadership via heartbeat majority, nor check that the
current leader has committed a no-op in its term — the two guarantees §8 of the
paper requires. A stale or partitioned node can therefore serve stale reads.
Adding this is well-understood work; it just isn't done.

**No cluster membership changes.** The peer set is fixed at startup. Adding or
removing nodes from a running cluster (§6, joint consensus) is not implemented.

**No sharding.** A single Raft group owns all keys. Systems like CockroachDB and
TiKV run one Raft group per key range; jin-kv does not.

**No authentication or TLS.** All traffic — both internal RPC and the client
HTTP API — is plaintext. Fine for a local cluster, not for anything exposed.

**Fixed snapshot threshold.** Snapshots trigger every N applied entries, with N
hardcoded. Production systems make this configurable and tune it against real
workloads.

---

## Why this exists

This was built deliberately by hand, from the paper, as a last substantial
manual programming project before moving toward agent-assisted development. No
consensus library, no copying from an existing implementation — the point was to
understand every mechanism well enough to debug it at 1am when three processes
disagree about who's in charge.

`JOURNAL.md` records that process honestly: what was built each session, what
broke, and how long the hard bugs actually took to find.