# DAY-1
today i wrote the base of what is called WAL , which ensures the whole program survives the crash by ensuring all data is saved to disk after written . i heard terms like BigEndian , i heard it before when i was digging network programming years back , those time i actually convert to endian myself . but i don't think that much low-level is necessary in this age , anyways i wrote encode , decode , append functions . laid  the ground for WAL system . learn about fdatasync , fsync . how to compute checksum , the checksum goes before the data on disk , not after - because on recovery you need to verify before you trust , not other way around which leads to acting on corrupted bytes . then i came to know difference about sha256 and CRC . thats it for today 

# DAY-2
today was crazy day . i learned more about WAL , checksum , why we do checksum on append . how to check checksum above all that i proven our WAL system . learned about seeking a file with io.SeekStart , wrote a huge ReadAll function with lot of repeated error checks , typical golang and learned about unexpectedErrorEof . thats it for today 

<mark>journal is written by claude code </mark>

## [Phase 1] — The WAL

**What got built**
Wrote the Write-Ahead Log from scratch in Go: `encode`/`decode` for turning
an Entry (Term, Index, Data) into raw bytes and back, `Open` to create/load
the log file, `Append` to durably write an entry (checksum first, then the
encoded entry, then fsync — no exceptions), and `ReadAll` to replay the
entire log on startup, stopping cleanly at the first corrupt or partial
record. Finished with `TruncateAfter`, which cuts the log at a given index
using `file.Truncate` — needed for when a follower's log conflicts with a
new leader's later in the project.

**The proof**
Wrote 5 entries, ran the process, killed it with `kill -9` mid-sleep to
simulate a real power loss, restarted it, and `ReadAll` recovered all 5
entries perfectly. That was the moment this stopped being an exercise and
became a real piece of infrastructure. Did it a second time on purpose,
properly, two terminals — same result. The WAL survives a crash. That's
the whole point of a WAL and it's proven, not assumed.

**What actually hurt**
- Understanding checksum wasn't "some function that returns a number" — it
  clicked once I understood we're re-encoding the entry on read to
  reproduce the exact bytes that were checksummed on write, so we can
  compare fingerprints. CRC32, not SHA256 — CRC32 for corruption detection
  (fast, cheap), SHA256 for tamper-proofing (slow, cryptographic). JIN uses
  SHA256 for its commits; the WAL uses CRC32 because power loss is the
  enemy here, not an attacker.
- §5.4.2 of the Raft paper — replicated on a majority is NOT the same as
  committed. That one sentence is the whole reason the no-op-on-election
  rule exists later.
- `ReadAll` turned into a big repetitive function — checksum, term, index,
  dataLen, data, each with the same EOF/ErrUnexpectedEOF/real-error
  three-way branch. Ugly, but decided to leave it as-is rather than
  over-abstract it — boring, standard Go is easier for the next person
  (including future me) to read than a clever helper.
- Almost called `ReadAll` from inside `TruncateAfter` while already holding
  the mutex — that's an instant deadlock, since Go mutexes aren't
  reentrant. Caught it before writing the bug, fixed it by writing a
  shared unexported `readOne` helper that both `ReadAll` and
  `TruncateAfter` call, with no locking inside it — the lock is only ever
  taken by the public entry points.
- Actually shipped a real bug in `TruncateAfter` — a stray `return nil`
  sitting outside the `if entry.Index == index` block, so the function
  silently "succeeded" after checking only the very first entry in the
  file, every time. Caught on review, not by running it. Good reminder to
  actually trace control flow, not just read it top to bottom.

**Tools set up along the way**
Installed Go 1.24 and JIN properly in WSL, fought with a broken nvim
install and abandoned it for VSCode + the Go extension, got JIN and GitHub
both wired as version control (JIN primary, GitHub mirror) — including one
real divergence where `.jin/jin.db`, a binary SQLite file, ended up
tracked in git and caused a merge conflict. Fixed properly: untracked
`.jin/` from git for good, added it to `.gitignore`, resolved the
divergent history with a merge commit. Won't happen again.

---

## [Phase 2] — Leader Election

**What got built**
The actual Raft brain. The `Raft` struct matching Figure 2 of the paper
field-for-field: `currentTerm`, `votedFor` (as `*uint64`, nil meaning "no
vote yet"), `log []wal.Entry`, `commitIndex`, `lastApplied`, and the
leader-only `nextIndex`/`matchIndex` maps. Randomized election timeouts
(150-300ms) to avoid every node timing out simultaneously and splitting
the vote forever. `becomeCandidate` — increments term, votes for self,
resets its own timer, fires `requestVotes` concurrently at every peer.
`HandleRequestVote` — the actual safety-critical logic from §5.2 and
§5.4.1: reject stale terms outright, step down on a newer term, grant the
vote only if we haven't already voted this term (or already voted for this
exact candidate) AND the candidate's log is at least as up-to-date as
ours. `becomeLeader` — flips state, initializes nextIndex/matchIndex for
every peer (deliberately left the no-op entry and heartbeat loop as open
TODOs, since they need AppendEntries, which doesn't exist yet).

Built the whole RPC + transport layer to make this actually reachable
over a real network: `RequestVoteArgs`/`Reply` structs in their own `rpc`
package, hand-rolled binary encode/decode using `encoding/binary`, a
message envelope `[type byte][length uint32][payload]` over raw TCP
(`net.Dial` / `net.Listen`, not `net/http` — too heavy for this), a
`SendRequestVote` client function and a `StartServer` + `handleConnection`
server loop that reads a frame, decodes it, calls into `raft.HandleRequestVote`,
and writes the reply back the same way.

**The proof**
Ran three actual processes on three actual ports (`:8080`, `:8081`,
`:8082`), each its own goroutine-driven Raft node, talking real TCP to
each other. Watched a real election happen across real terminals — votes
requested, granted, denied, terms climbing, a leader emerging. That's not
theoretical anymore. Three independent processes agreed on who's in charge
without me telling them.

**What actually hurt — and this was the hardest day of the project so far**
- Real import cycle: `raft` imported `transport` (to send RPCs),
  `transport` imported `raft` (to use `*raft.Raft` in the server). Go
  won't allow it, for good reason — compilation order becomes ambiguous.
  Fixed with dependency inversion: `raft` now defines its own `Transport`
  interface (just the method signature it needs), and `transport.Client`
  satisfies it structurally without `raft` ever importing `transport`
  directly. First real architecture lesson of the project, not just a
  syntax fix.
- Learned the hard way that a plain function can't satisfy an interface —
  had to wrap `SendRequestVote` in a `Client` struct as a method before it
  could be handed to `raft.NewRaft` as a `Transport`.
- Real design gap: `peers []string` doesn't work once you need
  `nextIndex`/`matchIndex` keyed by server ID. Switched peers to
  `map[uint64]string` (ID → address) — matches how the paper assumes
  cluster membership works (known configuration, not discovered), and
  fixes the ID-mapping problem by construction instead of needing a
  translation function.
- The actual big one: ran the 3-node test and watched terms climb from 1
  to 800+ in under a second. Traced it down to the election timer never
  being paused for a leader — `electionLoop` was calling `becomeCandidate`
  on every timer fire regardless of current state, so a leader would
  immediately re-elect itself, forever, racing its own term upward. Fixed
  by checking `state != Leader` before reacting to a timeout. Second bug
  stacked on top of it: nodes were sending RequestVote to themselves over
  real TCP because the peer map wasn't skipping self. Fixed by skipping
  `peerID == r.id` in the fan-out loop.
- Even after both fixes, the cluster still doesn't fully stabilize — and
  that's expected, not a bug. Without AppendEntries heartbeats, a follower
  has no way to know a leader is still alive, so it keeps timing out and
  forcing new elections. Watched this happen live: leader elected cleanly,
  goes quiet, follower times out anyway, forces a re-election, leader
  correctly steps down when it sees a higher term. Every individual piece
  of Raft safety worked exactly as the paper describes — the missing piece
  is heartbeats, which is Phase 3's first job.
- Also learned a genuinely important distributed-systems debugging lesson
  the hard way: when you fix a bug in shared logic, EVERY node needs the
  rebuild, not just the one you're staring at. Spent a while confused
  because two of three terminals were still running the old binary.

**Where this leaves things**
Phase 2's actual goal — prove that leader election works, safely, across
real processes on a real network — is done and demonstrated, not just
written. The instability left over is the honest, visible reason Phase 3
(AppendEntries, heartbeats, real log replication) needs to exist. Next
session starts there.

## [Phase 3, part 1] — Heartbeats, and watching the cluster actually survive

**What got built**
Finished `AppendEntriesArgs`/`Reply` encoding — this one was more involved
than RequestVote's, since `Entries []wal.Entry` is variable-length and
can't go through `binary.Write` directly like the fixed fields can.
Exported `EncodeEntry`/`DecodeEntry` from the wal package so rpc could
reuse the exact same entry format instead of duplicating it, then encoded
AppendEntriesArgs as: fixed fields via binary.Write, then an entry count,
then each entry length-prefixed and written with wal's own encoder. Wrote
`HandleAppendEntries` — for now heartbeat-only, empty Entries — handling
the term rules: reject stale terms outright, step down to follower on
term >= ours if not already a follower (this covers a candidate losing to
a legitimate leader at the same term, not just a strictly newer one),
update currentTerm and clear votedFor only on strictly newer terms, then
reset our own election timer since a valid heartbeat means the leader is
alive. Added `SendAppendEntries` on the transport client, same
dial/encode/writeMessage/readMessage/decode shape as SendRequestVote.
Wired the AppendEntries case into handleConnection's switch. Then the
actual fix for last night's chaos: `heartbeatLoop`, a goroutine the leader
starts on becoming leader, ticking every 100ms, fanning out empty
AppendEntries to every peer concurrently, stepping down immediately if any
reply comes back with a higher term than its own.

**The proof — this is the one that mattered**
Ran the same 3-node cluster as last night. This time it went quiet almost
immediately — one leader elected, then nothing. Let it sit for a full 10
minutes untouched. Zero re-elections. That alone was the fix for
yesterday's entire debugging saga confirmed working.

Then the real test: killed the leader's process outright. Watched one of
the two survivors time out, become candidate, get voted in by the other,
and the cluster kept going with a new leader. Killed that one too. Now
down to one lone node. It kept trying to become candidate, term climbing,
forever — and never once succeeded, because a single node out of three
can't reach a majority (needs 2, only has itself). The moment I brought
one of the dead nodes back, an election resolved almost instantly and the
cluster picked up again.

That whole sequence — stable under normal operation, clean failover on
leader death, correct refusal to elect without quorum, automatic recovery
the second quorum returns — is the actual point of Raft, and of this whole
project. Not simulated. Watched it happen across real killed terminals and
real TCP connections I wrote myself.

**Bugs along the way**
- `becomeLeader` had a copy-paste slip: was writing to `r.matchIndex`
  twice (once meant to be `r.nextIndex`) instead of setting both maps
  correctly. Wouldn't have shown up until real log replication was being
  tested, which is exactly the kind of quiet bug that's worth catching by
  reading carefully rather than waiting to trip over it later.
- Wrote a `fmt.Errorf(...)` in an if-err block and forgot to actually
  return it — the error just got constructed and thrown away, meaning an
  encode failure would have silently continued instead of stopping.
  Reminder to always ask "am I doing something with this error" whenever
  fmt.Errorf shows up without a return/log/assignment right next to it.
- Clarified for myself that AppendEntries and RequestVote are
  fundamentally different kinds of messages — RequestVote is a permission
  question (needs votedFor/log-up-to-date checks), AppendEntries is a
  leader asserting authority (no voting logic at all, just term
  validation and obedience). Almost copied the voting checks into the
  wrong handler before catching that distinction.

**What's deliberately not built yet**
The no-op entry a new leader is supposed to append on election (§5.3) —
decided to hold off since there's no real log content yet for it to
protect. It'll be the first thing built once real client writes and log
replication exist, not before. Didn't want to write code I couldn't
actually test.

**Where this leaves things**
The election + heartbeat foundation is done and proven under real failure
conditions, not just written and hoped to work. What's left in Phase 3 is
the part that turns this from "a cluster that agrees on who's in charge"
into an actual key-value store: real client writes, the leader appending
them to its log, replicating via non-empty AppendEntries, and advancing
commitIndex once a majority acknowledges. That's next.
