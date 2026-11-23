# RaftLab

**Deterministic Simulation Testing Framework for a Raft-Based Replicated Key-Value Store** — with an interactive browser dashboard.

> We built a time machine for distributed bugs: break the system in thousands of ways, then replay any failure exactly.

[![tests](https://github.com/YOUR-USERNAME/raftlab/actions/workflows/test.yml/badge.svg)](https://github.com/YOUR-USERNAME/raftlab/actions/workflows/test.yml)

Five Raft nodes run inside one program. Network, clocks, disk and randomness are replaced by controlled fakes driven by one **seed**,
so the same seed always produces the same run, event for event. After every event a **safety checker** verifies nine invariants against the real node state.

<!-- Add a screenshot: docs/screenshot.png -->
<!-- ![dashboard](docs/screenshot.png) -->

## Features
- Raft: leader election, terms, randomized timeouts, heartbeats, AppendEntries, log replication, majority (3 of 5) commit, leader replacement, KV state machine (PUT/GET/DELETE)
- Deterministic engine: seeded PRNG streams, virtual clock, priority queue of events, full trace with hash, exact replay
- 8 faults: delay, drop, reorder, duplicate, partition, crash, restart, clock drift; 7 profiles (healthy, lossy, partition, crashy, clock-skew, chaotic, scripted)
- Safety checker: INV01–INV09, verdicts PASS / SAFETY_FAIL / LIVENESS_FAIL / TIMEOUT / HARNESS_FAIL, with an explanation built from actual state
- Test mode with 4 planted bugs (`doubleVote`, `commitTwo`, `skipLogCheck`, `regressCommit`) to prove the checker catches real violations
- Dashboard: animated cluster, client panel, fault panel, live event timeline, safety panel, saved runs (localStorage) with replay
- No backend, no dependencies, no build step

## Quick start
```bash
git clone https://github.com/YOUR-USERNAME/raftlab.git
cd raftlab
# Dashboard: just open index.html in Chrome/Edge (or serve it: python -m http.server 8000)
npm test        # 10 scenarios + 60-seed baseline + 4 planted-bug tests (Node 18+)
npm run fuzz    # 200 seeds, chaotic profile; prints the seed of any failure
npm run build   # dist/raftlab-standalone.html (single file, shareable)
```
Replaying a failure: put the seed in the dashboard (profile + settings as reported) and press Start, or call
`new RL.Simulator({seed, profile}).runAll()` in Node.

## Using the dashboard (short version)
1. **Start** — watch the election. 2. Send `PUT name=Aman` — it turns *Committed* once 3 of 5 nodes hold it.
3. **Crash node** on the leader — a new leader is elected. 4. Tick **Test mode**, choose `commitTwo`, **Start** — the checker reports INV09 within a second.
Settings (seed, profile, sliders, test mode) are read on **Start/Reset**; the crash/restart/partition/heal/clock buttons act immediately.
**Replay** re-runs the same seed and your manual actions and reports whether the trace matched exactly.

## Project layout
```
index.html, css/style.css   dashboard
src/sim/prng.js             seeded PRNG streams
src/sim/queue.js            event priority queue
src/sim/raft.js             Raft node (no clock/network/random access)
src/sim/sim.js              virtual clock, network faults, profiles, trace/replay, invariants
src/ui/ui.js                dashboard logic (no Raft logic inside)
tests/run-tests.js          scenarios, baseline, planted bugs
scripts/                    fuzz.js, build-standalone.js
docs/ARCHITECTURE.md        design and invariant details
```

## Test results (reproduce with `npm test`)
Leader election, leader crash and replacement, 3-vs-2 partition, message loss / duplication / delay, log replication, majority commit,
same-seed reproducibility (seed 48213 → identical trace and hash), replay of a saved run with manual actions, 60 clean chaotic seeds all PASS,
and all 4 planted bugs detected (INV01, INV09, INV04, INV03).

## Limitations
- Always 5 nodes. No snapshots/log compaction, membership change, disk corruption, or linearizability checker.
- GET is served by the leader from applied state; it is not a linearizable read (an isolated old leader can return stale data).
- A client request not committed within 2 s of virtual time is shown as Failed, although its true outcome may be unknown.
- The checker uses a global ledger of committed entries (a test oracle real nodes would not have).
- Strong testing, not a proof of correctness. Results come from this JavaScript prototype.
- The dashboard uses plain SVG (no D3) so it works offline.

## Future scope
Linearizability checker, automatic shrinking of failing runs, snapshots and membership change, bug-hunting mode, nightly large fuzz runs, other algorithms (Paxos).

## Credits
Inspired by FoundationDB and TigerBeetle deterministic simulation testing, and the Raft paper (Ongaro & Ousterhout).

## License
MIT — see [LICENSE](LICENSE).
