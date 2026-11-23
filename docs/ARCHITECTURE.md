# Architecture

```
index.html ─ css/style.css
   └─ src/ui/ui.js ──reads/controls──▶ RL.Simulator (src/sim/sim.js)
                                          ├─ RL.Heap        (queue.js)  events ordered by (virtual time, insertion #)
                                          ├─ RL.stream      (prng.js)   independent seeded streams: net / elect / fault / workload
                                          └─ RL.RaftNode ×5 (raft.js)   pure Raft logic, talks only through `env`
```

## The four "doors" of a node (`env`)
| PDF interface | In this prototype |
|---|---|
| Clock (Now/After/Cancel) | `env.timer(...)` + virtual `sim.now`; cancelled timers are ignored via a generation counter |
| Transport (Send) | `env.send(from,to,msg)` → network model in `sim.send` |
| Storage (Load/Save) | node-local `term`, `votedFor`, `log` (kept across crash) |
| RNG (Intn/Float64) | `env.rng` seeded stream |

## Simulator step
1. apply recorded manual actions due after N processed events (replay) → 2. pop earliest event → 3. set clock → 4. dispatch (message delivery, timer, fault tick, workload, scripted action, phase change) → 5. update client requests → 6. check INV01–INV07, INV09 → 7. stop on SAFETY_FAIL.
INV08 (convergence) is evaluated after the fault phase ends (`duration`), for up to `settle` ms of virtual time.

## Determinism rules
No `Date.now`, no `Math.random`, no real timers, no threads. Tie-break by insertion order. Nodes and invariants iterate in fixed id order.
Every trace line is folded into an FNV hash; equal hash ⇒ identical run. Manual actions are stored as `{afterSeq, …}` (after N processed events) so replay does not depend on wall-clock time.

## Invariant checks (what is actually examined)
| ID | Check |
|---|---|
| INV01 | at most one leader recorded per term (checked when a node becomes leader) |
| INV02 | each node's term never decreases |
| INV03 | a live node's commitIndex never decreases (baseline reset on restart) |
| INV04 | no two nodes commit different entries at the same index (global ledger of first commits) |
| INV05 | every committed entry still exists on ≥3 of 5 node logs |
| INV06 | for every node pair, if terms match at index i then logs are identical up to i |
| INV07 | a new leader's log contains every entry committed so far |
| INV08 | after faults stop, all nodes alive, logs equal, commitIndex = last index, same key-value state |
| INV09 | when a client is told "committed", ≥3 nodes hold the entry and it matches the ledger |

## Planted bugs (test mode, `cfg.bugs`)
`doubleVote`, `commitTwo` (quorum 2), `skipLogCheck`, `regressCommit`. Each is a one-line switch in `raft.js`, marked `BUG`.
