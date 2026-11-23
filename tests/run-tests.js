// Run with: node tests/run-tests.js   (every line printed below comes from real executions)
['prng', 'queue', 'raft', 'sim'].forEach(f => require('../src/sim/' + f + '.js'));
const { Simulator: S, IDS } = RL; let pass = 0, fail = 0;
const T = (name, fn) => { try { const r = fn(); console.log('PASS', name, r ? '— ' + r : ''); pass++; } catch (e) { console.log('FAIL', name, '—', e.message); fail++; } };
const ok = (c, m) => { if (!c) throw new Error(m); };
const waitLeader = (s, max = 4000) => { while (!s.verdict && !s.leader() && s.nextT() <= max) s.step(); return s.leader(); };
const quiet = o => Object.assign({ workload: false }, o);
const put = (s, k, v, target) => { s.act({ type: 'req', op: 'PUT', key: k, value: v, target }); return s.reqs[s.reqs.length - 1]; };

T('1 leader election (5 nodes)', () => { const s = new S(quiet({ seed: 1 })); const L = waitLeader(s); ok(L, 'no leader'); s.runUntil(s.now + 2000);
  ok(!s.viol, 'violation'); ok(IDS.filter(i => s.nodes[i].role === 'leader').length === 1, 'not exactly one leader'); return `leader Node ${L}, term ${s.nodes[L].term}, t=${s.now}ms`; });
T('2 leader crash and replacement', () => { const s = new S(quiet({ seed: 2 })); const L = waitLeader(s); s.runUntil(s.now + 300); const t0 = s.nodes[L].term;
  s.act({ type: 'crash', node: L }); s.runUntil(s.now + 3000); const L2 = s.leader(); ok(L2 && L2 !== L, 'no new leader'); ok(s.nodes[L2].term > t0, 'term did not grow'); ok(!s.viol, 'violation'); return `Node ${L} → Node ${L2}, term ${t0} → ${s.nodes[L2].term}`; });
T('3 partition 3 vs 2', () => { const s = new S(quiet({ seed: 3 })); const L = waitLeader(s); s.runUntil(s.now + 300); const o = IDS.find(i => i !== L);
  s.act({ type: 'partition', nodes: [L, o] }); const r0 = put(s, 'a', '1', L); s.runUntil(s.now + 1500);
  ok(r0.status !== 'Committed', 'minority committed!'); const L2 = s.leader(); ok(L2 && L2 !== L && L2 !== o, 'majority side has no new leader');
  const r1 = put(s, 'b', '2'); s.runUntil(s.now + 800); ok(r1.status === 'Committed', 'majority could not commit');
  s.act({ type: 'heal' }); s.runUntil(s.now + 4000); ok(!s.viol, 'violation'); ok(s.converged(), 'did not converge after heal');
  ok(!s.nodes[1].kv.has('a') && s.nodes[1].kv.get('b') === '2', 'minority write survived or majority write lost'); return `minority PUT → ${r0.status}/${r0.note}; majority PUT → ${r1.status}`; });
T('4 message loss', () => { const s = new S({ seed: 4, profile: 'lossy' }); s.runAll(); const d = s.trace.filter(e => e.type === 'drop' && /loss/.test(e.desc)).length; ok(d > 0, 'no drops'); ok(s.verdict === 'PASS', s.verdict + ' ' + s.reason); return `${d} messages lost, verdict ${s.verdict}`; });
T('5 message duplication', () => { const s = new S({ seed: 5, dupP: .3 }); s.runAll(); const d = s.trace.filter(e => /duplicate/.test(e.desc)).length; ok(d > 0, 'no dups'); ok(s.verdict === 'PASS', s.verdict + ' ' + s.reason); return `${d} duplicates, verdict ${s.verdict}`; });
T('6 message delay', () => { const s = new S({ seed: 6, delayMin: 40, delayMax: 100 }); s.runAll(); const m = Math.max(...s.trace.filter(e => e.type === 'send').map(e => e.at - e.t)); ok(m >= 90, 'delays too small'); ok(s.verdict === 'PASS', s.verdict + ' ' + s.reason); return `max delay ${m}ms, verdict ${s.verdict}`; });
T('7 log replication', () => { const s = new S(quiet({ seed: 7 })); waitLeader(s); const rs = []; for (let i = 0; i < 5; i++) { rs.push(put(s, 'k' + i, 'v' + i)); s.runUntil(s.now + 200); }
  s.runUntil(s.now + 1000); ok(rs.every(r => r.status === 'Committed'), 'not all committed'); ok(s.converged(), 'logs differ'); ok(s.nodes[5].kv.get('k4') === 'v4', 'kv missing'); return `5 PUTs on all 5 nodes, log length ${s.nodes[1].last()}`; });
T('8 majority-based commitment', () => { const s = new S(quiet({ seed: 8 })); const L = waitLeader(s); s.runUntil(s.now + 300); const others = IDS.filter(i => i !== L);
  s.act({ type: 'crash', node: others[0] }); s.act({ type: 'crash', node: others[1] }); const a = put(s, 'x', '1', L); s.runUntil(s.now + 500); ok(a.status === 'Committed', '3 alive nodes must commit');
  s.act({ type: 'crash', node: others[2] }); const b = put(s, 'y', '2', L); s.runUntil(s.now + 2500); ok(b.status !== 'Committed', 'committed with only 2 of 5!'); ok(!s.viol, 'violation'); return `3 alive → ${a.status}; 2 alive → ${b.status} (${b.note})`; });
T('9 reproducibility (same seed)', () => { const a = new S({ seed: 48213, profile: 'chaotic' }), b = new S({ seed: 48213, profile: 'chaotic' }), c = new S({ seed: 48214, profile: 'chaotic' }); a.runAll(); b.runAll(); c.runAll();
  ok(a.hash === b.hash && a.trace.length === b.trace.length && a.verdict === b.verdict, 'same seed differed'); ok(a.hash !== c.hash, 'different seeds identical'); return `seed 48213 twice: ${a.trace.length} trace events, hash ${a.hash.toString(16)}, ${a.verdict}; seed 48214 differs`; });
T('10 replay of a saved simulation', () => { const a = new S({ seed: 77, profile: 'crashy' }); for (let i = 0; i < 300; i++) a.step(); a.act({ type: 'crash', node: 2 }); for (let i = 0; i < 400; i++) a.step();
  a.act({ type: 'req', op: 'PUT', key: 'name', value: 'Aman' }); a.act({ type: 'partition', nodes: [1, 3] }); for (let i = 0; i < 500; i++) a.step(); a.act({ type: 'heal' }); a.runAll();
  const rec = JSON.parse(JSON.stringify({ cfg: a.cfg, actions: a.recorded, hash: a.hash })); const b = new S(Object.assign({}, rec.cfg, { actions: rec.actions })); b.runAll();
  ok(b.hash === rec.hash && b.trace.length === a.trace.length, 'replay diverged'); return `${rec.actions.length} manual actions replayed, ${b.trace.length} events identical, ${b.verdict}`; });

console.log('\n--- baseline: correct Raft under the chaotic profile, seeds 1..60 ---');
const tally = {}; for (let sd = 1; sd <= 60; sd++) { const s = new S({ seed: sd, profile: 'chaotic' }); s.runAll(); tally[s.verdict] = (tally[s.verdict] || 0) + 1; if (s.verdict !== 'PASS') console.log('  seed', sd, s.verdict, s.reason.slice(0, 160)); }
console.log(JSON.stringify(tally)); T('baseline has no SAFETY_FAIL / HARNESS_FAIL', () => { ok(!tally.SAFETY_FAIL && !tally.HARNESS_FAIL, JSON.stringify(tally)); return JSON.stringify(tally); });

console.log('\n--- controlled bugs (test mode): the checker must catch each one ---');
const BUGS = { doubleVote: ['INV01'], commitTwo: ['INV05', 'INV09', 'INV04'], skipLogCheck: ['INV06', 'INV04', 'INV05', 'INV07', 'INV09'], regressCommit: ['INV03'] };
for (const [bug, allowed] of Object.entries(BUGS)) T('bug ' + bug, () => { for (let sd = 1; sd <= 400; sd++) { const s = new S({ seed: sd, profile: 'chaotic', bugs: [bug] }); s.runAll();
  if (s.verdict === 'SAFETY_FAIL') { ok(allowed.includes(s.viol.inv), 'caught by unexpected ' + s.viol.inv); return `caught at seed ${sd} by ${s.viol.inv} at t=${s.viol.t}ms: ${s.viol.msg}`; } } throw new Error('not detected in 400 seeds'); });
console.log(`\n${pass} passed, ${fail} failed`); process.exit(fail ? 1 : 0);
