// Deterministic simulator: virtual clock, priority queue, network + fault injection, trace, replay,
// and the invariant checker. No DOM, no Date.now(), no Math.random(): same seed + config + actions => same trace.
(function (R) {
  const IDS = [1, 2, 3, 4, 5];
  const DEF = { delayMin: 5, delayMax: 30, dropP: 0, dupP: 0, reorderP: 0, crashP: 0, partitionP: 0, drift: false,
    durMin: 400, durMax: 1500, tick: 500, maxDown: 2, duration: 15000, settle: 8000, workload: true, wlEvery: 250, bugs: [], script: [] };
  const PROFILES = {
    healthy: {}, lossy: { dropP: .04, dupP: .02, reorderP: .05 }, partition: { partitionP: .5 }, crashy: { crashP: .35 },
    'clock-skew': { drift: true },
    chaotic: { dropP: .03, dupP: .03, reorderP: .08, delayMax: 60, crashP: .25, partitionP: .3, drift: true },
    scripted: { script: [{ t: 1500, type: 'crash', node: 'leader' }, { t: 3500, type: 'restart', node: 'crashed' }, { t: 5000, type: 'partition', nodes: [4, 5] }, { t: 7000, type: 'heal' }] }
  };
  const INV = { INV01: 'Election safety', INV02: 'Term monotonicity', INV03: 'Commit monotonicity', INV04: 'State machine safety', INV05: 'Durability',
    INV06: 'Log matching', INV07: 'Leader completeness', INV08: 'Convergence', INV09: 'Client acknowledgement' };

  class Simulator {
    constructor(cfg) {
      const c = this.c = Object.assign({}, DEF, PROFILES[cfg.profile || 'healthy'], cfg);
      this.cfg = Object.assign({}, cfg); delete this.cfg.actions;
      this.seed = Number(c.seed) || 0;
      this.rN = R.stream(this.seed, 'net'); this.rE = R.stream(this.seed, 'elect'); this.rF = R.stream(this.seed, 'fault'); this.rW = R.stream(this.seed, 'workload');
      this.q = new R.Heap(); this.now = 0; this.trace = []; this.hash = 2166136261; this.ledger = [null];
      this.reqs = []; this.open = []; this.nextReq = 1; this.part = null; this.processed = 0; this.recorded = []; this.pending = (c.actions || []).slice();
      this.verdict = null; this.reason = ''; this.viol = null; this.failed = {}; this.leaders = new Map(); this.lastTerm = {}; this.lastCommit = {};
      this.progressT = 0; this.quiet = false; this.checks = 0; this.onTrace = null;
      const self = this;
      const env = { rng: this.rE, send: (f, t, m) => self.send(f, t, m), timer: (id, tk, d, gen) => self.q.push({ t: self.now + d, kind: 'timer', id, tk, gen }),
        log: (ty, d, s, t) => self.log(ty, d, s, t), onLeader: n => self.onLeader(n), onCommit: (n, o, k) => self.onCommit(n, o, k) };
      this.nodes = {}; for (const i of IDS) { this.nodes[i] = new R.RaftNode(i, env, c.bugs); this.lastTerm[i] = 0; this.lastCommit[i] = 0; }
      if (c.drift) for (const i of IDS) { this.nodes[i].rate = Math.round((0.7 + this.rF.next() * 0.8) * 100) / 100; this.log('drift', `Node ${i} clock runs at ${this.nodes[i].rate}x`, i); }
      for (const i of IDS) this.nodes[i].resetElection();
      this.q.push({ t: c.tick, kind: 'tick' }); if (c.workload) this.q.push({ t: 100, kind: 'wl' });
      for (const s of c.script || []) this.q.push({ t: s.t, kind: 'act', a: s });
      this.q.push({ t: c.duration, kind: 'quiesce' });
    }
    // ---- trace -------------------------------------------------------------------------------------
    log(type, desc, src = 0, dst = 0) {
      const e = { id: this.trace.length + 1, t: this.now, type, src, dst, desc }; this.trace.push(e);
      const s = `${e.t}|${type}|${src}|${dst}|${desc}`; let h = this.hash; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); } this.hash = h >>> 0;
      if (this.onTrace) this.onTrace(e); return e;
    }
    nextT() { const e = this.q.peek(); return e ? e.t : Infinity; }
    cut(f, t) { return !!this.part && this.part.has(f) !== this.part.has(t); }
    isolated(id) { return !!this.part && (this.part.size <= 2 ? this.part.has(id) : !this.part.has(id)); }
    leader() { let b = 0, bt = -1; for (const i of IDS) { const n = this.nodes[i]; if (n.alive && n.role === 'leader' && n.term > bt) { b = i; bt = n.term; } } return b; }
    // ---- network (delay, drop, reorder, duplicate, partition) -----------------------------------------
    send(f, t, m) {
      m.from = f; const c = this.c, r = this.rN;
      if (this.cut(f, t)) { this.log('drop', `${m.type} ${f}→${t} dropped (partition)`, f, t); return; }
      if (r.next() < c.dropP) { this.log('drop', `${m.type} ${f}→${t} dropped (message loss)`, f, t); return; }
      const copies = r.next() < c.dupP ? 2 : 1;
      for (let i = 0; i < copies; i++) {
        let d = r.int(c.delayMin, c.delayMax); if (r.next() < c.reorderP) d += r.int(20, 120); if (i) d += r.int(1, 40);
        this.q.push({ t: this.now + d, kind: 'msg', to: t, m: Object.assign({}, m) });
        this.log('send', `${m.type} ${f}→${t}${i ? ' (duplicate)' : ''} +${d}ms`, f, t).at = this.now + d;
      }
    }
    // ---- stepping ------------------------------------------------------------------------------------
    step() {
      if (this.verdict) return null;
      try {
        while (this.pending.length && this.pending[0].afterSeq <= this.processed) this.inject(this.pending.shift());
        if (this.verdict) return null;
        const ev = this.q.pop(); if (!ev) { this.finish('LIVENESS_FAIL', 'event queue ran dry'); return null; }
        this.now = ev.t; this.processed++; this.dispatch(ev); this.postEvent(); this.check(); return ev;
      } catch (e) { this.verdict = 'HARNESS_FAIL'; this.reason = String(e && e.stack || e); this.log('verdict', 'HARNESS_FAIL: ' + e.message); return null; }
    }
    runUntil(t) { while (!this.verdict && this.nextT() <= t) this.step(); }
    runAll(max = 3e6) { let n = 0; while (!this.verdict && n++ < max) this.step(); if (!this.verdict) this.finish('TIMEOUT', 'event budget exhausted'); }
    act(a) { if (this.verdict) return; this.recorded.push(Object.assign({ afterSeq: this.processed }, a)); this.inject(a); }
    inject(a) { this.apply(a); this.postEvent(); this.check(); }
    finish(v, why) { if (this.verdict) return; this.verdict = v; this.reason = why; this.log('verdict', `${v}: ${why}`); }
    dispatch(ev) {
      const c = this.c;
      switch (ev.kind) {
        case 'msg': { const n = this.nodes[ev.to], m = ev.m;
          if (this.cut(m.from, ev.to)) { this.log('drop', `${m.type} ${m.from}→${ev.to} lost in flight (partition)`, m.from, ev.to); break; }
          if (!n.alive) { this.log('drop', `${m.type} ${m.from}→${ev.to} lost (Node ${ev.to} is down)`, m.from, ev.to); break; }
          this.log('recv', `${m.type} ${m.from}→${ev.to} (term ${m.term})`, m.from, ev.to); n.recv(m); break; }
        case 'timer': { const n = this.nodes[ev.id]; if (n.alive && n.gen === ev.gen) n.onTimer(ev.tk); break; }
        case 'tick': if (!this.quiet) { this.faultTick(); this.q.push({ t: this.now + c.tick, kind: 'tick' }); } break;
        case 'wl': if (!this.quiet) { const r = this.rW, op = ['PUT', 'PUT', 'GET', 'DELETE'][r.int(0, 3)];
          this.request(op, 'k' + r.int(1, 5), 'v' + r.int(100, 999), 'auto'); this.q.push({ t: this.now + c.wlEvery + r.int(0, 100), kind: 'wl' }); } break;
        case 'act': this.apply(ev.a); break;
        case 'quiesce': this.quiet = true; this.log('phase', 'Fault phase over: network healed, nodes restarted, clocks back to 1x');
          this.apply({ type: 'heal' }); for (const i of IDS) { if (!this.nodes[i].alive) this.apply({ type: 'restart', node: i }); this.nodes[i].rate = 1; }
          Object.assign(c, { dropP: 0, dupP: 0, reorderP: 0, delayMin: Math.min(c.delayMin, 30), delayMax: Math.min(c.delayMax, 30) });
          this.q.push({ t: this.now + 500, kind: 'conv' }); break;
        case 'conv':
          if (this.converged()) { this.finish('PASS', 'all invariants held and all nodes converged (INV08)'); break; }
          if (this.now >= c.duration + c.settle) {
            if (this.leader() && this.now - this.progressT < 1500) this.finish('TIMEOUT', 'time limit reached while the cluster was still catching up');
            else this.finish('LIVENESS_FAIL', this.leader() ? 'leader exists but nodes never caught up (INV08)' : 'no leader after faults stopped (INV08)');
            this.failed.INV08 = 1; break;
          }
          this.q.push({ t: this.now + 500, kind: 'conv' }); break;
      }
    }
    faultTick() {
      const c = this.c, r = this.rF;
      if (r.next() < c.crashP) { const up = IDS.filter(i => this.nodes[i].alive);
        if (IDS.length - up.length < c.maxDown) { const id = up[r.int(0, up.length - 1)]; this.apply({ type: 'crash', node: id });
          this.q.push({ t: this.now + r.int(c.durMin, c.durMax), kind: 'act', a: { type: 'restart', node: id } }); } }
      if (r.next() < c.partitionP && !this.part) { const pool = IDS.slice(), g = [], k = r.int(1, 2);
        while (g.length < k) g.push(pool.splice(r.int(0, pool.length - 1), 1)[0]);
        this.apply({ type: 'partition', nodes: g.sort() }); this.q.push({ t: this.now + r.int(c.durMin, c.durMax), kind: 'act', a: { type: 'heal' } }); }
      if (c.drift && r.next() < .2) this.apply({ type: 'drift', node: r.int(1, 5), rate: Math.round((0.6 + r.next()) * 100) / 100 });
    }
    // ---- fault / client actions (scheduled, scripted or manual) ---------------------------------------
    apply(a) {
      const id = x => x === 'leader' ? this.leader() : x === 'crashed' ? IDS.find(i => !this.nodes[i].alive) : Number(x), n = this.nodes[id(a.node)];
      switch (a.type) {
        case 'crash': if (n && n.alive) { n.alive = false; n.gen++; this.log('crash', `Node ${n.id} crashes`, n.id); } break;
        case 'restart': if (n && !n.alive) { n.alive = true; n.boot(); this.lastCommit[n.id] = 0; n.resetElection(); this.log('restart', `Node ${n.id} restarts (term ${n.term}, ${n.last()} log entries kept)`, n.id); } break;
        case 'partition': this.part = new Set(a.nodes.map(Number)); this.log('partition', `Network partition: nodes [${[...this.part].sort()}] cut off from the rest`); break;
        case 'heal': if (this.part) { this.part = null; this.log('heal', 'Network partition healed'); } break;
        case 'drift': if (n) { n.rate = a.rate; this.log('drift', `Node ${n.id} clock now runs at ${a.rate}x`, n.id); } break;
        case 'req': this.request(a.op, a.key, a.value, a.target || 'auto'); break;
      }
    }
    request(op, key, value, target) {
      const q = { id: this.nextReq++, op, key, value: op === 'PUT' ? value : '', status: 'Pending', t: this.now, node: 0, idx: 0, note: '', result: '' }; this.reqs.push(q);
      const d = `Client #${q.id} ${op} ${key}${op === 'PUT' ? '=' + value : ''}`, fail = m => { q.status = 'Failed'; q.note = m; this.log('client', `${d} failed: ${m}`); };
      const n = (target === 'auto' || !target) ? this.nodes[this.leader() || 0] : this.nodes[target];
      if (!n) return fail('no leader available'); if (!n.alive) return fail(`Node ${n.id} is down`); if (n.role !== 'leader') return fail(`Node ${n.id} is not the leader`);
      if (op === 'GET') { // simplification: leader answers from its applied state once it has committed in its own term
        if (n.commitIndex >= n.noopIdx) { q.status = 'Committed'; q.node = n.id; q.result = n.kv.has(key) ? n.kv.get(key) : '(not found)'; this.log('client', `${d} → ${q.result} (served by Node ${n.id})`, n.id); }
        else fail('leader has not committed an entry in its term yet'); return;
      }
      q.node = n.id; q.term = n.term; q.idx = n.propose(op, key, value, q.id); this.open.push(q);
      this.log('client', `${d} submitted to Node ${n.id} (log index ${q.idx})`, n.id);
    }
    holders(q) { let c = 0; for (const i of IDS) { const e = this.nodes[i].log[q.idx]; if (e && e.id === 'r' + q.id) c++; } return c; }
    postEvent() {
      if (!this.open.length) return;
      this.open = this.open.filter(q => {
        const n = this.nodes[q.node], e = n.log[q.idx], mine = e && e.id === 'r' + q.id, tag = `Client #${q.id} ${q.op} ${q.key}`;
        if (n.alive && !mine) { q.status = 'Failed'; q.note = 'entry overwritten by a newer leader'; this.log('client', `${tag} failed: ${q.note}`, q.node); return false; }
        if (n.alive && mine && n.commitIndex >= q.idx) { q.status = 'Committed'; this.log('ack', `${tag} committed (index ${q.idx}) and acknowledged to client`, q.node); this.checkAck(q); return false; }
        if (this.now - q.t > 2000) { q.status = 'Failed'; q.note = 'timeout: no majority reachable'; this.log('client', `${tag} failed: ${q.note}`, q.node); return false; }
        if (q.status === 'Pending' && this.holders(q) >= 3) { q.status = 'Replicated'; this.log('repl', `${tag} stored on a majority (3 of 5)`, q.node); }
        return true;
      });
    }
    // ---- invariant checker -----------------------------------------------------------------------------
    V(inv, node, msg) {
      if (this.viol) return; const e = this.log('safety', `${inv} ${INV[inv]} VIOLATED: ${msg}`, node);
      this.viol = { inv, node, eventId: e.id, t: this.now, msg }; this.failed[inv] = 1; this.verdict = 'SAFETY_FAIL'; this.reason = `${inv}: ${msg}`;
    }
    onLeader(n) {
      this.log('leader', `Node ${n.id} becomes leader for term ${n.term}`, n.id);
      const prev = this.leaders.get(n.term);
      if (prev && prev !== n.id) this.V('INV01', n.id, `Node ${prev} and Node ${n.id} both became leader in term ${n.term}`); this.leaders.set(n.term, n.id);
      for (let i = 1; i < this.ledger.length; i++) { const l = this.ledger[i], e = n.log[i];
        if (!e || e.term !== l.term || e.id !== l.id) { this.V('INV07', n.id, `new leader Node ${n.id} (term ${n.term}) is missing committed entry ${i} (${l.id}, term ${l.term}); it has ${e ? e.id + ' (term ' + e.term + ')' : 'no entry there'}`); break; } }
    }
    onCommit(n, old, k) {
      for (let i = old + 1; i <= k; i++) { const e = n.log[i], l = this.ledger[i];
        if (!l) { this.ledger[i] = { term: e.term, id: e.id, by: n.id }; this.progressT = this.now; this.log('commit', `Entry ${i} (${e.op === 'NOOP' ? 'no-op' : e.op + ' ' + e.key + (e.op === 'PUT' ? '=' + e.value : '')}) committed — first by Node ${n.id}`, n.id); }
        else if (l.id !== e.id || l.term !== e.term) this.V('INV04', n.id, `Node ${n.id} committed ${e.id} (term ${e.term}) at index ${i}, but Node ${l.by} had committed ${l.id} (term ${l.term}) there`); }
    }
    checkAck(q) {
      const k = this.holders(q), l = this.ledger[q.idx];
      if (k < 3 || !l || l.id !== 'r' + q.id) this.V('INV09', q.node, `client #${q.id} was told "committed" for index ${q.idx} but only ${k} of 5 nodes hold the entry (majority needs 3)`);
    }
    check() {
      this.checks++; const A = IDS.map(i => this.nodes[i]);
      for (const n of A) {
        if (n.term < this.lastTerm[n.id]) this.V('INV02', n.id, `Node ${n.id} term went from ${this.lastTerm[n.id]} down to ${n.term}`); this.lastTerm[n.id] = n.term;
        if (n.alive) { if (n.commitIndex < this.lastCommit[n.id]) this.V('INV03', n.id, `Node ${n.id} commitIndex went from ${this.lastCommit[n.id]} back to ${n.commitIndex}`); this.lastCommit[n.id] = n.commitIndex; }
      }
      for (let i = 1; i < this.ledger.length && !this.viol; i++) { const l = this.ledger[i]; let c = 0;
        for (const n of A) { const e = n.log[i]; if (e && e.id === l.id && e.term === l.term) c++; }
        if (c < 3) this.V('INV05', 0, `committed entry ${i} (${l.id}) is stored on only ${c} of 5 nodes — a committed entry must survive on a majority`); }
      for (let a = 0; a < 5 && !this.viol; a++) for (let b = a + 1; b < 5 && !this.viol; b++) {
        const X = A[a].log, Y = A[b].log, m = Math.min(X.length, Y.length); let top = 0;
        for (let i = m - 1; i >= 1; i--) if (X[i].term === Y[i].term) { top = i; break; }
        for (let j = 1; j <= top; j++) if (X[j].term !== Y[j].term || X[j].id !== Y[j].id) { this.V('INV06', b + 1, `Node ${a + 1} and Node ${b + 1} agree on term ${X[top].term} at index ${top} but differ at index ${j} (${X[j].id} vs ${Y[j].id})`); break; }
      }
    }
    converged() {
      const A = IDS.map(i => this.nodes[i]); if (A.some(n => !n.alive)) return false; const L = A[0].log, kv = JSON.stringify([...A[0].kv].sort());
      return A.every(n => n.log.length === L.length && n.commitIndex === n.last() && n.log.every((e, i) => e.term === L[i].term && e.id === L[i].id) && JSON.stringify([...n.kv].sort()) === kv);
    }
    status(inv) { return this.failed[inv] ? 'FAIL' : (inv === 'INV08' && !this.verdict ? 'pending' : 'PASS'); }
  }
  R.Simulator = Simulator; R.PROFILES = PROFILES; R.DEF = DEF; R.INV = INV; R.IDS = IDS;
})(globalThis.RL = globalThis.RL || {});
