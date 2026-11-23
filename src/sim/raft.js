// One Raft node. It never touches real time, network or randomness: everything goes through `env`
// (rng, send, timer, log, onLeader, onCommit), which the simulator provides.
// Persistent state (term, votedFor, log) survives a crash; volatile state is rebuilt by boot().
(function (R) {
  const HB = 50, EMIN = 150, EMAX = 300;
  class RaftNode {
    constructor(id, env, bugs) {
      this.id = id; this.env = env; this.bugs = bugs || [];
      this.term = 0; this.votedFor = null; this.log = [{ term: 0, id: '0' }]; // index 0 = sentinel
      this.alive = true; this.rate = 1; this.gen = 0; this.boot();
    }
    boot() { this.role = 'follower'; this.commitIndex = 0; this.applied = 0; this.kv = new Map(); this.votes = new Set(); this.next = {}; this.match = {}; this.noopIdx = 0; }
    last() { return this.log.length - 1; }
    lastTerm() { return this.log[this.log.length - 1].term; }
    quorum() { return this.bugs.includes('commitTwo') ? 2 : 3; }      // majority of 5 is ALWAYS 3 (bug flag aside)
    peers() { return [1, 2, 3, 4, 5].filter(i => i !== this.id); }
    timer(kind, d) { this.gen++; this.env.timer(this.id, kind, Math.max(1, Math.round(d / this.rate)), this.gen); } // clock drift: rate scales timers
    resetElection() { this.timer('election', this.env.rng.int(EMIN, EMAX)); }
    onTimer(k) {
      if (k === 'election') { if (this.role !== 'leader') this.startElection(); }
      else if (this.role === 'leader') { this.broadcast(); this.timer('heartbeat', HB); }
    }
    startElection() {
      this.term++; this.role = 'candidate'; this.votedFor = this.id; this.votes = new Set([this.id]);
      this.env.log('election', `Node ${this.id} starts election for term ${this.term}`, this.id);
      this.resetElection();
      for (const p of this.peers()) this.env.send(this.id, p, { type: 'RequestVote', term: this.term, lli: this.last(), llt: this.lastTerm() });
    }
    stepDown(term) {
      const was = this.role; this.term = term; this.votedFor = null; this.role = 'follower';
      if (was !== 'follower') { this.env.log('role', `Node ${this.id} steps down to follower (term ${this.term})`, this.id); this.resetElection(); }
    }
    becomeLeader() {
      this.role = 'leader'; this.leaderId = this.id;
      for (const p of this.peers()) { this.next[p] = this.last() + 1; this.match[p] = 0; }
      this.log.push({ term: this.term, id: `noop-${this.term}-${this.id}`, op: 'NOOP' }); this.noopIdx = this.last();
      this.env.onLeader(this); this.broadcast(); this.timer('heartbeat', HB);
    }
    broadcast() { for (const p of this.peers()) this.sendAppend(p); }
    sendAppend(p) {
      const ni = Math.min(this.next[p], this.last() + 1), prev = ni - 1;
      this.env.send(this.id, p, { type: 'AppendEntries', term: this.term, pli: prev, plt: this.log[prev].term, entries: this.log.slice(ni, ni + 16), lc: this.commitIndex });
    }
    propose(op, key, value, req) {
      this.log.push({ term: this.term, id: 'r' + req, op, key, value });
      for (const p of this.peers()) this.sendAppend(p);
      return this.last();
    }
    recv(m) {
      if (m.term > this.term) this.stepDown(m.term);
      const me = this.id;
      if (m.type === 'RequestVote') {
        const upToDate = m.llt > this.lastTerm() || (m.llt === this.lastTerm() && m.lli >= this.last());
        const free = this.votedFor === null || this.votedFor === m.from || this.bugs.includes('doubleVote'); // BUG: vote twice in a term
        const ok = m.term >= this.term && free && upToDate;
        if (ok) { this.votedFor = m.from; this.resetElection(); }
        this.env.send(me, m.from, { type: 'RequestVoteReply', term: this.term, granted: ok });
      } else if (m.type === 'RequestVoteReply') {
        if (this.role === 'candidate' && m.term === this.term && m.granted) {
          this.votes.add(m.from);
          this.env.log('vote', `Node ${me} has ${this.votes.size} vote(s) for term ${this.term}`, me, m.from);
          if (this.votes.size >= this.quorum()) this.becomeLeader();
        }
      } else if (m.type === 'AppendEntries') {
        if (m.term < this.term) { this.env.send(me, m.from, { type: 'AppendEntriesReply', term: this.term, success: false, match: 0 }); return; }
        this.role = 'follower'; this.leaderId = m.from; this.resetElection();
        const skip = this.bugs.includes('skipLogCheck');                // BUG: accept entries without the prevLog consistency check
        const ok = skip || (m.pli <= this.last() && this.log[m.pli].term === m.plt);
        if (!ok) { this.env.send(me, m.from, { type: 'AppendEntriesReply', term: this.term, success: false, match: 0 }); return; }
        let idx = skip ? Math.min(m.pli, this.last()) : m.pli;
        for (const e of m.entries) {
          idx++;
          if (idx <= this.last()) { if (this.log[idx].term !== e.term) { this.log.length = idx; this.log.push(e); } }
          else this.log.push(e);
        }
        if (this.bugs.includes('regressCommit')) this.setCommit(Math.min(m.lc, idx));   // BUG (found in our own prototype): commit can move backwards
        else if (m.lc > this.commitIndex) this.setCommit(Math.max(this.commitIndex, Math.min(m.lc, idx)));
        this.env.send(me, m.from, { type: 'AppendEntriesReply', term: this.term, success: true, match: idx });
      } else if (m.type === 'AppendEntriesReply') {
        if (this.role !== 'leader' || m.term !== this.term) return;
        const f = m.from;
        if (m.success) { this.match[f] = Math.max(this.match[f], m.match); this.next[f] = this.match[f] + 1; this.advance(); if (this.next[f] <= this.last()) this.sendAppend(f); }
        else { this.next[f] = Math.max(this.match[f] + 1, this.next[f] - 1); this.sendAppend(f); }
      }
    }
    advance() { // commit the highest index stored on a majority whose entry is from the current term
      for (let n = this.last(); n > this.commitIndex; n--) {
        if (this.log[n].term !== this.term) continue;
        let c = 1; for (const p of this.peers()) if (this.match[p] >= n) c++;
        if (c >= this.quorum()) { this.setCommit(n); break; }
      }
    }
    setCommit(n) {
      const old = this.commitIndex; this.commitIndex = n;
      if (n !== old) this.env.onCommit(this, old, n);
      while (this.applied < this.commitIndex) { this.applied++; this.apply(this.log[this.applied]); }
    }
    apply(e) { if (e.op === 'PUT') this.kv.set(e.key, e.value); else if (e.op === 'DELETE') this.kv.delete(e.key); }
  }
  R.RaftNode = RaftNode;
})(globalThis.RL = globalThis.RL || {});
