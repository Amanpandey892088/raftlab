// Min-heap ordered by (virtual time, insertion sequence). The sequence is the fixed tie-break for same-time events.
(function (R) {
  class Heap {
    constructor() { this.a = []; this.n = 0; }
    less(x, y) { return x.t < y.t || (x.t === y.t && x.seq < y.seq); }
    push(it) {
      it.seq = this.n++; const a = this.a; a.push(it); let i = a.length - 1;
      while (i > 0) { const p = (i - 1) >> 1; if (!this.less(a[i], a[p])) break; [a[i], a[p]] = [a[p], a[i]]; i = p; }
    }
    peek() { return this.a[0]; }
    pop() {
      const a = this.a, top = a[0], last = a.pop();
      if (a.length) { a[0] = last; let i = 0; for (;;) { const l = 2 * i + 1, r = l + 1; let m = i;
        if (l < a.length && this.less(a[l], a[m])) m = l; if (r < a.length && this.less(a[r], a[m])) m = r;
        if (m === i) break; [a[i], a[m]] = [a[m], a[i]]; i = m; } }
      return top;
    }
    get size() { return this.a.length; }
  }
  R.Heap = Heap;
})(globalThis.RL = globalThis.RL || {});
