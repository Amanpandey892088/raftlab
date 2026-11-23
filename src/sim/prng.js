// Seeded PRNG (mulberry32). Each concern (network, elections, faults, workload) gets its own stream
// derived from the seed, so enabling one fault never shifts the random numbers used by another.
(function (R) {
  function hash(s) { let h = 2166136261 >>> 0; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619) >>> 0; } return h; }
  class PRNG {
    constructor(seed) { this.s = seed >>> 0; }
    next() { let t = this.s = (this.s + 0x6D2B79F5) >>> 0; t = Math.imul(t ^ t >>> 15, t | 1); t ^= t + Math.imul(t ^ t >>> 7, t | 61); return ((t ^ t >>> 14) >>> 0) / 4294967296; }
    int(a, b) { return a + Math.floor(this.next() * (b - a + 1)); }
  }
  R.PRNG = PRNG; R.hash = hash;
  R.stream = (seed, name) => new PRNG(hash(seed + ':' + name));
})(globalThis.RL = globalThis.RL || {});
