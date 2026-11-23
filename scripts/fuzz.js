// Usage: node scripts/fuzz.js [--count 200] [--start 1] [--profile chaotic]
// Runs many seeds against the (bug-free) cluster. Exits 1 if any seed is not PASS and prints the seed to replay.
['prng', 'queue', 'raft', 'sim'].forEach(f => require('../src/sim/' + f + '.js'));
const a = process.argv.slice(2), get = (k, d) => { const i = a.indexOf('--' + k); return i >= 0 ? a[i + 1] : d; };
const count = +get('count', 200), start = +get('start', 1), profile = get('profile', 'chaotic');
const tally = {}; let bad = 0, events = 0;
for (let seed = start; seed < start + count; seed++) {
  const s = new RL.Simulator({ seed, profile }); s.runAll(); events += s.trace.length;
  tally[s.verdict] = (tally[s.verdict] || 0) + 1;
  if (s.verdict !== 'PASS') { bad++; console.log(`seed ${seed}: ${s.verdict} — ${s.reason.slice(0, 200)}`); }
}
console.log(`profile=${profile} seeds=${start}..${start + count - 1} events=${events}`, JSON.stringify(tally));
process.exit(bad ? 1 : 0);
