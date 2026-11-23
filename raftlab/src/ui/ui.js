// Dashboard. Only talks to RL.Simulator (no Raft logic here). Plain SVG/DOM, no external libraries, so it works offline.
(function () {
  const $ = s => document.querySelector(s), IDS = RL.IDS, KEY = 'raftlab.history';
  const KNOBS = [['delayMax', 'Delay max (ms)', 5, 300, 5], ['dropP', 'Drop probability', 0, .5, .01], ['dupP', 'Duplicate probability', 0, .5, .01], ['reorderP', 'Reorder probability', 0, .5, .01],
    ['crashP', 'Crash chance per 500 ms', 0, 1, .05], ['partitionP', 'Partition chance per 500 ms', 0, 1, .05], ['durMax', 'Fault duration max (ms)', 500, 5000, 100]];
  const esc = s => String(s).replace(/[&<>]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
  let sim = null, vNow = 0, playing = false, flights = [], lastTs = 0, tlN = -1, tlAt = 0, replayOf = null, note = '';

  // ---- build static controls -------------------------------------------------------------------
  $('#profile').innerHTML = Object.keys(RL.PROFILES).map(p => `<option>${p}</option>`).join('');
  $('#knobs').innerHTML = KNOBS.map(k => `<label>${k[1]}<input id="k-${k[0]}" type="number" min="${k[2]}" max="${k[3]}" step="${k[4]}"></label>`).join('');
  $('#target').innerHTML = '<option value="auto">send to: current leader</option>' + IDS.map(i => `<option value="${i}">send to: Node ${i}</option>`).join('');
  $('#fnode').innerHTML = IDS.map(i => `<option value="${i}">Node ${i}</option>`).join('');
  function loadProfile() { const c = Object.assign({}, RL.DEF, RL.PROFILES[$('#profile').value]); KNOBS.forEach(k => $('#k-' + k[0]).value = c[k[0]]); $('#drift').checked = !!c.drift;
    $('#script').value = JSON.stringify(RL.PROFILES.scripted.script); }
  $('#profile').onchange = loadProfile; loadProfile();
  const P = [[260, 50], [405, 155], [350, 300], [170, 300], [115, 155]];
  const NS = 'http://www.w3.org/2000/svg', svg = $('#svg');
  const mk = (n, a, p) => { const e = document.createElementNS(NS, n); for (const k in a) e.setAttribute(k, a[k]); (p || svg).appendChild(e); return e; };
  const links = {}, nodes = {};
  for (let a = 1; a <= 5; a++) for (let b = a + 1; b <= 5; b++) links[a + '-' + b] = mk('line', { class: 'link', x1: P[a - 1][0], y1: P[a - 1][1], x2: P[b - 1][0], y2: P[b - 1][1] });
  const msgLayer = mk('g', {});
  IDS.forEach(i => { const g = mk('g', { class: 'nd' }); mk('circle', { cx: P[i - 1][0], cy: P[i - 1][1], r: 34 }, g);
    nodes[i] = { g, id: mk('text', { x: P[i - 1][0], y: P[i - 1][1] - 2, class: 'big' }, g), st: mk('text', { x: P[i - 1][0], y: P[i - 1][1] + 13, class: 'sm' }, g), sub: mk('text', { x: P[i - 1][0], y: P[i - 1][1] + 52, class: 'sm' }, g) }; });

  // ---- sim lifecycle ----------------------------------------------------------------------------
  function readCfg() {
    const c = { seed: +$('#seed').value || 0, profile: $('#profile').value, drift: $('#drift').checked };
    KNOBS.forEach(k => c[k[0]] = +$('#k-' + k[0]).value);
    if ($('#testmode').checked) c.bugs = [$('#bug').value];
    if (c.profile === 'scripted') { try { c.script = JSON.parse($('#script').value); } catch (e) { alert('Script JSON is invalid: ' + e.message); } }
    return c;
  }
  function create(cfg, actions) { sim = new RL.Simulator(actions ? Object.assign({}, cfg, { actions }) : cfg); sim.onTrace = e => { if (e.type === 'send') flights.push(e); };
    vNow = 0; flights = []; tlN = -1; note = ''; }
  function ensure() { if (!sim) create(readCfg()); }
  function advance(t) { let n = 0; while (!sim.verdict && sim.nextT() <= t && n++ < 4000) sim.step(); if (sim.verdict) done(); }
  function record() { return { seed: sim.seed, cfg: sim.cfg, actions: sim.recorded, hash: sim.hash, events: sim.trace.length, verdict: sim.verdict, profile: sim.cfg.profile || 'healthy' }; }
  function history() { try { return JSON.parse(localStorage.getItem(KEY) || '[]'); } catch (e) { return []; } }
  function done() {
    playing = false;
    if (replayOf) { const ok = sim.hash === replayOf.hash && sim.trace.length === replayOf.events; note = ok ? `Replay matched the saved run exactly (${sim.trace.length} events, hash ${replayOf.hash.toString(16)}).` : 'Replay DIVERGED from the saved run.'; replayOf = null; return; }
    try { const h = history(); h.unshift(record()); localStorage.setItem(KEY, JSON.stringify(h.slice(0, 30))); } catch (e) { /* storage may be unavailable */ }
  }
  function replay(rec) { replayOf = rec; create(rec.cfg, rec.actions); playing = true; }
  const b = (id, f) => $(id).onclick = f;
  b('#b-start', () => { create(readCfg()); playing = true; });
  b('#b-pause', () => playing = false);
  b('#b-resume', () => { ensure(); if (!sim.verdict) playing = true; });
  b('#b-reset', () => { playing = false; replayOf = null; create(readCfg()); });
  b('#b-step', () => { ensure(); playing = false; vNow = Math.max(vNow, sim.now) + 25; advance(vNow); });
  b('#b-next', () => { ensure(); playing = false; sim.step(); vNow = sim.now; if (sim.verdict) done(); });
  b('#b-all', () => { ensure(); playing = false; sim.runAll(); vNow = sim.now; done(); });
  b('#b-replay', () => { ensure(); if (!sim.verdict) { sim.runAll(); done(); } const h = history()[0]; replay(h && h.hash === sim.hash ? h : record()); });
  const act = a => { ensure(); sim.act(a); };
  b('#b-send', () => act({ type: 'req', op: $('#op').value, key: $('#key').value, value: $('#val').value, target: $('#target').value }));
  b('#b-crash', () => act({ type: 'crash', node: +$('#fnode').value })); b('#b-restart', () => act({ type: 'restart', node: +$('#fnode').value }));
  b('#b-drift', () => act({ type: 'drift', node: +$('#fnode').value, rate: +$('#fdrift').value }));
  b('#b-part', () => act({ type: 'partition', nodes: $('#fpart').value.split(',').map(Number).filter(n => n >= 1 && n <= 5) })); b('#b-heal', () => act({ type: 'heal' }));
  $('#shownet').onchange = () => tlN = -1;

  // ---- rendering ----------------------------------------------------------------------------------
  function render() {
    if (!sim) return;
    const N = sim.nodes, L = sim.leader(), alive = IDS.filter(i => N[i].alive);
    for (const k in links) { const [a, c] = k.split('-').map(Number); links[k].setAttribute('class', 'link' + (sim.cut(a, c) ? ' cut' : '')); }
    IDS.forEach(i => { const n = N[i], x = nodes[i], cls = !n.alive ? 'crashed' : sim.isolated(i) ? 'disc' : n.role;
      x.g.setAttribute('class', `nd ${cls}` + (sim.viol && sim.viol.node === i ? ' viol' : '')); x.id.textContent = 'Node ' + i;
      x.st.textContent = !n.alive ? 'crashed' : (sim.isolated(i) ? 'disconnected · ' : '') + n.role; x.sub.textContent = `term ${n.term} · log ${n.last()} · commit ${n.commitIndex}` + (n.rate !== 1 ? ` · ${n.rate}x` : ''); });
    msgLayer.innerHTML = ''; flights = flights.filter(f => f.at > vNow - 20);
    for (const f of flights) { if (f.t > vNow || f.at <= vNow) continue; const p = (vNow - f.t) / Math.max(1, f.at - f.t), A = P[f.src - 1], B = P[f.dst - 1];
      mk('circle', { cx: A[0] + (B[0] - A[0]) * p, cy: A[1] + (B[1] - A[1]) * p, r: 4, fill: /Vote/.test(f.desc) ? '#f0883e' : /Reply/.test(f.desc) ? '#8b96a5' : '#58a6ff' }, msgLayer); }
    const maxC = Math.max(0, ...alive.map(i => N[i].commitIndex)), term = Math.max(0, ...IDS.map(i => N[i].term));
    $('#info').innerHTML = [['Leader', L ? 'Node ' + L : 'none'], ['Term', term], ['Commit index', maxC], ['Active nodes', alive.length], ['Crashed nodes', 5 - alive.length], ['Committed entries', sim.ledger.length - 1 + ' (incl. no-ops)']]
      .map(([a, c]) => `<div><b>${c}</b><span>${a}</span></div>`).join('') + `<div style="grid-column:1/4"><span>Virtual time</span> <b>${Math.round(vNow)} ms</b> · ${sim.trace.length} trace events · seed ${sim.seed} · ${sim.cfg.profile || 'healthy'}${sim.c.bugs.length ? ' · BUG: ' + sim.c.bugs : ''}</div>`;
    $('#reqs').innerHTML = '<table><tr><th>#</th><th>Request</th><th>Status</th><th>Detail</th></tr>' + sim.reqs.slice(-8).reverse().map(q =>
      `<tr><td>${q.id}</td><td>${q.op} ${esc(q.key)}${q.op === 'PUT' ? '=' + esc(q.value) : ''}</td><td class="s-${q.status}">${q.status}</td><td>${esc(q.result || q.note || (q.node ? 'Node ' + q.node + ' idx ' + q.idx : ''))}</td></tr>`).join('') + '</table>';
    $('#safety').innerHTML = `<div class="verdict-box"><b class="${sim.verdict || 'pending'}">${sim.verdict || 'RUNNING'}</b> ${esc(sim.reason)}${note ? '<br>' + esc(note) : ''}${sim.viol ? `<br>Failed at event #${sim.viol.eventId} (${sim.viol.t} ms), node ${sim.viol.node || 'cluster'}` : ''}</div>`
      + '<table>' + Object.keys(RL.INV).map(k => `<tr><td>${k}</td><td>${RL.INV[k]}</td><td class="${sim.status(k) === 'FAIL' ? 'FAIL' : sim.status(k)}">${sim.status(k)}</td></tr>`).join('') + `</table><small class="pending">Checked after every event (${sim.checks} checks). INV08 is evaluated once faults stop.</small>`;
    if (sim.trace.length !== tlN && performance.now() - tlAt > 120) renderTimeline();
  }
  function renderTimeline() {
    tlAt = performance.now(); tlN = sim.trace.length; const all = $('#shownet').checked, box = $('#tl'), stick = box.scrollTop + box.clientHeight >= box.scrollHeight - 30;
    const rows = []; for (let i = sim.trace.length - 1; i >= 0 && rows.length < 300; i--) { const e = sim.trace[i]; if (all || (e.type !== 'send' && e.type !== 'recv')) rows.push(e); }
    box.innerHTML = rows.reverse().map(e => `<div class="r ${e.type}${sim.viol && sim.viol.eventId === e.id ? ' bad' : ''}"><b>${e.t} ms</b> – ${esc(e.desc)}</div>`).join(''); if (stick) box.scrollTop = box.scrollHeight;
  }
  function renderHist() {
    const h = history(); $('#hist').innerHTML = h.length ? '<table>' + h.slice(0, 8).map((r, i) => `<tr><td>seed ${r.seed}</td><td>${r.profile}${r.cfg.bugs ? ' +bug' : ''}</td><td class="${r.verdict}">${r.verdict}</td><td>${r.actions.length} actions</td><td><button data-i="${i}">Replay</button></td></tr>`).join('') + '</table>' : '<small class="pending">Finished runs are saved here (browser localStorage).</small>';
    $('#hist').querySelectorAll('button').forEach(x => x.onclick = () => replay(history()[+x.dataset.i]));
  }
  let histN = -1;
  function frame(ts) { if (playing && sim && !sim.verdict) { vNow += Math.min(100, ts - lastTs) * +$('#speed').value; advance(vNow); } lastTs = ts;
    const hn = history().length + (sim ? sim.trace.length * 0 : 0) + (sim && sim.verdict ? 1 : 0) * 1000; if (hn !== histN) { histN = hn; renderHist(); } render(); requestAnimationFrame(frame); }
  create(readCfg()); requestAnimationFrame(frame);
})();
