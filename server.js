const express = require('express'), http = require('http');
const { WebSocketServer } = require('ws');
const app = express();
app.use(express.static(__dirname ));
const KEY = process.env.YT_API_KEY;
const ID_RE = /(?:youtu\.be\/|[?&]v=|shorts\/|embed\/)([\w-]{11})/;
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36';
const dec = s => String(s).replace(/&amp;/g, '&').replace(/&#39;/g, "'").replace(/&quot;/g, '"').replace(/&lt;/g, '<').replace(/&gt;/g, '>');
const errMsg = e => ((e && e.message) || String(e)) + (e && e.cause ? ' [' + (e.cause.code || e.cause.message) + ']' : '');
function collect(data) {
  const out = [], seen = new Set();
  (function walk(o) {
    if (!o || typeof o !== 'object' || out.length >= 10) return;
    const v = o.videoRenderer;
    if (v && v.videoId && !seen.has(v.videoId)) {
      seen.add(v.videoId);
      const t = v.title && v.title.runs && v.title.runs[0] && v.title.runs[0].text;
      out.push({ id: v.videoId, title: t || ('YouTube video ' + v.videoId), thumb: 'https://i.ytimg.com/vi/' + v.videoId + '/mqdefault.jpg' });
      return;
    }
    for (const k in o) walk(o[k]);
  })(data);
  return out;
}
function parseResults(html) {
  const m = html.match(/var ytInitialData = (\{.*?\});<\/script>/s) || html.match(/ytInitialData"\]\s*=\s*(\{.*?\});/s);
  if (!m) throw new Error('YouTube did not return a results page');
  return collect(JSON.parse(m[1]));
}
async function apiSearch(q) {
  const u = 'https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&videoEmbeddable=true&safeSearch=none&maxResults=10&q=' + encodeURIComponent(q) + '&key=' + KEY;
  const j = await (await fetch(u, { signal: AbortSignal.timeout(8000) })).json();
  if (j.error) throw new Error(j.error.message);
  return (j.items || []).map(i => ({ id: i.id.videoId, title: dec(i.snippet.title), thumb: i.snippet.thumbnails.medium.url }));
}
async function innertube(q) {
  const r = await fetch('https://www.youtube.com/youtubei/v1/search?prettyPrint=false&key=AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8', {
    method: 'POST', signal: AbortSignal.timeout(8000),
    headers: { 'content-type': 'application/json', 'user-agent': UA, origin: 'https://www.youtube.com', cookie: 'SOCS=CAI' },
    body: JSON.stringify({ context: { client: { clientName: 'WEB', clientVersion: '2.20250801.00.00', hl: 'en', gl: 'IN' } }, query: q, params: 'EgIQAQ==' })
  });
  if (!r.ok) throw new Error('HTTP ' + r.status);
  return collect(await r.json());
}
async function scrape(q) {
  const r = await fetch('https://www.youtube.com/results?hl=en&gl=IN&sp=EgIQAQ%3D%3D&search_query=' + encodeURIComponent(q), {
    signal: AbortSignal.timeout(8000),
    headers: { 'user-agent': UA, 'accept-language': 'en-US,en;q=0.9', cookie: 'CONSENT=YES+1; SOCS=CAI' }
  });
  if (!r.ok) throw new Error('HTTP ' + r.status);
  return parseResults(await r.text());
}
const cache = new Map();
app.get('/search', async (req, res) => {
  const q = String(req.query.q || '').trim();
  if (!q) return res.json({ results: [] });
  const m = q.match(ID_RE);
  if (m) {
    const id = m[1]; let title = 'YouTube video ' + id;
    try { title = (await (await fetch('https://www.youtube.com/oembed?format=json&url=https://www.youtube.com/watch?v=' + id, { signal: AbortSignal.timeout(6000) })).json()).title || title; } catch (e) {}
    return res.json({ results: [{ id, title, thumb: 'https://i.ytimg.com/vi/' + id + '/mqdefault.jpg' }] });
  }
  const ck = q.toLowerCase(), hit = cache.get(ck);
  if (hit && Date.now() - hit.t < 3600000) return res.json({ results: hit.results, via: 'cache' });
  const steps = [];
  if (KEY) steps.push(['api', apiSearch]);
  steps.push(['innertube', innertube], ['html', scrape]);
  const errors = [];
  for (const [name, fn] of steps) {
    try {
      const results = await fn(q);
      if (results.length) {
        if (cache.size > 300) cache.delete(cache.keys().next().value);
        cache.set(ck, { t: Date.now(), results });
        return res.json({ results, via: name });
      }
      errors.push(name + ': no results');
    } catch (e) { errors.push(name + ': ' + errMsg(e)); }
  }
  res.json({ results: [], error: 'Search failed (' + errors.join(' | ') + '). You can still paste a YouTube link.' });
});
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
setInterval(() => wss.clients.forEach(c => c.readyState === 1 && c.ping()), 25000);
const rooms = Object.create(null);
const getRoom = n => rooms[n] || (rooms[n] = { queue: [], cur: null, startedAt: 0, paused: false, pausedPos: 0, users: new Set(), chat: [], pend: Object.create(null) });
const pos = r => r.paused ? r.pausedPos : (Date.now() - r.startedAt) / 1000;
function presence(r, kind, name, except) {
  const s = JSON.stringify({ type: 'presence', kind, name });
  r.users.forEach(x => x !== except && x.ws.readyState === 1 && x.ws.send(s));
}
function bc(r) {
  const s = JSON.stringify({ type: 'state', now: Date.now(), queue: r.queue, cur: r.cur, startedAt: r.startedAt,
    paused: r.paused, pausedPos: r.pausedPos, users: [...r.users].map(u => ({ id: u.id, name: u.name })) });
  r.users.forEach(u => u.ws.readyState === 1 && u.ws.send(s));
}
function next(r) { r.cur = r.queue.shift() || null; r.startedAt = Date.now(); r.paused = false; r.pausedPos = 0; }
wss.on('connection', ws => {
  let u = null, r = null;
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'join') {
      r = getRoom(String(m.room || 'main').slice(0, 30));
      const sid = String(m.sid || '').slice(0, 40);
      let returning = false;
      if (sid) {
        for (const x of [...r.users]) if (x.sid === sid) { r.users.delete(x); returning = true; try { x.ws.close(4000, 'replaced'); } catch (e) {} }
        if (r.pend[sid]) { clearTimeout(r.pend[sid]); delete r.pend[sid]; returning = true; }
      }
      u = { ws, sid, name: String(m.name || 'guest').slice(0, 20), id: Math.random().toString(36).slice(2, 10) };
      r.users.add(u); if (!returning) presence(r, 'join', u.name, u); ws.send(JSON.stringify({ type: 'welcome', id: u.id })); ws.send(JSON.stringify({ type: 'history', msgs: r.chat })); return bc(r);
    }
    if (!r) return;
    if (m.type === 'chat') {
      const text = String(m.text || '').trim().slice(0, 300);
      if (!text) return;
      const msg = { name: u.name, text, t: Date.now() };
      r.chat.push(msg); if (r.chat.length > 50) r.chat.shift();
      const s = JSON.stringify({ type: 'chat', msg });
      r.users.forEach(x => x.ws.readyState === 1 && x.ws.send(s));
      return;
    }
    if (m.type === 'signal') {
      const t = [...r.users].find(x => x.id === m.to);
      if (t && t.ws.readyState === 1) t.ws.send(JSON.stringify({ type: 'signal', from: u.id, data: m.data }));
      return;
    }
    if (m.type === 'add' && m.video) { r.cur ? r.queue.push(m.video) : (r.queue.push(m.video), next(r)); }
    else if (m.type === 'skip') next(r);
    else if (m.type === 'pause' && r.cur && !r.paused) { r.pausedPos = pos(r); r.paused = true; }
    else if (m.type === 'resume' && r.cur && r.paused) { r.startedAt = Date.now() - r.pausedPos * 1000; r.paused = false; }
    else if (m.type === 'seek' && r.cur) { r.startedAt = Date.now() - m.pos * 1000; r.pausedPos = m.pos; }
    else if (m.type === 'ended' && r.cur && r.cur.id === m.id) next(r);
    else if (m.type === 'remove') r.queue.splice(m.index, 1);
    bc(r);
  });
  ws.on('close', () => { if (r && u) { if (r.users.delete(u)) { const key = u.sid || u.id, rm = r; clearTimeout(rm.pend[key]); rm.pend[key] = setTimeout(() => { delete rm.pend[key]; presence(rm, 'leave', u.name, null); }, 3000); } r.users.size ? bc(r) : delete rooms[Object.keys(rooms).find(k => rooms[k] === r)]; } });
});
server.listen(process.env.PORT || 3000, () => console.log('SyncJam running'));
