const express = require('express'), http = require('http');
const { WebSocketServer } = require('ws');
const app = express();
app.use(express.static(__dirname + '/public'));
const KEY = process.env.YT_API_KEY;
const ID_RE = /(?:youtu\.be\/|[?&]v=|shorts\/|embed\/)([\w-]{11})/;
const dec = s => String(s).replace(/&amp;/g, '&').replace(/&#39;/g, "'").replace(/&quot;/g, '"').replace(/&lt;/g, '<').replace(/&gt;/g, '>');
function parseResults(html) {
  const m = html.match(/var ytInitialData = (\{.*?\});<\/script>/s) || html.match(/ytInitialData"\]\s*=\s*(\{.*?\});/s);
  if (!m) throw new Error('YouTube did not return a results page');
  const out = [], seen = new Set();
  (function walk(o) {
    if (!o || typeof o !== 'object' || out.length >= 8) return;
    const v = o.videoRenderer;
    if (v && v.videoId && !seen.has(v.videoId)) {
      seen.add(v.videoId);
      const t = v.title && v.title.runs && v.title.runs[0] && v.title.runs[0].text;
      out.push({ id: v.videoId, title: t || ('YouTube video ' + v.videoId), thumb: 'https://i.ytimg.com/vi/' + v.videoId + '/mqdefault.jpg' });
      return;
    }
    for (const k in o) walk(o[k]);
  })(JSON.parse(m[1]));
  return out;
}
async function scrape(q) {
  const r = await fetch('https://www.youtube.com/results?hl=en&gl=IN&sp=EgIQAQ%3D%3D&search_query=' + encodeURIComponent(q), {
    headers: { 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
      'accept-language': 'en-US,en;q=0.9', cookie: 'CONSENT=YES+1; SOCS=CAI' }
  });
  return parseResults(await r.text());
}
app.get('/search', async (req, res) => {
  const q = String(req.query.q || '').trim();
  if (!q) return res.json({ results: [] });
  try {
    const m = q.match(ID_RE);
    if (m) {
      const id = m[1]; let title = 'YouTube video ' + id;
      try { title = (await (await fetch('https://www.youtube.com/oembed?format=json&url=https://www.youtube.com/watch?v=' + id)).json()).title || title; } catch (e) {}
      return res.json({ results: [{ id, title, thumb: 'https://i.ytimg.com/vi/' + id + '/mqdefault.jpg' }] });
    }
    if (KEY) {
      try {
        const u = 'https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&videoEmbeddable=true&maxResults=8&q=' + encodeURIComponent(q) + '&key=' + KEY;
        const j = await (await fetch(u)).json();
        if (!j.error && j.items && j.items.length)
          return res.json({ results: j.items.map(i => ({ id: i.id.videoId, title: dec(i.snippet.title), thumb: i.snippet.thumbnails.medium.url })) });
      } catch (e) {}
    }
    const results = await scrape(q);
    res.json({ results, error: results.length ? undefined : 'No results found. Try different words or paste a YouTube link.' });
  } catch (e) { res.json({ results: [], error: 'Search failed: ' + (e.message || e) + '. You can still paste a YouTube link.' }); }
});
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const rooms = Object.create(null);
const fs = require('fs');
const FILE = __dirname + '/data.json';
const RU = process.env.UPSTASH_REDIS_REST_URL, RT = process.env.UPSTASH_REDIS_REST_TOKEN;
let DB = Object.create(null), canSave = true, saveT = null;
async function redis(cmd) {
  const r = await fetch(RU, { method: 'POST', headers: { Authorization: 'Bearer ' + RT }, body: JSON.stringify(cmd) });
  const j = await r.json();
  if (j.error) throw new Error(j.error);
  return j.result;
}
async function loadDB() {
  for (let i = 0; i < 3; i++) {
    try {
      let raw = null;
      if (RU && RT) raw = await redis(['GET', 'syncjam:db']);
      else if (fs.existsSync(FILE)) raw = fs.readFileSync(FILE, 'utf8');
      DB = Object.assign(Object.create(null), raw ? JSON.parse(raw) : {});
      console.log('Playlists loaded from ' + (RU && RT ? 'Upstash' : 'local file'));
      return;
    } catch (e) { console.log('Load failed:', e.message); await new Promise(r => setTimeout(r, 1000)); }
  }
  if (RU && RT) { canSave = false; console.log('Could not load playlists. Saving disabled to protect old data.'); }
}
async function writeDB() {
  const s = JSON.stringify(DB);
  if (RU && RT) await redis(['SET', 'syncjam:db', s]); else fs.writeFileSync(FILE, s);
}
function saveDB() {
  if (!canSave) return;
  clearTimeout(saveT);
  saveT = setTimeout(() => { saveT = null; writeDB().catch(e => console.log('Save failed:', e.message)); }, 800);
}
process.on('SIGTERM', async () => {
  if (saveT && canSave) { clearTimeout(saveT); try { await writeDB(); } catch (e) {} }
  process.exit(0);
});
const rid = () => Math.random().toString(36).slice(2, 10);
const VID = /^[\w-]{11}$/;
function sendPl(ws, r) { if (ws.readyState === 1) ws.send(JSON.stringify({ type: 'playlists', list: DB[r.name] || [] })); }
function pushPl(r) { saveDB(); r.users.forEach(x => sendPl(x.ws, r)); }
function handlePl(r, u, m) {
  const list = DB[r.name] || (DB[r.name] = []);
  if (m.type === 'pl_create') {
    const name = String(m.name || '').trim().slice(0, 40);
    if (!name || list.length >= 20) return;
    list.push({ id: rid(), name, owner: u.name, songs: [] });
    return pushPl(r);
  }
  const p = list.find(x => x.id === m.pid);
  if (!p) return;
  if (m.type === 'pl_add') {
    const v = m.video || {};
    if (!VID.test(String(v.id)) || p.songs.length >= 100 || p.songs.some(s => s.id === v.id)) return;
    p.songs.push({ id: v.id, title: String(v.title || 'YouTube video').slice(0, 200), thumb: 'https://i.ytimg.com/vi/' + v.id + '/mqdefault.jpg' });
    return pushPl(r);
  }
  if (m.type === 'pl_remove') { p.songs = p.songs.filter(s => s.id !== m.vid); return pushPl(r); }
  if (m.type === 'pl_delete') { if (p.owner !== u.name) return; DB[r.name] = list.filter(x => x !== p); return pushPl(r); }
  if (m.type === 'pl_play' && p.songs.length) { r.queue = p.songs.map(s => ({ ...s })); next(r); return bc(r); }
  if (m.type === 'pl_queue') {
    const s = p.songs.find(x => x.id === m.vid);
    if (s) { r.queue.push({ ...s }); if (!r.cur) next(r); bc(r); }
  }
}
const getRoom = n => rooms[n] || (rooms[n] = { name: n, queue: [], cur: null, startedAt: 0, paused: false, pausedPos: 0, users: new Set(), chat: [] });
const pos = r => r.paused ? r.pausedPos : (Date.now() - r.startedAt) / 1000;
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
      u = { ws, name: String(m.name || 'guest').slice(0, 20), id: Math.random().toString(36).slice(2, 10) };
      r.users.add(u); ws.send(JSON.stringify({ type: 'welcome', id: u.id })); ws.send(JSON.stringify({ type: 'history', msgs: r.chat })); sendPl(ws, r); return bc(r);
    }
    if (!r) return;
    if (typeof m.type === 'string' && m.type.startsWith('pl_')) return handlePl(r, u, m);
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
  ws.on('close', () => { if (r && u) { r.users.delete(u); r.users.size ? bc(r) : delete rooms[Object.keys(rooms).find(k => rooms[k] === r)]; } });
});
loadDB().then(() => server.listen(process.env.PORT || 3000, () => console.log('SyncJam running')));
