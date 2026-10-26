const express = require('express'), http = require('http');
const { WebSocketServer } = require('ws');
const app = express();
app.use(express.static(__dirname ));
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
const rooms = {};
const getRoom = n => rooms[n] || (rooms[n] = { queue: [], cur: null, startedAt: 0, paused: false, pausedPos: 0, users: new Set(), chat: [] });
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
      r.users.add(u); ws.send(JSON.stringify({ type: 'welcome', id: u.id })); ws.send(JSON.stringify({ type: 'history', msgs: r.chat })); return bc(r);
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
  ws.on('close', () => { if (r && u) { r.users.delete(u); r.users.size ? bc(r) : delete rooms[Object.keys(rooms).find(k => rooms[k] === r)]; } });
});
server.listen(process.env.PORT || 3000, () => console.log('SyncJam running'));
