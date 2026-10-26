const express = require('express'), http = require('http');
const { WebSocketServer } = require('ws');
const YT = require('youtube-sr').default;
const app = express();
app.use(express.static(__dirname + '/public'));
const KEY = process.env.YT_API_KEY;
const ID_RE = /(?:youtu\.be\/|[?&]v=|shorts\/|embed\/)([\w-]{11})/;
const dec = s => String(s).replace(/&amp;/g, '&').replace(/&#39;/g, "'").replace(/&quot;/g, '"').replace(/&lt;/g, '<').replace(/&gt;/g, '>');
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
      const u = 'https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&videoEmbeddable=true&maxResults=8&q=' + encodeURIComponent(q) + '&key=' + KEY;
      const j = await (await fetch(u)).json();
      if (j.error) return res.json({ results: [], error: 'YouTube API: ' + j.error.message });
      return res.json({ results: (j.items || []).map(i => ({ id: i.id.videoId, title: dec(i.snippet.title), thumb: i.snippet.thumbnails.medium.url })) });
    }
    const r = await YT.search(q, { limit: 8, type: 'video' });
    const results = r.map(v => ({ id: v.id, title: v.title, thumb: v.thumbnail && v.thumbnail.url }));
    res.json({ results, error: results.length ? undefined : 'No results. Paste a YouTube link instead, or add a YT_API_KEY on the server.' });
  } catch (e) { res.json({ results: [], error: 'Search failed: ' + (e.message || e) }); }
});
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const rooms = {};
const getRoom = n => rooms[n] || (rooms[n] = { queue: [], cur: null, startedAt: 0, paused: false, pausedPos: 0, users: new Set() });
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
      r.users.add(u); ws.send(JSON.stringify({ type: 'welcome', id: u.id })); return bc(r);
    }
    if (!r) return;
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
