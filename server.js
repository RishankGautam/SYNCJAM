const express = require('express'), http = require('http');
const { WebSocketServer } = require('ws');
const YT = require('youtube-sr').default;
const app = express();
app.use(express.static(__dirname));
app.get('/search', async (req, res) => {
  try {
    const r = await YT.search(String(req.query.q || ''), { limit: 6, type: 'video' });
    res.json(r.map(v => ({ id: v.id, title: v.title, thumb: v.thumbnail && v.thumbnail.url, dur: v.durationFormatted })));
  } catch (e) { res.json([]); }
});
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const rooms = {};
const getRoom = n => rooms[n] || (rooms[n] = { queue: [], cur: null, startedAt: 0, paused: false, pausedPos: 0, users: new Set() });
const pos = r => r.paused ? r.pausedPos : (Date.now() - r.startedAt) / 1000;
function bc(r) {
  const s = JSON.stringify({ type: 'state', now: Date.now(), queue: r.queue, cur: r.cur, startedAt: r.startedAt,
    paused: r.paused, pausedPos: r.pausedPos, users: [...r.users].map(u => u.name) });
  r.users.forEach(u => u.ws.readyState === 1 && u.ws.send(s));
}
function next(r) { r.cur = r.queue.shift() || null; r.startedAt = Date.now(); r.paused = false; r.pausedPos = 0; }
wss.on('connection', ws => {
  let u = null, r = null;
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'join') {
      r = getRoom(String(m.room || 'main').slice(0, 30));
      u = { ws, name: String(m.name || 'guest').slice(0, 20) };
      r.users.add(u); return bc(r);
    }
    if (!r) return;
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
