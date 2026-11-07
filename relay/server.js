const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;
const rooms = new Map();
const page = (f) => fs.readFileSync(path.join(__dirname, 'public', f));
const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/' || u === '/tv') { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' }); return res.end(page('tv.html')); }
  if (u === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  res.writeHead(404); res.end('404');
});
const wss = new WebSocketServer({ server, maxPayload: 2 * 1024 * 1024 });
function room(code) {
  if (!rooms.has(code)) rooms.set(code, { host: null, tvs: new Set(), game: '', started: false });
  return rooms.get(code);
}
const send = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };
const status = (r) => ({ t: 'status', host: !!r.host, tvs: r.tvs.size, game: r.game, started: r.started });
function broadcast(r) { send(r.host, status(r)); r.tvs.forEach((t) => send(t, status(r))); }
wss.on('connection', (ws, req) => {
  const q = new URL(req.url, 'http://x').searchParams;
  const role = q.get('role');
  const code = (q.get('room') || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
  if (!code || (role !== 'host' && role !== 'tv')) return ws.close();
  const r = room(code);
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  if (role === 'host') {
    if (r.host && r.host !== ws && r.host.readyState === 1) r.host.close();
    r.host = ws; r.game = ''; r.started = false;
  } else r.tvs.add(ws);
  broadcast(r);
  ws.on('message', (data, isBinary) => {
    if (role === 'host') {
      if (isBinary) { r.tvs.forEach((t) => { if (t.readyState === 1 && t.bufferedAmount < 256 * 1024) t.send(data, { binary: true }); }); return; }
      try {
        const m = JSON.parse(data.toString());
        if (m.t === 'game') { r.game = m.name || ''; r.started = false; broadcast(r); }
        else if (m.t === 'started') { r.started = true; broadcast(r); }
        else r.tvs.forEach((t) => send(t, m));
      } catch (e) {}
    } else if (!isBinary && r.host) r.host.send(data.toString());
  });
  ws.on('close', () => {
    if (role === 'host') { if (r.host === ws) { r.host = null; r.game = ''; r.started = false; } } else r.tvs.delete(ws);
    broadcast(r);
    if (!r.host && r.tvs.size === 0) rooms.delete(code);
  });
});
setInterval(() => { wss.clients.forEach((ws) => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; ws.ping(); }); }, 25000);
server.listen(PORT, () => console.log('AllCore relay :' + PORT));
