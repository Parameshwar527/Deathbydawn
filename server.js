'use strict';
// Death by Dawn: game page + WebSocket relay in ONE small Node server.
// Same job and same protocol as worker.js (Cloudflare), but it runs on a machine YOU choose (e.g. a server in India).
// Two kinds of room live side by side:
//  1) RELAY rooms (role=host / role=client): the game runs on the host's device; this only passes messages along and never parses game data.
//  2) SERVER-HOSTED rooms (role=create, then plain joins): THIS server runs the game itself (the game's own code, headless, see headless.js);
//     every player, including whoever created the room, is just a player. The room keeps running if the creator leaves.
//
// Frames (text), host <-> relay:
//   relay -> host:  "R"                 you are attached
//                   "J<id>"             a player connected
//                   "L<id>"             a player disconnected
//                   "M<id>\n<payload>"  message(s) from player <id>
//   host -> relay:  "B<payload>"        send to every player
//                   "T<id>\n<payload>"  send to one player
//                   "K<id>"             disconnect that player
// player <-> relay: "R" once attached, then the raw game messages.
// Close codes: 4004 no such room, 4009 room code taken, 4010 room full, 4001 host left, 4002 kicked, 4003 replaced.

const http = require('http');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { WebSocketServer } = require('ws');
const { loadGame } = require('./headless');

const PORT = +process.env.PORT || 8080;
const PUBLIC = path.join(__dirname, 'public');
const GAME_FILE = path.join(PUBLIC, 'index.html');   // the same page the browsers get; its <script> is also what runs the server-hosted rooms
const MAX_HOSTED = +process.env.MAX_HOSTED || 40;                   // server-hosted rooms at once (each one costs ~7 MB and ~5% of a core)
const HOSTED_IDLE_MS = +process.env.HOSTED_IDLE_MS || 90000;        // a server-hosted room with nobody connected is closed after this (longer than the game's reconnect window)
const GRACE_MS = 45000;           // host link dropped (not closed on purpose): keep players connected this long so the host can come back
const MAX_PLAYERS = 16;           // per room (same as worker.js)
const MAX_ROOMS = 200;            // safety cap so a stranger cannot fill the server's memory
const MAX_IN = 65536;             // a player can never send more than this per message
const SNAP_SKIP_BUF = 24 * 1024;  // a player whose link is backed up by more than this many bytes skips SNAPSHOT frames only (they are stale by the time they arrive); everything else is always delivered
const PING_MS = 15000;            // heartbeat: finds dead links quickly

const rooms = new Map();
const safeSend = (ws, s) => { try { if (ws && ws.readyState === 1) ws.send(s); } catch (e) {} };
const safeClose = (ws, code, why) => { try { ws.close(code, why); } catch (e) {} };

class Room {
  constructor(code) { this.code = code; this.host = null; this.hostKey = ''; this.clients = new Map(); this.n = 0; this.grace = null; }

  attachHost(ws, key) {
    if (this.host) {
      if (this.hostKey !== key) return safeClose(ws, 4009, 'taken');             // somebody else already owns this code
      const old = this.host; this.host = null; safeClose(old, 4003, 'replaced');   // same host coming back before the old link timed out
    } else if (this.hostKey && this.hostKey !== key) return safeClose(ws, 4009, 'taken');
    clearTimeout(this.grace); this.grace = null;
    this.host = ws; this.hostKey = key;
    safeSend(ws, 'R');
    ws.on('message', (data, isBinary) => { if (isBinary || this.host !== ws) return; this.fromHost(data.toString()); });
    ws.on('close', code => this.hostGone(ws, code));
  }

  attachClient(ws) {
    if (!this.host) return safeClose(ws, 4004, 'noroom');
    if (this.clients.size >= MAX_PLAYERS) return safeClose(ws, 4010, 'full');
    const id = 'c' + (++this.n).toString(36);
    this.clients.set(id, ws);
    safeSend(ws, 'R');
    safeSend(this.host, 'J' + id);
    ws.on('message', (data, isBinary) => {
      if (isBinary || this.clients.get(id) !== ws) return;
      const d = data.toString(); if (d.length > MAX_IN) return;
      if (this.host) safeSend(this.host, 'M' + id + '\n' + d);
    });
    ws.on('close', () => { if (this.clients.get(id) === ws) { this.clients.delete(id); if (this.host) safeSend(this.host, 'L' + id); } });
  }

  fromHost(d) {
    if (!d) return;
    const c = d.charCodeAt(0);
    if (c === 66) {                                   // B: broadcast
      const p = d.slice(1);
      const snap = p.startsWith('{"t":"s",') && p.indexOf('\n') < 0;   // a plain position snapshot: safe to skip for a player who is badly backed up
      for (const ws of this.clients.values()) { if (snap && ws.bufferedAmount > SNAP_SKIP_BUF) continue; safeSend(ws, p); }
    } else if (c === 84) {                            // T: to one player
      const i = d.indexOf('\n'); if (i < 0) return;
      safeSend(this.clients.get(d.slice(1, i)), d.slice(i + 1));
    } else if (c === 75) {                            // K: kick
      const id = d.slice(1), ws = this.clients.get(id); if (ws) { this.clients.delete(id); safeClose(ws, 4002, 'kicked'); }
    }
  }

  hostGone(ws, code) {
    if (this.host !== ws) return;                      // an old, already replaced socket
    this.host = null;
    if (code === 1000) return this.shutdown();         // the host left on purpose: close the room now
    clearTimeout(this.grace); this.grace = setTimeout(() => this.shutdown(), GRACE_MS);   // network blip: wait for the host
  }

  shutdown() {
    clearTimeout(this.grace); this.grace = null; this.hostKey = '';
    const cs = [...this.clients.values()]; this.clients.clear();
    for (const ws of cs) safeClose(ws, 4001, 'hostleft');
    if (rooms.get(this.code) === this) { rooms.delete(this.code); console.log(`[room ${this.code}] closed (${rooms.size} open)`); }
  }
}

/* ---------------- server-hosted rooms: the game itself runs here ---------------- */
const hosted = new Map();   // room code -> HostedRoom

class HostedRoom {
  constructor(code) {
    this.code = code; this.clients = new Map(); this.n = 0; this.idle = null; this.dead = false; this.errLogged = 0;
    const g = this.game = loadGame(GAME_FILE, { onError: (e, where, n) => this.onGameError(e, where, n), console: { log() {}, info() {}, debug() {}, warn() {}, error: (...a) => { if (this.errLogged++ < 20) console.error(`[room ${code}] game:`, ...a); } } });
    g.ctx.__net = { bc: (m, d) => this.bc(m, d), to: (id, m) => this.to(id, m), kick: id => this.kick(id) };
    // the game talks to the "network" through T: point it at this room's sockets, then start it exactly like a browser host does (minus the local player)
    g.run(`T.broadcast = (m, d) => __net.bc(m, d); T.sendTo = (i, m) => __net.to(i, m); T.kick = i => __net.kick(i);
      T.close = T.host = T.join = T.warm = () => {};
      S.role = 'host'; S.hosted = true; S.leaving = false; S.lobbyOn = true; S.code = ${JSON.stringify(code)}; HP.clear(); tuneRestore(); Timers.start(hostTick, onHb);`);
    this.parse = g.run('JSON.parse');   // parse INSIDE the game's realm so its objects behave like its own (instanceof etc.)
    this.T = g.run('T');                // the game's transport object (a script-level const, so it is not a property of ctx)
  }
  onGameError(e, where, n) { if (n <= 5 || n % 500 === 0) console.error(`[room ${this.code}] game error #${n} in ${where}:`, e && e.stack ? e.stack.split('\n').slice(0, 4).join('\n') : e); }
  call(fn) { try { fn(); } catch (e) { this.onGameError(e, 'call', this.game.errors + 1); } }

  attach(ws) {
    if (this.dead) return safeClose(ws, 4004, 'noroom');
    if (this.clients.size >= MAX_PLAYERS) return safeClose(ws, 4010, 'full');
    clearTimeout(this.idle); this.idle = null;
    const id = 'c' + (++this.n).toString(36);
    this.clients.set(id, ws);
    safeSend(ws, 'R');
    const T = this.T;
    this.call(() => T.onPeerJoin(id));
    ws.on('message', (data, isBinary) => {
      if (isBinary || this.clients.get(id) !== ws || this.dead) return;
      const d = data.toString(); if (d.length > MAX_IN) return;
      for (const part of d.split('\n')) { if (!part) continue; let m; try { m = this.parse(part); } catch (e) { continue; } this.call(() => T.onMessage(id, m)); }
    });
    ws.on('close', () => {
      if (this.clients.get(id) !== ws) return;         // kicked on purpose: the game already knows
      this.clients.delete(id); if (!this.dead) this.call(() => T.onPeerLeave(id));
      if (!this.clients.size && !this.dead) { clearTimeout(this.idle); this.idle = setTimeout(() => this.destroy('empty'), HOSTED_IDLE_MS); }
    });
  }
  bc(m, droppable) {   // game -> every player (droppable = a plain position snapshot, skipped for a player whose link is badly backed up)
    if (!this.clients.size) return;
    const s = JSON.stringify(m);
    for (const ws of this.clients.values()) { if (droppable && ws.bufferedAmount > SNAP_SKIP_BUF) continue; safeSend(ws, s); }
  }
  to(id, m) { safeSend(this.clients.get(id), JSON.stringify(m)); }
  kick(id) { const ws = this.clients.get(id); if (ws) { this.clients.delete(id); safeClose(ws, 4002, 'kicked'); } }
  destroy(why) {
    if (this.dead) return; this.dead = true; clearTimeout(this.idle);
    const cs = [...this.clients.values()]; this.clients.clear(); for (const ws of cs) safeClose(ws, 4001, 'hostleft');
    this.game.dispose();
    if (hosted.get(this.code) === this) { hosted.delete(this.code); console.log(`[room ${this.code}] server-hosted room closed: ${why} (${hosted.size} hosted open)`); }
  }
}

/* ---------------- static files (the game page) ---------------- */
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.json': 'application/json', '.webmanifest': 'application/manifest+json',
  '.svg': 'image/svg+xml', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.webp': 'image/webp', '.gif': 'image/gif', '.ico': 'image/x-icon', '.txt': 'text/plain; charset=utf-8',
  '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg', '.wav': 'audio/wav', '.woff2': 'font/woff2', '.woff': 'font/woff', '.ttf': 'font/ttf' };
const SQUASH = new Set(['.html', '.js', '.css', '.json', '.webmanifest', '.svg', '.txt']);
const fileCache = new Map();   // path -> { mtime, buf, gz }

function sendFile(req, res, file, st) {
  const ext = path.extname(file).toLowerCase();
  let c = fileCache.get(file);
  if (!c || c.mtime !== st.mtimeMs) {   // (re)load when the file changed on disk: replacing the game file needs no restart
    const buf = fs.readFileSync(file); c = { mtime: st.mtimeMs, buf, gz: SQUASH.has(ext) && buf.length > 1024 ? zlib.gzipSync(buf, { level: 9 }) : null }; fileCache.set(file, c);
  }
  const wantGz = c.gz && /\bgzip\b/.test(req.headers['accept-encoding'] || ''), body = wantGz ? c.gz : c.buf;
  const h = { 'Content-Type': TYPES[ext] || 'application/octet-stream', 'Content-Length': body.length, 'X-Content-Type-Options': 'nosniff',
    'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=3600', 'Vary': 'Accept-Encoding' };
  if (wantGz) h['Content-Encoding'] = 'gzip';
  res.writeHead(200, h); res.end(req.method === 'HEAD' ? undefined : body);
}

const server = http.createServer((req, res) => {
  try {
    if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405, { Allow: 'GET, HEAD' }); return res.end(); }
    let p; try { p = decodeURIComponent(new URL(req.url, 'http://x').pathname); } catch (e) { res.writeHead(400); return res.end('bad request'); }
    if (p === '/ping') { res.writeHead(200, { 'Content-Type': 'text/plain', 'Cache-Control': 'no-store', 'Access-Control-Allow-Origin': '*' }); return res.end('ok hosted'); }   // "hosted" tells the page this server can run the game itself; the CORS header lets a page on another site read it
    if (p.endsWith('/')) p += 'index.html';
    const file = path.normalize(path.join(PUBLIC, p));
    if (!file.startsWith(PUBLIC + path.sep) || path.basename(file).startsWith('.')) { res.writeHead(404); return res.end('Not found'); }
    let st; try { st = fs.statSync(file); } catch (e) { res.writeHead(404); return res.end('Not found'); }
    if (!st.isFile()) { res.writeHead(404); return res.end('Not found'); }
    sendFile(req, res, file, st);
  } catch (e) { try { res.writeHead(500); res.end('error'); } catch (e2) {} }
});

/* ---------------- websocket relay ---------------- */
const wss = new WebSocketServer({ noServer: true, perMessageDeflate: false, maxPayload: MAX_IN });   // no compression: it only adds delay for tiny messages

server.on('upgrade', (req, socket, head) => {
  let u; try { u = new URL(req.url, 'http://x'); } catch (e) { return socket.destroy(); }
  const code = String(u.searchParams.get('room') || '').toUpperCase();
  if (u.pathname !== '/ws' || !/^[A-HJ-NP-Z2-9]{5}$/.test(code)) { socket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n'); return socket.destroy(); }
  const role = u.searchParams.get('role'), isHost = role === 'host', isCreate = role === 'create', key = String(u.searchParams.get('hk') || '').slice(0, 64);
  wss.handleUpgrade(req, socket, head, ws => {
    ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; }); ws.on('message', () => { ws.isAlive = true; }); ws.on('error', () => {});
    const hr = hosted.get(code);
    if (isCreate) {                                    // ask THIS server to host a new room and join it as its first player
      if (hr || rooms.has(code)) return safeClose(ws, 4009, 'taken');
      if (hosted.size >= MAX_HOSTED) return safeClose(ws, 4010, 'full');
      let r; try { r = new HostedRoom(code); } catch (e) { console.error(`[room ${code}] could not start the game:`, e && e.stack || e); return safeClose(ws, 1011, 'error'); }
      hosted.set(code, r); console.log(`[room ${code}] server-hosted room opened (${hosted.size} hosted open)`); return r.attach(ws);
    }
    if (hr) return isHost ? safeClose(ws, 4009, 'taken') : hr.attach(ws);
    let room = rooms.get(code);
    if (isHost) {
      if (!room) { if (rooms.size >= MAX_ROOMS) return safeClose(ws, 4010, 'full'); room = new Room(code); rooms.set(code, room); console.log(`[room ${code}] opened (${rooms.size} open)`); }
      room.attachHost(ws, key);
    } else if (room) room.attachClient(ws); else safeClose(ws, 4004, 'noroom');
  });
});

const beat = setInterval(() => {
  for (const ws of wss.clients) { if (ws.isAlive === false) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch (e) {} }
}, PING_MS);

const stop = () => { clearInterval(beat); for (const r of [...hosted.values()]) r.destroy('server stopping'); for (const ws of wss.clients) safeClose(ws, 1001, 'restart'); server.close(); setTimeout(() => process.exit(0), 300); };
if (require.main === module) {
  server.listen(PORT, '0.0.0.0', () => console.log(`Death by Dawn relay + page listening on port ${PORT}  (game file: ${path.join('public', 'index.html')})`));
  process.on('SIGTERM', stop); process.on('SIGINT', stop);
}
module.exports = { server, rooms, hosted, beat, stop };
