'use strict';
// Headless loader: runs the game's own <script> in Node inside a vm context with a fake browser around it.
// Used by server.js so the SERVER can be the host of a room. One context per room; the compiled script is shared.
const fs = require('fs'), vm = require('vm'), path = require('path');

/* ---- universal stub: any property, call, or construction gives another stub; numbers read as 0, strings as '' ---- */
function makeStub(name = 'stub') {
  const store = Object.create(null);
  const target = function () {};
  return new Proxy(target, {
    get(_, k) {
      if (k === Symbol.toPrimitive) return h => (h === 'number' ? 0 : '');
      if (k === 'then' || k === 'toJSON' || k === Symbol.toStringTag || k === 'nodeType') return undefined;
      if (k === Symbol.iterator) return function* () {};
      if (k in store) return store[k];
      if (k === 'length') return 0;
      if (k === 'querySelectorAll' || k === 'getElementsByClassName' || k === 'getElementsByTagName') return () => [];
      if (k === 'getBoundingClientRect') return () => ({ left: 0, top: 0, right: 1280, bottom: 720, width: 1280, height: 720, x: 0, y: 0 });
      if (k === 'measureText') return t => ({ width: String(t || '').length * 6 });
      if (k === 'getImageData') return (x, y, w, h) => ({ width: w, height: h, data: new Uint8ClampedArray(Math.max(0, w * h * 4)) });
      if (k === 'createImageData') return (w, h) => ({ width: w, height: h, data: new Uint8ClampedArray(Math.max(0, w * h * 4)) });
      if (k === 'getContext') return () => makeStub(name + '.ctx');
      if (k === 'toDataURL') return () => '';
      if (k === 'contains' || k === 'matches') return () => false;
      return (store[k] = makeStub(name + '.' + String(k)));
    },
    set(_, k, v) { store[k] = v; return true; },
    has() { return true; },
    apply() { return makeStub(name + '()'); },
    construct() { return makeStub('new ' + name); },
  });
}

function mem() { const m = new Map(); return { getItem: k => (m.has(k) ? m.get(k) : null), setItem: (k, v) => { m.set(k, String(v)); }, removeItem: k => { m.delete(k); }, clear: () => m.clear() }; }

/* ---- the game's <script>, compiled once per file version and reused by every room ---- */
const compiled = new Map();
function getScript(file) {
  const st = fs.statSync(file); let c = compiled.get(file);
  if (!c || c.mtime !== st.mtimeMs) {
    const html = fs.readFileSync(file, 'utf8');
    const a = html.indexOf('<script>') + 8, b = html.lastIndexOf('</script>');
    if (a < 8 || b < a) throw new Error('no inline <script> found in ' + file);
    const lineOffset = html.slice(0, a).split('\n').length - 1;   // so error line numbers match the html file
    c = { mtime: st.mtimeMs, script: new vm.Script(html.slice(a, b), { filename: 'game.js', lineOffset }) };
    compiled.set(file, c);
  }
  return c.script;
}

/* opts: { search, onError(err, where, count) }  ->  { ctx, run(code), dispose(), errors } */
function loadGame(file, opts = {}) {
  const script = getScript(file);
  const live = new Set(); let dead = false, errors = 0, ctx = null;
  const report = (e, where) => { errors++; try { if (opts.onError) opts.onError(e, where, errors); } catch (x) {} };
  const guard = f => (typeof f === 'function' ? (...a) => { if (dead) return; try { return f(...a); } catch (e) { report(e, 'timer'); } } : f);

  class FakeWorker {   // the game's Timers uses a Worker only as a ticking clock
    constructor() { this.onmessage = null; this.onerror = null; this.a = null; this.b = null; }
    postMessage(d) {
      if (d === 'start') {
        const [hz, hb] = vm.runInContext('[CFG.TICK_HZ, CFG.HEARTBEAT]', ctx);
        const fire = k => () => { if (dead || !this.onmessage) return; try { this.onmessage({ data: k }); } catch (e) { report(e, k === 't' ? 'tick' : 'heartbeat'); } };
        this.a = setInterval(fire('t'), Math.round(1000 / hz)); this.b = setInterval(fire('h'), hb); live.add(this.a); live.add(this.b);
      } else this.terminate();
    }
    terminate() { clearInterval(this.a); clearInterval(this.b); live.delete(this.a); live.delete(this.b); }
  }

  const doc = makeStub('document'), els = new Map();
  doc.getElementById = id => { let e = els.get(id); if (!e) els.set(id, e = makeStub('#' + id)); return e; };   // same element every time, so a test can set .value on it
  const sb = {
    console: opts.console || console, crypto: require('crypto').webcrypto, setImmediate, queueMicrotask, URLSearchParams, TextEncoder, TextDecoder, AbortController, performance,
    Uint8Array, Uint8ClampedArray, Float32Array, Float64Array, Int32Array, Uint32Array, Uint16Array, Int16Array, Int8Array, ArrayBuffer, DataView,
    setTimeout: (f, ms, ...a) => { const h = setTimeout((...x) => { live.delete(h); guard(f)(...x); }, ms, ...a); live.add(h); return h; },
    setInterval: (f, ms, ...a) => { const h = setInterval(guard(f), ms, ...a); live.add(h); return h; },
    clearTimeout: h => { live.delete(h); clearTimeout(h); }, clearInterval: h => { live.delete(h); clearInterval(h); },
    document: doc, Worker: FakeWorker, Blob: class Blob {}, Image: class Image {}, Audio: class Audio {},
    localStorage: mem(), sessionStorage: mem(),
    navigator: { userAgent: 'headless', maxTouchPoints: 0, platform: 'node', language: 'en', languages: ['en'], hardwareConcurrency: 1, vibrate() { return false; }, wakeLock: undefined },
    location: Object.assign({ search: opts.search || '', href: 'http://headless/', protocol: 'http:', host: 'headless', hostname: 'headless', pathname: '/', hash: '' }, opts.location || {}),
    screen: { width: 1280, height: 720 }, innerWidth: 1280, innerHeight: 720, devicePixelRatio: 1,
    requestAnimationFrame: () => 0, cancelAnimationFrame() {},
    matchMedia: () => ({ matches: false, addEventListener() {}, addListener() {} }),
    getComputedStyle: () => makeStub('computedStyle'),
    addEventListener() {}, removeEventListener() {}, dispatchEvent() {},
    fetch: opts.fetch || (() => Promise.reject(new Error('no fetch in headless'))),
  };
  if (opts.WebSocket) sb.WebSocket = opts.WebSocket;   // lets a test run the page as a client against a real server
  sb.URL = Object.assign(function (...a) { return new URL(...a); }, { createObjectURL: () => 'blob:headless', revokeObjectURL() {} });
  sb.URL.prototype = URL.prototype;
  sb.window = sb; sb.self = sb; sb.globalThis = sb; sb.top = sb; sb.parent = sb;
  ctx = vm.createContext(sb);
  try { script.runInContext(ctx); } catch (e) { dead = true; for (const h of live) clearTimeout(h); live.clear(); throw e; }

  return {
    ctx, run: code => vm.runInContext(code, ctx, { filename: 'probe.js' }),
    get errors() { return errors; },
    dispose() { if (dead) return; try { vm.runInContext('Timers.stop()', ctx); } catch (e) {} dead = true; for (const h of live) clearTimeout(h); live.clear(); },
  };
}
module.exports = { loadGame, makeStub };

if (require.main === module) {
  const f = path.resolve(process.argv[2] || 'game.html');
  try { const g = loadGame(f); console.log('BOOT OK  TICK_HZ', g.run('CFG.TICK_HZ'), ' GAME_VERSION', g.run('GAME_VERSION')); g.dispose(); process.exit(0); }
  catch (e) { console.log('BOOT FAILED:', (e.stack || e).toString().split('\n').slice(0, 8).join('\n')); process.exit(1); }
}
