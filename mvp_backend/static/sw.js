/* GroundHog PWA service worker.
   Strategy:
     - HTML: network-first (so new deploys show up immediately when online)
     - Static assets (icons, manifest): cache-first
     - Map tiles (/tiles/...): cache-first (same-origin, served by our proxy
       — disk-cached upstream too). This is the foundation for "Pack for
       Trip" offline use: every viewed tile sticks around, and the prefetch
       UI (later) just primes this cache.
     - API calls (everything else under /): network-only (planner needs fresh data)
   Bump CACHE_VERSION on any breaking change so old clients drop the cache.
*/
const CACHE_VERSION = 'gh-v7';
const APP_CACHE = `${CACHE_VERSION}-app`;
const TILE_CACHE = `${CACHE_VERSION}-tiles`;

const APP_SHELL = [
  '/',
  '/static/manifest.webmanifest',
  '/static/icon-192.png',
  '/static/icon-512.png',
  '/static/apple-touch-icon.png'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(APP_CACHE).then((c) => c.addAll(APP_SHELL)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => !k.startsWith(CACHE_VERSION)).map((k) => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);

  // Same-origin only beyond this point — tiles, app shell, statics, API.
  if (url.origin !== self.location.origin) return;

  // Map tiles → cache-first (immutable for our purposes; provider rotates
  // chart cycles on the order of months, and the URL is stable).
  if (url.pathname.startsWith('/tiles/')) {
    event.respondWith((async () => {
      const cache = await caches.open(TILE_CACHE);
      const cached = await cache.match(req);
      if (cached) return cached;
      try {
        const res = await fetch(req);
        if (res && res.ok) cache.put(req, res.clone());
        return res;
      } catch (e) {
        // Offline and not cached — return a tiny transparent PNG so the
        // map doesn't show broken-image icons over uncached areas.
        return cached || Response.error();
      }
    })());
    return;
  }

  // App shell HTML → network-first, fall back to cached '/'
  if (req.mode === 'navigate' || (req.headers.get('accept') || '').includes('text/html')) {
    event.respondWith((async () => {
      try {
        const fresh = await fetch(req);
        const cache = await caches.open(APP_CACHE);
        cache.put('/', fresh.clone());
        return fresh;
      } catch {
        const cache = await caches.open(APP_CACHE);
        return (await cache.match('/')) || Response.error();
      }
    })());
    return;
  }

  // Static assets (icons, manifest) → cache-first
  if (url.pathname.startsWith('/static/')) {
    event.respondWith((async () => {
      const cache = await caches.open(APP_CACHE);
      const cached = await cache.match(req);
      if (cached) return cached;
      const res = await fetch(req);
      if (res && res.ok) cache.put(req, res.clone());
      return res;
    })());
    return;
  }

  // Everything else (API calls) → network-only; let the page handle errors
});

// ── Pack for Trip plumbing ──────────────────────────────────────────────
// The page can ask the SW to prefetch a list of URLs into the tile cache
// (or any named cache) and reports progress back via postMessage.
//   page → sw:  { type: 'PACK', cache: 'gh-v4-tiles', urls: [...] }
//   sw  → page: { type: 'PACK_PROGRESS', done, total, failed }
//   sw  → page: { type: 'PACK_DONE', done, total, failed }
self.addEventListener('message', (event) => {
  const msg = event.data || {};
  if (msg.type !== 'PACK' || !Array.isArray(msg.urls)) return;
  const cacheName = msg.cache || TILE_CACHE;
  const urls = msg.urls.slice();
  const total = urls.length;
  const concurrency = Math.min(6, Math.max(1, msg.concurrency | 0 || 4));
  const source = event.source;

  event.waitUntil((async () => {
    const cache = await caches.open(cacheName);
    let done = 0, failed = 0, lastProgress = 0;
    async function worker() {
      while (urls.length) {
        const u = urls.shift();
        try {
          const hit = await cache.match(u);
          if (!hit) {
            const res = await fetch(u, { cache: 'no-store' });
            if (res && res.ok) await cache.put(u, res.clone());
            else failed++;
          }
        } catch {
          failed++;
        }
        done++;
        const now = Date.now();
        if (now - lastProgress > 250 || done === total) {
          lastProgress = now;
          try { source && source.postMessage({ type: 'PACK_PROGRESS', done, total, failed }); } catch {}
        }
      }
    }
    await Promise.all(Array.from({ length: concurrency }, worker));
    try { source && source.postMessage({ type: 'PACK_DONE', done, total, failed }); } catch {}
  })());
});
