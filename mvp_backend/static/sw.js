/* GroundHog PWA service worker.
   Strategy:
     - HTML: network-first (so new deploys show up immediately when online)
     - Static assets (icons, manifest): cache-first
     - Map tiles: stale-while-revalidate (light caching of recently-viewed area)
     - API calls (everything else under /): network-only (planner needs fresh data)
   Bump CACHE_VERSION on any breaking change so old clients drop the cache.
*/
const CACHE_VERSION = 'gh-v3';
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

function isTileRequest(url) {
  const h = url.hostname;
  return h.endsWith('.tile.openstreetmap.org')
      || h.endsWith('.basemaps.cartocdn.com')
      || h.endsWith('.arcgisonline.com')
      || h.endsWith('.opentopomap.org');
}

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);

  // Cross-origin map tiles → stale-while-revalidate
  if (isTileRequest(url)) {
    event.respondWith((async () => {
      const cache = await caches.open(TILE_CACHE);
      const cached = await cache.match(req);
      const network = fetch(req).then((res) => {
        if (res && res.ok) cache.put(req, res.clone());
        return res;
      }).catch(() => cached);
      return cached || network;
    })());
    return;
  }

  // Same-origin only beyond this point
  if (url.origin !== self.location.origin) return;

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
