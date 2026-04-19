// 股析 StockAI — Service Worker v2.0
// 新增：Web Push 訂閱管理、豐富通知樣式
const CACHE_VERSION = 'stockai-v2.1.0'; // Android PWA 優化
const STATIC_CACHE  = `${CACHE_VERSION}-static`;
const DATA_CACHE    = `${CACHE_VERSION}-data`;

const STATIC_ASSETS = [
  '/index.html',
  '/manifest.json',
  '/icons/icon-192.png',
  '/icons/icon-512.png',
];

// ── Install ──────────────────────────────────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(STATIC_CACHE)
      .then(cache => cache.addAll(STATIC_ASSETS))
      .then(() => self.skipWaiting())
  );
});

// ── Activate ─────────────────────────────────────────
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(
        keys.filter(k => k !== STATIC_CACHE && k !== DATA_CACHE)
            .map(k => caches.delete(k))
      ))
      .then(() => self.clients.claim())
  );
});

// ── Fetch ─────────────────────────────────────────────
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);
  if (url.hostname.includes('twse.com.tw') ||
      url.hostname.includes('finance.yahoo.com') ||
      url.pathname.includes('/api/')) {
    event.respondWith(networkFirst(event.request, DATA_CACHE));
    return;
  }
  event.respondWith(cacheFirst(event.request, STATIC_CACHE));
});

async function networkFirst(request, cacheName) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(cacheName);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await caches.match(request);
    return cached || new Response(
      JSON.stringify({ error: '網路連線失敗', offline: true }),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }
}

async function cacheFirst(request, cacheName) {
  const cached = await caches.match(request);
  if (cached) return cached;
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(cacheName);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    return new Response('離線中', { status: 503 });
  }
}

// ── Push 通知接收 ──────────────────────────────────────
self.addEventListener('push', event => {
  if (!event.data) return;
  const data = event.data.json();

  // 依通知類型設定不同樣式
  const TYPE_STYLE = {
    verify:  { icon:'/icons/icon-192.png', badge:'/icons/icon-72.png', color:'#f5c842' },
    alert:   { icon:'/icons/icon-192.png', badge:'/icons/icon-72.png', color:'#ff4c6a' },
    vix:     { icon:'/icons/icon-192.png', badge:'/icons/icon-72.png', color:'#ff4c6a' },
    inst:    { icon:'/icons/icon-192.png', badge:'/icons/icon-72.png', color:'#9b7ffe' },
    default: { icon:'/icons/icon-192.png', badge:'/icons/icon-72.png', color:'#00d4aa' },
  };
  const style = TYPE_STYLE[data.type] || TYPE_STYLE.default;

  const options = {
    body:    data.body    || '',
    icon:    style.icon,
    badge:   style.badge,
    tag:     data.tag     || 'stockai-'+Date.now(),
    renotify: true,
    vibrate: [100, 50, 100],
    data:    { url: data.url || '/', type: data.type },
    actions: data.actions || [],
  };

  event.waitUntil(
    self.registration.showNotification(data.title || '股析 StockAI', options)
  );
});

// ── 通知點擊 ──────────────────────────────────────────
self.addEventListener('notificationclick', event => {
  event.notification.close();
  const url = event.notification.data?.url || '/';

  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true })
      .then(windowClients => {
        // 若已有開啟的視窗，聚焦並導航
        for (const client of windowClients) {
          if (client.url.includes(self.location.origin) && 'focus' in client) {
            client.focus();
            client.postMessage({ type: 'PUSH_CLICK', url, notifType: event.notification.data?.type });
            return;
          }
        }
        // 否則開新視窗
        if (clients.openWindow) return clients.openWindow(url);
      })
  );
});
