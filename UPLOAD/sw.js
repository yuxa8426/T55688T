// 股析 StockAI — Service Worker
// 版本號，更新時遞增以強制刷新快取
const CACHE_VERSION = 'stockai-v1.0.0';
const STATIC_CACHE  = `${CACHE_VERSION}-static`;
const DATA_CACHE    = `${CACHE_VERSION}-data`;

// 靜態資源（App Shell）
const STATIC_ASSETS = [
  '/index.html',
  '/manifest.json',
  '/icons/icon-192.png',
  '/icons/icon-512.png',
  'https://fonts.googleapis.com/css2?family=Noto+Sans+TC:wght@300;400;500;700&display=swap',
];

// ── Install：快取 App Shell ──
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(STATIC_CACHE).then(cache => {
      console.log('[SW] 快取靜態資源');
      return cache.addAll(STATIC_ASSETS);
    }).then(() => self.skipWaiting())
  );
});

// ── Activate：清除舊版快取 ──
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(k => k !== STATIC_CACHE && k !== DATA_CACHE)
          .map(k => {
            console.log('[SW] 刪除舊快取:', k);
            return caches.delete(k);
          })
      )
    ).then(() => self.clients.claim())
  );
});

// ── Fetch：網路優先（股市數據），快取優先（靜態資源）──
self.addEventListener('fetch', event => {
  const { request } = event;
  const url = new URL(request.url);

  // 股市 API 請求 → 網路優先，失敗用快取
  if (url.hostname.includes('twse.com.tw') ||
      url.hostname.includes('tpex.org.tw') ||
      url.hostname.includes('finance.yahoo.com') ||
      url.pathname.includes('/api/')) {
    event.respondWith(networkFirst(request, DATA_CACHE));
    return;
  }

  // 靜態資源 → 快取優先
  event.respondWith(cacheFirst(request, STATIC_CACHE));
});

// 網路優先策略
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
      JSON.stringify({ error: '網路連線失敗，請稍後再試', offline: true }),
      { headers: { 'Content-Type': 'application/json' } }
    );
  }
}

// 快取優先策略
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

// ── Push 通知（選擇性功能）──
self.addEventListener('push', event => {
  const data = event.data?.json() ?? {};
  event.waitUntil(
    self.registration.showNotification(data.title || '股析通知', {
      body: data.body || '',
      icon: '/icons/icon-192.png',
      badge: '/icons/icon-72.png',
      tag: data.tag || 'stockai',
      data: { url: data.url || '/' }
    })
  );
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil(
    clients.openWindow(event.notification.data.url)
  );
});
