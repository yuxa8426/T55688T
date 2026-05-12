// 股析 StockAI Service Worker
const CACHE_NAME = 'stockai-v1';

self.addEventListener('install', e => {
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  e.waitUntil(clients.claim());
});

self.addEventListener('fetch', e => {
  // 只快取靜態資源，API 請求直接穿透
  if(e.request.url.includes('stockai-proxy') || 
     e.request.url.includes('finmind') ||
     e.request.url.includes('yahoo') ||
     e.request.url.includes('twse')) {
    return; // API 不快取
  }
  e.respondWith(fetch(e.request).catch(() => caches.match(e.request)));
});
