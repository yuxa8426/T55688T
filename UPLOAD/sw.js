// 股析 StockAI — Service Worker
// 快取策略：
//   • API 請求（Worker proxy、FinMind、Yahoo 等）→ Network-First（永遠優先抓最新，失敗才用快取）
//   • 靜態資源（HTML/JS/CSS/圖示）→ Stale-While-Revalidate（先給快取秒開，背景更新）
// 這樣可根治「後端更新了但 App 顯示舊資料」的問題

const SW_VERSION = 'stockai-v202607230900';
const STATIC_CACHE = SW_VERSION + '-static';// 股析 StockAI — Service Worker
// 快取策略：
//   • API 請求（Worker proxy、FinMind、Yahoo 等）→ Network-First（永遠優先抓最新，失敗才用快取）
//   • 靜態資源（HTML/JS/CSS/圖示）→ Stale-While-Revalidate（先給快取秒開，背景更新）
// 這樣可根治「後端更新了但 App 顯示舊資料」的問題

const SW_VERSION = 'stockai-v202607241200';
const STATIC_CACHE = SW_VERSION + '-static';
const RUNTIME_CACHE = SW_VERSION + '-runtime';

// API 網域：這些一律 network-first，不讓舊資料卡住
const API_HOSTS = [
  'stockai-proxy.yuxa8426.workers.dev',
  'api.finmindtrade.com',
  'query1.finance.yahoo.com',
  'query2.finance.yahoo.com',
  'openapi.twse.com.tw',
  'www.twse.com.tw',
  'mis.twse.com.tw',
  'www.tpex.org.tw',
  'generativelanguage.googleapis.com',
  'fred.stlouisfed.org',
  'stooq.com',
];

// 安裝：立即接管，不等舊 SW
self.addEventListener('install', (e) => {
  self.skipWaiting();
});

// 啟用：清掉所有舊版本快取
self.addEventListener('activate', (e) => {
  e.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(
      keys.filter(k => !k.startsWith(SW_VERSION)).map(k => caches.delete(k))
    );
    await self.clients.claim();
  })());
});

// 接收前端的 SKIP_WAITING 訊息
self.addEventListener('message', (e) => {
  if (e.data && e.data.type === 'SKIP_WAITING') self.skipWaiting();
});

self.addEventListener('fetch', (e) => {
  const req = e.request;
  // 只處理 GET
  if (req.method !== 'GET') return;

  let url;
  try { url = new URL(req.url); } catch { return; }

  // 跨來源的非 API 資源（例如字型 CDN）：交給瀏覽器預設處理
  const isAPI = API_HOSTS.includes(url.hostname);

  if (isAPI) {
    // ── Network-First：API 永遠優先抓最新 ──
    e.respondWith((async () => {
      try {
        const fresh = await fetch(req);
        // 成功才更新快取（當備援）
        if (fresh && fresh.status === 200) {
          const cache = await caches.open(RUNTIME_CACHE);
          cache.put(req, fresh.clone()).catch(()=>{});
        }
        return fresh;
      } catch (err) {
        // 斷網時才回退快取
        const cached = await caches.match(req);
        if (cached) return cached;
        throw err;
      }
    })());
    return;
  }

  // ── 同源靜態資源：Stale-While-Revalidate（先給快取，背景更新）──
  if (url.origin === self.location.origin) {
    e.respondWith((async () => {
      const cache = await caches.open(STATIC_CACHE);
      const cached = await cache.match(req);
      const fetchPromise = fetch(req).then(fresh => {
        if (fresh && fresh.status === 200) cache.put(req, fresh.clone()).catch(()=>{});
        return fresh;
      }).catch(() => cached);
      return cached || fetchPromise;
    })());
    return;
  }
  // 其他跨來源：預設處理（不攔截）
});
const RUNTIME_CACHE = SW_VERSION + '-runtime';

// API 網域：這些一律 network-first，不讓舊資料卡住
const API_HOSTS = [
  'stockai-proxy.yuxa8426.workers.dev',
  'api.finmindtrade.com',
  'query1.finance.yahoo.com',
  'query2.finance.yahoo.com',
  'openapi.twse.com.tw',
  'www.twse.com.tw',
  'mis.twse.com.tw',
  'www.tpex.org.tw',
  'generativelanguage.googleapis.com',
  'fred.stlouisfed.org',
  'stooq.com',
];

// 安裝：立即接管，不等舊 SW
self.addEventListener('install', (e) => {
  self.skipWaiting();
});

// 啟用：清掉所有舊版本快取
self.addEventListener('activate', (e) => {
  e.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(
      keys.filter(k => !k.startsWith(SW_VERSION)).map(k => caches.delete(k))
    );
    await self.clients.claim();
  })());
});

// 接收前端的 SKIP_WAITING 訊息
self.addEventListener('message', (e) => {
  if (e.data && e.data.type === 'SKIP_WAITING') self.skipWaiting();
});

self.addEventListener('fetch', (e) => {
  const req = e.request;
  // 只處理 GET
  if (req.method !== 'GET') return;

  let url;
  try { url = new URL(req.url); } catch { return; }

  // 跨來源的非 API 資源（例如字型 CDN）：交給瀏覽器預設處理
  const isAPI = API_HOSTS.includes(url.hostname);

  if (isAPI) {
    // ── Network-First：API 永遠優先抓最新 ──
    e.respondWith((async () => {
      try {
        const fresh = await fetch(req);
        // 成功才更新快取（當備援）
        if (fresh && fresh.status === 200) {
          const cache = await caches.open(RUNTIME_CACHE);
          cache.put(req, fresh.clone()).catch(()=>{});
        }
        return fresh;
      } catch (err) {
        // 斷網時才回退快取
        const cached = await caches.match(req);
        if (cached) return cached;
        throw err;
      }
    })());
    return;
  }

  // ── 同源靜態資源：Stale-While-Revalidate（先給快取，背景更新）──
  if (url.origin === self.location.origin) {
    e.respondWith((async () => {
      const cache = await caches.open(STATIC_CACHE);
      const cached = await cache.match(req);
      const fetchPromise = fetch(req).then(fresh => {
        if (fresh && fresh.status === 200) cache.put(req, fresh.clone()).catch(()=>{});
        return fresh;
      }).catch(() => cached);
      return cached || fetchPromise;
    })());
    return;
  }
  // 其他跨來源：預設處理（不攔截）
});
