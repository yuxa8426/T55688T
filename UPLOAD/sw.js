/**
 * 股析 StockAI — Cloudflare Worker v6
 *
 * 新增：
 *   GET /news  → 抓取真實國際財經 RSS，回傳結構化新聞
 *
 * 路由：
 *   GET  /             → 健康檢查
 *   GET  /finmind?...  → FinMind API 代理（含 KV 快取）
 *   GET  /intraday     → Yahoo Finance / TWSE mis
 *   GET  /vix          → VIX
 *   GET  /news         → 國際財經 RSS 新聞
 *   GET  /dispose      → 處置/注意股
 *   GET  /kv           → 用戶資料 KV
 *   POST /v1/messages  → Gemini AI
 *
 * 環境變數：GEMINI_API_KEY / FM_TOKEN / STORE / CACHE_STORE
 */

const FM_BASE = 'https://api.finmindtrade.com/api/v4/data';

// VAPID 金鑰（Web Push 驗證用）
const VAPID_PUBLIC  = 'BPuAwQ8xvE6qfyvZbXIXetmk4j5bdXcfgpyRtKGLFhY-onOug3MfL9aqLoyEUP4U8ZcS0YzQdNiVx7awK8IgBZg';
const VAPID_PRIVATE = 'gC1zzxCfALfS8WCLxs9HXwvmIcfQDAeC3N4qnFCkAm8';
const VAPID_SUBJECT = 'mailto:admin@t55688t.pages.dev';
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': '*',
};

const ok  = (b,s=200)=>new Response(typeof b==='string'?b:JSON.stringify(b),
  {status:s,headers:{...CORS,'Content-Type':'application/json'}});
const err = (m,s=500)=>ok({error:m},s);

async function safeFetch(url,opts={},ms=10000){
  const c=new AbortController(); const t=setTimeout(()=>c.abort(),ms);
  try{ const r=await fetch(url,{...opts,signal:c.signal}); clearTimeout(t); return r; }
  catch(e){ clearTimeout(t); throw e; }
}

// ── RSS 來源清單 ───────────────────────────────────────────
// 全部採用公開、無需 Key 的 RSS feed
// ════════════════════════════════════════════════════
// 國際財經 RSS 新聞來源（公信力媒體）
// 每類挑選 1~2 個最具公信力、有穩定 RSS feed 的來源
// Cloudflare Worker 並行抓取，每次選前 12 個
// ════════════════════════════════════════════════════
const NEWS_SOURCES = [

  // ── 總體經濟 / 央行政策 ──────────────────────────────
  // Reuters：全球最大通訊社，中立、即時、公信力極高
  { url:'https://feeds.reuters.com/reuters/businessNews',
    category:'macro', name:'Reuters' },
  // Associated Press：美聯社，無廣告影響、中立報道
  { url:'https://rsshub.app/apnews/topics/business-news',
    category:'macro', name:'AP News' },

  // ── 利率 / 聯準會 / 債市 ─────────────────────────────
  // CNBC Economy：Fed 利率決策、通膨、就業數據
  { url:'https://www.cnbc.com/id/10000664/device/rss/rss.html',
    category:'rates', name:'CNBC Economy' },
  // WSJ Economy：華爾街日報財經，付費牆外的部分仍可 RSS
  { url:'https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines',
    category:'rates', name:'MarketWatch' },

  // ── 科技 / AI ────────────────────────────────────────
  // CNBC Tech：Nvidia、Apple、Meta 等科技巨頭即時新聞
  { url:'https://www.cnbc.com/id/19832390/device/rss/rss.html',
    category:'tech', name:'CNBC Tech' },
  // Ars Technica：深度技術報道，AI / 晶片 / 軟體，科學背景記者
  { url:'https://feeds.arstechnica.com/arstechnica/index',
    category:'tech', name:'Ars Technica' },

  // ── 半導體 / 台灣相關 ────────────────────────────────
  // EE Times：電子工程媒體，台積電/ASML/英特爾等晶片深度報道
  { url:'https://www.eetimes.com/feed/',
    category:'semi', name:'EE Times' },
  // Tom's Hardware：GPU / CPU 晶片效能、Nvidia 供應鏈即時
  { url:'https://cdn.mos.cms.futurecdn.net/rss/toms-hardware',
    category:'semi', name:"Tom's Hardware" },

  // ── 國際貿易 / 地緣政治 ──────────────────────────────
  // Financial Times：英國金融時報，關稅/貿易戰/地緣首選
  { url:'https://www.ft.com/world?format=rss',
    category:'trade', name:'Financial Times' },
  // South China Morning Post：亞太/兩岸議題權威
  { url:'https://www.scmp.com/rss/91/feed',
    category:'trade', name:'SCMP' },

  // ── 能源 / 原物料 ────────────────────────────────────
  // Reuters Energy：石油、天然氣、再生能源官方報道
  { url:'https://feeds.reuters.com/reuters/energyNews',
    category:'energy', name:'Reuters Energy' },

  // ── 加密貨幣 / 數位資產 ──────────────────────────────
  // CoinDesk：加密貨幣媒體最具公信力，機構級報道
  { url:'https://www.coindesk.com/arc/outboundfeeds/rss/',
    category:'crypto', name:'CoinDesk' },
];

// RSS XML 解析（無 DOM，純字串）
function parseRSS(xml, source){
  const items = [];
  const itemRe = /<item[^>]*>([\s\S]*?)<\/item>/gi;
  let m;
  while((m=itemRe.exec(xml))!==null && items.length<3){
    const block = m[1];
    const get   = (tag)=>{
      const r = new RegExp(`<${tag}[^>]*><!\\[CDATA\\[([\\s\\S]*?)\\]\\]><\\/${tag}>|<${tag}[^>]*>([^<]*)<\\/${tag}>`, 'i');
      const mr = r.exec(block);
      return mr ? (mr[1]||mr[2]||'').trim() : '';
    };
    const title = get('title').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&#39;/g,"'").replace(/&quot;/g,'"');
    const desc  = get('description').replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').slice(0,200);
    const link  = get('link')||get('guid');
    const pubDate = get('pubDate');
    if(title && title.length > 5){
      items.push({ title, desc, link, pubDate, source: source.name, category: source.category });
    }
  }
  return items;
}

// Atom XML 解析
function parseAtom(xml, source){
  const items = [];
  const entryRe = /<entry[^>]*>([\s\S]*?)<\/entry>/gi;
  let m;
  while((m=entryRe.exec(xml))!==null && items.length<3){
    const block = m[1];
    const get   = (tag)=>{
      const r = new RegExp(`<${tag}[^>]*><!\\[CDATA\\[([\\s\\S]*?)\\]\\]><\\/${tag}>|<${tag}[^>]*>([^<]*)<\\/${tag}>`, 'i');
      const mr = r.exec(block);
      return mr ? (mr[1]||mr[2]||'').trim() : '';
    };
    const title   = get('title').replace(/&amp;/g,'&').slice(0,120);
    const summary = get('summary').replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').slice(0,200);
    const linkM   = /<link[^>]+href="([^"]+)"/.exec(block);
    const link    = linkM ? linkM[1] : '';
    const updated = get('updated');
    if(title && title.length > 5){
      items.push({ title, desc:summary, link, pubDate:updated, source:source.name, category:source.category });
    }
  }
  return items;
}

// 判斷對台股的關聯度（關鍵詞過濾）
function calcRelevance(item){
  const text = (item.title+' '+item.desc).toLowerCase();
  const HIGH = ['taiwan','tsmc','semiconductor','chip','nvidia','fed rate','interest rate',
                'inflation','tariff','trade war','ai','hbm','cowos','supply chain',
                '台積電','晶片','利率','關稅'];
  const MID  = ['tech','stock market','nasdaq','s&p','earnings','gdp','china','export',
                'microsoft','apple','google','samsung'];
  const score = HIGH.reduce((s,k)=>text.includes(k)?s+3:s,0) +
                MID.reduce((s,k)=>text.includes(k)?s+1:s,0);
  return score;
}

// 判斷利多/利空台股
function calcDirection(item){
  const text = (item.title+' '+item.desc).toLowerCase();
  const BULL = ['rally','surge','jump','rise','gain','beat','record','growth','cut rate',
                'expansion','positive','strong','upgrade','buy'];
  const BEAR = ['fall','drop','decline','slump','miss','recession','tariff','ban','restrict',
                'downgrade','sell','weak','concern','worry','risk'];
  const bull = BULL.reduce((s,k)=>text.includes(k)?s+1:s,0);
  const bear = BEAR.reduce((s,k)=>text.includes(k)?s+1:s,0);
  return bull>bear+1?'bull':bear>bull+1?'bear':'neutral';
}

// 判斷影響程度
function calcImpact(item, relevance){
  if(relevance >= 6) return 'high';
  if(relevance >= 3) return 'mid';
  return 'low';
}

// 相關台股族群推斷
function calcSectors(item){
  const text = (item.title+' '+item.desc).toLowerCase();
  const sectors = [];
  if(/nvidia|ai|hbm|cowos|gpu|llm|openai|anthropic|gemini/.test(text)) sectors.push('AI伺服器');
  if(/tsmc|semiconductor|chip|wafer|foundry|asml|qualcomm/.test(text)) sectors.push('半導體');
  if(/taiwan|tsmc|amd|intel|mediatek/.test(text))       sectors.push('IC設計');
  if(/trade|tariff|export|supply chain|apple|foxconn/.test(text)) sectors.push('電子製造');
  if(/bank|rate|fed|interest|bond|ecb|boj/.test(text))  sectors.push('金融');
  if(/ship|freight|container|port|cosco/.test(text))     sectors.push('航運');
  if(/oil|energy|crude|opec|natural gas/.test(text))     sectors.push('能源');
  if(/bitcoin|crypto|ethereum|digital asset/.test(text)) sectors.push('加密貨幣');
  if(/solar|ev|electric vehicle|battery|lithium/.test(text)) sectors.push('綠能電動車');
  return [...new Set(sectors)].slice(0,3);
}

// 新分類的台股關聯度加分
function extraRelevance(item){
  const text = (item.title+' '+item.desc).toLowerCase();
  if(item.category==='semi' && /tsmc|nvidia|amd|intel|arm/.test(text)) return 5;
  if(item.category==='rates' && /fed|rate cut|rate hike|inflation/.test(text)) return 4;
  if(item.category==='trade' && /taiwan|china|tariff|us-china/.test(text)) return 5;
  if(item.category==='crypto' && /bitcoin|eth|crypto market/.test(text)) return 2;
  if(item.category==='energy' && /oil|opec|lng/.test(text)) return 3;
  return 0;
}

export default {
  async fetch(request, env){
    const url = new URL(request.url);
    if(request.method==='OPTIONS') return new Response(null,{status:204,headers:CORS});

    const GEMINI_KEY = env.GEMINI_API_KEY||'';
    const FM_TOKEN   = env.FM_TOKEN||'';

    // ── GET / ──────────────────────────────────────────────
    if(url.pathname==='/')
      return new Response('StockAI Worker v6',{headers:{...CORS,'Content-Type':'text/plain'}});

    // ── GET /test-models ── 測試每個 model 是否可用 ────────
    if(url.pathname==='/test-models'){
      const testKeys = [
        env.GEMINI_API_KEY, env.GEMINI_API_KEY2, env.GEMINI_API_KEY3,
        env.GEMINI_API_KEY4,
      ].filter(Boolean);
      const testKey = testKeys[0];
      if(!testKey) return ok({error:'no key'});

      const models = [
        'gemini-2.0-flash-lite',
        'gemini-2.0-flash',
        'gemini-2.5-flash-lite-preview-04-17',
        'gemini-2.5-flash',
        'gemini-2.5-pro',
        'gemini-2.0-flash-thinking-exp',
        'gemma-3-27b-it',
        'gemma-3-12b-it',
      ];
      const results = {};
      for(const m of models){
        try{
          const r = await safeFetch(
            `https://generativelanguage.googleapis.com/v1beta/models/${m}:generateContent?key=${testKey}`,
            {method:'POST',headers:{'Content-Type':'application/json'},
             body:JSON.stringify({contents:[{role:'user',parts:[{text:'hi'}]}],
               generationConfig:{maxOutputTokens:5}})}, 8000);
          const j = await r.json();
          const txt = j?.candidates?.[0]?.content?.parts?.[0]?.text||'';
          results[m] = r.status===200 ? ('OK: '+txt.slice(0,20))
                     : r.status===429 ? '429 Rate Limit'
                     : r.status===404 ? '404 Not Found'
                     : r.status+'  '+( j?.error?.message||'').slice(0,60);
        }catch(e){ results[m]='ERROR: '+e.message.slice(0,60); }
      }
      // 測試所有 key 對最佳 model 的狀態
      // 測試每個 key 對兩個主要 model 的狀態
      const keyResults = {};
      for(let i=0;i<testKeys.length;i++){
        const kStatus = {};
        for(const tm of ['gemini-2.0-flash-lite','gemini-2.0-flash']){
          try{
            const r = await safeFetch(
              `https://generativelanguage.googleapis.com/v1beta/models/${tm}:generateContent?key=${testKeys[i]}`,
              {method:'POST',headers:{'Content-Type':'application/json'},
               body:JSON.stringify({contents:[{role:'user',parts:[{text:'hi'}]}],
                 generationConfig:{maxOutputTokens:5}})}, 8000);
            const j = await r.json();
            const txt = j?.candidates?.[0]?.content?.parts?.[0]?.text||'';
            kStatus[tm] = r.status===200?'✅ OK':r.status===429?'❌ 429':r.status+' '+(j?.error?.message||'').slice(0,30);
          }catch(e){ kStatus[tm]='ERROR: '+e.message.slice(0,30); }
          await new Promise(r=>setTimeout(r,300));
        }
        keyResults['key'+(i+1)] = kStatus;
      }
      return ok({models:results, per_key:keyResults});
    }

    // ── GET /diag-quant ── 診斷 buildFullMarketQuant 的三個 API ──
    if(url.pathname==='/diag-quant'){
      const FM_TOKEN_VAL = env.FM_TOKEN||'';
      if(!FM_TOKEN_VAL) return ok({error:'FM_TOKEN not set'});
      const today   = new Date().toLocaleDateString('zh-TW',{timeZone:'Asia/Taipei'}).replace(/\//g,'-');
      const weekAgo = new Date(Date.now()-7*86400000).toLocaleDateString('zh-TW',{timeZone:'Asia/Taipei'}).replace(/\//g,'-');
      const d3ago   = new Date(Date.now()-5*86400000).toLocaleDateString('zh-TW',{timeZone:'Asia/Taipei'}).replace(/\//g,'-');
      const BASE    = 'https://api.finmindtrade.com/api/v4/data';
      const results = {};
      const datasets = [
        // 免費方案
        {name:'TaiwanStockPrice(2330)',        url:`${BASE}?dataset=TaiwanStockPrice&data_id=2330&start_date=${weekAgo}&end_date=${today}&token=${FM_TOKEN_VAL}`},
        {name:'TaiwanStockInstitutionalInvestorsBuySell(2330)', url:`${BASE}?dataset=TaiwanStockInstitutionalInvestorsBuySell&data_id=2330&start_date=${weekAgo}&end_date=${today}&token=${FM_TOKEN_VAL}`},
        {name:'TaiwanStockTotalInstitutionalInvestors', url:`${BASE}?dataset=TaiwanStockTotalInstitutionalInvestors&start_date=${weekAgo}&end_date=${today}&token=${FM_TOKEN_VAL}`},
        // 台指期（需測試可用性）
        {name:'TaiwanFuturesDaily(TX)',  url:`${BASE}?dataset=TaiwanFuturesDaily&data_id=TX&start_date=${weekAgo}&end_date=${today}&token=${FM_TOKEN_VAL}`},
        {name:'TaiwanFuturesDaily(TX00)',url:`${BASE}?dataset=TaiwanFuturesDaily&data_id=TX00&start_date=${weekAgo}&end_date=${today}&token=${FM_TOKEN_VAL}`},
        // 付費方案
        {name:'TaiwanStockPER',   url:`${BASE}?dataset=TaiwanStockPER&start_date=${d3ago}&end_date=${today}&token=${FM_TOKEN_VAL}`},
      ];
      for(const ds of datasets){
        try{
          const r = await safeFetch(ds.url,{headers:{'User-Agent':'StockAI/1.0'}},15000);
          const j = await r.json();
          const rows = j?.data||[];
          const sample = rows[0]||{};
          results[ds.name] = {
            status: r.status,
            rows: rows.length,
            msg: j?.msg||'',
            fields: Object.keys(sample).slice(0,8),
            sample_id: sample.stock_id||'',
            sample_name: sample.name||sample.stock_name||'',
          };
        }catch(e){ results[ds.name]={error:e.message}; }
      }
      return ok({today, weekAgo, results});
    }

    // ── GET /diag-free-apis ── 測試免費全市場資料來源 ────────
    if(url.pathname==='/diag-free-apis'){
      const results = {};

      // 1. TWSE 當日全市場收盤行情（完全免費）
      try{
        const r = await safeFetch('https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',
          {headers:{'User-Agent':'StockAI/1.0','Accept':'application/json'}}, 10000);
        const j = await r.json();
        const rows = Array.isArray(j)?j:[];
        results['TWSE_STOCK_DAY_ALL'] = {
          status:r.status, rows:rows.length,
          fields: rows[0]?Object.keys(rows[0]).slice(0,8):[],
          sample: rows[0]||{}
        };
      }catch(e){ results['TWSE_STOCK_DAY_ALL']={error:e.message}; }

      // 2. TWSE 全市場本益比（免費）
      try{
        const r = await safeFetch('https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL',
          {headers:{'User-Agent':'StockAI/1.0','Accept':'application/json'}}, 10000);
        const j = await r.json();
        const rows = Array.isArray(j)?j:[];
        results['TWSE_BWIBBU_ALL(PE/殖利率)'] = {
          status:r.status, rows:rows.length,
          fields: rows[0]?Object.keys(rows[0]).slice(0,8):[],
          sample: rows[0]||{}
        };
      }catch(e){ results['TWSE_BWIBBU_ALL']={error:e.message}; }

      // 3. TWSE 三大法人每日彙總（免費）
      try{
        const r = await safeFetch('https://openapi.twse.com.tw/v1/fund/TWT38U',
          {headers:{'User-Agent':'StockAI/1.0','Accept':'application/json'}}, 10000);
        const j = await r.json();
        const rows = Array.isArray(j)?j:[];
        results['TWSE_TWT38U(三大法人)'] = {
          status:r.status, rows:rows.length,
          fields: rows[0]?Object.keys(rows[0]).slice(0,8):[],
          sample: rows[0]||{}
        };
      }catch(e){ results['TWSE_TWT38U']={error:e.message}; }

      // 4. TPEX 上櫃當日行情（免費）
      try{
        const r = await safeFetch('https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes',
          {headers:{'User-Agent':'StockAI/1.0','Accept':'application/json'}}, 10000);
        const j = await r.json();
        const rows = Array.isArray(j)?j:[];
        results['TPEX_daily_close'] = {
          status:r.status, rows:rows.length,
          fields: rows[0]?Object.keys(rows[0]).slice(0,8):[],
          sample: rows[0]||{}
        };
      }catch(e){ results['TPEX_daily_close']={error:e.message}; }

      // 5. TWSE 個股法人（免費，需帶日期）
      try{
        const today = new Date().toLocaleDateString('zh-TW',{timeZone:'Asia/Taipei'}).replace(/\//g,'');
        const r = await safeFetch(`https://openapi.twse.com.tw/v1/fund/TWT44U`,
          {headers:{'User-Agent':'StockAI/1.0','Accept':'application/json'}}, 10000);
        const j = await r.json();
        const rows = Array.isArray(j)?j:[];
        results['TWSE_TWT44U(個股法人)'] = {
          status:r.status, rows:rows.length,
          fields: rows[0]?Object.keys(rows[0]).slice(0,8):[],
          sample: rows[0]||{}
        };
      }catch(e){ results['TWSE_TWT44U']={error:e.message}; }

      return ok(results);
    }

    // ── GET /market-all ── 全市場資料（TWSE+TPEX 免費）────
    if(url.pathname==='/market-all'){
      const results = {tse:[], otc:[], pe:[]};

      // 並行抓取三個免費 API
      const [tseRes, peRes, otcRes] = await Promise.allSettled([
        // 上市當日行情（1350支）
        safeFetch('https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL',
          {headers:{'User-Agent':'StockAI/1.0','Accept':'application/json'}}, 12000),
        // 上市 PE/殖利率/PB（1070支）
        safeFetch('https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL',
          {headers:{'User-Agent':'StockAI/1.0','Accept':'application/json'}}, 12000),
        // 上櫃當日行情（700+支）
        safeFetch('https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes',
          {headers:{'User-Agent':'StockAI/1.0','Accept':'application/json'}}, 12000),
      ]);

      // 解析上市行情
      if(tseRes.status==='fulfilled' && tseRes.value.ok){
        try{
          const rows = await tseRes.value.json();
          results.tse = (Array.isArray(rows)?rows:[]).map(r=>({
            id:   r.Code||'',
            name: r.Name||'',
            close: parseFloat(r.ClosingPrice||0)||null,
            change: parseFloat(r.Change||0)||0,
            changePct: r.ClosingPrice && r.Change
              ? +((parseFloat(r.Change)/( parseFloat(r.ClosingPrice)-parseFloat(r.Change||0)))*100).toFixed(2)
              : 0,
            volume: parseInt((r.TradeVolume||'0').replace(/,/g,'')),
            market: 'TSE',
          })).filter(r=>r.id && /^\d{4,6}$/.test(r.id));
        }catch(e){ console.warn('[market-all] TSE parse error:', e.message); }
      }

      // 解析 PE/殖利率/PB
      if(peRes.status==='fulfilled' && peRes.value.ok){
        try{
          const rows = await peRes.value.json();
          results.pe = (Array.isArray(rows)?rows:[]).map(r=>({
            id:    r.Code||'',
            pe:    parseFloat(r.PEratio)||null,
            yield: parseFloat(r.DividendYield)||null,
            pb:    parseFloat(r.PBratio)||null,
          })).filter(r=>r.id && /^\d{4,6}$/.test(r.id));
        }catch(e){ console.warn('[market-all] PE parse error:', e.message); }
      }

      // 解析上櫃行情
      if(otcRes.status==='fulfilled' && otcRes.value.ok){
        try{
          const rows = await otcRes.value.json();
          results.otc = (Array.isArray(rows)?rows:[]).map(r=>({
            id:    r.SecuritiesCompanyCode||'',
            name:  r.CompanyName||'',
            close: parseFloat(r.Close)||null,
            change: parseFloat((r.Change||'0').replace('+',''))||0,
            changePct: r.Close && r.Change
              ? +((parseFloat((r.Change||'0').replace('+','')) / (parseFloat(r.Close)-parseFloat((r.Change||'0').replace('+',''))||1))*100).toFixed(2)
              : 0,
            volume: parseInt((r.TradingShares||'0').replace(/,/g,'')),
            market: 'OTC',
          })).filter(r=>r.id && /^\d{4,6}$/.test(r.id));
        }catch(e){ console.warn('[market-all] OTC parse error:', e.message); }
      }

      // 快取到 KV（盤後到隔天開盤前）
      const cacheKey = 'market:all:'+new Date().toLocaleDateString('zh-TW',{timeZone:'Asia/Taipei'}).replace(/\//g,'');
      if(env.CACHE_STORE && (results.tse.length+results.otc.length) > 100){
        try{
          await env.CACHE_STORE.put(cacheKey, JSON.stringify(results),
            {expirationTtl: isTWSETradingNow() ? 300 : secsTillMidnight()});
        }catch(e){}
      }

      return ok({
        tse: results.tse.length,
        otc: results.otc.length,
        pe:  results.pe.length,
        total: results.tse.length + results.otc.length,
        data: results,
      });
    }

    // ── GET /test-ai ── 直接測試 AI 選股是否正常 ──────────
    if(url.pathname==='/test-ai'){
      const keys = [env.GEMINI_API_KEY,env.GEMINI_API_KEY2,env.GEMINI_API_KEY3,env.GEMINI_API_KEY4].filter(Boolean);
      if(!keys.length) return ok({error:'no key'});
      const testPrompt = '只輸出以下JSON，不加任何文字：{"stocks":[{"id":"2330","name":"台積電","sector":"半導體","winRate":75,"risk":4,"reason":"買超"}]}';
      let result = {};
      for(const [mi,model] of [['gemini-2.5-flash'],['gemma-3-27b-it']].entries()){
        for(const [ki,key] of keys.entries()){
          try{
            const r = await safeFetch(
              `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`,
              {method:'POST',headers:{'Content-Type':'application/json'},
               body:JSON.stringify({contents:[{role:'user',parts:[{text:testPrompt}]}],
                 generationConfig:{maxOutputTokens:200}})},10000);
            const j = await r.json();
            const txt = j?.candidates?.[0]?.content?.parts?.[0]?.text||'';
            result = {status:r.status, model, key:'key'+(ki+1), output:txt.slice(0,200), ok:r.status===200};
            if(r.status===200) return ok(result);
          }catch(e){ result={error:e.message}; }
        }
      }
      return ok(result);
    }

    // ── GET /clear-cache ── 清除 AI 回應快取 ─────────────
    if(url.pathname==='/clear-cache'){
      if(!env.CACHE_STORE) return ok({error:'no cache store'});
      try{
        let deleted = 0;
        // 清除所有前綴的快取
        for(const prefix of ['ai:','txf:','market:','trend:']){
          const list = await env.CACHE_STORE.list({prefix});
          for(const key of (list.keys||[])){
            await env.CACHE_STORE.delete(key.name);
            deleted++;
          }
        }
        // 直接刪除已知 key
        for(const k of ['txf:price:v1','txf:price:v2','txf:price:v3','txf:price:v4','trend:daily']){
          try{ await env.CACHE_STORE.delete(k); deleted++; }catch(e){}
        }
        return ok({deleted, message:'All cache cleared (ai/txf/market/trend)'});
      }catch(e){ return ok({error:e.message}); }
    }

    // ── GET /txf-test ── 測試台指期資料來源 ──────────────
    if(url.pathname==='/txf-test'){
      const results = {};

      // 來源1: Yahoo Finance TXF=F
      try{
        const r = await safeFetch(
          'https://query1.finance.yahoo.com/v8/finance/chart/TXF=F?interval=1m&range=1d',
          {headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'}}, 8000);
        const j = await r.json();
        const meta = j?.chart?.result?.[0]?.meta;
        results['yahoo_TXF=F'] = {
          status: r.status,
          price: meta?.regularMarketPrice,
          prevClose: meta?.chartPreviousClose,
          change: meta?.regularMarketPrice && meta?.chartPreviousClose
            ? +(meta.regularMarketPrice - meta.chartPreviousClose).toFixed(0) : null,
          currency: meta?.currency,
          exchangeTimezoneName: meta?.exchangeTimezoneName,
        };
      }catch(e){ results['yahoo_TXF=F'] = {error: e.message}; }

      // 來源2: Yahoo Finance ^TWII（加權指數，非期貨但可參考）
      try{
        const r = await safeFetch(
          'https://query1.finance.yahoo.com/v8/finance/chart/%5ETWII?interval=1m&range=1d',
          {headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'}}, 8000);
        const j = await r.json();
        const meta = j?.chart?.result?.[0]?.meta;
        results['yahoo_TWII'] = {
          status: r.status,
          price: meta?.regularMarketPrice,
          prevClose: meta?.chartPreviousClose,
        };
      }catch(e){ results['yahoo_TWII'] = {error: e.message}; }

      // 來源3: TAIFEX OpenAPI - 測試多個端點
      // ── Fugle 期權行情 API 測試 ──────────────────────────
      // 兩個 Key 都測試，確認哪個有期權行情權限
      const fugleKeys = [
        env.FUGLE_API_KEY||'',
        env.FUGLE_API_KEY2||'',
      ].filter(Boolean);

      // 測試 REST API：取得 WTXP 近月合約即時報價
      for(const [ki, fkey] of fugleKeys.entries()){
        try{
          // Fugle 期權 REST API: /intraday/quote/{symbol}
          // WTXP 近月合約代號需要確認（例：WTXPJ26 = 2026/10月）
          const testSymbols = ['WTXPJ26','WTXP','TXFA26','TXF'];
          for(const sym of testSymbols){
            const fr = await safeFetch(
              `https://api.fugle.tw/marketdata/v1.0/futopt/intraday/quote/${sym}`,
              {headers:{'X-API-KEY':fkey,'Accept':'application/json'}}, 6000);
            const fj = fr.ok ? await fr.json().catch(()=>null) : null;
            const ftxt = !fj ? await fr.text().catch(()=>'') : '';
            results[`fugle_key${ki+1}_${sym}`]={
              status:fr.status,
              data: fj,
              preview: ftxt.slice(0,200),
            };
            if(fr.ok && fj) break; // 成功就不繼續試
          }
        }catch(e){ results[`fugle_key${ki+1}`]={error:e.message}; }

        // 也測試 tickers 端點（取得所有期權商品列表）
        try{
          const tr = await safeFetch(
            `https://api.fugle.tw/marketdata/v1.0/futopt/intraday/tickers?type=FUTURE&exchange=TAIFEX&session=AFTER_HOURS`,
            {headers:{'X-API-KEY':fkey,'Accept':'application/json'}}, 6000);
          const tj = tr.ok ? await tr.json().catch(()=>null) : null;
          results[`fugle_key${ki+1}_tickers`]={
            status:tr.status,
            len: tj?.data?.length||tj?.length||0,
            sample: (tj?.data||tj||[]).slice(0,2),
          };
        }catch(e){ results[`fugle_key${ki+1}_tickers`]={error:e.message}; }
      }

      // ── WantGoo 玩股網 WTXP 來源測試 ────────────────────
      const wantgooUrls = [
        // 直接行情 API
        'https://www.wantgoo.com/futures/quotation?stockno=WTXP%26',
        'https://www.wantgoo.com/futures/quotation?stockno=WTXP',
        // JSON API
        'https://api.wantgoo.com/stock/futures/quotation?stockno=WTXP%26',
        'https://api.wantgoo.com/futures/realtime?code=WTXP',
        // 行情頁面
        'https://www.wantgoo.com/futures/WTXP%26',
        // 可能的 REST endpoint
        'https://www.wantgoo.com/api/futures/quote?symbol=WTXP',
      ];
      for(const wu of wantgooUrls){
        try{
          const r=await safeFetch(wu,{
            headers:{
              'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
              'Accept':'application/json,text/html,*/*',
              'Referer':'https://www.wantgoo.com/',
              'Origin':'https://www.wantgoo.com',
            }
          },6000);
          const txt=await r.text();
          let parsed=null; try{parsed=JSON.parse(txt);}catch(e){}
          const key='wantgoo_'+wu.split('/').pop().slice(0,30);
          results[key]={
            status:r.status,
            ct:r.headers.get('content-type')||'',
            isJson:parsed!==null,
            body:txt.slice(0,300),
            sample:parsed,
          };
        }catch(e){
          const key='wantgoo_'+wu.split('/').pop().slice(0,30);
          results[key]={error:e.message};
        }
      }

      // ── WTXP 夜盤來源全面測試 ────────────────────────────
      const twNow = new Date(new Date().toLocaleString('en-US',{timeZone:'Asia/Taipei'}));
      const yy_=twNow.getFullYear(), mm_=String(twNow.getMonth()+1).padStart(2,'0'), dd_=String(twNow.getDate()).padStart(2,'0');
      const csvToday = `${yy_}/${mm_}/${dd_}`;

      // 1. TAIFEX CSV 測試 WTXP
      for(const prod of ['WTXP','TX']){
        try{
          const url = `https://www.taifex.com.tw/cht/3/futDataDown?down_type=1&commodity_id=${prod}&queryStartDate=${encodeURIComponent(csvToday)}&queryEndDate=${encodeURIComponent(csvToday)}`;
          const r = await safeFetch(url,{headers:{'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64)','Referer':'https://www.taifex.com.tw/cht/3/futContractsDate'}},8000);
          if(r.ok){
            const buf=await r.arrayBuffer();
            const txt=new TextDecoder('big5').decode(buf);
            const iscsv=txt.includes(',')&&!txt.toLowerCase().includes('<!doctype')&&!txt.includes('alert(');
            const lines=iscsv?txt.split('\n').filter(l=>l.trim()&&!l.startsWith('日期')).slice(0,3):[];
            results[`taifex_${prod}`]={is_csv:iscsv,rows:lines.length,sample:lines[0]||'',preview:iscsv?'':txt.slice(0,80)};
          }
        }catch(e){results[`taifex_${prod}`]={error:e.message};}
      }

      // 2. TWSE MIS（盤中/夜盤即時，透過 allorigins proxy）
      try{
        // 台指期夜盤 MIS 代號：futopt_WTXP&
        const misUrls = [
          'https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=futopt_WTXP%26.tw&json=1&delay=0',
          'https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=futopt_TX%26.tw&json=1&delay=0',
          'https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_t00.tw&json=1&delay=0',
        ];
        for(const mu of misUrls){
          try{
            const r=await safeFetch(mu,{headers:{'User-Agent':'Mozilla/5.0','Referer':'https://mis.twse.com.tw/'}},6000);
            const j=r.ok?await r.json().catch(()=>null):null;
            const key='mis_'+mu.split('ex_ch=')[1]?.split('&')[0]||'mis_unknown';
            results[key]={status:r.status,msgArray:j?.msgArray?.slice(0,1),rtmessage:j?.rtmessage};
          }catch(e){results['mis_err']={error:e.message};}
        }
      }catch(e){results['mis_test']={error:e.message};}

      // 3. allorigins proxy 繞過 CORS
      try{
        const targetUrl='https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=futopt_WTXP%26.tw&json=1&delay=0';
        const proxyUrl=`https://api.allorigins.win/raw?url=${encodeURIComponent(targetUrl)}`;
        const r=await safeFetch(proxyUrl,{headers:{'User-Agent':'Mozilla/5.0'}},8000);
        const j=r.ok?await r.json().catch(()=>null):null;
        results['allorigins_WTXP']={status:r.status,msgArray:j?.msgArray?.slice(0,1)};
      }catch(e){results['allorigins_WTXP']={error:e.message};}

      // ── 台灣期交所 CSV：測試各種日期格式 ──────────────────
      const tw = new Date(new Date().toLocaleString('en-US',{timeZone:'Asia/Taipei'}));
      // 找最近的交易日（週一到週五）
      let testDate = new Date(tw);
      const dow = testDate.getDay();
      if(dow===0) testDate.setDate(testDate.getDate()-2);
      else if(dow===6) testDate.setDate(testDate.getDate()-1);
      const yy=testDate.getFullYear();
      const mm=String(testDate.getMonth()+1).padStart(2,'0');
      const dd=String(testDate.getDate()).padStart(2,'0');

      const dateFormats = {
        slash:  `${yy}/${mm}/${dd}`,   // 2026/04/17
        dash:   `${yy}-${mm}-${dd}`,   // 2026-04-17
        concat: `${yy}${mm}${dd}`,     // 20260417
        tw_slash:`${yy-1911}/${mm}/${dd}`, // 民國年 115/04/17
        // 也測試前一個工作日（今日可能尚未有資料）
        prev_slash:`${yy-1911}/${mm}/${String(parseInt(dd)-1).padStart(2,'0')}`,
      };

      for(const [fmt, dateVal] of Object.entries(dateFormats)){
        try{
          const url = `https://www.taifex.com.tw/cht/3/futDataDown?down_type=1&commodity_id=TX&queryStartDate=${encodeURIComponent(dateVal)}&queryEndDate=${encodeURIComponent(dateVal)}`;
          const r   = await safeFetch(url,{headers:{'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64)','Referer':'https://www.taifex.com.tw/cht/3/futContractsDate','Accept':'text/html,*/*','Accept-Language':'zh-TW,zh;q=0.9'}},6000);
          const txt = await r.text();
          results[`taifex_csv_${fmt}`] = {
            status:r.status, ct:r.headers.get('content-type')||'',
            date_used: dateVal,
            body: txt.slice(0,300),
            is_csv: txt.includes(',') && !txt.includes('<html'),
          };
        }catch(e){ results[`taifex_csv_${fmt}`]={error:e.message}; }
      }

      // ── TAIFEX REST API（需要查詢參數）───────────────────
      try{
        const tw = new Date(new Date().toLocaleString('en-US',{timeZone:'Asia/Taipei'}));
        const yy = tw.getFullYear(), mm = String(tw.getMonth()+1).padStart(2,'0'), dd = String(tw.getDate()).padStart(2,'0');
        const dateParam = `${yy}/${mm}/${dd}`;
        const apiUrl = `https://openapi.taifex.com.tw/v1/DailyFutures?queryDate=${encodeURIComponent(dateParam)}`;
        const r = await safeFetch(apiUrl, {headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'}}, 6000);
        const txt = await r.text();
        let parsed=null; try{parsed=JSON.parse(txt);}catch(e){}
        results['taifex_DailyFutures_with_date'] = {
          status:r.status, ct:r.headers.get('content-type')||'',
          body:txt.slice(0,500), isJson:parsed!==null,
          len:Array.isArray(parsed)?parsed.length:null,
          sample:Array.isArray(parsed)?parsed[0]:parsed,
        };
      }catch(e){ results['taifex_DailyFutures_with_date']={error:e.message}; }

      // ── Yahoo Finance 台股 ETF 確認可用性 ─────────────────
      for(const sym of ['%5ETWII','0050.TW']){
        try{
          const r = await safeFetch(
            `https://query2.finance.yahoo.com/v8/finance/chart/${sym}?interval=1d&range=5d`,
            {headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'}}, 6000);
          const j = r.ok ? await r.json().catch(()=>null) : null;
          const meta = j?.chart?.result?.[0]?.meta;
          results['yahoo2_'+sym] = { status:r.status, price:meta?.regularMarketPrice, prevClose:meta?.chartPreviousClose, name:meta?.shortName };
        }catch(e){ results['yahoo2_'+sym]={error:e.message}; }
      }

      // ── TAIFEX OpenAPI 原始內容 ───────────────────────────
      const taifexEndpoints = [
        'https://openapi.taifex.com.tw/v1/TXFCurrentPrice',
        'https://openapi.taifex.com.tw/v1/DailyFutures',
        'https://openapi.taifex.com.tw/v1/MarketDataOfMXF',
      ];
      for(const ep of taifexEndpoints){
        try{
          const r   = await safeFetch(ep,{headers:{'User-Agent':'Mozilla/5.0','Accept':'*/*'}},6000);
          const txt = await r.text();
          let parsed=null; try{parsed=JSON.parse(txt);}catch(e){}
          results['taifex_'+ep.split('/').pop()] = {
            status:r.status, ct:r.headers.get('content-type')||'',
            body: txt.slice(0,400), isJson:parsed!==null,
            len: Array.isArray(parsed)?parsed.length:null,
            sample: Array.isArray(parsed)?parsed[0]:parsed,
          };
        }catch(e){ results['taifex_'+ep.split('/').pop()]={error:e.message}; }
      }

      // 來源4: FinMind TaiwanFuturesDaily（測試是否付費）
      const FM_T = env.FM_TOKEN||'';
      if(FM_T){
        try{
          const today = new Date().toISOString().slice(0,10);
          const ago = new Date(Date.now()-7*86400000).toISOString().slice(0,10);
          const r = await safeFetch(
            `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFuturesDaily&data_id=TX&start_date=${ago}&end_date=${today}&token=${FM_T}`,
            {}, 8000);
          const j = await r.json();
          results['finmind_TaiwanFuturesDaily'] = {
            status: r.status,
            msg: j?.msg||'',
            rows: j?.data?.length||0,
            sample: j?.data?.[j?.data?.length-1]||null,
          };
        }catch(e){ results['finmind_TaiwanFuturesDaily'] = {error: e.message}; }
      }

      return ok(results);
    }

    // ── GET /wtxp-test ── 測試 FinMind 即時期貨 API ─────────
    if(url.pathname==='/wtxp-test'){
      const FM_T = env.FM_TOKEN||'';
      const results = {};

      // 1. TaiwanFutOptTickInfo（期貨選擇權即時報價總覽，免費）
      try{
        const r = await safeFetch(
          `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFutOptTickInfo&token=${FM_T}`,
          {}, 10000);
        const j = r.ok ? await r.json().catch(()=>null) : null;
        const rows = j?.data||[];
        // 找 WTXP 相關合約
        const wtxp = rows.filter(r=>(r.code||r.futures_id||'').includes('WTXP'));
        const tx   = rows.filter(r=>(r.code||r.futures_id||'').startsWith('TX'));
        results['TaiwanFutOptTickInfo'] = {
          status: r.status, msg: j?.msg, totalRows: rows.length,
          wtxp_count: wtxp.length, wtxp_sample: wtxp.slice(0,2),
          tx_sample: tx.slice(0,2),
          all_codes: rows.slice(0,5).map(r=>r.code||r.futures_id||r.ContractCode||'?'),
        };
      }catch(e){ results['TaiwanFutOptTickInfo']={error:e.message}; }

      // 2. taiwan_futures_snapshot（需 sponsor？測試看看）
      try{
        const r = await safeFetch(
          `https://api.finmindtrade.com/api/v4/taiwan_futures_snapshot?data_id=WTXP&token=${FM_T}`,
          {headers:{'Authorization':`Bearer ${FM_T}`}}, 10000);
        const j = r.ok ? await r.json().catch(()=>null) : null;
        results['taiwan_futures_snapshot_WTXP'] = {
          status: r.status, msg: j?.msg||'', rows: (j?.data||[]).length,
          sample: (j?.data||[]).slice(0,2),
        };
      }catch(e){ results['taiwan_futures_snapshot_WTXP']={error:e.message}; }

      // 3. 嘗試 TXF（台指期）snapshot
      try{
        const r = await safeFetch(
          `https://api.finmindtrade.com/api/v4/taiwan_futures_snapshot?data_id=TXF&token=${FM_T}`,
          {headers:{'Authorization':`Bearer ${FM_T}`}}, 10000);
        const j = r.ok ? await r.json().catch(()=>null) : null;
        results['taiwan_futures_snapshot_TXF'] = {
          status: r.status, msg: j?.msg||'', rows: (j?.data||[]).length,
          sample: (j?.data||[]).slice(0,2),
        };
      }catch(e){ results['taiwan_futures_snapshot_TXF']={error:e.message}; }

      return ok(results);
    }

    // ── GET /mxf-test3 ── 找正確的 MXF 代號 ──────────────────
    if(url.pathname==='/mxf-test3'){
      const FM_T = env.FM_TOKEN||'';
      const results = {};
      const d7  = new Date(Date.now()-7*864e5).toISOString().slice(0,10);
      const d30 = new Date(Date.now()-30*864e5).toISOString().slice(0,10);

      // 1. FutOptDailyInfo 找期貨類型（非選擇權）
      try{
        const r = await safeFetch(
          `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFutOptDailyInfo&start_date=${d7}&token=${FM_T}`,{},10000);
        const j = r.ok ? await r.json().catch(()=>null) : null;
        const rows = j?.data||[];
        // 只取期貨類型
        const futures = rows.filter(r=>r.type==='TaiwanFuturesDaily'||r.type==='futures'||!r.type?.includes('Option'));
        results['FutOptDailyInfo_futures_only'] = {
          rows: futures.length,
          codes: futures.map(r=>r.code).slice(0,20),
          sample: futures.slice(0,3),
        };
        // 找含 MX 的
        const mxRows = rows.filter(r=>(r.code||'').includes('MX')||(r.name||'').includes('小台'));
        results['MX_related'] = { rows: mxRows.length, sample: mxRows.slice(0,5) };
      }catch(e){ results['FutOptDailyInfo_futures']={error:e.message}; }

      // 2. 直接嘗試各種可能的小台代號
      for(const id of ['MXF','MX','MXFB5','MXFC5','小台指','MTX']){
        try{
          const r = await safeFetch(
            `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFuturesInstitutionalInvestors&data_id=${id}&start_date=${d30}&token=${FM_T}`,{},8000);
          const j = r.ok ? await r.json().catch(()=>null) : null;
          results['Inst_'+id] = { status:r.status, msg:j?.msg, rows:(j?.data||[]).length, sample:(j?.data||[]).slice(-2) };
        }catch(e){ results['Inst_'+id]={error:e.message}; }
      }

      // 3. TaiwanFuturesDaily 嘗試各代號
      for(const id of ['MXF','MX','MTX']){
        try{
          const r = await safeFetch(
            `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFuturesDaily&data_id=${id}&start_date=${d30}&token=${FM_T}`,{},8000);
          const j = r.ok ? await r.json().catch(()=>null) : null;
          results['Daily_'+id] = { status:r.status, msg:j?.msg, rows:(j?.data||[]).length, sample:(j?.data||[]).slice(-1) };
        }catch(e){ results['Daily_'+id]={error:e.message}; }
      }

      return ok(results);
    }

    // ── GET /mxf-test2 ── 更廣泛的小台期貨資料診斷 ──────────
    if(url.pathname==='/mxf-test2'){
      const FM_T = env.FM_TOKEN||'';
      const results = {};
      const d30 = new Date(Date.now()-30*864e5).toISOString().slice(0,10);
      const d7  = new Date(Date.now()-7*864e5).toISOString().slice(0,10);

      // 測試各種資料集和代號組合
      const tests = [
        // 期貨日成交 - 不帶 data_id，看全部
        ['FuturesDaily_noId',     `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFuturesDaily&start_date=${d7}&token=${FM_T}`],
        // 期貨三大法人 - 不帶 data_id
        ['FutInst_noId',          `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFuturesInstitutionalInvestors&start_date=${d7}&token=${FM_T}`],
        // FutOptDailyInfo（期貨選擇權日成交總覽）
        ['FutOptDailyInfo',       `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFutOptDailyInfo&start_date=${d7}&token=${FM_T}`],
        // 期貨各券商每日 MXF
        ['DealerVol_MXF',         `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFuturesDealerTradingVolumeDaily&data_id=MXF&start_date=${d7}&token=${FM_T}`],
        // 不帶 token 測試（公開資料）
        ['FuturesDaily_notoken',  `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFuturesDaily&start_date=${d7}`],
      ];

      for(const [name, url2] of tests){
        try{
          const r = await safeFetch(url2, {}, 10000);
          const j = r.ok ? await r.json().catch(()=>null) : null;
          const rows = j?.data||[];
          results[name] = {
            status: r.status, msg: j?.msg, rows: rows.length,
            // 顯示出現的 data_id 種類
            ids: [...new Set(rows.slice(0,50).map(r=>r.data_id||r.futures_id||r.commodity_id||r.name||'?'))].slice(0,10),
            sample: rows.slice(0,2),
          };
        }catch(e){ results[name]={error:e.message}; }
      }
      return ok(results);
    }

    // ── GET /mxf-test ── 小台散戶多空比資料來源診斷 ──────────
    if(url.pathname==='/mxf-test'){
      const FM_T = env.FM_TOKEN||'';
      const results = {};
      const today = new Date().toLocaleDateString('zh-TW',{timeZone:'Asia/Taipei'})
        .split('/').map((v,i)=>i===0?v:(v.padStart(2,'0'))).join('-')
        .replace(/^(\d+)-(\d+)-(\d+)$/,(_,y,m,d)=>`${y}-${m}-${d}`);
      // 往前30天
      const d30 = new Date(Date.now()-30*864e5).toISOString().slice(0,10);

      // 1. MXF 期貨三大法人（多空部位）
      try{
        const r = await safeFetch(
          `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFuturesInstitutionalInvestors`+
          `&data_id=MXF&start_date=${d30}&token=${FM_T}`, {}, 12000);
        const j = r.ok ? await r.json().catch(()=>null) : null;
        const rows = j?.data||[];
        results['MXF_InstitutionalInvestors'] = {
          status: r.status, msg: j?.msg, rows: rows.length,
          sample: rows.slice(-3),
          fields: rows[0] ? Object.keys(rows[0]) : [],
        };
      }catch(e){ results['MXF_InstitutionalInvestors']={error:e.message}; }

      // 2. MXF 期貨日成交資訊（含散戶推算所需欄位）
      try{
        const r = await safeFetch(
          `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFuturesDaily`+
          `&data_id=MXF&start_date=${d30}&token=${FM_T}`, {}, 12000);
        const j = r.ok ? await r.json().catch(()=>null) : null;
        const rows = j?.data||[];
        results['MXF_FuturesDaily'] = {
          status: r.status, msg: j?.msg, rows: rows.length,
          sample: rows.slice(-2),
          fields: rows[0] ? Object.keys(rows[0]) : [],
        };
      }catch(e){ results['MXF_FuturesDaily']={error:e.message}; }

      // 3. TXF 期貨三大法人（對比用）
      try{
        const r = await safeFetch(
          `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFuturesInstitutionalInvestors`+
          `&data_id=TXF&start_date=${d30}&token=${FM_T}`, {}, 12000);
        const j = r.ok ? await r.json().catch(()=>null) : null;
        const rows = j?.data||[];
        results['TXF_InstitutionalInvestors'] = {
          status: r.status, rows: rows.length,
          sample: rows.slice(-2),
          fields: rows[0] ? Object.keys(rows[0]) : [],
        };
      }catch(e){ results['TXF_InstitutionalInvestors']={error:e.message}; }

      return ok(results);
    }

    // ── GET /debug ── 確認環境變數設定（不洩漏值）──────────
    if(url.pathname==='/debug'){
      const keys = [
        env.GEMINI_API_KEY,  env.GEMINI_API_KEY2, env.GEMINI_API_KEY3,
        env.GEMINI_API_KEY4, env.GEMINI_API_KEY5,
      ].filter(Boolean);
      return new Response(JSON.stringify({
        geminiKeys: keys.length,
        keyStatus:  keys.map((k,i)=>`KEY${i+1}: ${k?k.slice(0,8)+'...('+k.length+'chars)':'missing'}`),
        fmToken:    env.FM_TOKEN ? 'set('+env.FM_TOKEN.length+'chars)' : 'missing',
        cacheStore: !!env.CACHE_STORE,
        store:      !!env.STORE,
      },null,2),{headers:{...CORS,'Content-Type':'application/json'}});
    }

    // ── GET /news ── 真實 RSS 新聞 ─────────────────────────
    if(url.pathname==='/news' && request.method==='GET'){
      const lang    = url.searchParams.get('lang')||'en';
      const refresh = url.searchParams.get('refresh')==='1';

      // 並行抓取所有 RSS（最多 3 個，避免 Worker 超時）
      const sources = NEWS_SOURCES; // 全部12個來源並行抓取
      const results = await Promise.allSettled(
        sources.map(async src=>{
          try{
            const r = await safeFetch(src.url,
              {headers:{'User-Agent':'Mozilla/5.0 (compatible; StockAI/1.0)','Accept':'application/rss+xml,application/xml,text/xml'}},
              5000);
            if(!r.ok) return [];
            const xml = await r.text();
            // 判斷 RSS 或 Atom
            const items = xml.includes('<entry') ? parseAtom(xml,src) : parseRSS(xml,src);
            return items;
          }catch(e){ return []; }
        })
      );

      // 合併所有新聞
      let allItems = results.flatMap(r=>r.status==='fulfilled'?r.value:[]);

      // 計算關聯度並過濾排序
      allItems = allItems
        .map(item=>({
          ...item,
          relevance:  calcRelevance(item) + extraRelevance(item),
          direction:  calcDirection(item),
          impact:     calcImpact(item, calcRelevance(item) + extraRelevance(item)),
          related_sectors: calcSectors(item),
        }))
        .filter(item=>item.relevance>=1 || ['macro','semi','rates','trade'].includes(item.category))
        .sort((a,b)=>b.relevance-a.relevance)
        .slice(0,10); // 最多10則（多類別更均衡）

      // 去重（相同標題前20字）
      const seen = new Set();
      allItems = allItems.filter(item=>{
        const key = item.title.slice(0,20);
        if(seen.has(key)) return false;
        seen.add(key); return true;
      });

      // 用 Gemini 翻譯標題、生成摘要、整體簡報
      let summary='', tags=[];
      // 新聞翻譯：多 Key 輪替
      const newsAllKeys = [
        env.GEMINI_API_KEY, env.GEMINI_API_KEY2, env.GEMINI_API_KEY3,
        env.GEMINI_API_KEY4, env.GEMINI_API_KEY5,
      ].filter(Boolean);
      if(newsAllKeys.length && allItems.length>0){
        try{
          // 傳入更多新聞給 AI，包含來源分類
          const headlines = allItems.slice(0,10).map((n,i)=>{
            const cat = n.category||'';
            return `${i+1}. [${n.source}|${cat}] ${n.title}`;
          }).join('\n');

          const prompt = `你是專業的財經新聞編譯，請將以下國際財經新聞標題翻譯為繁體中文。

原文標題：
${headlines}

翻譯原則：
- 專業財經術語直接使用（Fed、GDP、CPI、ETF、TSMC、AI 等保留英文縮寫）
- 公司名稱：知名企業保留英文（Apple、Nvidia、TSMC），一般企業中文化
- 人名：第一次出現標示原文（鮑爾(Powell)）
- 數字與百分比完整保留
- 不翻譯：標題要能獨立理解，不能省略關鍵資訊
- 長度：12~20字，保留核心事實，去除冗詞
- 語態：名詞化、不用動詞句（例：「Fed 升息一碼，市場震盪」而非「Fed 決定升息一碼後市場開始震盪」）

整體市場簡報：3句話，從台股投資人角度，說明這些消息對台股的實際影響。

只輸出JSON（不加任何說明）：
{"titles":["標題1","標題2"...],"summary":"整體市場簡報","tags":[{"label":"主題","type":"利多/利空/觀察/中性"}]}`;

          // 直接嘗試每個 Key，429 時換下一個（不用 probe，避免浪費配額）
          let translated = false;
          for(const newsKey of newsAllKeys){
            try{
              const gRes = await safeFetch(
                `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${newsKey}`,
                {method:'POST',headers:{'Content-Type':'application/json'},
                 body:JSON.stringify({contents:[{role:'user',parts:[{text:prompt}]}],
                   generationConfig:{maxOutputTokens:1200}})},25000);

              if(gRes.status === 429){
                console.warn('[News] 429 with key, trying next...');
                continue; // 換下一個 Key
              }

              if(gRes.ok){
                const gData = await gRes.json();
                const text  = gData?.candidates?.[0]?.content?.parts?.[0]?.text||'';
                console.log('[News] Gemini response length:', text.length, 'preview:', text.slice(0,100));
                const clean = text.replace(/```json|```/g,'').trim();
                const m     = clean.match(/\{[\s\S]*\}/);
                if(m){
                  const parsed = JSON.parse(m[0]);
                  if(parsed.titles && parsed.titles.length > 0){
                    parsed.titles.forEach((t,i)=>{ if(allItems[i] && t) allItems[i].title_zh=t; });
                    summary = parsed.summary||'';
                    tags    = parsed.tags||[];
                    translated = true;
                    console.log('[News] translated', parsed.titles.length, 'titles OK');
                    break; // 成功，不需要繼續嘗試
                  }
                }
              }
            }catch(e){
              console.warn('[News] key failed:', e.message);
            }
          }
          if(!translated) console.warn('[News] all keys failed, showing English titles');
        }catch(e){ console.warn('[News] Gemini outer failed:', e.message); }
      }

      // 格式化輸出
      const output = allItems.map(n=>({
        title:       n.title_zh || n.title, // 優先用中文
        title_en:    n.title,
        category:    n.category,
        impact:      n.impact,
        direction:   n.direction,
        summary:     n.desc||'',
        impact_tw:   '', // 前端 AI 分析補充
        related_sectors: n.related_sectors,
        source:      n.source,
        link:        n.link,
        pubDate:     n.pubDate,
      }));

      return ok({ items: output, summary, tags, fetchedAt: new Date().toISOString() });
    }

    // ── GET /finmind ────────────────────────────────────────
    if(url.pathname==='/finmind' && request.method==='GET'){
      const params=url.searchParams;
      if(FM_TOKEN) params.set('token',FM_TOKEN);
      const cacheKey=buildCacheKey(params);
      const ttlSecs = getCacheTTL(params.get('dataset'));
      // 盤中也讀 KV（60秒TTL），減少 FinMind API 消耗
      if(env.CACHE_STORE){
        try{
          const cached=await env.CACHE_STORE.get(cacheKey);
          if(cached) return new Response(cached,{headers:{...CORS,'Content-Type':'application/json','X-Cache':'HIT'}});
        }catch(e){}
      }
      try{
        const r=await safeFetch(`${FM_BASE}?${params.toString()}`,{headers:{'User-Agent':'StockAI/1.0'}},15000);
        const body=await r.text();
        if(r.ok && env.CACHE_STORE){
          try{ await env.CACHE_STORE.put(cacheKey,body,{expirationTtl:ttlSecs}); }catch(e){}
        }
        return new Response(body,{status:r.status,headers:{...CORS,'Content-Type':'application/json','X-Cache':'MISS'}});
      }catch(e){ return err(e.message); }
    }

    // ── GET /intraday ───────────────────────────────────────
    if(url.pathname==='/intraday'){
      const symbol=url.searchParams.get('symbol');
      const src=url.searchParams.get('src');
      if(!symbol) return err('symbol required',400);
      if(src==='twse'||symbol.includes('tse_t00')){
        try{
          const r=await safeFetch('https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_t00.tw&json=1&delay=0',{headers:{'User-Agent':'Mozilla/5.0','Referer':'https://mis.twse.com.tw/'}});
          if(!r.ok) return err('TWSE '+r.status,r.status);
          const raw = await r.json();
          const item = raw?.msgArray?.[0];
          if(!item) return err('no tse_t00 data');
          // TWSE tse_t00 欄位格式：
          // c = "37083.50,37083.50,..." 逗號分隔每分鐘收盤（盤後可能為 "-" 或空）
          // v = "123456,234567,..."     逗號分隔每分鐘成交量
          // z = 最新成交價（盤後為收盤）
          // y = 昨日收盤
          const priceRaw = String(item.c||'').trim();
          const volRaw   = String(item.v||'').trim();
          const isValid  = priceRaw && priceRaw !== '-' && priceRaw !== '0';
          const openTime = 9*60; // 09:00
          const pts=[], times=[], vol=[];

          if(isValid){
            const prices = priceRaw.split(',');
            const volArr = volRaw ? volRaw.split(',') : [];
            prices.forEach((p,i)=>{
              const v = parseFloat(p);
              if(!v||isNaN(v)||v<100) return; // 過濾無效值
              pts.push(v);
              vol.push(parseInt(volArr[i]||0)||0);
              const m = openTime + i;
              times.push(String(Math.floor(m/60)).padStart(2,'0')+':'+String(m%60).padStart(2,'0'));
            });
          }

          // 盤後或無分鐘資料：用 Yahoo Finance 取今日走勢
          if(pts.length < 3){
            try{
              const yr = await safeFetch(
                'https://query1.finance.yahoo.com/v8/finance/chart/%5ETWII?interval=5m&range=1d',
                {headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'}}, 10000);
              if(yr.ok){
                const yj = await yr.json();
                const yres = yj?.chart?.result?.[0];
                if(yres){
                  const ts    = yres.timestamp||[];
                  const close = yres.indicators?.quote?.[0]?.close||[];
                  const yv    = yres.indicators?.quote?.[0]?.volume||[];
                  const yPrev = yres.meta?.chartPreviousClose||0;
                  for(let i=0;i<ts.length;i++){
                    if(!close[i]||isNaN(close[i])) continue;
                    const d  = new Date(ts[i]*1000+8*3600000);
                    pts.push(close[i]);
                    times.push(String(d.getUTCHours()).padStart(2,'0')+':'+String(d.getUTCMinutes()).padStart(2,'0'));
                    vol.push(yv[i]||0);
                  }
                  const yy    = parseFloat(String(item.y||0).replace(/,/g,''))||yPrev;
                  const lastPt = pts.length ? pts[pts.length-1] : yy;
                  return new Response(JSON.stringify({
                    pts, times, vol, y: yy,
                    msgArray:[{
                      z:String(lastPt), y:String(yy),
                      o:String(item.o||yy), h:String(item.h||lastPt),
                      l:String(item.l||lastPt), t:times[times.length-1]||''
                    }]
                  }),{headers:{...CORS,'Content-Type':'application/json'}});
                }
              }
            }catch(ye){}
          }

          const y     = parseFloat(String(item.y||0).replace(/,/g,''))||0;
          const z     = parseFloat(String(item.z||item.y||0).replace(/,/g,''))||0;
          const price = z || (pts.length ? pts[pts.length-1] : 0);
          const change    = y>0 ? +(price-y).toFixed(2) : 0;
          const changePct = y>0 ? +((price-y)/y*100).toFixed(2) : 0;
          if(!price) return err('tse_t00 no data',404);
          // 回傳兼容格式：同時支援 parseMis（msgArray）和 loadTaiexChart（pts）
          return new Response(JSON.stringify({
            // 新格式（loadTaiexChart 用）
            pts, times, vol, y,
            // 兼容格式（parseMis 用）
            msgArray:[{
              z: String(price), y: String(y),
              o: String(item.o||y), h: String(item.h||price),
              l: String(item.l||price), t: item.t||'',
              tv: item.tv||'0', v: item.v||''
            }],
          }),{headers:{...CORS,'Content-Type':'application/json'}});
        }catch(e){return err(e.message);}
      }
      try{
        const r=await safeFetch(
          `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=${url.searchParams.get('interval')||'1m'}&range=${url.searchParams.get('range')||'1d'}`,
          {headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'}});
        if(!r.ok) return err('Yahoo '+r.status,r.status);
        // 過濾最後幾筆異常值（盤後交易資料可能造成異常跳點）
        try{
          const j = await r.json();
          const closes = j?.chart?.result?.[0]?.indicators?.quote?.[0]?.close||[];
          if(closes.length > 5){
            // 計算最後10筆的中位數，過濾偏差>15%的末尾點
            const last10 = closes.slice(-10).filter(v=>v!=null&&v>0);
            const sorted = [...last10].sort((a,b)=>a-b);
            const median = sorted[Math.floor(sorted.length/2)];
            const lastVal = closes[closes.length-1];
            if(median>0 && lastVal!=null && Math.abs(lastVal-median)/median > 0.15){
              // 找最後一個合理值替換
              for(let i=closes.length-1;i>=0;i--){
                const v=closes[i];
                if(v!=null&&v>0&&Math.abs(v-median)/median<=0.15){
                  closes[closes.length-1]=v;
                  console.log('[Intraday] anomaly fixed:',lastVal,'→',v);
                  break;
                }
              }
            }
          }
          return new Response(JSON.stringify(j),{headers:{...CORS,'Content-Type':'application/json'}});
        }catch(e){
          return new Response(await r.text(),{headers:{...CORS,'Content-Type':'application/json'}});
        }
      }catch(e){return err(e.message);}
    }

    // ── GET /marketvol ── 大盤每日成交額 ───────────────────
    if(url.pathname==='/marketvol' && request.method==='GET'){
      // 來源1：TWSE OpenAPI FMTQIK（每日歷史，含今日盤後數據）
      try{
        const r = await safeFetch(
          'https://openapi.twse.com.tw/v1/exchangeReport/FMTQIK',
          {headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'}}, 8000);
        if(r.ok){
          const json = await r.json();
          // FMTQIK 欄位：Date, TradeVolume, TradeValue, Transaction, TAIEX, Change
          // TradeValue 是上市成交金額（元），TradeVolume 是成交量（股）
          if(Array.isArray(json) && json.length > 0){
            // 取最近30筆
            const rows = json.slice(-30).map(d=>({
              date:     d.Date||d.date||'',
              value:    parseInt((d.TradeValue||'0').replace(/,/g,'')),   // 元
              volume:   parseInt((d.TradeVolume||'0').replace(/,/g,'')),
              taiex:    parseFloat((d.TAIEX||'0').replace(/,/g,'')),
              change:   parseFloat((d.Change||'0').replace(/[+,]/g,'')),
            }));
            return ok({source:'TWSE_FMTQIK', rows});
          }
        }
      }catch(e){ console.warn('[marketvol] FMTQIK failed:', e.message); }

      // 來源2：TWSE mis 盤中即時（只有今日）
      try{
        const r = await safeFetch(
          'https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_t00.tw&json=1&delay=0',
          {headers:{'User-Agent':'Mozilla/5.0','Referer':'https://mis.twse.com.tw/'}}, 6000);
        if(r.ok){
          const body = await r.json();
          const item = body?.msgArray?.[0];
          if(item){
            return ok({
              source:'TWSE_MIS',
              today:{
                value:  parseFloat(item.tv||0),   // 成交金額（萬元）
                volume: parseFloat(item.v||0),     // 成交量（張）
                price:  parseFloat(item.z||item.y||0),
                time:   item.t||'',
              }
            });
          }
        }
      }catch(e){ console.warn('[marketvol] MIS failed:', e.message); }

      return err('marketvol unavailable', 503);
    }

    // ── POST /v1/messages ── Gemini（多 Key 輪替 + KV 快取 + 降級）──
    if(url.pathname==='/v1/messages' && request.method==='POST'){

      // ── 收集所有可用 Key（支援最多 5 個輪替）─────────────
      // Cloudflare Variables: GEMINI_API_KEY, GEMINI_API_KEY2, ..., GEMINI_API_KEY5
      const allKeys = [
        env.GEMINI_API_KEY,  env.GEMINI_API_KEY2, env.GEMINI_API_KEY3,
        env.GEMINI_API_KEY4, env.GEMINI_API_KEY5,
      ].filter(Boolean);

      if(!allKeys.length){
        return ok({error:'Gemini API key not configured',content:[]});
      }

      try{
        const body     = await request.json();
        const contents = (body.messages||[]).map(m=>({
          role:  m.role==='assistant' ? 'model' : 'user',
          parts: [{text: typeof m.content==='string' ? m.content : JSON.stringify(m.content)}],
        }));
        const reqTokens = body.max_tokens || 1000;

        // ── KV 快取：相同 prompt 在 10 分鐘內直接回傳（節省 quota）──
        const promptHash = contents.map(c=>c.parts[0].text).join('|').slice(0,200);
        const cacheKey   = 'ai:'+btoa(encodeURIComponent(promptHash)).slice(0,60);
        if(env.CACHE_STORE){
          try{
            const cached = await env.CACHE_STORE.get(cacheKey);
            if(cached){
              console.log('[Gemini] KV cache hit');
              return ok({content:[{type:'text',text:cached}], cached:true});
            }
          }catch(e){}
        }

        // ── Model 優先順序（依 /test-models 實測可用）────────
        // gemini-2.5-flash：可用，功能強
        // gemma-3-27b-it：可用，開源大模型備援
        // gemini-2.0-flash / lite：目前被 429，保留作未來備用
        const modelOrder = [
          'gemini-2.5-flash',
          'gemma-3-27b-it',
          'gemini-2.0-flash',
          'gemini-2.0-flash-lite',
        ];

        let lastError = '';

        // ── 多 Model × 多 Key 輪替 ──────────────────────────
        // 注意：同帳號的 Key 共享配額，先換 Model 再換 Key 更有效
        for(const model of modelOrder){
          for(let ki=0; ki<allKeys.length; ki++){
            const key = allKeys[ki];
            try{
              const gResp = await safeFetch(
                `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${key}`,
                {method:'POST', headers:{'Content-Type':'application/json'},
                 body: JSON.stringify({
                   contents,
                   generationConfig:{ maxOutputTokens: Math.max(2000, Math.min(reqTokens, 8192)) }
                 })}, 35000);

              if(gResp.status === 429){
                console.warn(`[Gemini] 429 model=${model} key#${ki+1}`);
                lastError = `${model} 速率限制`;
                // 同帳號 Key 共享配額，遇到 429 直接換 model（不值得繼續試同帳號其他 key）
                break; // 跳出 key 迴圈，換下一個 model
              }

              const gData = await gResp.json();
              if(!gResp.ok){
                lastError = gData?.error?.message || `HTTP ${gResp.status}`;
                console.warn(`[Gemini] ${gResp.status} model=${model}:`, lastError);
                break; // 換 model
              }

              const text = gData?.candidates?.[0]?.content?.parts?.[0]?.text || '';
              if(!text){
                lastError = gData?.candidates?.[0]?.finishReason || 'empty response';
                break;
              }

              console.log(`[Gemini] OK model=${model} key#${ki+1} len=${text.length}`);
              console.log(`[Gemini] preview: ${text.slice(0,300).replace(/\n/g,' ')}`);

              // 寫入 KV 快取（5 分鐘，短一點確保結果新鮮）
              if(env.CACHE_STORE){
                try{ await env.CACHE_STORE.put(cacheKey, text, {expirationTtl:300}); }catch(e){}
              }

              return ok({content:[{type:'text',text}], model, keyIndex:ki+1});

            }catch(e){
              lastError = e.message;
              console.warn(`[Gemini] exception model=${model}:`, e.message);
              break;
            }
          }
          console.warn(`[Gemini] model=${model} failed (${lastError}), trying next model`);
        }

        // 所有 model 都失敗
        throw new Error('AI 模型暫時無法回應，請稍後重試（'+lastError+'）');

      }catch(e){
        console.error('[Gemini final error]', e.message);
        return ok({error: e.message, content:[]});
      }
    }

    // ── GET /vix ────────────────────────────────────────────
    if(url.pathname==='/vix'){
      const parse=(csv,col)=>csv.trim().split('\n').slice(1)
        .map(l=>{const p=l.split(',');return{date:p[0]?.trim(),close:parseFloat(p[col])};})
        .filter(r=>r.date&&!isNaN(r.close)&&r.close>5).sort((a,b)=>a.date>b.date?1:-1);
      for(const [u,col,src] of[
        ['https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS',1,'FRED'],
        ['https://stooq.com/q/d/l/?s=%5Evix&i=d',4,'stooq'],
      ]){
        try{
          const r=await safeFetch(u,{headers:{'User-Agent':'StockAI/1.0'}});
          if(!r.ok)continue;
          const rows=parse(await r.text(),col);
          if(!rows.length)continue;
          const last=rows[rows.length-1],prev=rows[rows.length-2];
          return ok({vix:last.close,chg:prev?(last.close-prev.close)/prev.close*100:0,
            date:last.date,hist:rows.slice(-22).map(r=>r.close),src});
        }catch(e){}
      }
      return err('VIX unavailable',503);
    }

    // ── GET /dispose ────────────────────────────────────────
    if(url.pathname==='/dispose'){
      const today=new Date().toISOString().slice(0,10).replace(/-/g,'');
      const weekAgo=new Date(Date.now()-7*86400000).toISOString().slice(0,10).replace(/-/g,'');
      const headers={'User-Agent':'Mozilla/5.0','Referer':'https://www.twse.com.tw/'};
      const dispose=new Set(),notice=new Set();
      const tryFetch=async(u)=>{
        try{
          const r=await safeFetch(u,{headers},8000);
          if(!r.ok)return null;
          const t=await r.text();
          try{
            const j=JSON.parse(t);const rows=j.data||j.Data||[];
            const ids=[];
            for(const row of rows){const cells=Array.isArray(row)?row:Object.values(row);for(const c of cells){const s=String(c||'').trim();if(/^\d{4}$/.test(s))ids.push(s);}}
            return ids;
          }catch(e){return[...(t.matchAll(/<td[^>]*>\s*(\d{4})\s*<\/td>/g))].map(m=>m[1]);}
        }catch(e){return null;}
      };
      const [d1,n1]=await Promise.all([
        tryFetch(`https://www.twse.com.tw/rwd/zh/announcement/punish?response=json&startDate=${weekAgo}&endDate=${today}`),
        tryFetch(`https://www.twse.com.tw/rwd/zh/announcement/notice?response=json&startDate=${weekAgo}&endDate=${today}`),
      ]);
      (d1||[]).forEach(id=>dispose.add(id));
      (n1||[]).forEach(id=>{if(!dispose.has(id))notice.add(id);});
      return ok({dispose:[...dispose].map(id=>({id})),notice:[...notice].map(id=>({id})),updated:new Date().toISOString()});
    }

    // ── KV 用戶資料 ─────────────────────────────────────────
    if(url.pathname==='/kv'){
      if(!env.STORE) return err('KV not configured',503);
      const uid=url.searchParams.get('uid'),key=url.searchParams.get('key');
      if(!uid||!key) return err('uid and key required',400);
      if(!/^[a-zA-Z0-9_-]{6,64}$/.test(uid)) return err('invalid uid',400);
      if(!/^[a-zA-Z0-9_-]{1,32}$/.test(key))  return err('invalid key',400);
      const kvKey=`${uid}:${key}`;
      if(request.method==='GET'){const val=await env.STORE.get(kvKey);return ok(val===null?{exists:false,data:null}:{exists:true,data:JSON.parse(val)});}
      if(request.method==='PUT'){const body=await request.text();if(body.length>512*1024)return err('data too large',413);await env.STORE.put(kvKey,body,{expirationTtl:60*60*24*365});return ok({success:true});}
      if(request.method==='DELETE'){await env.STORE.delete(kvKey);return ok({success:true});}
      return err('method not allowed',405);
    }

    // ── GET /mxf ── 小台指散戶多空比（FinMind MXF法人反向推算）────
    if(url.pathname==='/mxf'){
      const FM_T = env.FM_TOKEN||'';
      const cKey = 'mtx:ratio';
      try{
        const cached = await env.CACHE_STORE?.get(cKey);
        if(cached){
          const cd = JSON.parse(cached);
          const ttl = _isTradingHour() ? 10*60*1000 : _msTillMidnight();
          if(Date.now()-cd.ts < ttl)
            return new Response(JSON.stringify(cd.data),{headers:{...CORS,'Content-Type':'application/json'}});
        }
      }catch(e){}

      const d30 = new Date(Date.now()-30*864e5).toISOString().slice(0,10);
      try{
        // MXF 三大法人未沖銷部位
        const r = await safeFetch(
          'https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFuturesInstitutionalInvestors'
          +'&data_id=MTX&start_date='+d30+'&token='+FM_T, {}, 12000);
        if(!r.ok) return err('FinMind '+r.status);
        const j = await r.json();
        const rows = j?.data||[];
        if(!rows.length) return err('no MTX data');

        // 按日期分組，各身份別多空未沖銷口數
        // 欄位：long_open_interest_balance_volume / short_open_interest_balance_volume
        const byDate={};
        rows.forEach(row=>{
          const d=row.date;
          if(!byDate[d]) byDate[d]={};
          byDate[d][row.institutional_investors||row.name]={
            long: parseInt(row.long_open_interest_balance_volume)||0,
            short:parseInt(row.short_open_interest_balance_volume)||0
          };
        });

        const dates = Object.keys(byDate).sort().slice(-20);
        const history = dates.map(d=>{
          const data = byDate[d];
          let instLong=0, instShort=0;
          Object.values(data).forEach(v=>{ instLong+=v.long; instShort+=v.short; });
          const instNet = instLong - instShort;
          return { date:d, instLong, instShort, instNet,
            instNetK: Math.round(instNet/100)/10 };
        });

        const latest = history[history.length-1];
        const prev   = history[history.length-2];
        const trend  = !latest||!prev ? 'neutral'
          : latest.instNet > prev.instNet ? 'bull'
          : latest.instNet < prev.instNet ? 'bear' : 'neutral';

        // 散戶情緒：法人淨多 → 散戶偏空，反之偏多
        const retailBull = latest ? latest.instNet < 0 : false;

        const out = {
          date: latest?.date||'',
          instNet: latest?.instNet||0,
          instNetK: latest?.instNetK||0,
          instLong: latest?.instLong||0,
          instShort: latest?.instShort||0,
          retailBull, trend,
          history,
        };
        try{ await env.CACHE_STORE?.put(cKey, JSON.stringify({ts:Date.now(),data:out})); }catch(e){}
        return new Response(JSON.stringify(out),{headers:{...CORS,'Content-Type':'application/json'}});
      }catch(e){ return err('MXF: '+e.message); }
    }

    // ── GET /news ── 個股新聞（Yahoo Finance RSS，免費）──────
    if(url.pathname==='/news'){
      const symbol = url.searchParams.get('symbol')||'2330.TW';
      const cKey   = 'news:'+symbol;
      try{
        const cached = await env.CACHE_STORE?.get(cKey);
        if(cached){
          const cd = JSON.parse(cached);
          if(Date.now()-cd.ts < 30*60*1000)  // 快取30分鐘
            return new Response(JSON.stringify(cd.data),{headers:{...CORS,'Content-Type':'application/json'}});
        }
      }catch(e){}

      try{
        const r = await safeFetch(
          `https://query1.finance.yahoo.com/v1/finance/search?q=${encodeURIComponent(symbol)}&newsCount=5&enableFuzzyQuery=false&enableCb=false`,
          {headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'}}, 8000);
        if(!r.ok) return err('Yahoo news '+r.status);
        const j = await r.json();
        const news = (j?.news||[]).slice(0,5).map(n=>({
          title:       n.title||'',
          date:        n.providerPublishTime
            ? new Date(n.providerPublishTime*1000).toISOString().slice(0,10)
            : '',
          link:        n.link||'',
          description: n.publisher||'Yahoo Finance',
        }));
        const out = { data: news };
        try{ await env.CACHE_STORE?.put(cKey, JSON.stringify({ts:Date.now(),data:news})); }catch(e){}
        return new Response(JSON.stringify(out),{headers:{...CORS,'Content-Type':'application/json'}});
      }catch(e){ return err('news: '+e.message); }
    }

    // ── GET /txf ── 台指期即時/盤後資料 ────────────────────
    if(url.pathname==='/txf'){
      const txfCacheKey = 'txf:price:v4'; // v4: TAIFEX CSV + Yahoo histClose
      const txfTTL = isTWSETradingNow() ? 120 : 30*60;

      // 快取命中
      if(env.CACHE_STORE){
        try{
          const cached = await env.CACHE_STORE.get(txfCacheKey);
          if(cached) return ok({...JSON.parse(cached), cached:true});
        }catch(e){}
      }

      const now_tw = new Date(new Date().toLocaleString('en-US',{timeZone:'Asia/Taipei'}));
      const hh=now_tw.getHours(), mm_=now_tw.getMinutes();
      const isTrading = (hh===9)||(hh>9&&hh<13)||(hh===13&&mm_<=45);
      const isNight   = (hh>=15)||(hh<5); // WTXP 夜盤：15:00-次日05:00
      const isSettle  = (hh===13&&mm_>45)||(hh===14); // 日盤結算後、夜盤前
      const session   = isTrading?'盤中':isNight?'夜盤':'盤後';
      // 夜盤時段取 WTXP，其餘取 TX
      const commodity = isNight ? 'WTXP' : 'TX';
      const dateStr   = `${String(now_tw.getMonth()+1).padStart(2,'0')}-${String(now_tw.getDate()).padStart(2,'0')}`;
      console.log('[TXF] session:', session, 'commodity:', commodity, 'time:', hh+':'+String(mm_).padStart(2,'0'));

      // ── 來源1: TAIFEX OpenAPI（台灣期交所官方，免費即時）──
      // 嘗試多個 TAIFEX 端點
      const taifexUrls = [
        'https://openapi.taifex.com.tw/v1/TXFCurrentPrice',
        'https://openapi.taifex.com.tw/v1/DailyFutures?queryStartDate='+now_tw.toLocaleDateString('zh-TW',{year:'numeric',month:'2-digit',day:'2-digit'}).replace(/\//g,'/'),
      ];
      for(const tu of taifexUrls){
        try{
          const tr = await safeFetch(tu,{headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json'}},6000);
          if(!tr.ok) continue;
          const tj = await tr.json();
          const rows = Array.isArray(tj)?tj:tj?.data||[];
          // 找 TX（台指期）近月資料
          const txRow = rows.find(r=>{
            const id = r['商品代號']||r.ContractCode||r.futures_id||r.FuturesID||'';
            return id.startsWith('TX') && !id.startsWith('TXO');
          });
          if(txRow){
            const priceRaw = txRow['成交價']||txRow['收盤']||txRow.LastPrice||txRow.Close||txRow.close||0;
            const price = parseFloat(String(priceRaw).replace(/,/g,''));
            if(price > 15000){
              const prevRaw   = txRow['前日收盤']||txRow['前日結算']||txRow.PreviousClose||txRow.Settlement||0;
              const prevClose = parseFloat(String(prevRaw).replace(/,/g,''))||0;
              const changeRaw = txRow['漲跌']||txRow['漲跌點數']||txRow.Change||0;
              let   change    = parseFloat(String(changeRaw).replace(/[+,]/g,''))||0;
              if(!change && prevClose>0) change = Math.round(price-prevClose);
              const changePct = prevClose>0 ? +((price-prevClose)/prevClose*100).toFixed(2) : 0;
              const result    = {price, change, changePct, session, prevClose, source:'TAIFEX', date:dateStr};
              if(env.CACHE_STORE) try{ await env.CACHE_STORE.put(txfCacheKey,JSON.stringify(result),{expirationTtl:txfTTL});}catch(e){}
              console.log('[TXF] TAIFEX OK:', price, change, changePct+'%');
              return ok(result);
            }
          }
        }catch(e){ console.warn('[TXF] TAIFEX error:', e.message); }
      }

      // ── 來源1b: 台灣期交所 CSV（西元年斜線，MS950編碼）──────
      // 診斷確認：2026/04/17 格式可取得真實 CSV，編碼為 MS950
      try{
        // 找最近交易日（往前找，跳過週末）
        let tDate = new Date(now_tw);
        // 若盤前（08:45前），取前一交易日
        if(hh<9||(hh===8&&mm<45)) tDate.setDate(tDate.getDate()-1);
        while(tDate.getDay()===0||tDate.getDay()===6) tDate.setDate(tDate.getDate()-1);
        const tYY = tDate.getFullYear();
        const tMM = String(tDate.getMonth()+1).padStart(2,'0');
        const tDD = String(tDate.getDate()).padStart(2,'0');
        const csvDate = `${tYY}/${tMM}/${tDD}`; // 正確格式：2026/04/17

        const csvUrl = `https://www.taifex.com.tw/cht/3/futDataDown?down_type=1&commodity_id=${commodity}&queryStartDate=${encodeURIComponent(csvDate)}&queryEndDate=${encodeURIComponent(csvDate)}`;
        console.log('[TXF] fetching', commodity, csvUrl.slice(-50));
        const cr = await safeFetch(csvUrl,{
          headers:{
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer':'https://www.taifex.com.tw/cht/3/futContractsDate',
            'Accept':'text/html,application/xhtml+xml,*/*',
            'Accept-Language':'zh-TW,zh;q=0.9,en;q=0.8',
          }
        },10000);

        if(cr.ok){
          // MS950（Big5）編碼解碼
          const buf    = await cr.arrayBuffer();
          const csvTxt = new TextDecoder('big5').decode(buf);

          // 確認是 CSV 資料（含數字列，不含 HTML）
          if(csvTxt.includes(',') && !csvTxt.toLowerCase().includes('<!doctype') && !csvTxt.includes('alert(')){
            const lines = csvTxt.split('\n')
              .map(l=>l.trim())
              .filter(l=>l && !l.startsWith('日期') && !l.startsWith(','));

            // 找近月一般盤（trading_session 為空或「一般」）
            // CSV 欄位：[0]日期,[1]商品,[2]到期月,[3]開盤,[4]最高,[5]最低,[6]收盤,[7]漲跌,[8]漲跌%,[9]成交量,[10]結算價,...,[17]交易時段
            // WTXP 夜盤或 TX 日盤，找對應商品代號的近月合約
            const targetId = commodity;
            const txLine = lines.find(l=>{
              const cols = l.split(',');
              const sess = (cols[17]||cols[18]||'').trim();
              return (cols[1]===targetId||cols[1]==='TX')&&(sess===''||sess==='一般'||sess==='夜盤'||!sess);
            }) || lines.find(l=>l.split(',')[1]===targetId)
              || lines.find(l=>l.split(',')[1]==='TX');

            if(txLine){
              const cols    = txLine.split(',').map(s=>s.trim().replace(/"/g,''));
              const price   = parseFloat(cols[6]||0);
              const chg     = parseFloat(cols[7]||0);
              const chgPctS = (cols[8]||'0%').replace('%','');
              const chgPct  = parseFloat(chgPctS)||0;
              const settle  = parseFloat(cols[10]||0);

              if(price > 15000){
                const prevClose = settle>0 ? settle : Math.round(price-chg);
                const result    = {
                  price, change:Math.round(chg), changePct:+chgPct.toFixed(2),
                  session, prevClose, source:commodity==='WTXP'?'TAIFEX夜盤':'TAIFEX',
                  date: tMM+'-'+tDD,
                };
                if(env.CACHE_STORE) try{
                  await env.CACHE_STORE.put(txfCacheKey,JSON.stringify(result),{expirationTtl:txfTTL});
                }catch(e){}
                console.log('[TXF] TAIFEX CSV OK:', price, chg, chgPct+'%');
                return ok(result);
              }
            }
            console.warn('[TXF] CSV no TX row, lines:', lines.length, 'sample:', lines[0]?.slice(0,50));
          } else {
            console.warn('[TXF] CSV not valid, first 100:', csvTxt.slice(0,100));
          }
        }
      }catch(e){ console.warn('[TXF] TAIFEX CSV error:', e.message); }

      // ── 來源2: Stooq（台指期日K CSV，免費可靠）────────────
      try{
        const sr = await safeFetch('https://stooq.com/q/d/l/?s=txf.f&i=d',
          {headers:{'User-Agent':'Mozilla/5.0'}}, 8000);
        if(sr.ok){
          const txt   = await sr.text();
          const lines = txt.trim().split('\n').filter(l=>l&&!l.startsWith('Date'));
          if(lines.length){
            const last = lines[lines.length-1].split(',');
            const price = parseFloat(last[4]); // Close
            const prev  = lines.length>1 ? parseFloat(lines[lines.length-2].split(',')[4]) : 0;
            if(price > 15000){
              const change    = prev>0 ? Math.round(price-prev) : 0;
              const changePct = prev>0 ? +((price-prev)/prev*100).toFixed(2) : 0;
              const result    = {price, change, changePct, session, prevClose:prev,
                                 source:'Stooq', date:last[0]?.slice(5)||dateStr};
              if(env.CACHE_STORE) try{ await env.CACHE_STORE.put(txfCacheKey,JSON.stringify(result),{expirationTtl:txfTTL});}catch(e){}
              console.log('[TXF] Stooq OK:', price, change, changePct+'%');
              return ok(result);
            }
          }
        }
      }catch(e){ console.warn('[TXF] Stooq error:', e.message); }

      // ── 來源3: Yahoo Finance（多個代號嘗試）─────────────
      // %5ETWII = ^TWII 加權指數（現貨），台指期備援參考
      const yahooSymbols = ['TXFA26.TW','TXF=F','TXFJ26.TW','TXFK26.TW','%5ETWII'];
      for(const sym of yahooSymbols){
        try{
          const yr = await safeFetch(
            `https://query1.finance.yahoo.com/v8/finance/chart/${sym}?interval=1d&range=5d`,
            {headers:{'User-Agent':'Mozilla/5.0','Accept':'application/json','Accept-Language':'zh-TW'}}, 7000);
          if(!yr.ok){ console.warn('[TXF] Yahoo',sym,'status:',yr.status); continue; }
          const yj   = await yr.json();
          const meta = yj?.chart?.result?.[0]?.meta;
          if(!meta) continue;
          const price = meta.regularMarketPrice||0;
          // 用 5d 日K 的倒數第二根當 prevClose（比 chartPreviousClose 更準）
          const closes    = (yj?.chart?.result?.[0]?.indicators?.quote?.[0]?.close||[]).filter(v=>v!=null&&v>0);
          const prevClose = closes.length>=2 ? closes[closes.length-2] : (meta.chartPreviousClose||meta.previousClose||0);
          if(price < 15000){ continue; }
          const change    = prevClose>0 ? Math.round(price-prevClose) : 0;
          const changePct = prevClose>0 ? +((price-prevClose)/prevClose*100).toFixed(2) : 0;
          const isSrc     = sym.includes('TWII') ? '加權指數(參考)' : 'Yahoo';
          const result    = {price, change, changePct, session, prevClose, source:isSrc, date:dateStr};
          if(env.CACHE_STORE) try{ await env.CACHE_STORE.put(txfCacheKey,JSON.stringify(result),{expirationTtl:txfTTL});}catch(e){}
          console.log('[TXF] Yahoo',sym,'OK:', price, change);
          return ok(result);
        }catch(e){ console.warn('[TXF] Yahoo',sym,'error:', e.message); }
      }

      // ── 來源3: FinMind（過濾正確行情資料）──────────────
      const FM_T = env.FM_TOKEN||'';
      if(FM_T){
        try{
          const today = now_tw.toISOString().slice(0,10);
          const ago   = new Date(Date.now()-7*86400000).toISOString().slice(0,10);
          const fmr   = await safeFetch(
            `https://api.finmindtrade.com/api/v4/data?dataset=TaiwanFuturesDaily&data_id=TX&start_date=${ago}&end_date=${today}&token=${FM_T}`,
            {}, 10000);
          if(fmr.ok){
            const rows = (await fmr.json())?.data||[];
            // 過濾：只取一般盤（非 position/夜盤）且 close 合理（>15000）
            const valid = rows
              .filter(r=>parseFloat(r.close||0)>15000 &&
                (r.trading_session===''||r.trading_session==='一般'||r.trading_session==='regular'||!r.trading_session))
              .sort((a,b)=>a.date>b.date?-1:1);
            if(valid.length){
              const r       = valid[0];
              const price   = parseFloat(r.close);
              const settle  = parseFloat(r.settlement_price||0);
              const spread  = parseFloat(r.spread||0);
              const change  = spread!==0?Math.round(spread):settle>0?Math.round(price-settle):0;
              let   pct     = parseFloat(r.spread_per||0);
              if(Math.abs(pct)>20) pct/=100;
              const result  = {price, change, changePct:+pct.toFixed(2), session:'盤後',
                               source:'FinMind', date:(r.date||'').slice(5)};
              if(env.CACHE_STORE) try{ await env.CACHE_STORE.put(txfCacheKey,JSON.stringify(result),{expirationTtl:txfTTL});}catch(e){}
              console.log('[TXF] FinMind OK:', price, change);
              return ok(result);
            }
          }
        }catch(e){ console.warn('[TXF] FinMind error:', e.message); }
      }

      return ok({error:'台指期資料暫時無法取得', price:null, source:'none'});
    }

    // ── GET /trend ── 趨勢股（KV 快取，Cron 每日更新）──────
    if(url.pathname==='/trend'){
      // 先嘗試從 KV 取昨日計算結果
      if(env.CACHE_STORE){
        try{
          const cached = await env.CACHE_STORE.get('trend:daily');
          if(cached) return new Response(cached,{headers:{...CORS,'Content-Type':'application/json','X-Cache':'HIT'}});
        }catch(e){}
      }
      return ok({items:[],note:'KV 快取尚未建立，請等待 Cron 執行'});
    }

    // ── POST /push/subscribe ── 儲存訂閱 ─────────────────
    if(url.pathname==='/push/subscribe' && request.method==='POST'){
      if(!env.STORE) return err('KV not configured',503);
      try{
        const body = await request.json();
        const { uid, subscription } = body;
        if(!uid || !subscription?.endpoint) return err('missing uid or subscription',400);
        // 驗證 UID 格式
        if(!/^[a-zA-Z0-9_-]{6,64}$/.test(uid)) return err('invalid uid',400);
        // 儲存訂閱（以 uid 為 key）
        await env.STORE.put(
          `push:${uid}`,
          JSON.stringify({ subscription, uid, updatedAt: new Date().toISOString() }),
          { expirationTtl: 60*60*24*365 }
        );
        console.log('[Push] subscribed uid:', uid);
        return ok({ success: true });
      }catch(e){ return err(e.message); }
    }

    // ── DELETE /push/unsubscribe ── 取消訂閱 ──────────────
    if(url.pathname==='/push/unsubscribe' && request.method==='DELETE'){
      if(!env.STORE) return err('KV not configured',503);
      try{
        const body = await request.json();
        const { uid } = body;
        if(!uid) return err('missing uid',400);
        await env.STORE.delete(`push:${uid}`);
        return ok({ success: true });
      }catch(e){ return err(e.message); }
    }

    // ── POST /push/send ── 發送推播（內部呼叫，驗證 secret）
    if(url.pathname==='/push/send' && request.method==='POST'){
      const secret = url.searchParams.get('secret');
      if(!secret || secret !== (env.PUSH_SECRET||'stockai-push-2026')) return err('unauthorized',401);
      if(!env.STORE) return err('KV not configured',503);
      try{
        const body    = await request.json();
        const { uid, title, message, type, pushUrl } = body;
        if(!uid || !title) return err('missing uid or title',400);

        const stored  = await env.STORE.get(`push:${uid}`);
        if(!stored) return ok({ sent: false, reason: 'no subscription' });
        const { subscription } = JSON.parse(stored);

        const payload = JSON.stringify({
          title,
          body:    message || '',
          type:    type    || 'default',
          url:     pushUrl || '/',
          tag:     type+'-'+Date.now(),
        });

        // 使用 Web Push Protocol 發送
        const sent = await _sendWebPush(subscription, payload, env);
        return ok({ sent: sent.ok, status: sent.status });
      }catch(e){ return err(e.message); }
    }

    return err('Not Found',404);
  },

  // ── Cron Trigger：每日 01:00 台灣時間執行全市場掃描 ────
  async scheduled(event, env, ctx){
    ctx.waitUntil((async()=>{
      await _runDailyTrendScan(env);
      // 每日 09:05 台灣時間（UTC 01:05）推播驗證到期提醒
      const tw = new Date(new Date().toLocaleString('en-US',{timeZone:'Asia/Taipei'}));
      const h = tw.getHours(), m = tw.getMinutes();
      if(h===9 && m<=10){
        await _cronPushNotifications(env);
      }
    })());
  }
};

// 全市場趨勢掃描（供 Cron 呼叫）
async function _runDailyTrendScan(env){
  const FM_TOKEN = env.FM_TOKEN||'';
  const today    = new Date().toISOString().slice(0,10);
  const weekAgo  = new Date(Date.now()-7*86400000).toISOString().slice(0,10);

  console.log('[Cron] start trend scan', today, 'token:', FM_TOKEN?'ok':'missing');
  console.log('[Cron] CACHE_STORE bound:', !!env.CACHE_STORE);
  console.log('[Cron] FM_TOKEN length:', FM_TOKEN.length);

  // 股票池：每族群 10 支，共 100 支（涵蓋主要族群）
  const POOL = [
    // 半導體（10）
    '2330','2303','6770','2344','3711','2351','5347','3034','2049','4967',
    // IC設計（10）
    '2454','2379','3661','2449','4915','3406','2404','3533','2458',
    // AI伺服器/雲端（10）
    '2382','3231','6669','2376','3295','4977','2399','3017','5364',
    // 電子製造/零組件（10）
    '2317','2308','2357','2301','2353','2313','2354','4938','3037',
    // 網通/電信（10）
    '2345','2412','4904','3260','6415','3706','4906','3047','2332','6257',
    // 金融（10）
    '2892','2881','2882','2883','2884','2885','2886','2887','2888','5880',
    // 航運（10）
    '2609','2603','2618','2615','5608','2637','2616','2207','5210','2606',
    // 石化/鋼鐵/傳產（10）
    '1301','1303','2002','1326','2006','1305','1308','2015','2027','1402',
    // 消費/零售/生技（10）
    '9910','2723','1216','2912','4763','2707','2801','2915','6547',
    // 光學/工控/其他（10）
    '3008','2395','3673','6505','2474','2059','4955','3443','6285','2492',
  ].filter((v,i,a)=>a.indexOf(v)===i); // 去重

  // 分批抓各股資料（每批 5 支，共 6 批）
  const BATCH = 5;
  const scored = [];

  for(let i=0; i<POOL.length; i+=BATCH){
    const batch = POOL.slice(i, i+BATCH);
    console.log('[Cron] fetching batch', i/BATCH+1, ':', batch.join(','));
    const batchResults = await Promise.allSettled(batch.map(async id=>{
      // 取近5日股價
      const priceUrl = `${FM_BASE}?dataset=TaiwanStockPrice&data_id=${id}&start_date=${weekAgo}&end_date=${today}&token=${FM_TOKEN}`;
      // 取近5日法人
      const instUrl  = `${FM_BASE}?dataset=TaiwanStockInstitutionalInvestorsBuySell&data_id=${id}&start_date=${weekAgo}&end_date=${today}&token=${FM_TOKEN}`;

      const [pr, ir] = await Promise.all([
        safeFetch(priceUrl, {}, 12000),
        safeFetch(instUrl,  {}, 12000),
      ]);

      // 讀取 body（每個 Response body 只能讀一次）
      const [prText, irText] = await Promise.all([pr.text(), ir.text()]);
      let priceRaw={data:[]}, instRaw={data:[]};
      try{ priceRaw = JSON.parse(prText); }catch(e){}
      try{ instRaw  = JSON.parse(irText);  }catch(e){}

      const priceData = pr.ok ? (priceRaw.data||[]) : [];
      const instData  = ir.ok ? (instRaw.data||[])  : [];

      if(!pr.ok) console.warn('[Cron]', id, 'price HTTP', pr.status, prText.slice(0,100));
      if(!ir.ok) console.warn('[Cron]', id, 'inst  HTTP', ir.status, irText.slice(0,100));
      if(!priceData.length){ console.warn('[Cron]', id, 'no price data'); return null; }

      // 最新一日收盤 + 5日漲幅
      const sorted = priceData.sort((a,b)=>a.date>b.date?1:-1);
      const last   = sorted[sorted.length-1];
      const first  = sorted[0];
      const close  = parseFloat(last.close||0);
      const open   = parseFloat(last.open||close);
      const ret1   = open>0 ? (close-open)/open*100 : 0;
      const ret5   = first&&parseFloat(first.close)>0
        ? (close-parseFloat(first.close))/parseFloat(first.close)*100 : 0;
      const vol    = parseFloat(last.Trading_Volume||0);

      // 法人近5日累積
      const instNet = instData.reduce((sum,r)=>{
        const nm = (r.name||'').toLowerCase();
        if(nm.includes('外資')||nm.includes('foreign')||nm.includes('fini'))
          return sum + parseFloat(r.buy||0) - parseFloat(r.sell||0);
        return sum;
      }, 0);

      // 評分
      const score = (instNet>0?30:instNet<0?-10:0)
                  + (ret5>5?25:ret5>2?15:ret5>0?8:0)
                  + (ret1>2?15:ret1>0?8:0)
                  + (vol>5000?10:0);

      return {id, score, close, ret1:+ret1.toFixed(2), ret5:+ret5.toFixed(2), instNet:Math.round(instNet)};
    }));

    batchResults.forEach(r=>{ if(r.status==='fulfilled' && r.value) scored.push(r.value); });

    // 批次間暫停 500ms 避免限速
    if(i+BATCH < POOL.length) await new Promise(r=>setTimeout(r,500));
  }

  console.log('[Cron] scored', scored.length, 'stocks');

  if(!scored.length){
    console.error('[Cron] no stocks scored, aborting KV write');
    return;
  }

  scored.sort((a,b)=>b.score-a.score);
  const top = scored.slice(0,30); // 最多30支，前端會依分數顯示前8

  const result = {items: top, date: today, ts: Date.now()};

  if(!env.CACHE_STORE){
    console.error('[Cron] CACHE_STORE not bound! Check Cloudflare KV bindings.');
    return;
  }

  try{
    await env.CACHE_STORE.put('trend:daily', JSON.stringify(result), {expirationTtl: 48*60*60});
    console.log('[Cron] wrote', top.length, 'trend stocks to KV. Top:', top.slice(0,3).map(s=>s.id).join(','));
  }catch(e){
    console.error('[Cron] KV write failed:', e.message);
  }
}

// ── Web Push 發送函式 ────────────────────────────────────
// 實作 RFC8291/RFC8292 Web Push Protocol（不依賴外部套件）
async function _sendWebPush(subscription, payloadStr, env){
  // Cloudflare Worker 原生支援 WebCrypto + fetch，可直接實作 Web Push
  // 使用 VAPID JWT 驗證
  try{
    const endpoint = subscription.endpoint;
    const keys     = subscription.keys || {};
    const p256dh   = keys.p256dh || '';
    const auth     = keys.auth   || '';

    // 建立 VAPID JWT header.payload.signature
    const now     = Math.floor(Date.now()/1000);
    const exp     = now + 12*3600; // 12小時有效
    const origin  = new URL(endpoint).origin;

    const header  = { typ:'JWT', alg:'ES256' };
    const payload = { aud:origin, exp, sub: VAPID_SUBJECT };

    const b64url = s => btoa(String.fromCharCode(...new TextEncoder().encode(
      typeof s==='string' ? s : JSON.stringify(s)
    ))).replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,'');

    const sigInput = b64url(header)+'.'+b64url(payload);

    // 匯入 VAPID 私鑰（ES256）
    const privKeyBytes = _base64urlToBytes(VAPID_PRIVATE);
    const cryptoKey = await crypto.subtle.importKey(
      'pkcs8',
      _buildPKCS8(privKeyBytes),
      { name:'ECDSA', namedCurve:'P-256' },
      false, ['sign']
    );

    const sig = await crypto.subtle.sign(
      { name:'ECDSA', hash:'SHA-256' },
      cryptoKey,
      new TextEncoder().encode(sigInput)
    );

    const jwt = sigInput + '.' + btoa(String.fromCharCode(...new Uint8Array(sig)))
                  .replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,'');

    const vapidAuth = `vapid t=${jwt},k=${VAPID_PUBLIC}`;

    // 發送（不加密 payload，使用 urgency=high）
    const res = await fetch(endpoint, {
      method:  'POST',
      headers: {
        'Authorization': vapidAuth,
        'Content-Type':  'application/octet-stream',
        'Content-Encoding': 'aes128gcm',
        'TTL':   '86400',
        'Urgency': 'high',
      },
      body: new TextEncoder().encode(payloadStr),
    });

    console.log('[Push] sent to', endpoint.slice(0,50), 'status:', res.status);
    return { ok: res.status < 300, status: res.status };
  }catch(e){
    console.error('[Push] send error:', e.message);
    return { ok: false, status: 500 };
  }
}

function _base64urlToBytes(b64url){
  const b64 = b64url.replace(/-/g,'+').replace(/_/g,'/');
  const bin = atob(b64);
  return new Uint8Array(bin.length).map((_,i)=>bin.charCodeAt(i));
}

function _buildPKCS8(rawPriv32){
  // P-256 PKCS8 wrapper (OID 1.2.840.10045.2.1 + 1.2.840.10045.3.1.7)
  const header = new Uint8Array([
    0x30,0x41,0x02,0x01,0x00,0x30,0x13,0x06,0x07,0x2a,0x86,0x48,0xce,0x3d,0x02,0x01,
    0x06,0x08,0x2a,0x86,0x48,0xce,0x3d,0x03,0x01,0x07,0x04,0x27,0x30,0x25,0x02,0x01,
    0x01,0x04,0x20
  ]);
  const result = new Uint8Array(header.length + 32);
  result.set(header);
  result.set(rawPriv32.slice(0,32), header.length);
  return result.buffer;
}

// ── Cron：每日推播觸發 ───────────────────────────────────
async function _cronPushNotifications(env){
  if(!env.STORE) return;
  const today = new Date().toISOString().slice(0,10);
  console.log('[CronPush] checking push triggers for', today);

  // 列出所有訂閱用戶
  const list = await env.STORE.list({ prefix:'push:' }).catch(()=>null);
  if(!list?.keys?.length){ console.log('[CronPush] no subscribers'); return; }

  console.log('[CronPush] subscribers:', list.keys.length);

  for(const key of list.keys){
    try{
      const stored = await env.STORE.get(key.name);
      if(!stored) continue;
      const { subscription, uid } = JSON.parse(stored);
      if(!subscription) continue;

      // ① P0：策略驗證到期提醒（09:05 台灣時間）
      const btKey = `${uid}:backtest`;
      const btData = await env.STORE.get(btKey);
      if(btData){
        const records = JSON.parse(btData) || [];
        const pending = records.filter(r=>
          !r.verified && r.checkDate && r.checkDate <= today
        );
        if(pending.length > 0){
          await _sendWebPush(subscription, JSON.stringify({
            title: '📋 策略驗證到期提醒',
            body:  `你有 ${pending.length} 筆回測記錄今日到期，點擊前往驗證`,
            type:  'verify',
            url:   '/?page=watchlist#verify',
            tag:   'verify-'+today,
          }), env);
          console.log('[CronPush] sent verify reminder to', uid);
        }
      }

    }catch(e){ console.warn('[CronPush] uid error:', e.message); }
  }
}

// ── Helper 函式 ──────────────────────────────────────────
function isTWSETradingNow(){
  const tw=new Date(new Date().toLocaleString('en-US',{timeZone:'Asia/Taipei'}));
  const wd=tw.getDay(),tot=tw.getHours()*60+tw.getMinutes();
  return wd>0&&wd<6&&tot>=555&&tot<=815;
}
function secsTillMidnight(){
  const tw=new Date(new Date().toLocaleString('en-US',{timeZone:'Asia/Taipei'}));
  const m=new Date(tw);m.setHours(24,0,0,0);
  return Math.max(Math.floor((m-tw)/1000),1800);
}
function getCacheTTL(dataset){
  if(isTWSETradingNow()) return 60; // 盤中：60秒更新
  // 盤後：依資料類型設定不同過期時間
  const till = secsTillMidnight();
  switch(dataset){
    case 'TaiwanStockPrice':
      // 股價：盤後存到明日開盤前（確保每日拿最新收盤價）
      return till;
    case 'TaiwanStockInstitutionalInvestorsBuySell':
    case 'TaiwanStockTotalInstitutionalInvestors':
      // 法人：盤後更新，存到明天
      return till;
    case 'TaiwanStockShareholding':
    case 'TaiwanStockMarginPurchaseShortSale':
    case 'TaiwanStockTotalMarginPurchaseShortSale':
      // 籌碼：每日一次，存到明天
      return till;
    default:
      return Math.min(till, 6*60*60); // 其他最多快取6小時
  }
}
function buildCacheKey(params){
  const ds   = params.get('dataset')||'';
  const did  = params.get('data_id')||'ALL';
  const sd   = params.get('start_date')||'';
  const today= new Date().toLocaleDateString('zh-TW',{timeZone:'Asia/Taipei'}).replace(/\//g,'-');
  // start_date 加入 key：避免短期/長期查詢互相覆蓋
  // 只取 start_date 的月份（避免 key 太長），精確到月
  const sdMonth = sd.slice(0,7); // e.g. "2026-01"
  return `fm:${ds}:${did}:${sdMonth}:${today}`;
}
