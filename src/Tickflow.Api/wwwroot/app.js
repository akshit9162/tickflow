// tickflow live screen. No framework: a small API client, a reconnecting WebSocket feed, an observable
// store that the panels subscribe to (Observer pattern), and one render function per panel.

const $ = (id) => document.getElementById(id);
const fmt = (n, d = 2) => Number(n).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });

// ---- API client -------------------------------------------------------------------------------------------
const api = {
  async post(path, body) {
    const r = await fetch(path, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
    return { ok: r.ok, body: await r.json() };
  },
  async del(path) {
    const r = await fetch(path, { method: 'DELETE' });
    return { ok: r.ok, body: await r.json() };
  },
  async get(path) {
    const r = await fetch(path);
    return r.json();
  },
};

// ---- Observable store ---------------------------------------------------------------------------------------
// Panels subscribe to the keys they care about; set() notifies only those subscribers.
function createStore(initial) {
  const state = { ...initial };
  const listeners = new Map();
  return {
    get: (key) => state[key],
    set(key, value) {
      state[key] = value;
      (listeners.get(key) || []).forEach((fn) => fn(value));
    },
    subscribe(key, fn) {
      if (!listeners.has(key)) listeners.set(key, []);
      listeners.get(key).push(fn);
      fn(state[key]);
    },
  };
}

const store = createStore({ symbol: 'ACME', book: null, trades: [], mine: new Map(), feed: 'connecting', health: null, vwap: null });

// ---- WebSocket feed with exponential backoff -------------------------------------------------------------
function connectFeed(onTrade) {
  let delay = 500;
  const open = () => {
    const ws = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws/trades`);
    ws.onopen = () => { delay = 500; store.set('feed', 'live'); };
    ws.onmessage = (e) => onTrade(JSON.parse(e.data));
    ws.onclose = () => {
      store.set('feed', 'reconnecting');
      setTimeout(open, delay);
      delay = Math.min(delay * 2, 8000);   // 0.5 s, 1 s, 2 s ... capped at 8 s
    };
  };
  open();
}

// ---- Data flow -------------------------------------------------------------------------------------------------
const MAX_TRADES = 200;
let bookTimer = null;

function refreshBookSoon() {             // many trades in a burst cause one book request, not one each
  if (bookTimer) return;
  bookTimer = setTimeout(async () => {
    bookTimer = null;
    store.set('book', await api.get(`/book/${store.get('symbol')}?depth=12`));
  }, 120);
}

async function loadSymbol(symbol) {
  store.set('symbol', symbol);
  const trades = await api.get(`/trades?symbol=${encodeURIComponent(symbol)}&limit=${MAX_TRADES}`);
  store.set('trades', trades.reverse());
  refreshBookSoon();
  refreshStats();
}

function onTrade(trade) {
  // Keep my open orders up to date when they fill.
  const mine = store.get('mine');
  for (const id of [trade.buyOrderId, trade.sellOrderId]) {
    const o = mine.get(id);
    if (!o) continue;
    o.remaining -= trade.quantity;
    if (o.remaining <= 0) mine.delete(id);
    store.set('mine', mine);
  }
  if (trade.symbol !== store.get('symbol')) return;
  const trades = store.get('trades');
  trades.push(trade);
  if (trades.length > MAX_TRADES) trades.shift();
  store.set('trades', trades);
  refreshBookSoon();
}

async function refreshStats() {
  const bars = await api.get(`/stats/${store.get('symbol')}?minutes=5`);
  store.set('vwap', bars.length ? bars[bars.length - 1] : null);
}

// ---- Renderers --------------------------------------------------------------------------------------------------
function cell(text, cls) {
  const td = document.createElement('td');
  const span = document.createElement('span');
  span.textContent = text;
  td.append(span);
  if (cls) td.className = cls;
  return td;
}

function renderLevels(tbody, levels, maxQty, reverse) {
  tbody.replaceChildren();
  const rows = levels.map((l) => {
    const tr = document.createElement('tr');
    const price = cell(fmt(l.price));
    const bar = document.createElement('div');
    bar.className = 'depth';
    bar.style.width = `${Math.max(2, (l.quantity / maxQty) * 100)}%`;
    price.prepend(bar);
    tr.append(price, cell(l.quantity.toLocaleString()), cell(l.orders));
    return tr;
  });
  (reverse ? rows.reverse() : rows).forEach((r) => tbody.append(r));
}

store.subscribe('book', (book) => {
  if (!book) return;
  const maxQty = Math.max(1, ...book.asks.map((l) => l.quantity), ...book.bids.map((l) => l.quantity));
  renderLevels($('asks'), book.asks, maxQty, true);   // lowest ask nearest the middle
  renderLevels($('bids'), book.bids, maxQty, false);
  const bid = book.bids[0]?.price, ask = book.asks[0]?.price;
  $('mid').textContent = bid && ask ? fmt((bid + ask) / 2, 3) : '–';
  $('spread').textContent = bid && ask ? `spread ${fmt(ask - bid)}` : '';
});

store.subscribe('trades', (trades) => {
  const tbody = $('trades');
  tbody.replaceChildren();
  trades.slice(-60).reverse().forEach((t, i) => {
    const tr = document.createElement('tr');
    if (i === 0) tr.className = 'flash';
    tr.append(cell(new Date(t.time).toLocaleTimeString()), cell(fmt(t.price)), cell(t.quantity), cell(t.aggressor, t.aggressor));
    tbody.append(tr);
  });
  $('count').textContent = trades.length ? `${trades.length} shown` : '';
  const last = trades[trades.length - 1];
  $('last').textContent = last ? fmt(last.price) : '';
  drawChart(trades.map((t) => t.price));
});

store.subscribe('mine', (mine) => {
  const ul = $('mine');
  ul.replaceChildren();
  if (!mine.size) {
    const li = document.createElement('li');
    li.className = 'empty';
    li.textContent = 'None yet';
    ul.append(li);
    return;
  }
  for (const [id, o] of mine) {
    const li = document.createElement('li');
    const label = document.createElement('span');
    label.textContent = `#${id} ${o.side} ${o.remaining} @ ${fmt(o.price)}`;
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.textContent = 'Cancel';
    btn.setAttribute('aria-label', `Cancel order ${id}`);
    btn.onclick = async () => {
      await api.del(`/orders/${o.symbol}/${id}`);
      mine.delete(id);
      store.set('mine', mine);
      refreshBookSoon();
    };
    li.append(label, btn);
    ul.append(li);
  }
});

store.subscribe('feed', (state) => {
  $('feed-dot').className = `dot ${state === 'live' ? 'on' : state === 'reconnecting' ? 'off' : ''}`;
  $('feed-text').textContent = state === 'live' ? 'live feed' : state;
});

store.subscribe('health', (h) => {
  if (h) $('health').textContent = `log ${h.logRecords.toLocaleString()} · trades ${h.tradesExecuted.toLocaleString()} · ${h.durability}`;
});

store.subscribe('vwap', (bar) => {
  $('vwap').textContent = bar ? `this minute: VWAP ${fmt(bar.vwap, 4)} · volume ${bar.volume.toLocaleString()} · ${bar.trades} trades (computed in SQL)` : '';
});

function drawChart(prices) {
  const canvas = $('chart');
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth, h = canvas.clientHeight || 220;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, w, h);
  if (prices.length < 2) return;
  const lo = Math.min(...prices), hi = Math.max(...prices), pad = (hi - lo) * 0.1 || 0.05;
  const y = (p) => h - 18 - ((p - (lo - pad)) / ((hi + pad) - (lo - pad))) * (h - 30);
  const x = (i) => (i / (prices.length - 1)) * (w - 50);
  ctx.strokeStyle = '#232a38';
  ctx.fillStyle = '#8b95a7';
  ctx.font = '11px ui-monospace, Menlo, monospace';
  for (const p of [lo, (lo + hi) / 2, hi]) {
    ctx.beginPath(); ctx.moveTo(0, y(p)); ctx.lineTo(w - 50, y(p)); ctx.stroke();
    ctx.fillText(fmt(p), w - 46, y(p) + 4);
  }
  ctx.strokeStyle = '#818cf8';
  ctx.lineWidth = 1.6;
  ctx.beginPath();
  prices.forEach((p, i) => (i ? ctx.lineTo(x(i), y(p)) : ctx.moveTo(x(i), y(p))));
  ctx.stroke();
}

// ---- Order ticket ------------------------------------------------------------------------------------------------
let seq = 0;
const newId = (prefix) => `${prefix}-${Date.now().toString(36)}-${(seq++).toString(36)}`;

async function placeOrder({ side, type, price, quantity }, source = 'ui') {
  const symbol = store.get('symbol');
  const body = { clientOrderId: newId(source), symbol, side, type, quantity: Number(quantity) };
  if (type === 'Limit') body.price = Number(price);
  const res = await api.post('/orders', body);
  if (res.ok && source === 'ui' && res.body.remaining > 0 && type === 'Limit') {
    const mine = store.get('mine');
    mine.set(res.body.orderId, { symbol, side, price: Number(price), remaining: res.body.remaining });
    store.set('mine', mine);
  }
  refreshBookSoon();
  return res;
}

$('ticket').addEventListener('submit', async (e) => {
  e.preventDefault();
  const f = new FormData(e.target);
  const btn = $('submit');
  btn.disabled = true;
  try {
    const res = await placeOrder(Object.fromEntries(f));
    const r = res.body;
    $('result').className = res.ok ? 'result' : 'result err';
    $('result').textContent = res.ok
      ? `#${r.orderId} ${r.status}: filled ${r.filled}, remaining ${r.remaining}${r.trades.length ? ` across ${r.trades.length} trade(s)` : ''}`
      : (r.reason || 'Rejected');
  } finally {
    btn.disabled = false;
  }
});

$('ticket').elements.type.addEventListener('change', (e) => {
  $('ticket').elements.price.disabled = e.target.value === 'Market';
});

$('symbol').addEventListener('change', (e) => loadSymbol(e.target.value));

// ---- Market simulator ------------------------------------------------------------------------------------------
let simTimer = null;
function simulateOne() {
  const trades = store.get('trades');
  const mid = trades.length ? trades[trades.length - 1].price : 100;
  const side = Math.random() < 0.5 ? 'Buy' : 'Sell';
  const market = Math.random() < 0.1;
  const offset = (Math.floor(Math.random() * 9) - 4) / 100;
  placeOrder({
    side, type: market ? 'Market' : 'Limit',
    price: Math.max(0.01, Math.round((mid + offset) * 100) / 100), quantity: 1 + Math.floor(Math.random() * 50),
  }, 'sim');
}
function setSim(on) {
  clearInterval(simTimer);
  simTimer = on ? setInterval(simulateOne, 1000 / Number($('rate').value)) : null;
  $('sim').textContent = on ? 'Stop' : 'Start';
}
$('sim').addEventListener('click', () => setSim(!simTimer));
$('rate').addEventListener('input', (e) => {
  $('rate-out').textContent = e.target.value;
  if (simTimer) setSim(true);
});

// ---- Start ---------------------------------------------------------------------------------------------------------
connectFeed(onTrade);
loadSymbol('ACME');
setInterval(refreshBookSoon, 1000);                       // resting orders that did not trade
setInterval(refreshStats, 5000);
setInterval(async () => store.set('health', await api.get('/health')), 3000);
api.get('/health').then((h) => store.set('health', h));
window.addEventListener('resize', () => drawChart(store.get('trades').map((t) => t.price)));
