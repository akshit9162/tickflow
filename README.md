# tickflow

A low-latency order-matching engine in **C# (.NET 8)** with an **ASP.NET Core** API, a write-ahead log for
crash safety, a live **WebSocket** trade feed, **SQLite** trade storage, and **Python** tools for load testing,
crash testing and ETL.

Clients send buy and sell orders; the engine matches them in a limit order book (price-time priority), streams
every trade to subscribers, and stores it in SQL. Every order is written to a log before it is applied, so a
crash at any moment loses nothing the server has acknowledged.

## Results

Measured on an Apple M2 laptop (8 cores, macOS), .NET 8, Release build. Reproduce with the commands below.

**Latency** (`bench/Tickflow.Bench`, 64 concurrent clients for the end-to-end rows):

| Path | Orders | Throughput (ops/s) | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| Matching engine alone | 1,000,000 | 476,367 | 0.3 µs | 2.2 µs | 6.0 µs |
| End to end, no fsync (survives process crash only) | 200,000 | 148,800 | 0.32 ms | 2.9 ms | 20.8 ms |
| End to end, **group commit** (survives power loss) | 200,000 | **20,318** | 3.0 ms | 6.0 ms | 30.3 ms |
| End to end, fsync per order (survives power loss) | 3,000 | 372 | 189 ms | 210 ms | 210 ms |

- **Group commit gives the same guarantee as fsync-per-order at about 55× the throughput**: one fsync (≈3 ms on
  this disk) covers every order in the batch, and each client is acknowledged only after that fsync.
- End-to-end latency is mostly queueing, not work: with 64 orders in flight, p50 ≈ 64 ÷ throughput
  (Little's law). The engine itself is sub-microsecond.
- The engine's single worst call (153 ms in 1M) is a garbage-collection pause; p99.9 is 6 µs.

**Crash test** (`python/crash_test.py`): 10 rounds of `kill -9` on the server while 32 clients place orders,
then restart and re-send every acknowledged order. **43,808 acknowledged orders, 0 lost.**

**Through HTTP** (`python/loadgen.py`, 64 clients from one Python process, group commit): 50,000 orders at
5,253 orders/s, p50 11.6 ms; the WebSocket subscriber received all 46,305 trades. The single-process Python
client is the bottleneck here, not the server.

**SQL analytics** (`GET /stats/{symbol}`): per-minute open/high/low/close, volume, VWAP and buy/sell volume,
computed in SQL with window functions. Over 1,000,000 stored trades, one symbol (p50 of 30 runs):

| Range | Bars | With (symbol, time) index | Without | |
|---|---:|---:|---:|---:|
| Last 1 hour | 60 | 16.3 ms | 82.2 ms | 5.1× faster |
| Full 24 hours | 1,440 | 410.9 ms | 433.6 ms | 1.1× |

The index pays off on narrow ranges, which is what a dashboard asks for. Over a full day the query reads a
tenth of the table and the window-function sort dominates, so the index barely matters.

**Tests**: 27 xUnit tests (matching rules, recovery in every durability mode, torn and corrupted logs,
idempotent retries, HTTP end to end including a restart, SQL bars and their query plan).

## How it works

```
 clients ──HTTP──▶ ASP.NET Core API ──▶ bounded channel ──▶ engine thread (single writer)
                                                              │ 1. validate
                                                              │ 2. append to write-ahead log
                                                              │ 3. match in the order book
                                                              │ 4. fsync once per batch (group commit)
                                                              │ 5. acknowledge the whole batch
                                                              ▼
                                               trades ──▶ WebSocket feed (per-client queues)
                                                      └──▶ SQLite writer (batched, INSERT OR IGNORE)
```

- **Order book:** per symbol, two sorted maps (price → level, bids highest-first, asks lowest-first); each level
  is a FIFO list; an id index makes cancels O(1) to find. Prices are integer ticks, never floating point.
- **One thread owns the engine**, so matching needs no locks and the log order is exactly the execution order.
- **Write-ahead log of commands:** records are `[length][CRC32][payload]`. On startup the log is replayed;
  anything after the last record with a valid checksum (a write cut short by a crash) is truncated.
- **Deterministic replay:** the same commands in the same order rebuild the same book, ids and trades, so the
  log is the source of truth and SQLite is a projection that can be re-derived.
- **Idempotent retries:** re-sending an order with the same `clientOrderId` returns the original result instead
  of trading twice, including after a restart.
- **Backpressure:** the request queue is bounded (callers wait when it is full); slow WebSocket clients lose
  their oldest messages instead of slowing the engine.

See [DESIGN.md](DESIGN.md) for the reasoning behind each choice and the trade-offs.

## API

| Method | Path | |
|---|---|---|
| POST | `/orders` | `{clientOrderId, symbol, side: Buy\|Sell, type: Limit\|Market, price?, quantity}` |
| DELETE | `/orders/{symbol}/{orderId}` | cancel; 404 if not resting |
| GET | `/book/{symbol}?depth=10` | aggregated price levels |
| GET | `/trades?symbol=&limit=100` | recent trades from SQLite |
| GET | `/stats/{symbol}?minutes=60` | per-minute OHLC, volume, VWAP, buy/sell volume (SQL window functions) |
| GET | `/health` | durability mode, log size, recovery info |
| WS | `/ws/trades?symbol=` | live trades as JSON |

```bash
curl -X POST localhost:5077/orders -H 'content-type: application/json' \
  -d '{"clientOrderId":"a1","symbol":"ACME","side":"Sell","type":"Limit","price":100.25,"quantity":10}'
```

## Run it

```bash
dotnet test                                                    # 27 tests
dotnet run --project src/Tickflow.Api --urls http://127.0.0.1:5077
dotnet run -c Release --project bench/Tickflow.Bench -- results   # latency + SQL tables

cd python && uv venv && uv pip install aiohttp pandas matplotlib
.venv/bin/python crash_test.py --rounds 10                     # kill -9 test
.venv/bin/python loadgen.py --orders 50000 --clients 64        # HTTP load + WebSocket check
.venv/bin/python etl.py <data-dir>/trades.db                   # 1-second OHLCV/VWAP bars + chart
```

Settings: `Tickflow:DataDir` (log and database location), `Tickflow:Durability` = `None` | `Group` | `Full`.

## Limits

Single process and single engine thread (no replication or failover); the idempotency table and the log grow
without bound (a real system would snapshot the book and trim the log, and expire client ids per session);
fsync durability is as good as the disk's honesty about flushing; latency numbers are from one laptop.
