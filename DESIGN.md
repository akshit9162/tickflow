# Design notes (and the questions they answer)

Written so every choice can be explained, including what it costs.

## 1. Why prices are integers

`decimal`/`double` on the hot path is slower and floating point cannot represent 0.10 exactly, so two equal
prices could compare unequal. Prices are `long` ticks (0.01). The API converts at the edge and rejects prices
that are not a whole number of ticks (`100.123` → 400).

## 2. The order book data structures

- `SortedDictionary<long, PriceLevel>` per side (a red-black tree). Best bid/ask is O(log n); inserting a new
  price level is O(log n).
- Each `PriceLevel` holds a `LinkedList<Order>`: FIFO gives time priority, and removing from the middle is O(1)
  once you have the node.
- `Dictionary<orderId, LinkedListNode<Order>>`: cancel finds the order in O(1), and only touches the tree if the
  level becomes empty.

**Alternatives:** an array indexed by price offset from a reference price gives O(1) best-price updates and
better cache behaviour; real exchanges use it when the price range is bounded. A tree is simpler and handles any
price range. That is the first thing to change for lower latency.

**Matching rules:** a buy matches the lowest ask while `ask ≤ limit`; it trades at the *resting* order's price
(the maker set it first). Within a price, oldest first. Market orders are immediate-or-cancel: whatever cannot fill
is dropped, never rested.

## 3. Why one thread owns the engine

Matching is a sequence of dependent reads and writes on shared state. With locks, every order pays for
contention and the execution order becomes hard to reason about. With one owner thread (the LMAX / single-writer
pattern) there are no locks in matching, the cache stays warm, and the order commands are applied in is a total
order, which is exactly what the log needs. Requests reach the thread through a bounded `Channel<T>`.

**Cost:** one core's worth of matching. Scaling out means sharding by symbol (one engine thread per group of
symbols), not adding threads to one book.

## 4. Write-ahead log of commands, not trades

The log stores the *inputs* (new order, cancel), not the outputs (trades). The engine is deterministic, so
replaying inputs reproduces outputs exactly: same order ids, same trade sequence numbers, same book. That makes
the log small and the single source of truth. Timestamps are taken at the API edge and stored in the command, so
replay does not depend on the clock.

Record format: `[length u32][crc32 u32][payload]`. Recovery reads until the first record that is short or fails
its checksum, then truncates the file there. Corruption in the middle stops replay at the last good record:
anything after an unverifiable record cannot be trusted.

**Validation happens before logging**, so rejected orders never reach the log. **Duplicates** (same
`clientOrderId`) are answered from memory and not logged again.

## 5. Durability modes and group commit

| Mode | What an ack means | Cost |
|---|---|---|
| None | written to the OS page cache | survives `kill -9`, not a power cut |
| Group | fsync'd, shared with the rest of its batch | one fsync per batch |
| Full | fsync'd on its own | one fsync per order |

**Group commit:** the engine thread takes up to 256 queued requests, logs and applies them, fsyncs **once**, then
acknowledges all of them. Under load the batch fills naturally, so the fsync cost is shared: 20,318 vs 372
orders/s, ≈55×. Under light load the batch is one order and it behaves like Full.

**Why acks wait for the fsync:** if the server answered before the fsync and then lost power, a client would
believe an order exists that the restarted server has never heard of. That is the worst failure an exchange can
have.

**Questions to expect:**
- *Doesn't batching add latency?* Yes, up to one fsync (~3 ms here) per order. You trade a bounded latency for
  much higher throughput; the queueing latency you'd get without it is far worse under load.
- *Reads see uncommitted state?* Book snapshots are answered by the engine thread mid-batch, so a snapshot can
  show an order a few ms before it is durable. Acceptable for a market-data view; an ack never precedes durability.

## 6. Idempotency

Networks fail after the server acts but before the client hears back, so clients retry. A retry with the same
`clientOrderId` returns the first result (`duplicate: true`) and trades nothing. Because the log replays
`clientOrderId`s, this survives restarts. That is what the crash test relies on: after a crash, every acknowledged
order re-sent must come back as a duplicate with the same id.

**Cost:** the id table grows forever here. Real systems scope ids to a trading session or day and expire them.

## 7. SQLite as a projection

Trades go to SQLite on a background thread in batches (up to 1,000 rows or 20 ms per transaction), so the engine
never waits for the database. If the process dies with trades still queued, they are not lost: replay
re-publishes every trade and `INSERT OR IGNORE` on the trade sequence number skips the ones already stored.

**Analytics in SQL.** `/stats/{symbol}` builds per-minute bars in one query: `FIRST_VALUE` window functions
pick each minute's first and last trade (by sequence number) for open and close; `GROUP BY` gives high, low,
volume, trade count, VWAP (`SUM(price*qty)/SUM(qty)`) and the buy/sell split. A `(symbol, ts_ticks)` index makes
the time filter a range scan (a test checks the query plan). Measured over 1M trades: 5.1x faster for the last
hour, 1.1x for a whole day. *Why so little for a day?* That reads 10% of the table, and the sort for the window
functions dominates; an index helps selective queries, not bulk reads. The next step would be maintaining
pre-aggregated minute bars as trades arrive.

## 8. Backpressure

- The request channel is bounded (65,536); when full, callers wait instead of memory growing until the process
  dies.
- Each WebSocket subscriber has its own bounded queue with `DropOldest`: one slow client cannot slow the engine
  or other clients. A trading client that needs every trade would use the REST trade history to fill gaps
  (trade sequence numbers make gaps detectable).

## 9. Testing strategy

- Unit tests for each matching rule, plus a 20,000-step randomized test asserting the book is never crossed and
  every result's fills equal the sum of its trades.
- Recovery tests in all three durability modes: replay rebuilds the identical book and identical trades.
- Torn tail (half-written record) and mid-file corruption are injected directly into the log file.
- HTTP tests host the real app, including a restart that checks the book is restored and no trade is stored
  twice.
- `crash_test.py` does it for real: `kill -9` the process mid-flight, restart, verify every acknowledged order.

## 10. What I would do next

1. Snapshots of the book every N commands so recovery does not replay from the beginning.
2. Array-based price levels and pooled `Order` objects to remove the remaining GC pauses (the 153 ms max).
3. Shard symbols across engine threads.
4. Replicate the log to a standby for failover.
