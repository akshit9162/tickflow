using System.Threading.Channels;
using Microsoft.Data.Sqlite;
using Tickflow.Engine;

namespace Tickflow.Api;

/// <summary>
/// Trades in SQLite, written off the hot path. The engine only enqueues; a background loop inserts in batches
/// (up to 1,000 rows or 20 ms per transaction). The table is a projection of the write-ahead log, not the source
/// of truth: after a crash the log replay re-publishes every trade and INSERT OR IGNORE on the trade sequence
/// makes that safe, so trades the writer had not flushed yet are recovered instead of lost.
/// </summary>
public sealed class TradeStore : ITradeSink, IAsyncDisposable
{
    private readonly Channel<Trade> _queue = Channel.CreateUnbounded<Trade>(new UnboundedChannelOptions { SingleReader = true });
    private readonly string _connectionString;
    private readonly Task _writer;
    private long _enqueued;
    private long _written;

    public TradeStore(string dbPath)
    {
        _connectionString = new SqliteConnectionStringBuilder { DataSource = dbPath, Pooling = false }.ToString();
        using (var db = Open())
        {
            Execute(db, """
                PRAGMA journal_mode = WAL;
                CREATE TABLE IF NOT EXISTS trades (
                    seq           INTEGER PRIMARY KEY,
                    symbol        TEXT    NOT NULL,
                    buy_order_id  INTEGER NOT NULL,
                    sell_order_id INTEGER NOT NULL,
                    price_ticks   INTEGER NOT NULL,
                    quantity      INTEGER NOT NULL,
                    aggressor     TEXT    NOT NULL,
                    ts_ticks      INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_trades_symbol_seq ON trades (symbol, seq);
                """);
        }
        _writer = Task.Run(WriteLoop);
    }

    public void Publish(Trade trade)
    {
        Interlocked.Increment(ref _enqueued);
        _queue.Writer.TryWrite(trade);
    }

    /// <summary>Waits until everything published so far is in the database (used by tests and shutdown).</summary>
    public async Task FlushAsync(TimeSpan? timeout = null)
    {
        var target = Interlocked.Read(ref _enqueued);
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (Interlocked.Read(ref _written) < target && DateTime.UtcNow < deadline) await Task.Delay(5);
    }

    public async Task<IReadOnlyList<TradeDto>> RecentAsync(string? symbol, int limit)
    {
        await using var db = Open();
        await using var cmd = db.CreateCommand();
        cmd.CommandText = symbol is null
            ? "SELECT * FROM trades ORDER BY seq DESC LIMIT $limit"
            : "SELECT * FROM trades WHERE symbol = $symbol ORDER BY seq DESC LIMIT $limit";
        cmd.Parameters.AddWithValue("$limit", limit);
        if (symbol is not null) cmd.Parameters.AddWithValue("$symbol", symbol);

        var rows = new List<TradeDto>();
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
            rows.Add(new TradeDto(r.GetInt64(0), r.GetString(1), r.GetInt64(2), r.GetInt64(3),
                Prices.ToDecimal(r.GetInt64(4)), r.GetInt64(5), r.GetString(6),
                new DateTime(r.GetInt64(7), DateTimeKind.Utc)));
        return rows;
    }

    public async Task<long> CountAsync()
    {
        await using var db = Open();
        await using var cmd = db.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM trades";
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private async Task WriteLoop()
    {
        var reader = _queue.Reader;
        var batch = new List<Trade>(1000);
        while (await reader.WaitToReadAsync())
        {
            batch.Clear();
            var deadline = DateTime.UtcNow.AddMilliseconds(20);
            while (batch.Count < 1000)
            {
                if (reader.TryRead(out var t)) batch.Add(t);
                else if (DateTime.UtcNow < deadline && batch.Count > 0) await Task.Delay(1);
                else break;
            }
            Insert(batch);
            Interlocked.Add(ref _written, batch.Count);
        }
    }

    private void Insert(List<Trade> batch)
    {
        using var db = Open();
        using var tx = db.BeginTransaction();
        using var cmd = db.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = """
            INSERT OR IGNORE INTO trades (seq, symbol, buy_order_id, sell_order_id, price_ticks, quantity, aggressor, ts_ticks)
            VALUES ($seq, $symbol, $buy, $sell, $price, $qty, $aggressor, $ts)
            """;
        var p = new (string Name, SqliteType Type)[]
            {
                ("$seq", SqliteType.Integer), ("$symbol", SqliteType.Text), ("$buy", SqliteType.Integer),
                ("$sell", SqliteType.Integer), ("$price", SqliteType.Integer), ("$qty", SqliteType.Integer),
                ("$aggressor", SqliteType.Text), ("$ts", SqliteType.Integer),
            }
            .Select(x => cmd.Parameters.Add(x.Name, x.Type)).ToArray();
        foreach (var t in batch)
        {
            p[0].Value = t.Seq; p[1].Value = t.Symbol; p[2].Value = t.BuyOrderId; p[3].Value = t.SellOrderId;
            p[4].Value = t.Price; p[5].Value = t.Quantity; p[6].Value = t.Aggressor.ToString(); p[7].Value = t.TimestampTicks;
            cmd.ExecuteNonQuery();
        }
        tx.Commit();
    }

    private SqliteConnection Open()
    {
        var db = new SqliteConnection(_connectionString);
        db.Open();
        return db;
    }

    private static void Execute(SqliteConnection db, string sql)
    {
        using var cmd = db.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    public async ValueTask DisposeAsync()
    {
        _queue.Writer.TryComplete();
        await _writer;   // completing twice is harmless; awaiting a finished task is too
    }
}
