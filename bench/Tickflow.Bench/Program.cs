using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using Tickflow.Api;
using Tickflow.Engine;

// Latency benchmark.
//  1. Engine only: one thread applies commands to MatchingEngine; every call is timed.
//  2. End to end: 64 concurrent clients submit through EngineService (queue -> write-ahead log -> fsync per
//     durability mode -> ack); each submit is timed from call to acknowledgment.
// Usage: dotnet run -c Release --project bench/Tickflow.Bench [outputDir]

CultureInfo.DefaultThreadCurrentCulture = CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
var outDir = args.Length > 0 ? args[0] : "results";
Directory.CreateDirectory(outDir);
var results = new List<Result>();

Console.WriteLine($"{Environment.ProcessorCount} cores, .NET {Environment.Version}, {RuntimeOsName()}");

results.Add(EngineOnly(warmup: 200_000, measured: 1_000_000));
foreach (var (mode, n) in new[] { (Durability.None, 200_000), (Durability.Group, 200_000), (Durability.Full, 3_000) })
    results.Add(await EndToEnd(mode, n, clients: 64));

Console.WriteLine();
Console.WriteLine("| Path | Orders | Throughput (ops/s) | p50 (µs) | p99 (µs) | p99.9 (µs) | max (µs) |");
Console.WriteLine("|---|---:|---:|---:|---:|---:|---:|");
foreach (var r in results)
    Console.WriteLine($"| {r.Name} | {r.Count:N0} | {r.OpsPerSecond:N0} | {r.P50:N1} | {r.P99:N1} | {r.P999:N1} | {r.Max:N1} |");

var sql = await SqlStats(trades: 1_000_000, runs: 30);
Console.WriteLine();
Console.WriteLine($"SQL minute bars, one symbol, {sql.Trades:N0} trades in the table (p50 of 30 runs):");
Console.WriteLine("| Range | Bars | With (symbol, ts) index | Without (full scan) | Speed-up |");
Console.WriteLine("|---|---:|---:|---:|---:|");
foreach (var w in sql.Windows)
    Console.WriteLine($"| {w.Range} | {w.Bars:N0} | {w.IndexedMs:N1} ms | {w.ScanMs:N1} ms | {w.ScanMs / w.IndexedMs:N1}x |");

File.WriteAllText(Path.Combine(outDir, "sql.json"),
    JsonSerializer.Serialize(sql, new JsonSerializerOptions { WriteIndented = true }));
File.WriteAllText(Path.Combine(outDir, "bench.json"),
    JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true }));
return;

static Result EngineOnly(int warmup, int measured)
{
    var engine = new MatchingEngine();
    var flow = new OrderFlow(seed: 42);
    for (var i = 0; i < warmup; i++) engine.Apply(flow.Next(engine));   // JIT and caches

    var latencies = new long[measured];
    var total = Stopwatch.StartNew();
    for (var i = 0; i < measured; i++)
    {
        var command = flow.Next(engine);
        var t0 = Stopwatch.GetTimestamp();
        var result = engine.Apply(command);
        latencies[i] = Stopwatch.GetTimestamp() - t0;
        flow.Observe(result);
    }
    total.Stop();
    return Result.From("Engine only (matching)", latencies, total.Elapsed);
}

static async Task<Result> EndToEnd(Durability mode, int count, int clients)
{
    var dir = Directory.CreateTempSubdirectory("tickflow-bench-").FullName;
    try
    {
        await using var service = new EngineService(Path.Combine(dir, "commands.wal"), mode, Array.Empty<ITradeSink>());
        var latencies = new long[count];
        var next = -1;
        var total = Stopwatch.StartNew();
        await Task.WhenAll(Enumerable.Range(0, clients).Select(c => Task.Run(async () =>
        {
            var rng = new Random(c);
            int i;
            while ((i = Interlocked.Increment(ref next)) < count)
            {
                var order = new NewOrder($"b{i}", "ACME", rng.Next(2) == 0 ? Side.Buy : Side.Sell,
                    OrderType.Limit, 10_000 + rng.Next(-10, 11), rng.Next(1, 100), 0);
                var t0 = Stopwatch.GetTimestamp();
                await service.SubmitAsync(order);
                latencies[i] = Stopwatch.GetTimestamp() - t0;
            }
        })));
        total.Stop();
        return Result.From($"End to end, durability = {mode}", latencies, total.Elapsed);
    }
    finally
    {
        Directory.Delete(dir, recursive: true);
    }
}

// 1M synthetic trades over 24 h across 10 symbols; time the per-minute bars query for one symbol with the
// (symbol, ts_ticks) index, then drop the index and time it again.
static async Task<SqlResult> SqlStats(int trades, int runs)
{
    var dir = Directory.CreateTempSubdirectory("tickflow-sql-").FullName;
    try
    {
        var dbPath = Path.Combine(dir, "trades.db");
        var store = new TradeStore(dbPath);
        var rng = new Random(5);
        var start = new DateTime(2026, 1, 5, 0, 0, 0, DateTimeKind.Utc);
        var step = TimeSpan.FromDays(1).Ticks / trades;
        for (var i = 0; i < trades; i++)
            store.Publish(new Trade(i + 1, $"SYM{i % 10}", i, i, 10_000 + rng.Next(-50, 51), rng.Next(1, 100),
                rng.Next(2) == 0 ? Side.Buy : Side.Sell, start.Ticks + i * step));
        await store.FlushAsync(TimeSpan.FromMinutes(5));
        await store.DisposeAsync();

        var reader = new TradeStore(dbPath);
        var ranges = new (string Name, DateTime From, DateTime To)[]
        {
            ("last 1 hour", start.AddHours(23), start.AddDays(1)),
            ("full 24 hours", start, start.AddDays(1)),
        };
        async Task<(double Ms, int Bars)> Time(DateTime from, DateTime to)
        {
            var ms = new List<double>();
            var bars = 0;
            for (var r = 0; r < runs; r++)
            {
                var t0 = Stopwatch.GetTimestamp();
                bars = (await reader.MinuteBarsAsync("SYM3", from, to)).Count;
                ms.Add(Stopwatch.GetElapsedTime(t0).TotalMilliseconds);
            }
            ms.Sort();
            return (ms[ms.Count / 2], bars);
        }

        var indexed = new List<(double Ms, int Bars)>();
        foreach (var (_, from, to) in ranges) indexed.Add(await Time(from, to));
        await using (var db = new SqliteConnection($"Data Source={dbPath};Pooling=False"))
        {
            db.Open();
            using var cmd = db.CreateCommand();
            cmd.CommandText = "DROP INDEX ix_trades_symbol_ts; DROP INDEX ix_trades_symbol_seq;";
            cmd.ExecuteNonQuery();
        }
        var windows = new List<SqlWindow>();
        for (var k = 0; k < ranges.Length; k++)
        {
            var scan = await Time(ranges[k].From, ranges[k].To);
            windows.Add(new SqlWindow(ranges[k].Name, indexed[k].Bars, indexed[k].Ms, scan.Ms));
        }
        await reader.DisposeAsync();
        return new SqlResult(trades, windows);
    }
    finally
    {
        SqliteConnection.ClearAllPools();
        Directory.Delete(dir, recursive: true);
    }
}

static string RuntimeOsName() => System.Runtime.InteropServices.RuntimeInformation.OSDescription;

/// <summary>Realistic-ish flow around a drifting mid price: 70% limits near mid, 10% market, 20% cancels.</summary>
sealed class OrderFlow(int seed)
{
    private readonly Random _rng = new(seed);
    private readonly List<long> _resting = new();
    private long _mid = 10_000;
    private long _id;

    public Command Next(MatchingEngine engine)
    {
        _mid += _rng.Next(-1, 2);
        var roll = _rng.NextDouble();
        if (roll < 0.2 && _resting.Count > 0)
        {
            var k = _rng.Next(_resting.Count);
            var id = _resting[k];
            _resting[k] = _resting[^1];
            _resting.RemoveAt(_resting.Count - 1);
            return new CancelOrder("ACME", id, 0);
        }
        var side = _rng.Next(2) == 0 ? Side.Buy : Side.Sell;
        return roll < 0.3
            ? new NewOrder($"o{++_id}", "ACME", side, OrderType.Market, 0, _rng.Next(1, 50), 0)
            : new NewOrder($"o{++_id}", "ACME", side, OrderType.Limit, _mid + _rng.Next(-8, 9), _rng.Next(1, 100), 0);
    }

    public void Observe(OrderResult r)
    {
        if (r.Status is OrderStatus.Resting or OrderStatus.PartiallyFilledResting) _resting.Add(r.OrderId);
    }
}

sealed record Result(string Name, int Count, double OpsPerSecond, double P50, double P99, double P999, double Max)
{
    public static Result From(string name, long[] ticks, TimeSpan elapsed)
    {
        Array.Sort(ticks);
        double Us(long t) => t * 1_000_000.0 / Stopwatch.Frequency;
        double P(double q) => Us(ticks[Math.Min(ticks.Length - 1, (int)(q * ticks.Length))]);
        var r = new Result(name, ticks.Length, ticks.Length / elapsed.TotalSeconds, P(0.50), P(0.99), P(0.999), Us(ticks[^1]));
        Console.WriteLine($"  {name}: {r.OpsPerSecond:N0} ops/s, p50 {r.P50:N1} µs, p99 {r.P99:N1} µs");
        return r;
    }
}

sealed record SqlWindow(string Range, int Bars, double IndexedMs, double ScanMs);
sealed record SqlResult(int Trades, List<SqlWindow> Windows);
