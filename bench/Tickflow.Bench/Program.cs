using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
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
