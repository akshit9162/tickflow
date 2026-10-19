using Tickflow.Engine;

namespace Tickflow.Tests;

public sealed class RecoveryTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("tickflow-").FullName;
    private string LogPath => Path.Combine(_dir, "commands.wal");

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private sealed class Collect : ITradeSink
    {
        public List<Trade> Trades { get; } = new();
        public void Publish(Trade trade) { lock (Trades) Trades.Add(trade); }
    }

    private static IEnumerable<NewOrder> RandomOrders(int n, int seed)
    {
        var rng = new Random(seed);
        for (var i = 0; i < n; i++)
            yield return new NewOrder($"o{seed}-{i}", "ACME", rng.Next(2) == 0 ? Side.Buy : Side.Sell,
                OrderType.Limit, 10_000 + rng.Next(-10, 11), rng.Next(1, 100), i);
    }

    [Theory]
    [InlineData(Durability.None)]
    [InlineData(Durability.Group)]
    [InlineData(Durability.Full)]
    public async Task Replaying_the_log_rebuilds_the_same_book_and_trades(Durability durability)
    {
        var before = new Collect();
        BookSnapshot expected;
        await using (var engine = new EngineService(LogPath, durability, new[] { before }))
        {
            await Task.WhenAll(RandomOrders(2_000, seed: 1).Select(engine.SubmitAsync));
            expected = await engine.SnapshotAsync("ACME", 50);
        }

        var after = new Collect();
        await using var restarted = new EngineService(LogPath, durability, new[] { after });

        Assert.Equal(2_000, restarted.Recovery.CommandsReplayed);
        Assert.False(restarted.Recovery.TornTailTruncated);
        Assert.Equivalent(expected, await restarted.SnapshotAsync("ACME", 50), strict: true);
        Assert.Equal(before.Trades.OrderBy(t => t.Seq), after.Trades);   // same trades, same sequence numbers
    }

    [Fact]
    public async Task Half_written_last_record_is_truncated_and_the_log_stays_usable()
    {
        await using (var engine = new EngineService(LogPath, Durability.Group, Array.Empty<ITradeSink>()))
            await Task.WhenAll(RandomOrders(100, seed: 2).Select(engine.SubmitAsync));
        var goodLength = new FileInfo(LogPath).Length;
        await using (var f = new FileStream(LogPath, FileMode.Append))
            f.Write(new byte[] { 0x2A, 0x00, 0x00, 0x00, 0xDE, 0xAD });   // a crash in the middle of a write

        await using (var engine = new EngineService(LogPath, Durability.Group, Array.Empty<ITradeSink>()))
        {
            Assert.True(engine.Recovery.TornTailTruncated);
            Assert.Equal(100, engine.Recovery.CommandsReplayed);
            Assert.Equal(goodLength, new FileInfo(LogPath).Length);
            await engine.SubmitAsync(new NewOrder("after-crash", "ACME", Side.Buy, OrderType.Limit, 9_000, 1, 0));
        }

        await using var again = new EngineService(LogPath, Durability.Group, Array.Empty<ITradeSink>());
        Assert.Equal(101, again.Recovery.CommandsReplayed);
        Assert.False(again.Recovery.TornTailTruncated);
    }

    [Fact]
    public async Task Corrupted_record_stops_replay_at_the_last_good_record()
    {
        await using (var engine = new EngineService(LogPath, Durability.Group, Array.Empty<ITradeSink>()))
            await Task.WhenAll(RandomOrders(10, seed: 3).Select(engine.SubmitAsync));
        var bytes = File.ReadAllBytes(LogPath);
        bytes[bytes.Length / 2] ^= 0xFF;   // flip bits in the middle of the file
        File.WriteAllBytes(LogPath, bytes);

        await using var engine2 = new EngineService(LogPath, Durability.Group, Array.Empty<ITradeSink>());

        Assert.True(engine2.Recovery.TornTailTruncated);
        Assert.InRange(engine2.Recovery.CommandsReplayed, 1, 9);   // never replays a record it cannot verify
    }

    [Fact]
    public async Task Duplicate_and_rejected_orders_are_not_written_to_the_log()
    {
        await using (var engine = new EngineService(LogPath, Durability.Group, Array.Empty<ITradeSink>()))
        {
            var order = new NewOrder("same", "ACME", Side.Buy, OrderType.Limit, 10_000, 5, 0);
            await engine.SubmitAsync(order);
            var retry = await engine.SubmitAsync(order);
            var bad = await engine.SubmitAsync(order with { ClientOrderId = "bad", Quantity = 0 });

            Assert.True(retry.Duplicate);
            Assert.Equal(OrderStatus.Rejected, bad.Status);
            Assert.Equal(1, engine.LogRecords);
        }
    }
}
