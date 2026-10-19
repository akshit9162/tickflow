namespace Tickflow.Engine;

public enum Side : byte { Buy = 0, Sell = 1 }

public enum OrderType : byte { Limit = 0, Market = 1 }

/// <summary>
/// A live order. Prices are integer ticks (1 tick = 0.01) so matching never touches floating point.
/// </summary>
public sealed class Order
{
    public long Id { get; init; }
    public string ClientOrderId { get; init; } = "";
    public string Symbol { get; init; } = "";
    public Side Side { get; init; }
    public OrderType Type { get; init; }
    public long Price { get; init; }      // ignored for market orders
    public long Quantity { get; init; }
    public long Remaining { get; internal set; }
}

public readonly record struct Trade(
    long Seq, string Symbol, long BuyOrderId, long SellOrderId,
    long Price, long Quantity, Side Aggressor, long TimestampTicks);

/// <summary>
/// Inputs to the engine. These, not the trades, are what the write-ahead log stores: replaying the same
/// commands in the same order rebuilds exactly the same book and the same trades (the engine is deterministic).
/// </summary>
public abstract record Command(long TimestampTicks);

public sealed record NewOrder(
    string ClientOrderId, string Symbol, Side Side, OrderType Type,
    long Price, long Quantity, long TimestampTicks) : Command(TimestampTicks);

public sealed record CancelOrder(string Symbol, long OrderId, long TimestampTicks) : Command(TimestampTicks);

public enum OrderStatus
{
    Rejected,
    Resting,                    // nothing matched; the whole order is on the book
    PartiallyFilledResting,     // some matched, the rest is on the book
    Filled,
    PartiallyFilledCancelled,   // market order that ran out of liquidity
    Cancelled,
    NotFound,
}

public sealed record OrderResult(
    long OrderId, string ClientOrderId, OrderStatus Status, long Filled, long Remaining,
    IReadOnlyList<Trade> Trades, bool Duplicate = false, string? Reason = null);

public sealed record Level(long Price, long Quantity, int Orders);

public sealed record BookSnapshot(string Symbol, IReadOnlyList<Level> Bids, IReadOnlyList<Level> Asks);
