using Tickflow.Engine;

namespace Tickflow.Api;

/// <summary>Prices are decimals on the wire and integer ticks (0.01) inside the engine.</summary>
public static class Prices
{
    public const decimal TickSize = 0.01m;

    public const decimal MaxPrice = 1_000_000_000m;

    public static bool TryToTicks(decimal price, out long ticks)
    {
        ticks = 0;
        if (price <= 0 || price > MaxPrice) return false;
        var scaled = price / TickSize;
        ticks = (long)scaled;
        return scaled == decimal.Truncate(scaled);   // reject prices that are not a whole number of ticks
    }

    public static decimal ToDecimal(long ticks) => ticks * TickSize;
}

public sealed record PlaceOrderRequest(
    string ClientOrderId, string Symbol, Side Side, OrderType Type, decimal? Price, long Quantity);

public sealed record TradeDto(
    long Seq, string Symbol, long BuyOrderId, long SellOrderId, decimal Price, long Quantity,
    string Aggressor, DateTime Time)
{
    public static TradeDto From(Trade t) =>
        new(t.Seq, t.Symbol, t.BuyOrderId, t.SellOrderId, Prices.ToDecimal(t.Price), t.Quantity,
            t.Aggressor.ToString(), new DateTime(t.TimestampTicks, DateTimeKind.Utc));
}

public sealed record OrderResponse(
    long OrderId, string ClientOrderId, OrderStatus Status, long Filled, long Remaining,
    IReadOnlyList<TradeDto> Trades, bool Duplicate, string? Reason)
{
    public static OrderResponse From(OrderResult r) =>
        new(r.OrderId, r.ClientOrderId, r.Status, r.Filled, r.Remaining,
            r.Trades.Select(TradeDto.From).ToList(), r.Duplicate, r.Reason);
}

public sealed record LevelDto(decimal Price, long Quantity, int Orders);

public sealed record BookResponse(string Symbol, IReadOnlyList<LevelDto> Bids, IReadOnlyList<LevelDto> Asks)
{
    public static BookResponse From(BookSnapshot s) =>
        new(s.Symbol,
            s.Bids.Select(l => new LevelDto(Prices.ToDecimal(l.Price), l.Quantity, l.Orders)).ToList(),
            s.Asks.Select(l => new LevelDto(Prices.ToDecimal(l.Price), l.Quantity, l.Orders)).ToList());
}

public sealed record MinuteBar(
    DateTime Minute, decimal Open, decimal High, decimal Low, decimal Close,
    long Volume, long Trades, decimal Vwap, long BuyVolume, long SellVolume);
