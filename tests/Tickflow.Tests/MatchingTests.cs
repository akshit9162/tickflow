using Tickflow.Engine;

namespace Tickflow.Tests;

public class MatchingTests
{
    private readonly MatchingEngine _engine = new();
    private int _id;

    private OrderResult Limit(Side side, long price, long qty, string? clientId = null) =>
        _engine.Apply(new NewOrder(clientId ?? $"c{++_id}", "ACME", side, OrderType.Limit, price, qty, 0));

    private OrderResult Market(Side side, long qty) =>
        _engine.Apply(new NewOrder($"c{++_id}", "ACME", side, OrderType.Market, 0, qty, 0));

    [Fact]
    public void Order_with_nothing_to_match_rests_on_the_book()
    {
        var r = Limit(Side.Buy, 10_000, 50);

        Assert.Equal(OrderStatus.Resting, r.Status);
        var book = _engine.Snapshot("ACME", 5);
        Assert.Equal(new Level(10_000, 50, 1), Assert.Single(book.Bids));
        Assert.Empty(book.Asks);
    }

    [Fact]
    public void Crossing_order_trades_at_the_resting_price()
    {
        var sell = Limit(Side.Sell, 10_000, 100);
        var buy = Limit(Side.Buy, 10_100, 60);   // willing to pay more than the ask

        Assert.Equal(OrderStatus.Filled, buy.Status);
        var trade = Assert.Single(buy.Trades);
        Assert.Equal(10_000, trade.Price);       // maker's price, not the taker's limit
        Assert.Equal(60, trade.Quantity);
        Assert.Equal(buy.OrderId, trade.BuyOrderId);
        Assert.Equal(sell.OrderId, trade.SellOrderId);
        Assert.Equal(new Level(10_000, 40, 1), Assert.Single(_engine.Snapshot("ACME", 5).Asks));
    }

    [Fact]
    public void Better_prices_fill_first()
    {
        Limit(Side.Sell, 10_100, 100);
        Limit(Side.Sell, 10_000, 100);

        var buy = Limit(Side.Buy, 10_100, 150);

        Assert.Equal(new long[] { 10_000, 10_100 }, buy.Trades.Select(t => t.Price));
        Assert.Equal(new long[] { 100, 50 }, buy.Trades.Select(t => t.Quantity));
    }

    [Fact]
    public void At_the_same_price_the_oldest_order_fills_first()
    {
        var first = Limit(Side.Sell, 10_000, 100);
        var second = Limit(Side.Sell, 10_000, 100);

        var buy = Limit(Side.Buy, 10_000, 50);

        Assert.Equal(first.OrderId, Assert.Single(buy.Trades).SellOrderId);
        Assert.NotEqual(second.OrderId, buy.Trades[0].SellOrderId);
    }

    [Fact]
    public void Partially_filled_limit_order_rests_the_remainder()
    {
        Limit(Side.Sell, 10_000, 30);

        var buy = Limit(Side.Buy, 10_000, 100);

        Assert.Equal(OrderStatus.PartiallyFilledResting, buy.Status);
        Assert.Equal((30, 70), (buy.Filled, buy.Remaining));
        Assert.Equal(new Level(10_000, 70, 1), Assert.Single(_engine.Snapshot("ACME", 5).Bids));
    }

    [Fact]
    public void Market_order_sweeps_levels_and_drops_what_it_cannot_fill()
    {
        Limit(Side.Sell, 10_000, 50);
        Limit(Side.Sell, 10_200, 50);

        var buy = Market(Side.Buy, 150);

        Assert.Equal(OrderStatus.PartiallyFilledCancelled, buy.Status);
        Assert.Equal((100, 50), (buy.Filled, buy.Remaining));
        Assert.Empty(_engine.Snapshot("ACME", 5).Asks);
        Assert.Empty(_engine.Snapshot("ACME", 5).Bids);   // market orders never rest
    }

    [Fact]
    public void Market_order_with_no_liquidity_is_cancelled()
    {
        Assert.Equal(OrderStatus.Cancelled, Market(Side.Sell, 10).Status);
    }

    [Fact]
    public void Cancel_removes_the_order_once()
    {
        var resting = Limit(Side.Buy, 9_900, 40);

        var cancel = _engine.Apply(new CancelOrder("ACME", resting.OrderId, 0));
        var again = _engine.Apply(new CancelOrder("ACME", resting.OrderId, 0));

        Assert.Equal((OrderStatus.Cancelled, 40L), (cancel.Status, cancel.Remaining));
        Assert.Equal(OrderStatus.NotFound, again.Status);
        Assert.Empty(_engine.Snapshot("ACME", 5).Bids);
    }

    [Fact]
    public void Cancelling_a_partly_filled_order_returns_only_the_unfilled_part()
    {
        var sell = Limit(Side.Sell, 10_000, 100);
        Limit(Side.Buy, 10_000, 30);

        var cancel = _engine.Apply(new CancelOrder("ACME", sell.OrderId, 0));

        Assert.Equal((30L, 70L), (cancel.Filled, cancel.Remaining));
    }

    [Fact]
    public void Retried_order_with_the_same_client_id_does_not_trade_twice()
    {
        Limit(Side.Sell, 10_000, 100);
        var first = Limit(Side.Buy, 10_000, 40, clientId: "retry-me");

        var retry = Limit(Side.Buy, 10_000, 40, clientId: "retry-me");

        Assert.True(retry.Duplicate);
        Assert.Equal(first.OrderId, retry.OrderId);
        Assert.Empty(retry.Trades);
        Assert.Equal(new Level(10_000, 60, 1), Assert.Single(_engine.Snapshot("ACME", 5).Asks));
    }

    [Theory]
    [InlineData("", "ACME", OrderType.Limit, 100, 1, "clientOrderId")]
    [InlineData("a", "", OrderType.Limit, 100, 1, "symbol")]
    [InlineData("a", "ACME", OrderType.Limit, 100, 0, "quantity")]
    [InlineData("a", "ACME", OrderType.Limit, 0, 1, "price")]
    public void Invalid_orders_are_rejected(string clientId, string symbol, OrderType type, long price, long qty, string field)
    {
        var reason = MatchingEngine.Validate(new NewOrder(clientId, symbol, Side.Buy, type, price, qty, 0));
        Assert.Contains(field, reason);
    }

    [Fact]
    public void Book_is_never_left_crossed_under_random_flow()
    {
        var rng = new Random(7);
        var resting = new List<long>();
        for (var i = 0; i < 20_000; i++)
        {
            if (resting.Count > 0 && rng.NextDouble() < 0.2)
            {
                var k = rng.Next(resting.Count);
                _engine.Apply(new CancelOrder("ACME", resting[k], 0));
                resting.RemoveAt(k);
            }
            else
            {
                var side = rng.Next(2) == 0 ? Side.Buy : Side.Sell;
                var r = rng.NextDouble() < 0.1
                    ? Market(side, rng.Next(1, 200))
                    : Limit(side, 10_000 + rng.Next(-20, 21), rng.Next(1, 200));
                if (r.Remaining > 0 && r.Status is OrderStatus.Resting or OrderStatus.PartiallyFilledResting)
                    resting.Add(r.OrderId);
                Assert.All(r.Trades, t => Assert.True(t.Quantity > 0));
                Assert.Equal(r.Filled, r.Trades.Sum(t => t.Quantity));
            }

            var book = _engine.Snapshot("ACME", 1);
            if (book.Bids.Count > 0 && book.Asks.Count > 0)
                Assert.True(book.Bids[0].Price < book.Asks[0].Price, $"crossed at step {i}");
        }
    }
}
