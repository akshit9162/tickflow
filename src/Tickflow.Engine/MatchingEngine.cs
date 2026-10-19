namespace Tickflow.Engine;

/// <summary>
/// Deterministic, single-threaded matching across symbols. No locks: exactly one thread calls Apply
/// (see EngineService). Given the same commands in the same order it produces the same ids, trades and book.
/// </summary>
public sealed class MatchingEngine
{
    public const int MaxSymbolLength = 16;
    public const int MaxClientOrderIdLength = 64;

    private readonly Dictionary<string, OrderBook> _books = new(StringComparer.Ordinal);
    // Idempotency: a retried order (same client id) returns the first result instead of trading twice.
    private readonly Dictionary<string, OrderResult> _results = new(StringComparer.Ordinal);
    private long _nextOrderId = 1;
    private long _nextTradeSeq = 1;

    public long CommandsApplied { get; private set; }
    public long TradesExecuted => _nextTradeSeq - 1;

    /// <summary>Validation runs before the write-ahead log, so rejected input is never persisted.</summary>
    public static string? Validate(Command command) => command switch
    {
        NewOrder o when string.IsNullOrWhiteSpace(o.ClientOrderId) || o.ClientOrderId.Length > MaxClientOrderIdLength
            => $"clientOrderId must be 1-{MaxClientOrderIdLength} characters",
        NewOrder o when string.IsNullOrWhiteSpace(o.Symbol) || o.Symbol.Length > MaxSymbolLength
            => $"symbol must be 1-{MaxSymbolLength} characters",
        NewOrder o when o.Quantity <= 0 => "quantity must be positive",
        NewOrder o when o.Type == OrderType.Limit && o.Price <= 0 => "limit price must be positive",
        CancelOrder c when c.OrderId <= 0 => "orderId must be positive",
        _ => null,
    };

    public bool IsDuplicate(NewOrder order, out OrderResult first) =>
        _results.TryGetValue(order.ClientOrderId, out first!);

    public OrderResult Apply(Command command) => command switch
    {
        NewOrder o => Apply(o),
        CancelOrder c => Apply(c),
        _ => throw new ArgumentOutOfRangeException(nameof(command)),
    };

    public OrderResult Apply(NewOrder c)
    {
        if (_results.TryGetValue(c.ClientOrderId, out var first))
            return first with { Duplicate = true, Trades = Array.Empty<Trade>() };   // never re-publish its trades

        CommandsApplied++;
        var book = BookFor(c.Symbol);
        var order = new Order
        {
            Id = _nextOrderId++, ClientOrderId = c.ClientOrderId, Symbol = c.Symbol,
            Side = c.Side, Type = c.Type, Price = c.Price, Quantity = c.Quantity, Remaining = c.Quantity,
        };

        var trades = new List<Trade>();
        book.Match(order, trades, () => _nextTradeSeq++, c.TimestampTicks);

        OrderStatus status;
        if (order.Remaining == 0) status = OrderStatus.Filled;
        else if (order.Type == OrderType.Market)
            // Market orders never rest (immediate-or-cancel): the unfilled part is dropped.
            status = order.Remaining == order.Quantity ? OrderStatus.Cancelled : OrderStatus.PartiallyFilledCancelled;
        else
        {
            book.Rest(order);
            status = order.Remaining == order.Quantity ? OrderStatus.Resting : OrderStatus.PartiallyFilledResting;
        }

        var result = new OrderResult(order.Id, order.ClientOrderId, status,
            order.Quantity - order.Remaining, order.Remaining, trades);
        _results[c.ClientOrderId] = result;
        return result;
    }

    public OrderResult Apply(CancelOrder c)
    {
        CommandsApplied++;
        var cancelled = _books.TryGetValue(c.Symbol, out var book) ? book.Cancel(c.OrderId) : null;
        return cancelled is null
            ? new OrderResult(c.OrderId, "", OrderStatus.NotFound, 0, 0, Array.Empty<Trade>())
            : new OrderResult(c.OrderId, cancelled.ClientOrderId, OrderStatus.Cancelled,
                cancelled.Quantity - cancelled.Remaining, cancelled.Remaining, Array.Empty<Trade>());
    }

    public BookSnapshot Snapshot(string symbol, int depth) =>
        _books.TryGetValue(symbol, out var book)
            ? book.Snapshot(depth)
            : new BookSnapshot(symbol, Array.Empty<Level>(), Array.Empty<Level>());

    public int RestingOrders(string symbol) => _books.TryGetValue(symbol, out var b) ? b.OrderCount : 0;

    private OrderBook BookFor(string symbol)
    {
        if (!_books.TryGetValue(symbol, out var book))
        {
            book = new OrderBook(symbol);
            _books.Add(symbol, book);
        }
        return book;
    }
}
