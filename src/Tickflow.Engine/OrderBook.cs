namespace Tickflow.Engine;

/// <summary>All resting orders at one price, oldest first (time priority).</summary>
internal sealed class PriceLevel
{
    public PriceLevel(long price) => Price = price;
    public long Price { get; }
    public LinkedList<Order> Orders { get; } = new();
    public long TotalQuantity { get; set; }
}

/// <summary>
/// Limit order book for one symbol.
///  - Each side is a sorted map price → level (red-black tree): best price in O(log n).
///  - Each level is a FIFO linked list: fills go to the oldest order first.
///  - An id → list-node index makes cancel O(1) to find, O(log n) only if the level empties.
/// </summary>
public sealed class OrderBook
{
    private static readonly Comparer<long> Descending = Comparer<long>.Create((a, b) => b.CompareTo(a));

    private readonly SortedDictionary<long, PriceLevel> _bids = new(Descending);   // highest first
    private readonly SortedDictionary<long, PriceLevel> _asks = new();             // lowest first
    private readonly Dictionary<long, LinkedListNode<Order>> _index = new();

    public OrderBook(string symbol) => Symbol = symbol;

    public string Symbol { get; }
    public int OrderCount => _index.Count;
    public long? BestBid => First(_bids)?.Price;
    public long? BestAsk => First(_asks)?.Price;

    /// <summary>Fills the taker against the opposite side until it is filled or no longer crosses.</summary>
    internal void Match(Order taker, List<Trade> trades, Func<long> nextTradeSeq, long timestamp)
    {
        var opposite = taker.Side == Side.Buy ? _asks : _bids;
        while (taker.Remaining > 0 && First(opposite) is { } level)
        {
            if (taker.Type == OrderType.Limit && !Crosses(taker, level.Price)) break;

            var node = level.Orders.First;
            while (node is not null && taker.Remaining > 0)
            {
                var maker = node.Value;
                var qty = Math.Min(taker.Remaining, maker.Remaining);
                maker.Remaining -= qty;
                taker.Remaining -= qty;
                level.TotalQuantity -= qty;

                var (buyId, sellId) = taker.Side == Side.Buy ? (taker.Id, maker.Id) : (maker.Id, taker.Id);
                // Trades happen at the resting order's price: the maker set it first.
                trades.Add(new Trade(nextTradeSeq(), Symbol, buyId, sellId, level.Price, qty, taker.Side, timestamp));

                var next = node.Next;
                if (maker.Remaining == 0)
                {
                    level.Orders.Remove(node);
                    _index.Remove(maker.Id);
                }
                node = next;
            }
            if (level.Orders.Count == 0) opposite.Remove(level.Price);
        }
    }

    internal void Rest(Order order)
    {
        var side = order.Side == Side.Buy ? _bids : _asks;
        if (!side.TryGetValue(order.Price, out var level))
        {
            level = new PriceLevel(order.Price);
            side.Add(order.Price, level);
        }
        _index[order.Id] = level.Orders.AddLast(order);
        level.TotalQuantity += order.Remaining;
    }

    internal Order? Cancel(long orderId)
    {
        if (!_index.Remove(orderId, out var node)) return null;
        var order = node.Value;
        var side = order.Side == Side.Buy ? _bids : _asks;
        var level = side[order.Price];
        level.Orders.Remove(node);
        level.TotalQuantity -= order.Remaining;
        if (level.Orders.Count == 0) side.Remove(order.Price);
        return order;
    }

    public BookSnapshot Snapshot(int depth) =>
        new(Symbol, Take(_bids, depth), Take(_asks, depth));

    private static bool Crosses(Order taker, long levelPrice) =>
        taker.Side == Side.Buy ? levelPrice <= taker.Price : levelPrice >= taker.Price;

    private static PriceLevel? First(SortedDictionary<long, PriceLevel> side)
    {
        foreach (var kv in side) return kv.Value;   // struct enumerator: no allocation
        return null;
    }

    private static List<Level> Take(SortedDictionary<long, PriceLevel> side, int depth)
    {
        var levels = new List<Level>(Math.Min(depth, side.Count));
        foreach (var kv in side)
        {
            if (levels.Count == depth) break;
            levels.Add(new Level(kv.Key, kv.Value.TotalQuantity, kv.Value.Orders.Count));
        }
        return levels;
    }
}
