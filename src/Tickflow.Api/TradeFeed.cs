using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text.Json;
using System.Threading.Channels;
using Tickflow.Engine;

namespace Tickflow.Api;

/// <summary>
/// Live trade stream over WebSockets. Each subscriber gets its own bounded queue; a client that cannot keep up
/// loses its oldest messages (DropOldest) instead of slowing the engine or other subscribers down.
/// </summary>
public sealed class TradeFeed : ITradeSink
{
    public const int SubscriberQueue = 4096;

    private readonly ConcurrentDictionary<Guid, Subscriber> _subscribers = new();

    public int Subscribers => _subscribers.Count;

    public void Publish(Trade trade)
    {
        foreach (var s in _subscribers.Values)
            if (s.Symbol is null || s.Symbol == trade.Symbol) s.Queue.Writer.TryWrite(trade);
    }

    public async Task StreamAsync(WebSocket socket, string? symbol, JsonSerializerOptions json, CancellationToken ct)
    {
        var id = Guid.NewGuid();
        var subscriber = new Subscriber(symbol, Channel.CreateBounded<Trade>(
            new BoundedChannelOptions(SubscriberQueue) { SingleReader = true, FullMode = BoundedChannelFullMode.DropOldest }));
        _subscribers[id] = subscriber;
        try
        {
            _ = DrainIncomingAsync(socket, id, ct);   // notice when the client closes
            await foreach (var trade in subscriber.Queue.Reader.ReadAllAsync(ct))
            {
                var bytes = JsonSerializer.SerializeToUtf8Bytes(TradeDto.From(trade), json);
                await socket.SendAsync(bytes, WebSocketMessageType.Text, endOfMessage: true, ct);
            }
        }
        catch (Exception e) when (e is OperationCanceledException or WebSocketException) { }
        finally
        {
            _subscribers.TryRemove(id, out _);
        }
    }

    private async Task DrainIncomingAsync(WebSocket socket, Guid id, CancellationToken ct)
    {
        var buffer = new byte[256];
        try
        {
            while (socket.State == WebSocketState.Open)
            {
                var msg = await socket.ReceiveAsync(buffer, ct);
                if (msg.MessageType == WebSocketMessageType.Close) break;
            }
        }
        catch (Exception e) when (e is OperationCanceledException or WebSocketException) { }
        if (_subscribers.TryRemove(id, out var s)) s.Queue.Writer.TryComplete();
    }

    private sealed record Subscriber(string? Symbol, Channel<Trade> Queue);
}
