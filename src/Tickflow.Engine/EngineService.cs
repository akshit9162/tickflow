using System.Threading.Channels;

namespace Tickflow.Engine;

/// <summary>Receives every trade the engine produces (WebSocket feed, SQL store, ...). Must not block.</summary>
public interface ITradeSink
{
    void Publish(Trade trade);
}

/// <summary>
/// Single-writer front end for the engine.
///  - All requests go through one bounded channel and are applied by one loop, so matching needs no locks
///    and the order of commands is exactly the order written to the log.
///  - Commands are logged before they are applied (write-ahead), and a caller is only acknowledged once its
///    command is durable under the chosen mode. In Group mode the loop drains up to BatchSize requests,
///    appends them all, fsyncs once, then acknowledges the whole batch.
///  - On startup the log is replayed through the same engine, rebuilding the book and re-publishing the trades
///    (sinks treat trades idempotently by sequence number).
/// </summary>
public sealed class EngineService : IAsyncDisposable
{
    public const int BatchSize = 256;

    private readonly Channel<Request> _inbox =
        Channel.CreateBounded<Request>(new BoundedChannelOptions(65_536)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,   // backpressure: callers wait instead of the queue growing forever
        });

    private readonly MatchingEngine _engine = new();
    private readonly WriteAheadLog _log;
    private readonly IReadOnlyList<ITradeSink> _sinks;
    private readonly Task _loop;

    public EngineService(string logPath, Durability durability, IEnumerable<ITradeSink> sinks)
    {
        _sinks = sinks.ToList();
        _log = WriteAheadLog.Open(logPath, durability, out var commands, out var info);
        Recovery = info;
        foreach (var command in commands) PublishTrades(_engine.Apply(command));
        _loop = Task.Factory.StartNew(Run, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    public RecoveryInfo Recovery { get; }
    public Durability Durability => _log.Durability;
    public long LogRecords => Interlocked.Read(ref _logRecords);
    public long TradesExecuted => Interlocked.Read(ref _trades);
    private long _logRecords;
    private long _trades;

    public Task<OrderResult> SubmitAsync(NewOrder order) => Enqueue(new CommandRequest(order));
    public Task<OrderResult> CancelAsync(CancelOrder cancel) => Enqueue(new CommandRequest(cancel));
    public Task<BookSnapshot> SnapshotAsync(string symbol, int depth) => Enqueue(new SnapshotRequest(symbol, depth));

    private async Task<T> Enqueue<T>(Request<T> request)
    {
        await _inbox.Writer.WriteAsync(request);
        return await request.Completion.Task;
    }

    private void Run()
    {
        var reader = _inbox.Reader;
        var batch = new List<(CommandRequest Request, OrderResult Result)>(BatchSize);

        while (reader.WaitToReadAsync().AsTask().GetAwaiter().GetResult())
        {
            batch.Clear();
            while (batch.Count < BatchSize && reader.TryRead(out var request))
            {
                switch (request)
                {
                    case SnapshotRequest s:
                        s.Completion.TrySetResult(_engine.Snapshot(s.Symbol, s.Depth));
                        break;
                    case CommandRequest c:
                        Handle(c, batch);
                        break;
                }
            }

            _log.Commit();                                    // one fsync for the batch (Group mode)
            Interlocked.Exchange(ref _logRecords, _log.Records);
            Interlocked.Exchange(ref _trades, _engine.TradesExecuted);
            foreach (var (request, result) in batch)          // acknowledge only after the data is durable
            {
                PublishTrades(result);
                request.Completion.TrySetResult(result);
            }
        }
    }

    private void Handle(CommandRequest request, List<(CommandRequest, OrderResult)> batch)
    {
        var command = request.Command;
        if (MatchingEngine.Validate(command) is { } reason)
        {
            request.Completion.TrySetResult(Rejected(command, reason));
            return;
        }
        if (command is NewOrder order && _engine.IsDuplicate(order, out var first))
        {
            // Retries are not logged again, but still wait for this batch's fsync: the original may be in it.
            batch.Add((request, first with { Duplicate = true, Trades = Array.Empty<Trade>() }));
            return;
        }
        try
        {
            _log.Append(command);                             // write-ahead: log first, then apply
        }
        catch (IOException e)
        {
            request.Completion.TrySetException(e);
            return;
        }
        batch.Add((request, _engine.Apply(command)));
    }

    private void PublishTrades(OrderResult result)
    {
        foreach (var trade in result.Trades)
            foreach (var sink in _sinks) sink.Publish(trade);
    }

    private static OrderResult Rejected(Command command, string reason) => command switch
    {
        NewOrder o => new OrderResult(0, o.ClientOrderId, OrderStatus.Rejected, 0, o.Quantity, Array.Empty<Trade>(), Reason: reason),
        CancelOrder c => new OrderResult(c.OrderId, "", OrderStatus.Rejected, 0, 0, Array.Empty<Trade>(), Reason: reason),
        _ => throw new ArgumentOutOfRangeException(nameof(command)),
    };

    private int _disposed;

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
        _inbox.Writer.TryComplete();
        await _loop;
        _log.Dispose();
    }

    private abstract class Request;

    private abstract class Request<T> : Request
    {
        public TaskCompletionSource<T> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class CommandRequest(Command command) : Request<OrderResult>
    {
        public Command Command { get; } = command;
    }

    private sealed class SnapshotRequest(string symbol, int depth) : Request<BookSnapshot>
    {
        public string Symbol { get; } = symbol;
        public int Depth { get; } = depth;
    }
}
