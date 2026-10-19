using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Http.Json;
using Tickflow.Api;
using Tickflow.Engine;

// Content root = the folder holding the dll, so wwwroot and defaults resolve wherever the app is launched from.
var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = args, ContentRootPath = AppContext.BaseDirectory });
builder.Logging.SetMinimumLevel(LogLevel.Warning);

var dataDir = builder.Configuration["Tickflow:DataDir"] ?? Path.Combine(AppContext.BaseDirectory, "data");
var durability = Enum.Parse<Durability>(builder.Configuration["Tickflow:Durability"] ?? nameof(Durability.Group), ignoreCase: true);
Directory.CreateDirectory(dataDir);

builder.Services.Configure<JsonOptions>(o =>
{
    o.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    o.SerializerOptions.Converters.Add(new JsonStringEnumConverter());
});
builder.Services.AddSingleton(_ => new TradeStore(Path.Combine(dataDir, "trades.db")));
builder.Services.AddSingleton<TradeFeed>();
builder.Services.AddSingleton(sp => new EngineService(
    Path.Combine(dataDir, "commands.wal"), durability,
    new ITradeSink[] { sp.GetRequiredService<TradeStore>(), sp.GetRequiredService<TradeFeed>() }));

var app = builder.Build();
var started = Stopwatch.StartNew();
var engine = app.Services.GetRequiredService<EngineService>();   // start now: replays the log before serving
var store = app.Services.GetRequiredService<TradeStore>();
var feed = app.Services.GetRequiredService<TradeFeed>();
var json = app.Services.GetRequiredService<Microsoft.Extensions.Options.IOptions<JsonOptions>>().Value.SerializerOptions;

app.UseWebSockets();
app.UseDefaultFiles();   // serves wwwroot/index.html at "/": the live trading screen
app.UseStaticFiles();

app.MapPost("/orders", async (PlaceOrderRequest req) =>
{
    long ticks = 0;
    if (req.Type == OrderType.Limit && (req.Price is null || !Prices.TryToTicks(req.Price.Value, out ticks)))
        return Results.BadRequest(new { reason = $"limit orders need a price in steps of {Prices.TickSize}" });

    var result = await engine.SubmitAsync(new NewOrder(
        req.ClientOrderId ?? "", req.Symbol ?? "", req.Side, req.Type, ticks, req.Quantity, DateTime.UtcNow.Ticks));
    return result.Status == OrderStatus.Rejected
        ? Results.BadRequest(OrderResponse.From(result))
        : Results.Ok(OrderResponse.From(result));
});

app.MapDelete("/orders/{symbol}/{orderId:long}", async (string symbol, long orderId) =>
{
    var result = await engine.CancelAsync(new CancelOrder(symbol, orderId, DateTime.UtcNow.Ticks));
    return result.Status switch
    {
        OrderStatus.NotFound => Results.NotFound(OrderResponse.From(result)),
        OrderStatus.Rejected => Results.BadRequest(OrderResponse.From(result)),
        _ => Results.Ok(OrderResponse.From(result)),
    };
});

app.MapGet("/book/{symbol}", async (string symbol, int? depth) =>
    Results.Ok(BookResponse.From(await engine.SnapshotAsync(symbol, Math.Clamp(depth ?? 10, 1, 100)))));

app.MapGet("/trades", async (string? symbol, int? limit) =>
    Results.Ok(await store.RecentAsync(symbol, Math.Clamp(limit ?? 100, 1, 1000))));

app.MapGet("/stats/{symbol}", async (string symbol, int? minutes) =>
{
    var to = DateTime.UtcNow.AddSeconds(1);
    var from = to.AddMinutes(-Math.Clamp(minutes ?? 60, 1, 24 * 60));
    return Results.Ok(await store.MinuteBarsAsync(symbol, from, to));
});

app.MapGet("/health", () => Results.Ok(new
{
    status = "ok",
    durability = engine.Durability.ToString(),
    logRecords = engine.LogRecords,
    tradesExecuted = engine.TradesExecuted,
    recovery = engine.Recovery,
    feedSubscribers = feed.Subscribers,
    uptimeSeconds = (long)started.Elapsed.TotalSeconds,
}));

app.Map("/ws/trades", async (HttpContext ctx, string? symbol) =>
{
    if (!ctx.WebSockets.IsWebSocketRequest)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        return;
    }
    using var socket = await ctx.WebSockets.AcceptWebSocketAsync();
    await feed.StreamAsync(socket, symbol, json, ctx.RequestAborted);
});

// On shutdown the host disposes singletons in reverse creation order: the engine first (finishes queued
// requests and fsyncs the log), then the trade store (drains pending inserts).
app.Run();

public partial class Program;   // lets the integration tests host the app
