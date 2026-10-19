using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Tickflow.Api;
using Tickflow.Engine;

namespace Tickflow.Tests;

public sealed class ApiTests : IDisposable
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() },
    };

    private readonly string _dir = Directory.CreateTempSubdirectory("tickflow-api-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private WebApplicationFactory<Program> Server() =>
        new WebApplicationFactory<Program>().WithWebHostBuilder(b => b.UseSetting("Tickflow:DataDir", _dir));

    private static async Task<OrderResponse> Place(HttpClient http, string id, Side side, decimal? price, long qty,
        OrderType type = OrderType.Limit)
    {
        var res = await http.PostAsJsonAsync("/orders", new PlaceOrderRequest(id, "ACME", side, type, price, qty), Json);
        res.EnsureSuccessStatusCode();
        return (await res.Content.ReadFromJsonAsync<OrderResponse>(Json))!;
    }

    [Fact]
    public async Task Orders_match_over_http_and_trades_are_stored()
    {
        await using var server = Server();
        var http = server.CreateClient();

        await Place(http, "s1", Side.Sell, 100.25m, 10);
        var buy = await Place(http, "b1", Side.Buy, 100.50m, 4);

        Assert.Equal(OrderStatus.Filled, buy.Status);
        Assert.Equal(100.25m, Assert.Single(buy.Trades).Price);

        var book = await http.GetFromJsonAsync<BookResponse>("/book/ACME", Json);
        Assert.Equal(new LevelDto(100.25m, 6, 1), Assert.Single(book!.Asks));

        await server.Services.GetRequiredService<TradeStore>().FlushAsync();
        var trades = await http.GetFromJsonAsync<List<TradeDto>>("/trades?symbol=ACME", Json);
        Assert.Equal(4, Assert.Single(trades!).Quantity);
    }

    [Fact]
    public async Task Bad_input_gets_a_400()
    {
        await using var server = Server();
        var http = server.CreateClient();

        var offTick = await http.PostAsJsonAsync("/orders",
            new PlaceOrderRequest("x", "ACME", Side.Buy, OrderType.Limit, 100.123m, 1), Json);
        var zeroQty = await http.PostAsJsonAsync("/orders",
            new PlaceOrderRequest("y", "ACME", Side.Buy, OrderType.Limit, 100m, 0), Json);

        Assert.Equal(HttpStatusCode.BadRequest, offTick.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, zeroQty.StatusCode);
    }

    [Fact]
    public async Task Cancel_returns_404_the_second_time()
    {
        await using var server = Server();
        var http = server.CreateClient();
        var order = await Place(http, "c1", Side.Buy, 99m, 5);

        var first = await http.DeleteAsync($"/orders/ACME/{order.OrderId}");
        var second = await http.DeleteAsync($"/orders/ACME/{order.OrderId}");

        Assert.Equal(HttpStatusCode.OK, first.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, second.StatusCode);
    }

    [Fact]
    public async Task Restart_restores_the_book_without_duplicating_stored_trades()
    {
        await using (var server = Server())
        {
            var http = server.CreateClient();
            await Place(http, "s1", Side.Sell, 101m, 10);
            await Place(http, "b1", Side.Buy, 101m, 3);
            await Place(http, "b2", Side.Buy, 100m, 7);   // rests
        }

        await using var restarted = Server();
        var http2 = restarted.CreateClient();
        var book = await http2.GetFromJsonAsync<BookResponse>("/book/ACME", Json);
        await restarted.Services.GetRequiredService<TradeStore>().FlushAsync();
        var store = restarted.Services.GetRequiredService<TradeStore>();

        Assert.Equal(new LevelDto(101m, 7, 1), Assert.Single(book!.Asks));
        Assert.Equal(new LevelDto(100m, 7, 1), Assert.Single(book.Bids));
        Assert.Equal(1, await store.CountAsync());   // the replayed trade was ignored, not inserted again

        var retry = await Place(http2, "b1", Side.Buy, 101m, 3);   // a client retrying after the restart
        Assert.True(retry.Duplicate);
    }

    [Fact]
    public async Task Stats_returns_minute_bars_computed_in_sql()
    {
        await using var server = Server();
        var http = server.CreateClient();
        await Place(http, "s1", Side.Sell, 100.00m, 10);
        await Place(http, "s2", Side.Sell, 101.00m, 10);
        await Place(http, "b1", Side.Buy, 101.00m, 15);   // trades 10 @ 100.00 then 5 @ 101.00
        await Place(http, "b2", Side.Buy, 99.00m, 4);     // rests
        await Place(http, "s3", Side.Sell, 99.00m, 4);    // trades 4 @ 99.00, seller is the aggressor
        await server.Services.GetRequiredService<TradeStore>().FlushAsync();

        var bars = await http.GetFromJsonAsync<List<MinuteBar>>("/stats/ACME?minutes=5", Json);

        var total = bars!.Sum(b => b.Volume);
        Assert.Equal(19, total);
        Assert.Equal(3, bars.Sum(b => b.Trades));
        if (bars.Count == 1)                              // all three trades landed in the same minute
        {
            var bar = bars[0];
            Assert.Equal((100.00m, 101.00m, 99.00m, 99.00m), (bar.Open, bar.High, bar.Low, bar.Close));
            Assert.Equal(Math.Round((10 * 100.00m + 5 * 101.00m + 4 * 99.00m) / 19, 4), bar.Vwap);
            Assert.Equal((15L, 4L), (bar.BuyVolume, bar.SellVolume));
        }
    }

    [Fact]
    public async Task Stats_query_uses_the_symbol_time_index()
    {
        await using var server = Server();
        var plan = await server.Services.GetRequiredService<TradeStore>().ExplainMinuteBarsAsync();
        Assert.Contains("ix_trades_symbol_ts", plan);
        Assert.DoesNotContain("SCAN trades", plan);       // no full table scan
    }
}
