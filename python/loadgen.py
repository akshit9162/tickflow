"""HTTP load generator: many concurrent clients place orders and time each round trip, while a WebSocket
subscriber counts the live trades it receives.

    python loadgen.py --orders 50000 --clients 64          # starts its own server in a temp folder
    python loadgen.py --url http://127.0.0.1:5077 ...      # or targets a running one
"""
import argparse
import asyncio
import json
import random
import statistics
import tempfile
import time
from pathlib import Path

import aiohttp

from server import Server, build

SYMBOLS = ["ACME", "GLOBEX", "INITECH"]


def make_order(rng: random.Random, i: int, mid: dict) -> dict:
    sym = rng.choice(SYMBOLS)
    mid[sym] += rng.choice((-1, 0, 1))
    if rng.random() < 0.1:
        return {"clientOrderId": f"lg-{i}", "symbol": sym, "side": rng.choice(("Buy", "Sell")),
                "type": "Market", "quantity": rng.randint(1, 50)}
    price = (mid[sym] + rng.randint(-8, 8)) / 100
    return {"clientOrderId": f"lg-{i}", "symbol": sym, "side": rng.choice(("Buy", "Sell")),
            "type": "Limit", "price": round(price, 2), "quantity": rng.randint(1, 100)}


async def client(session, url, ids, rng, mid, latencies, statuses):
    for i in ids:
        order = make_order(rng, i, mid)
        t0 = time.perf_counter()
        async with session.post(url + "/orders", json=order) as r:
            body = await r.json()
        latencies.append((time.perf_counter() - t0) * 1e6)
        statuses[body.get("status", r.status)] = statuses.get(body.get("status", r.status), 0) + 1


async def subscribe(url, counter, stop):
    async with aiohttp.ClientSession() as s, s.ws_connect(url.replace("http", "ws") + "/ws/trades") as ws:
        while not stop.is_set():
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=0.2)
            except asyncio.TimeoutError:
                continue
            if msg.type == aiohttp.WSMsgType.TEXT:
                counter[0] += 1


async def run(url: str, orders: int, clients: int) -> dict:
    latencies, statuses, received = [], {}, [0]
    mid = {s: 10_000 for s in SYMBOLS}
    stop = asyncio.Event()
    feed = asyncio.create_task(subscribe(url, received, stop))
    await asyncio.sleep(0.3)
    conn = aiohttp.TCPConnector(limit=clients)
    async with aiohttp.ClientSession(connector=conn) as session:
        t0 = time.perf_counter()
        chunks = [range(c, orders, clients) for c in range(clients)]
        await asyncio.gather(*(client(session, url, ids, random.Random(k), mid, latencies, statuses)
                               for k, ids in enumerate(chunks)))
        elapsed = time.perf_counter() - t0
        async with session.get(url + "/health") as r:
            health = await r.json()
    await asyncio.sleep(0.5)
    stop.set()
    await feed
    q = statistics.quantiles(latencies, n=1000)
    return {
        "orders": orders, "clients": clients, "seconds": round(elapsed, 2),
        "orders_per_second": round(orders / elapsed),
        "latency_us": {"p50": round(q[499]), "p99": round(q[989]), "p99.9": round(q[998]), "max": round(max(latencies))},
        "statuses": statuses,
        "trades_executed": health["tradesExecuted"], "trades_received_on_websocket": received[0],
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url")
    ap.add_argument("--orders", type=int, default=50_000)
    ap.add_argument("--clients", type=int, default=64)
    ap.add_argument("--durability", default="Group")
    ap.add_argument("--out", default=str(Path(__file__).resolve().parent.parent / "results/loadgen.json"))
    args = ap.parse_args()

    server = None
    url = args.url
    if url is None:
        build()
        server = Server(Path(tempfile.mkdtemp(prefix="tickflow-load-")), args.durability)
        print(f"data folder: {server.data_dir}")
        server.start()
        url = server.url
    try:
        result = asyncio.run(run(url, args.orders, args.clients))
    finally:
        if server:
            server.stop()
    result["durability"] = args.durability
    Path(args.out).parent.mkdir(exist_ok=True)
    Path(args.out).write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
