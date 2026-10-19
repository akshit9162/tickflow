"""Crash test: kill -9 the server while clients are placing orders, restart it, and check that every order the
server acknowledged before the crash is still there.

How "still there" is checked: each acknowledged order is re-sent with the same clientOrderId. If the server
recovered it from the write-ahead log, it answers duplicate=true with the same orderId. If it had been lost, the
server would treat it as new and give it a different id.

    python crash_test.py --rounds 20 --durability Group
"""
import argparse
import asyncio
import json
import random
import tempfile
from pathlib import Path

import aiohttp

from server import Server, build


async def place(session, url, order):
    async with session.post(url + "/orders", json=order) as r:
        return await r.json()


async def fire_until_killed(server, rng, round_no, acked):
    url = server.url
    async with aiohttp.ClientSession() as session:

        async def worker(w):
            i = 0
            while True:
                i += 1
                order = {"clientOrderId": f"r{round_no}-w{w}-{i}", "symbol": "ACME",
                         "side": rng.choice(("Buy", "Sell")), "type": "Limit",
                         "price": round(100 + rng.randint(-20, 20) / 100, 2), "quantity": rng.randint(1, 50)}
                try:
                    res = await place(session, url, order)
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    return                                  # server is gone
                acked[order["clientOrderId"]] = (order, res["orderId"])

        workers = [asyncio.create_task(worker(w)) for w in range(32)]
        await asyncio.sleep(rng.uniform(0.3, 1.5))
        server.kill()                                       # mid-flight, no warning
        await asyncio.gather(*workers, return_exceptions=True)


async def verify(server, acked):
    lost = 0
    async with aiohttp.ClientSession() as session:
        for client_id, (order, order_id) in acked.items():
            res = await place(session, server.url, order)
            if not (res.get("duplicate") and res.get("orderId") == order_id):
                lost += 1
    return lost


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=20)
    ap.add_argument("--durability", default="Group")
    ap.add_argument("--out", default=str(Path(__file__).resolve().parent.parent / "results/crash_test.json"))
    args = ap.parse_args()

    build()
    rng = random.Random(1)
    server = Server(Path(tempfile.mkdtemp(prefix="tickflow-crash-")), args.durability)
    total_acked, total_lost, torn = 0, 0, 0
    for round_no in range(args.rounds):
        server.start()
        acked = {}
        asyncio.run(fire_until_killed(server, rng, round_no, acked))
        health = server.start()                             # restart: replays the log
        torn += int(health["recovery"]["tornTailTruncated"])
        lost = asyncio.run(verify(server, acked))
        server.kill()
        total_acked += len(acked)
        total_lost += lost
        print(f"round {round_no + 1:>2}: acked {len(acked):>5}, lost {lost}, "
              f"replayed {health['recovery']['commandsReplayed']}, torn tail {health['recovery']['tornTailTruncated']}")

    result = {"durability": args.durability, "rounds": args.rounds, "acked_orders": total_acked,
              "lost_acked_orders": total_lost, "rounds_with_torn_tail_repaired": torn}
    Path(args.out).parent.mkdir(exist_ok=True)
    Path(args.out).write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
