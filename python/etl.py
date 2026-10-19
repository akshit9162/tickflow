"""ETL over the trade store: SQLite trades -> per-symbol one-second bars (open/high/low/close, volume, VWAP,
trade count) -> CSV, plus a price/volume chart.

    python etl.py path/to/trades.db
"""
import sqlite3
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

TICK = 0.01
DOTNET_EPOCH_TICKS = 621_355_968_000_000_000   # .NET ticks at 1970-01-01


def extract(db: Path) -> pd.DataFrame:
    with sqlite3.connect(db) as con:
        df = pd.read_sql_query(
            "SELECT seq, symbol, price_ticks, quantity, aggressor, ts_ticks FROM trades ORDER BY seq", con)
    df["price"] = df["price_ticks"] * TICK
    df["time"] = pd.to_datetime((df["ts_ticks"] - DOTNET_EPOCH_TICKS) // 10, unit="us", utc=True)
    return df


def transform(trades: pd.DataFrame, freq: str = "1s") -> pd.DataFrame:
    trades = trades.assign(notional=trades["price"] * trades["quantity"])
    grouped = trades.set_index("time").groupby("symbol").resample(freq)
    bars = grouped["price"].ohlc()
    bars["volume"] = grouped["quantity"].sum()
    bars["trades"] = grouped["seq"].count()
    bars["vwap"] = grouped["notional"].sum() / bars["volume"]
    return bars.dropna(subset=["open"]).reset_index()


def load(bars: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    bars.to_csv(out_dir / "bars_1s.csv", index=False)
    symbols = list(bars["symbol"].unique())
    fig, axes = plt.subplots(len(symbols), 1, figsize=(10, 3 * len(symbols)), squeeze=False)
    for ax, sym in zip(axes[:, 0], symbols):
        b = bars[bars["symbol"] == sym]
        ax.plot(b["time"], b["vwap"], color="#3b6ea5", label="VWAP")
        ax.fill_between(b["time"], b["low"], b["high"], color="#3b6ea5", alpha=0.15, label="high-low")
        ax.set_title(sym)
        ax.set_ylabel("price")
        ax2 = ax.twinx()
        ax2.bar(b["time"], b["volume"], width=1 / 86400, color="#999", alpha=0.35)
        ax2.set_ylabel("volume")
        ax.legend(loc="upper left", fontsize=8)
    fig.tight_layout()
    fig.savefig(out_dir / "bars.png", dpi=130)


def main() -> None:
    db = Path(sys.argv[1])
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).resolve().parent.parent / "results"
    trades = extract(db)
    bars = transform(trades)
    load(bars, out)
    print(f"{len(trades):,} trades -> {len(bars):,} one-second bars across {bars['symbol'].nunique()} symbols")
    print(bars.groupby("symbol")[["volume", "trades"]].sum())


if __name__ == "__main__":
    main()
