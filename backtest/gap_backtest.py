"""
Backtest of the NIO opening-gap rule, on hourly bars that include pre-market trading.

Same definitions as the dbt model `gold_gap_signals` (New York time):
- gap        = first pre-market open (from 04:00) vs the previous regular-session close
- gap filled = the regular session (09:30-16:00) traded back through the previous close
- trend      = previous close above / below its 20-day average

And the scanner's rule (`scripts/scanner_nio.py`): gap bigger than 1% -> trade against it.
Here the trade enters at the 09:30 open and exits at the previous close if price gets there,
otherwise at the 16:00 close. Before costs, slippage and borrow fees; no stop-loss.

Run from the repo root:
    python backtest/gap_backtest.py             # uses the saved snapshot in backtest/data/
    python backtest/gap_backtest.py --refresh   # downloads a new snapshot from Yahoo first
"""

import argparse
from datetime import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
SNAPSHOT = ROOT / "data" / "nio_1h.csv"
RESULTS = ROOT / "results"

NY = "America/New_York"
PRE_MARKET_START = time(4, 0)
SESSION_START, SESSION_END = time(9, 30), time(16, 0)
SIGNAL_THRESHOLD_PCT = 1.0
SMA_DAYS = 20
BUCKETS = ["<1%", "1-2%", "2-4%", ">=4%"]


def daily_table(bars: pd.DataFrame) -> pd.DataFrame:
    """One row per trading day from hourly OHLC bars with a time-zone-aware index."""
    ny = bars.tz_convert(NY)
    clock = ny.index.time
    dates = ny.index.tz_localize(None).normalize()

    pre = ny[clock >= PRE_MARKET_START]
    session = ny[(clock >= SESSION_START) & (clock < SESSION_END)]
    pre_by_day = pre.groupby(dates[clock >= PRE_MARKET_START])
    session_by_day = session.groupby(dates[(clock >= SESSION_START) & (clock < SESSION_END)])

    days = pd.DataFrame(
        {
            "pre_market_open": pre_by_day["Open"].first(),
            "regular_open": session_by_day["Open"].first(),
            "regular_close": session_by_day["Close"].last(),
            "rth_high": session_by_day["High"].max(),
            "rth_low": session_by_day["Low"].min(),
        }
    ).sort_index()

    days["prev_close"] = days["regular_close"].shift(1)
    prev_sma = days["regular_close"].rolling(SMA_DAYS).mean().shift(1)
    days["trend"] = np.where(prev_sma.isna(), "n/a", np.where(days["prev_close"] > prev_sma, "UPWARD", "DOWNWARD"))
    days["gap_pct"] = (days["pre_market_open"] - days["prev_close"]) / days["prev_close"] * 100
    days["gap_filled"] = ((days["gap_pct"] > 0) & (days["rth_low"] <= days["prev_close"])) | (
        (days["gap_pct"] < 0) & (days["rth_high"] >= days["prev_close"])
    )
    days.index.name = "date"
    return days.dropna(subset=["prev_close", "pre_market_open", "regular_open"])


def gap_bucket(gap_pct: float) -> str:
    size = abs(gap_pct)
    if size < 1:
        return "<1%"
    if size < 2:
        return "1-2%"
    if size < 4:
        return "2-4%"
    return ">=4%"


def fill_rates(days: pd.DataFrame, by: list[str] | None = None) -> pd.DataFrame:
    """Days and share of gaps filled, by gap size, direction and trend (or the columns in `by`).

    Days that open exactly at the previous close have no gap and are left out.
    """
    by = by or ["gap_size", "direction", "trend"]
    gaps = days[days["gap_pct"] != 0]
    grouped = gaps.assign(
        gap_size=pd.Categorical(gaps["gap_pct"].map(gap_bucket), categories=BUCKETS, ordered=True),
        direction=np.where(gaps["gap_pct"] > 0, "up", "down"),
    ).groupby(by, observed=True)
    table = grouped["gap_filled"].agg(days="size", filled="sum").reset_index()
    table["fill_rate_pct"] = (100 * table["filled"] / table["days"]).round(1)
    return table


def fade_trades(days: pd.DataFrame, threshold_pct: float = SIGNAL_THRESHOLD_PCT) -> pd.DataFrame:
    """The scanner's rule: trade against gaps bigger than the threshold, from the 09:30 open."""
    signals = days[days["gap_pct"].abs() > threshold_pct]
    short = signals["gap_pct"] > 0
    # Skip days where the 09:30 open is already at or through the previous close: nothing left to fade
    still_open = np.where(short, signals["regular_open"] > signals["prev_close"], signals["regular_open"] < signals["prev_close"])
    signals, short = signals[still_open], short[still_open]

    exit_price = np.where(signals["gap_filled"], signals["prev_close"], signals["regular_close"])
    move_pct = (exit_price - signals["regular_open"]) / signals["regular_open"] * 100
    return pd.DataFrame(
        {
            "gap_pct": signals["gap_pct"],
            "side": np.where(short, "short", "long"),
            "entry": signals["regular_open"],
            "exit_price": exit_price,
            "exit": np.where(signals["gap_filled"], "target", "close"),
            "return_pct": np.where(short, -move_pct, move_pct),
        },
        index=signals.index,
    )


def trade_summary(trades: pd.DataFrame) -> dict:
    returns = trades["return_pct"]
    if returns.empty:
        return {"trades": 0}
    return {
        "trades": int(len(returns)),
        "win_rate_pct": round(float(100 * (returns > 0).mean()), 2),
        "avg_return_pct": round(float(returns.mean()), 2),
        "avg_win_pct": round(float(returns[returns > 0].mean()), 2),
        "avg_loss_pct": round(float(returns[returns <= 0].mean()), 2),
        "total_return_pct": round(float(returns.sum()), 2),
        "worst_trade_pct": round(float(returns.min()), 2),
    }


def download_bars(path: Path) -> None:
    """Saves Yahoo's hourly NIO bars (with pre/after-market) for the last 730 days, the most it offers."""
    import yfinance as yf

    bars = yf.Ticker("NIO").history(period="730d", interval="1h", prepost=True)
    if bars.empty:
        raise RuntimeError("Yahoo Finance returned no NIO bars")
    path.parent.mkdir(parents=True, exist_ok=True)
    bars = bars[["Open", "High", "Low", "Close", "Volume"]].tz_convert("UTC").round(4)
    bars.index.name = "time_utc"
    bars.to_csv(path, lineterminator="\r\n")


def load_bars(path: Path) -> pd.DataFrame:
    bars = pd.read_csv(path, index_col="time_utc")
    bars.index = pd.to_datetime(bars.index, utc=True)
    return bars


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Backtest the NIO opening-gap rule.")
    parser.add_argument("--refresh", action="store_true", help="download a new snapshot from Yahoo first")
    args = parser.parse_args(argv)

    if args.refresh or not SNAPSHOT.exists():
        download_bars(SNAPSHOT)
    days = daily_table(load_bars(SNAPSHOT))
    rates = fill_rates(days)
    rates_by_size = fill_rates(days, by=["gap_size", "direction"])
    trades = fade_trades(days)
    summary = pd.DataFrame(
        [{"side": "all", **trade_summary(trades)}]
        + [{"side": side, **trade_summary(trades[trades["side"] == side])} for side in ("short", "long")]
    )

    RESULTS.mkdir(exist_ok=True)
    rates.to_csv(RESULTS / "fill_rates.csv", index=False, lineterminator="\r\n")
    rates_by_size.to_csv(RESULTS / "fill_rates_by_size.csv", index=False, lineterminator="\r\n")
    trades.round(4).to_csv(RESULTS / "trades.csv", lineterminator="\r\n")
    summary.to_csv(RESULTS / "summary.csv", index=False, lineterminator="\r\n")

    gaps = days[days["gap_pct"] != 0]
    print(f"Trading days: {len(days)} ({days.index.min().date()} to {days.index.max().date()}), with a gap: {len(gaps)}")
    print(f"Gaps filled overall: {100 * gaps['gap_filled'].mean():.1f}%\n")
    print(rates_by_size.to_string(index=False), "\n")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
