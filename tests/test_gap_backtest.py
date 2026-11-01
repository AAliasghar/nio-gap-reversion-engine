"""Tests for the gap backtest, on small made-up hourly bars (no network)."""

import pandas as pd
import pytest

from backtest.gap_backtest import daily_table, fade_trades, fill_rates, gap_bucket, trade_summary

NY = "America/New_York"


def bars(rows):
    """rows: (timestamp 'YYYY-MM-DD HH:MM', open, high, low, close) in New York time."""
    df = pd.DataFrame(rows, columns=["time", "Open", "High", "Low", "Close"])
    df.index = pd.DatetimeIndex(pd.to_datetime(df.pop("time"))).tz_localize(NY)
    return df


def day(date, pre_open, rth_open, rth_high, rth_low, rth_close):
    """One trading day: a 04:00 pre-market bar, two regular-session bars, one after-hours bar."""
    return [
        (f"{date} 04:00", pre_open, pre_open, pre_open, pre_open),
        (f"{date} 09:30", rth_open, rth_high, rth_low, rth_open),
        (f"{date} 15:30", rth_open, rth_open, rth_open, rth_close),
        (f"{date} 16:00", 999, 999, 999, 999),  # after hours: must be ignored
    ]


def test_daily_table_uses_pre_market_open_and_regular_session_only():
    days = daily_table(bars(day("2025-01-02", 9, 9, 11, 9, 10) + day("2025-01-03", 11, 10.8, 11, 10.5, 10.6)))
    row = days.loc[pd.Timestamp("2025-01-03")]
    assert row["pre_market_open"] == 11
    assert row["regular_open"] == 10.8
    assert row["prev_close"] == 10          # the 15:30 bar's close, not the 16:00 after-hours bar
    assert row["rth_high"] == 11 and row["rth_low"] == 10.5
    assert row["gap_pct"] == pytest.approx(10.0)
    assert len(days) == 1                   # the first day has no previous close


def test_gap_up_fills_when_the_session_trades_down_to_the_previous_close():
    days = daily_table(bars(day("2025-01-02", 9, 9, 10, 9, 10) + day("2025-01-03", 10.5, 10.4, 10.6, 9.9, 10.2)))
    assert days["gap_filled"].tolist() == [True]


def test_gap_up_does_not_fill_when_the_low_stays_above_the_previous_close():
    days = daily_table(bars(day("2025-01-02", 9, 9, 10, 9, 10) + day("2025-01-03", 10.5, 10.4, 10.6, 10.1, 10.2)))
    assert days["gap_filled"].tolist() == [False]


def test_gap_down_fills_when_the_session_trades_up_to_the_previous_close():
    days = daily_table(bars(day("2025-01-02", 9, 9, 10, 9, 10) + day("2025-01-03", 9.5, 9.6, 10.0, 9.4, 9.8)))
    assert days["gap_pct"].iloc[0] < 0
    assert days["gap_filled"].tolist() == [True]


@pytest.mark.parametrize(
    "gap, bucket", [(0.5, "<1%"), (-0.99, "<1%"), (1.0, "1-2%"), (-1.5, "1-2%"), (2.0, "2-4%"), (3.9, "2-4%"), (4.0, ">=4%"), (-12, ">=4%")]
)
def test_gap_bucket_uses_the_size_of_the_gap(gap, bucket):
    assert gap_bucket(gap) == bucket


def test_fill_rates_count_days_and_share_filled():
    days = pd.DataFrame(
        {"gap_pct": [1.5, 1.2, -1.1, 0.2], "gap_filled": [True, False, True, True], "trend": ["UPWARD"] * 4}
    )
    table = fill_rates(days).set_index(["gap_size", "direction", "trend"])
    assert table.loc[("1-2%", "up", "UPWARD"), "days"] == 2
    assert table.loc[("1-2%", "up", "UPWARD"), "fill_rate_pct"] == 50.0
    assert table.loc[("1-2%", "down", "UPWARD"), "fill_rate_pct"] == 100.0


def trade_day(gap_pct, regular_open, prev_close, gap_filled, regular_close):
    return {
        "gap_pct": gap_pct, "regular_open": regular_open, "prev_close": prev_close,
        "gap_filled": gap_filled, "regular_close": regular_close,
    }


def test_short_trade_exits_at_the_previous_close_when_the_gap_fills():
    days = pd.DataFrame([trade_day(5.0, 10.4, 10.0, True, 10.2)], index=[pd.Timestamp("2025-01-03")])
    trade = fade_trades(days).iloc[0]
    assert trade["side"] == "short" and trade["exit"] == "target"
    assert trade["return_pct"] == pytest.approx((10.4 - 10.0) / 10.4 * 100)


def test_long_trade_exits_at_the_close_when_the_gap_does_not_fill():
    days = pd.DataFrame([trade_day(-5.0, 9.6, 10.0, False, 9.3)], index=[pd.Timestamp("2025-01-03")])
    trade = fade_trades(days).iloc[0]
    assert trade["side"] == "long" and trade["exit"] == "close"
    assert trade["return_pct"] == pytest.approx((9.3 - 9.6) / 9.6 * 100)


def test_no_trade_when_the_gap_is_small_or_already_closed_at_the_open():
    days = pd.DataFrame(
        [
            trade_day(0.5, 10.05, 10.0, True, 10.0),   # gap below 1%
            trade_day(3.0, 9.9, 10.0, True, 10.1),     # gapped up, but opened below the previous close
            trade_day(-3.0, 10.0, 10.0, True, 10.1),   # gapped down, but opened at the previous close
        ],
        index=pd.to_datetime(["2025-01-03", "2025-01-06", "2025-01-07"]),
    )
    assert fade_trades(days).empty


def test_trade_summary():
    trades = pd.DataFrame({"return_pct": [2.0, -1.0, 4.0, -3.0]})
    summary = trade_summary(trades)
    assert summary == {
        "trades": 4, "win_rate_pct": 50.0, "avg_return_pct": 0.5, "avg_win_pct": 3.0, "avg_loss_pct": -2.0,
        "total_return_pct": 2.0, "worst_trade_pct": -3.0,
    }


def test_days_without_a_gap_are_left_out_of_fill_rates():
    days = pd.DataFrame({"gap_pct": [0.0, 0.5], "gap_filled": [False, True], "trend": ["UPWARD"] * 2})
    table = fill_rates(days)
    assert table["direction"].tolist() == ["up"]
    assert table["days"].tolist() == [1]


def test_fill_rates_can_group_by_size_and_direction_only():
    days = pd.DataFrame(
        {"gap_pct": [1.5, 1.2], "gap_filled": [True, False], "trend": ["UPWARD", "DOWNWARD"]}
    )
    table = fill_rates(days, by=["gap_size", "direction"])
    assert list(table.columns) == ["gap_size", "direction", "days", "filled", "fill_rate_pct"]
    assert table["days"].tolist() == [2]
