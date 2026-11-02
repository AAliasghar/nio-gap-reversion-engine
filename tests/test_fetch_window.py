import pandas as pd

from scripts.fetch_window import fetch_start

NOW = pd.Timestamp("2026-11-02 22:00", tz="UTC")


def test_no_stored_data_means_a_full_initial_load():
    assert fetch_start(None, NOW) == (None, 0)


def test_a_recent_last_bar_is_the_start():
    last = pd.Timestamp("2026-10-30 19:55", tz="UTC")
    assert fetch_start(last, NOW) == (last, 0)


def test_a_last_bar_exactly_at_the_limit_is_still_used():
    last = NOW - pd.Timedelta(days=59)
    assert fetch_start(last, NOW) == (last, 0)


def test_an_old_last_bar_moves_to_the_oldest_date_yahoo_serves_and_reports_the_hole():
    # Yahoo only serves 5-minute bars for the last 60 days; asking for more returns nothing
    last = pd.Timestamp("2026-05-04 19:50", tz="UTC")
    start, missing_days = fetch_start(last, NOW)
    assert start == pd.Timestamp("2026-09-04 22:00", tz="UTC")
    assert missing_days == 123
