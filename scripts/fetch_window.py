"""Where the next incremental fetch of 5-minute bars should start."""

from datetime import timedelta

# Yahoo serves 5-minute bars for the last 60 days only; a request reaching further back
# returns no data at all. One day of margin keeps the request inside the limit.
MAX_LOOKBACK_DAYS = 59


def fetch_start(last_bar, now, max_lookback_days=MAX_LOOKBACK_DAYS):
    """Returns (start, missing_days).

    start is None when nothing is stored yet (do a full initial load). Otherwise it is the
    last stored bar, moved forward to the oldest bar Yahoo still serves if needed;
    missing_days says how many days of history were skipped because of that.
    """
    if last_bar is None:
        return None, 0
    oldest_available = now - timedelta(days=max_lookback_days)
    if last_bar >= oldest_available:
        return last_bar, 0
    return oldest_available, (oldest_available - last_bar).days
