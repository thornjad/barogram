import time


def _sector(ts: int) -> int:
    """Map a Unix timestamp to a time-of-day sector (local time).

    0 = night (00:00–05:59)
    1 = morning (06:00–11:59)
    2 = afternoon (12:00–17:59)
    3 = evening (18:00–23:59)
    """
    h = time.localtime(ts).tm_hour
    if h < 6:  return 0
    if h < 12: return 1
    if h < 18: return 2
    return 3
