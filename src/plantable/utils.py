from datetime import datetime

from .const import DT_FMT, TZ


# divide chunks
def divide_chunks(x: list, chunk_size: int):
    for i in range(0, len(x), chunk_size):
        yield x[i : i + chunk_size]


# parse string datetime
def parse_str_datetime(x):
    if x.endswith("Z"):
        x = x.replace("Z", "+00:00", 1)
    try:
        x = datetime.strptime(x, DT_FMT)
    except Exception:
        x = datetime.fromisoformat(x)
    return x.astimezone(TZ)
