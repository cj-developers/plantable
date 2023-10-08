import os

import pytz

TZ_INFO = os.getenv("TZ", "Asia/Seoul")
TZ = pytz.timezone(TZ_INFO) if TZ_INFO else pytz.UTC
DT_FMT = "%Y-%m-%dT%H:%M:%S.%f%z"

################################################################
# SeaTable
################################################################
# System Fields
SYSTEM_FIELDS = {
    "_id": {"type": "text"},
    "_locked": {"type": "checkbox"},
    "_locked_by": {"type": "text"},
    "_archived": {"type": "checkbox"},
    "_creator": {"type": "creator"},
    "_ctime": {"type": "ctime"},
    "_mtime": {"type": "mtime"},
    "_last_modifier": {"type": "last-modifier"},
}
