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
    "_id": {"column_type": "text"},
    "_locked": {"column_type": "checkbox"},
    "_locked_by": {"column_type": "text"},
    "_archived": {"column_type": "checkbox"},
    "_creator": {"column_type": "creator"},
    "_ctime": {"column_type": "ctime"},
    "_mtime": {"column_type": "mtime"},
    "_last_modifier": {"column_type": "last-modifier"},
}
