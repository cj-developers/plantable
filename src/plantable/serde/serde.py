import logging
from datetime import datetime
from typing import Any, List, Union
from pydantic import BaseModel
import pytz

from ..model import Table

logger = logging.getLogger(__name__)

KST = pytz.timezone("Asia/Seoul")
DT_FMT = "%Y-%m-%dT%H:%M:%S.%f%z"
SYSTEM_COLUMNS = {
    "_id": {"type": "text"},
    "_locked": {"type": "checkbox"},
    "_locked_by": {"type": "text"},
    "_archived": {"type": "checkbox"},
    "_creator": {"type": "creator"},
    "_ctime": {"type": "ctime"},
    "_mtime": {"type": "mtime"},
    "_last_modifier": {"type": "last-modifier"},
}


def to_datetime(x, dt_fmt=DT_FMT):
    try:
        dt = datetime.strptime(x, dt_fmt)
    except ValueError as ex:
        dt = datetime.fromisoformat(x)
    dt = dt.astimezone(KST)
    return dt


def to_str_datetime(x):
    return x.isoformat(timespec="milliseconds")


# PyDantic Model Schema to SeaTable Schema
def pydantic_to_seatable_schema(model: BaseModel):
    def json_type_to_seatable_type(k, v):
        _type = v["type"]
        if _type == "string":
            _type = "text"
            if "format" in v:
                _format = v["format"]
                if _format == "date-time":
                    _type = "date"
        elif _type == "array":
            _type = "multiple-select"
        elif _type in ["integer", "number"]:
            _type = "number"
        elif _type == "boolean":
            _type = "checkbox"
        else:
            raise KeyError

        return {"column_name": k, "column_type": _type}

    json_schema = model.schema()["properties"]
    return [json_type_to_seatable_type(k, v) for k, v in json_schema.items()]


# Seatable to Python Data Types
class Sea2Py:
    def __init__(self, table_info: Table, users: dict = None, incl_sys_cols: bool = True):
        self.table_info = table_info
        self.columns = {x.name: {"type": x.type, "data": x.data} for x in self.table_info.columns}
        _ = (
            self.columns.update(SYSTEM_COLUMNS)
            if incl_sys_cols
            else self.columns.update({"_id": SYSTEM_COLUMNS["_id"]})
        )
        self.users = users

    def __call__(self, row):
        records = {k: self.value_deserializer(k, v) for k, v in row.items() if k in self.columns}
        return {k: records[k] for k in self.columns if k in records}

    def value_deserializer(self, key: str, value: Any):
        if value is None:
            return value
        _type = self.columns[key]["type"].replace("-", "_")
        _data = self.columns[key].get("data", None)
        return getattr(self, "_{}".format(_type))(value, data=_data)

    # BOOLEAN
    def _checkbox(self, value, data: dict = None) -> bool:
        return value

    # STRING
    def _text(self, value: str, data: dict = None) -> str:
        return value

    def _long_text(self, value: str, data: dict = None) -> str:
        return value

    def _email(self, value: str, data: dict = None) -> str:
        return value

    def _url(self, value: str, data: dict = None) -> str:
        return value

    # INTEGER
    def _rate(self, value: int, data: dict = None) -> int:
        return value

    # INTEGER OR FLOAT
    def _number(self, value: Union[int, float], data: dict = None) -> Union[int, float]:
        if data and data["enable_precision"] and data["precision"] == 0:
            return int(value)
        return float(value)

    # DATETIME
    def _date(self, value: str, data: dict = None) -> datetime:
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00", 1)
        return to_datetime(value)

    def _duration(self, value: str, data: dict = None) -> int:
        """
        return seconds
        """
        return value

    def _ctime(self, value, data: dict = None):
        return self._date(value, data)

    def _mtime(self, value, data: dict = None):
        return self._date(value, data)

    # SELECT
    def _single_select(self, value: str, data: dict = None) -> str:
        return value

    def _multiple_select(self, value: List[str], data: dict = None) -> List[str]:
        return value

    # LINK
    def _link(self, value: List[Any], data: dict = None) -> list:
        value = [x["display_value"] for x in value]
        if not value:
            return
        if data:
            if "array_type" in data and data["array_type"] == "single-select":
                kv = {x["id"]: x["name"] for x in data["array_data"]["options"]}
                print(kv)
                value = [kv[x] if x in kv else x for x in value]
            if "is_multiple" in data and not data["is_multiple"]:
                value = value[0]
        return value

    def _link_formula(self, value, data: dict = None):
        return value

    # USER
    def _user(self, user: str):
        return self.user[user] if self.users and user in self.users else user

    def _collaborator(self, value: List[str], data: dict = None) -> List[str]:
        return [self._user(x) for x in value]

    def _creator(self, value: str, data: dict = None) -> str:
        return self._user(value)

    def _last_modifier(self, value: str, data: dict = None) -> str:
        return self._user(value)

    # BINARY
    def _file(self, value, data: dict = None):
        return value

    def _image(self, value, data: dict = None):
        return value

    # Formula
    def _formula(self, value, data: dict = None):
        if data:
            try:
                value = getattr(self, "_{}".format(data["result_type"]))(value)
            except Exception as ex:
                if value != "#VALUE!":
                    raise ex
                value = None
        return value

    def _auto_number(self, value, data: dict = None):
        return value
