import logging
from datetime import datetime
from typing import Any, List, Union

import pendulum

from .model import Table

logger = logging.getLogger(__name__)

KST = pendulum.timezone("Asia/Seoul")

RESERVED_COLUMNS = ["_id", "_locked", "_locked_by", "_archived", "_creator", "_ctime", "_mtime", "_last_modifier"]

SCHEMA_MAP = {
    "text": "text",
    "long_text": "long-text",
    "number": "number",
    "collaborator": "collaborator",
    "date": "date",
    "duration": "duration",
    "single_select": "single-select",
    "multiple_select": "multiple-select",
    "image": "image",
    "file": "file",
    "email": "email",
    "url": "url",
    "checkbox": "checkbox",
    "rating": "rating",
    "formula": "formula",
    "link": "link",
    "link_formula": "link-formula",
    "creator": "creator",
    "ctime": "ctime",
    "last_modifier": "last-modifier",
    "mtime": "mtime",
    "auto_number": "auto-number",
}


class Sea2Py:
    def __init__(self, table_info: Table, users: dict = None):
        self.table_info = table_info
        self.columns = {x.name: {"type": x.type, "data": x.data} for x in self.table_info.columns}
        self.users = users

    def __call__(self, row):
        return {self.key_deserializer(k): self.value_deserializer(k, v) for k, v in row.items() if k in self.columns}

    def key_deserializer(self, key: str):
        return key.replace(" ", "_")

    def value_deserializer(self, key: str, value: Any):
        _type = self.columns[key]["type"].replace("-", "_")
        _data = self.columns[key]["data"]
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
        return datetime.fromisoformat(value)

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
        return [x["display_value"] for x in value]

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
