import logging
from datetime import date, datetime
from typing import Any, List, Union

import requests

from ...const import DT_FMT, SYSTEM_FIELDS, TZ
from ...model import Table

logger = logging.getLogger(__name__)


################################################################
# Converter
################################################################
class FromPython:
    def __init__(self, table: Table, overwrite_none: bool = False):
        self.table_name = table.name
        self.overwrite_none = overwrite_none

        self.schema = {column.name: column for column in table.columns}

    def __call__(self, row):
        if row is None:
            return
        row = {
            column: getattr(self, self.schema[column].type.replace("-", "_"))(
                value=value, data=self.schema[column].data
            )
            for column, value in row.items()
            if column in self.schema
        }
        row = {k: v for k, v in row.items() if v != "__ignore__"}
        if not self.overwrite_none:
            row = {k: v for k, v in row.items() if v is not None}
        return row

    @staticmethod
    def _ensure_list(x):
        if x is None:
            return x
        return x if isinstance(x, list) else [x]

    def checkbox(self, value, data: dict = None) -> bool:
        return value

    def text(self, value: str, data: dict = None) -> str:
        return value

    def button(self, value: str, data: dict = None) -> str:
        return value

    def long_text(self, value: str, data: dict = None) -> str:
        return value

    def email(self, value: str, data: dict = None) -> str:
        return value

    def url(self, value: str, data: dict = None) -> str:
        return value

    def rate(self, value: int, data: dict = None) -> int:
        return value

    def number(self, value: Union[int, float], data: dict = None) -> Union[int, float]:
        return value

    def date(self, value: Union[date, datetime], data: dict = None) -> Union[date, datetime]:
        return str(value)

    def duration(self, value: Union[str, int], data: dict = None) -> int:
        # [TODO] currently H:MM:SS only
        if isinstance(value, int):
            h, value = value // 3600, value % 3600
            m, s = value // 60, value % 60
            value = f"{h}:{m:02}:{s:02}"
        return value

    def ctime(self, value: datetime, data: dict = None):
        return str(value)

    def mtime(self, value: datetime, data: dict = None):
        return str(value)

    def single_select(self, value: str, data: dict = None) -> str:
        return value

    def multiple_select(self, value: List[str], data: dict = None) -> List[str]:
        return self._ensure_list(value)

    def link(self, value: list, data: dict = None) -> list:
        # [NOTE] Link 값은 create link로 따로 입력해야 함.
        return "__ignore__"

    def link_formula(self, value, data: dict = None):
        raise KeyError("you cannot insert the value for link-formula column")

    def user(self, user: str, data: dict = None):
        return user

    def collaborator(self, value: List[str], data: dict = None) -> List[str]:
        return self._ensure_list(value)

    def creator(self, value: str, data: dict = None) -> str:
        return value

    def last_modifier(self, value: str, data: dict = None) -> str:
        return value

    def file(self, value, data: dict = None):
        return self._ensure_list(value)

    def image(self, value, data: dict = None):
        return self._ensure_list(value)

    def formula(self, value, data: dict = None):
        raise KeyError("you cannot insert the value for fomula column")

    def auto_number(self, value, data: dict = None):
        raise KeyError("you cannot insert the value for auto-number column")
