import logging
import os
from datetime import datetime
from typing import Any, List, Union

import pytz

from ..model import Table
from .const import DT_FMT, SYSTEM_FIELDS, TZ

logger = logging.getLogger(__name__)


################################################################
# Converter
################################################################
class ToPythonDict:
    def __init__(self, table: Table, users: dict = None):
        self.table = table
        self.users = users

        self.columns = {
            **{column.name: {"column_type": column.type, "column_data": column.data} for column in table.columns},
            **SYSTEM_FIELDS,
        }

        self.user_map = (
            {user.email: f"{user.name} ({user.contact_email})" for user in self.users} if self.users else None
        )
        self.row_id_map = {column.key: column.name for column in table.columns}

    def __call__(self, row):
        return {
            column: getattr(self, self.columns[column]["column_type"].replace("-", "_"))(
                value=row[column], data=self.columns[column].get("column_data")
            )
            for column in self.columns
            if column in row
        }

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
        if not value:
            return value
        if data and data.get("enable_precision") and data["precision"] == 0:
            return int(value)
        return float(value)

    def date(self, value: str, data: dict = None) -> datetime:
        if not value:
            return value
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00", 1)
        try:
            dt = datetime.strptime(value, DT_FMT)
        except ValueError as ex:
            dt = datetime.fromisoformat(value)
        return dt.astimezone(TZ)

    def duration(self, value: str, data: dict = None) -> int:
        """
        return seconds
        """
        return value

    def ctime(self, value, data: dict = None):
        return self.date(value, data)

    def mtime(self, value, data: dict = None):
        return self.date(value, data)

    def single_select(self, value: str, data: dict = None) -> str:
        return value

    def multiple_select(self, value: List[str], data: dict = None) -> List[str]:
        return value

    def link(self, value: List[Any], data: dict = None) -> list:
        value = [x["display_value"] for x in value]
        if not value:
            return
        if data:
            if "array_type" in data and data["array_type"] == "single-select":
                kv = {x["id"]: x["name"] for x in data["array_data"]["options"]}
                value = [kv[x] if x in kv else x for x in value]
            if "is_multiple" in data and not data["is_multiple"]:
                value = value[0]
        return value

    def link_formula(self, value, data: dict = None):
        if not value:
            return
        if data:
            if "array_type" in data:
                array_data = data.get("array_data")
                if not isinstance(value, list):
                    value = [value]
                value = [getattr(self, data["array_type"])(x, array_data) for x in value]
            link_column = self.columns[self.row_id_map[data["link_column_key"]]]
            if not link_column["column_data"]["is_multiple"]:
                value = value[0]
        return value

    def user(self, user: str):
        if not self.user_map:
            return user
        return self.user_map[user]

    def collaborator(self, value: List[str], data: dict = None) -> List[str]:
        if not value:
            return value
        return [self.user(x) for x in value]

    def creator(self, value: str, data: dict = None) -> str:
        return self.user(value)

    def last_modifier(self, value: str, data: dict = None) -> str:
        return self.user(value)

    def file(self, value, data: dict = None):
        return value

    def image(self, value, data: dict = None):
        return value

    def formula(self, value, data: dict = None):
        if data:
            try:
                value = getattr(self, "_{}".format(data["result_type"]))(value)
            except Exception as ex:
                if value != "#VALUE!":
                    raise ex
                value = None
        return value

    def auto_number(self, value, data: dict = None):
        return value
